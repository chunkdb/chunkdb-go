package chunkdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// One context covers the entire wait, including periodic progress. Next has
// already decoded each event; only expected ancillary events may be skipped.
func waitWatchEvent(ctx context.Context, next func(context.Context) (Event, error), from Position, kind string, revision, schemaVersion uint64) (Event, error) {
	frontier := from.Revision
	for {
		if err := ctx.Err(); err != nil {
			return nil, timeoutErrorf("WATCH wait", err, "waiting for %s at revision %d: %s", kind, revision, err)
		}
		event, err := next(ctx)
		if err != nil {
			return nil, fmt.Errorf("waiting for %s at revision %d: %w", kind, revision, err)
		}
		position := event.GetPosition()
		if position.Epoch != from.Epoch {
			return nil, fmt.Errorf("waiting for %s: unexpected epoch in %#v", kind, event)
		}
		switch e := event.(type) {
		case *ProgressEvent:
			ceiling := revision
			if kind == "change" || kind == "chunks" {
				if revision == 0 {
					return nil, fmt.Errorf("mutation revision must be positive")
				}
				ceiling-- // Progress cannot pass a mutation that is still pending.
			}
			if position.Revision < frontier || position.Revision > ceiling {
				return nil, fmt.Errorf("waiting for %s: progress %d outside [%d,%d]", kind, position.Revision, frontier, ceiling)
			}
			frontier = position.Revision
			continue
		case *SchemaEvent:
			if e.Version != schemaVersion || position.Revision > revision {
				return nil, fmt.Errorf("waiting for %s: wrong schema %#v", kind, event)
			}
			if kind == "change" || kind == "chunks" {
				continue
			}
			if kind != "schema" {
				return nil, fmt.Errorf("waiting for %s: unexpected schema %#v", kind, event)
			}
		case *ChangeEvent:
			if kind != "change" || e.SchemaVersion != schemaVersion {
				return nil, fmt.Errorf("waiting for %s: unexpected change %#v", kind, event)
			}
		case *ChunksEvent:
			if kind != "chunks" || e.SchemaVersion != schemaVersion {
				return nil, fmt.Errorf("waiting for %s: unexpected chunks %#v", kind, event)
			}
		case *ResyncEvent:
			if kind != "resync" {
				return nil, fmt.Errorf("waiting for %s: rebuild after resync %#v", kind, event)
			}
		default:
			return nil, fmt.Errorf("waiting for %s: unexpected event %#v", kind, event)
		}
		if position.Revision != revision || position.Revision < frontier {
			return nil, fmt.Errorf("waiting for %s at revision %d: wrong position %#v", kind, revision, position)
		}
		return event, nil
	}
}

func nextExpectedWatch(t *testing.T, w *Watch, kind string, revision, schemaVersion uint64, after ...Position) Event {
	t.Helper()
	from := w.Start()
	if len(after) != 0 {
		from = after[0] // Ordinary AFTER may replay below the current producer frontier.
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	event, err := waitWatchEvent(ctx, w.Next, from, kind, revision, schemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestWatchWaitInterleavedProgress(t *testing.T) {
	columns := []string{fakeColumn(1, "v", "u8", false, false, respNull)}
	progress := push(respBulk("progress"), respBulk(watchEpoch), respInt(10))
	schema := push(respBulk("schema"), respBulk(watchEpoch), respInt(11), respInt(1), respArray(columns...))
	for _, frames := range []string{progress + schema + progress, schema + progress} {
		_, w := fakeWatch(t, frames+changePush(1, respNull, respArray(respInt(7))), columns)
		event := nextExpectedWatch(t, w, "change", 11, 1)
		change := event.(*ChangeEvent)
		if len(change.Blocks) != 1 || change.Blocks[0].After["v"] != uint64(7) {
			t.Fatal(change)
		}
	}
}

func TestWatchWaitRejectsUnexpectedEvents(t *testing.T) {
	for _, event := range []Event{
		&ProgressEvent{Position: Position{watchEpoch, 9}},
		&ProgressEvent{Position: Position{watchEpoch, 11}},
		&ProgressEvent{Position: Position{strings.Repeat("f", 32), 10}},
		&SchemaEvent{Position: Position{watchEpoch, 11}, Version: 2},
		&ResyncEvent{Position: Position{watchEpoch, 11}},
		&CatalogEvent{Position: Position{watchEpoch, 11}},
		&ChangeEvent{Position: Position{watchEpoch, 12}, SchemaVersion: 1},
	} {
		t.Run(fmt.Sprintf("%T/%v", event, event.GetPosition()), func(t *testing.T) {
			_, err := waitWatchEvent(t.Context(), func(context.Context) (Event, error) { return event, nil }, Position{watchEpoch, 10}, "change", 11, 1)
			if err == nil {
				t.Fatal("unexpected event was accepted", event)
			}
		})
	}
}

func TestWatchWaitContextBoundsProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	calls := 0
	_, err := waitWatchEvent(ctx, func(supplied context.Context) (Event, error) {
		calls++
		got, ok := supplied.Deadline()
		if supplied != ctx || !ok || got != deadline {
			t.Fatal("progress renewed the supplied context/deadline")
		}
		if calls == 2 {
			cancel() // An explicit signal, rather than waiting on a timer or sleeping.
		}
		return &ProgressEvent{Position: Position{watchEpoch, 10}}, nil
	}, Position{watchEpoch, 10}, "change", 11, 1)
	if calls != 2 || !errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatal(calls, err)
	}
	expired, stop := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer stop()
	_, err = waitWatchEvent(expired, func(context.Context) (Event, error) {
		t.Fatal("expired wait attempted another read")
		return nil, nil
	}, Position{watchEpoch, 10}, "change", 11, 1)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired wait lost the deadline error", err)
	}
	_, w := fakeWatch(t, "", []string{fakeColumn(1, "v", "u8", false, false, respNull)})
	if _, err := waitWatchEvent(ctx, w.Next, w.Start(), "change", 11, 1); !errors.Is(err, ErrTimeout) {
		t.Fatal("cancelled wait did not stop", err)
	}
}

func TestWatchWaitSchemaAndResync(t *testing.T) {
	columns := []string{fakeColumn(1, "v", "u8", false, false, respNull)}
	progress := push(respBulk("progress"), respBulk(watchEpoch), respInt(10))
	schema := push(respBulk("schema"), respBulk(watchEpoch), respInt(11), respInt(1), respArray(columns...))
	_, w := fakeWatch(t, progress+schema+progress+changePush(1, respNull, respArray(respInt(7))), columns)
	if e := nextExpectedWatch(t, w, "schema", 11, 1).(*SchemaEvent); e.Version != 1 {
		t.Fatal(e)
	}
	// Schema at 11 did not deliver a mutation: progress 10 is still valid.
	if e := nextExpectedWatch(t, w, "change", 11, 1).(*ChangeEvent); e.Blocks[0].After["v"] != uint64(7) {
		t.Fatal(e)
	}
	_, resumed := fakeWatch(t, progress+push(respBulk("resync"), respBulk(watchEpoch), respInt(12)), columns)
	if e := nextExpectedWatch(t, resumed, "resync", 12, 1).(*ResyncEvent); e.Position.Revision != 12 {
		t.Fatal(e)
	}
}

func TestWatchWaitMalformedProgress(t *testing.T) {
	columns := []string{fakeColumn(1, "v", "u8", false, false, respNull)}
	for _, revision := range []string{respBulk("10"), "(18446744073709551616\r\n"} {
		_, w := fakeWatch(t, push(respBulk("progress"), respBulk(watchEpoch), revision), columns)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		_, err := waitWatchEvent(ctx, w.Next, w.Start(), "change", 11, 1)
		cancel()
		if !errors.Is(err, ErrProtocol) {
			t.Fatal("malformed progress did not fail decoding", err)
		}
	}
}
