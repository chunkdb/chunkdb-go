package chunkdb

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const replacementWatchEpoch = "fedcba9876543210fedcba9876543210"

func epochDescription(epoch string, version int64, column string) string {
	description := describeReply("world", version, []string{fakeColumn(1, column, "u8", false, false, respNull)})
	if epoch == "" {
		return description
	}
	return strings.Replace(description, "%6\r\n", "%10\r\n", 1) +
		respBulk("epoch") + respBulk(epoch) + respBulk("revision") + respInt(10) +
		respBulk("dims") + respInt(2) + respBulk("chunk_columns") + respArray()
}

func epochChange(epoch string, legacy bool) string {
	fields := []string{respBulk("change"), respBulk(epoch), respInt(11), respInt(123), respNull, respInt(1), respArray(respArray(respInt(0), respInt(0), respNull, respArray(respInt(7))))}
	if !legacy {
		fields = append(fields, respNull, respNull, respArray())
	}
	return push(fields...)
}

func TestWatchEpochReplacementWithoutSchemaPush(t *testing.T) {
	for _, test := range []struct {
		name, seedEpoch, seedColumn, fallbackEpoch string
		fallbackVersion                            int64
		legacyChange, wantError                    bool
		wantDescribes                              int32
	}{
		{"modern", watchEpoch, "old", replacementWatchEpoch, 1, false, false, 2},
		{"legacy_change", watchEpoch, "old", replacementWatchEpoch, 1, true, false, 2},
		{"wrong_fallback_epoch", watchEpoch, "old", strings.Repeat("f", 32), 1, false, true, 2},
		{"wrong_fallback_version", watchEpoch, "old", replacementWatchEpoch, 2, false, true, 2},
		{"legacy_fallback", watchEpoch, "old", "", 1, false, false, 2},
		{"legacy_seed", "", "new", "", 1, false, false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			described, requested, attach := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var describes atomic.Int32
			server := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
				switch verbOf(command) {
				case "DESCRIBE":
					if describes.Add(1) == 1 {
						writeRaw(cn, epochDescription(test.seedEpoch, 1, test.seedColumn))
						close(described)
					} else {
						select {
						case <-attach:
							writeRaw(cn, epochDescription(test.fallbackEpoch, test.fallbackVersion, "new"))
						case <-ctx.Done():
						}
					}
				case "WATCH":
					select {
					case requested <- struct{}{}:
					case <-ctx.Done():
						return
					}
					select {
					case <-attach:
						// Replace A with B only after DESCRIBE(A) and before
						// WATCH's reply. No initial schema push hides the race.
						writeRaw(cn, respMap("epoch", respBulk(replacementWatchEpoch), "revision", respInt(10))+epochChange(replacementWatchEpoch, test.legacyChange))
					case <-ctx.Done():
					}
				case "UNWATCH":
					writeSimple(cn, "OK")
				}
			}))
			c := newTestClient(t, server, nil)
			type watchResult struct {
				watch *Watch
				err   error
			}
			result := make(chan watchResult)
			constructorDone := make(chan struct{})
			go func() {
				defer close(constructorDone)
				w, err := c.Watch(ctx, "world", WatchOptions{})
				select {
				case result <- watchResult{w, err}:
				case <-ctx.Done():
					if w != nil {
						_ = w.Close()
					}
				}
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-constructorDone:
				case <-time.After(3 * time.Second):
					t.Fatal("WATCH constructor did not stop after cancellation")
				}
			})
			for _, gate := range []<-chan struct{}{described, requested} {
				select {
				case <-gate:
				case <-ctx.Done():
					t.Fatal("waiting for DESCRIBE/WATCH replacement request gates", ctx.Err())
				}
			}
			close(attach)
			var constructed watchResult
			select {
			case constructed = <-result:
			case <-ctx.Done():
				t.Fatal("waiting for replacement WATCH construction", ctx.Err())
			}
			if constructed.err != nil {
				t.Fatal(constructed.err)
			}
			w := constructed.watch
			defer w.Close()
			event, err := w.Next(ctx)
			if test.wantError {
				if !errors.Is(err, ErrProtocol) || event != nil {
					t.Fatal("mismatched fallback must not return a decoded row", event, err)
				}
				if w.schemaDetails[watchSchemaKey{replacementWatchEpoch, 1}] != nil {
					t.Fatal("mismatched fallback polluted the replacement cache")
				}
			} else {
				change, ok := event.(*ChangeEvent)
				if err != nil || !ok || len(change.Blocks) != 1 || !reflect.DeepEqual(change.Blocks[0].After, Record{"new": uint64(7)}) {
					t.Fatal("replacement row used another epoch's layout", event, err)
				}
			}
			if w.Start() != (Position{replacementWatchEpoch, 10}) || describes.Load() != test.wantDescribes {
				t.Fatal("replacement start or fallback count", w.Start(), describes.Load())
			}
		})
	}
}

func TestWatchEpochSchemaPushOwnership(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "modern", true: "legacy"}[legacy], func(t *testing.T) {
			w := &Watch{}
			for _, owner := range []struct{ epoch, column string }{{watchEpoch, "old"}, {replacementWatchEpoch, "new"}} {
				columns := respArray(fakeColumn(1, owner.column, "u8", false, false, respNull))
				frame := push(respBulk("schema"), respMap("epoch", respBulk(owner.epoch), "revision", respInt(10), "schema_version", respInt(1), "dims", respInt(2), "columns", columns, "chunk_columns", respArray()))
				if legacy {
					frame = push(respBulk("schema"), respBulk(owner.epoch), respInt(10), respInt(1), columns)
				}
				event, err := w.decode(t.Context(), readOne(t, frame))
				if err != nil {
					t.Fatal(err)
				}
				event.(*SchemaEvent).Columns[0].Name = "caller mutation"
			}
			for _, owner := range []struct{ epoch, column string }{{watchEpoch, "old"}, {replacementWatchEpoch, "new"}} {
				event, err := w.decode(t.Context(), readOne(t, epochChange(owner.epoch, legacy)))
				if err != nil || !reflect.DeepEqual(event.(*ChangeEvent).Blocks[0].After, Record{owner.column: uint64(7)}) {
					t.Fatal("schema push overwrote another epoch or retained caller alias", owner, event, err)
				}
			}
		})
	}
}

func TestWatchEpochUnknownAfterResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var describes atomic.Int32
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			describes.Add(1)
			writeRaw(cn, epochDescription(replacementWatchEpoch, 1, "new"))
		case "WATCH":
			writeRaw(cn, respMap("epoch", respBulk(watchEpoch), "revision", respInt(5))+
				push(respBulk("resync"), respBulk(replacementWatchEpoch), respInt(10), respNull, respNull)+epochChange(replacementWatchEpoch, false))
		case "UNWATCH":
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, s, nil)
	requested := Position{watchEpoch, 5}
	w, err := c.Watch(ctx, "world", WatchOptions{After: &requested})
	if err != nil {
		t.Fatal("actual description and unknown AFTER remain legal", err)
	}
	defer w.Close()
	event, err := w.Next(ctx)
	resync, ok := event.(*ResyncEvent)
	if err != nil || !ok || resync.Position != (Position{replacementWatchEpoch, 10}) {
		t.Fatal("unknown AFTER resync", event, err)
	}
	w.ackMu.Lock()
	delivered := w.delivered
	w.ackMu.Unlock()
	if delivered != requested {
		t.Fatal("resync advanced ACK eligibility", delivered)
	}
	event, err = w.Next(ctx)
	change, ok := event.(*ChangeEvent)
	if err != nil || !ok || !reflect.DeepEqual(change.Blocks[0].After, Record{"new": uint64(7)}) || w.Start() != requested || describes.Load() != 1 {
		t.Fatal("description authority was replaced by requested start", event, err, w.Start(), describes.Load())
	}
	w.ackMu.Lock()
	delivered = w.delivered
	w.ackMu.Unlock()
	if delivered != requested {
		t.Fatal("mutation promoted an old ACK bound across epochs", delivered)
	}
	if err := w.Ack(ctx, 11); !errors.Is(err, ErrProtocol) {
		t.Fatal("ordinary resynced watch unexpectedly allowed ACK", err)
	}
}
