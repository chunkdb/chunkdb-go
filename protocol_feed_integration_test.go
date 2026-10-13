package chunkdb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func foundationNativeFeed(t *testing.T, ctx context.Context, tls bool, args ...string) *Client {
	t.Helper()
	s := startServer(t, serverConfig{tls: tls, workers: 4, args: args})
	c := connectIntegration(t, s, func(o *Options) {
		if tls {
			o.CA = s.caPEM
		}
	})
	for _, feature := range []string{"protocol_foundation", "feed_progress", "slot_filters"} {
		if !c.ServerInfo().Supports(feature) {
			t.Fatalf("native feed requires negotiated feature %s", feature)
		}
	}
	if err := c.CreateTable(ctx, "feed", TableSpec{
		Columns:    []ColumnDef{{Name: "v", Type: TypeUint(9)}, {Name: "label", Type: TypeText(16), Null: true}},
		ChunkWidth: 2, ChunkHeight: 2,
	}); err != nil {
		t.Fatal("create native feed table", err)
	}
	return c
}

func nativeSlotPosition(t *testing.T, ctx context.Context, c *Client) Position {
	t.Helper()
	slots, err := c.Slots(ctx, "feed")
	if err != nil || len(slots) != 1 || slots[0].Name != "consumer" || slots[0].Lost {
		t.Fatal("native consumer slot position", slots, err)
	}
	return Position{slots[0].Epoch, slots[0].Acked}
}

func nativeFeedMutation(t *testing.T, ctx context.Context, w *Watch, kind string, revision uint64, from Position) Event {
	t.Helper()
	event, err := waitWatchEvent(ctx, w.Next, from, kind, revision, 1)
	if err != nil {
		t.Fatal("native mutation delivery", err)
	}
	return event
}

// A terminal error must come from the server, not the wait context. Schema
// does not advance the applied position; progress cannot pass this mutation.
func nativeFeedTerminal(t *testing.T, ctx context.Context, w *Watch, at Position, code, reason string) int {
	t.Helper()
	progressCount := 0
	for {
		event, err := w.Next(ctx)
		if err != nil {
			var serverError *Error
			if ctx.Err() != nil || !errors.As(err, &serverError) || serverError.Kind != KindServer || serverError.ServerCode != code || serverError.ServerKeys["reason"] != reason || serverError.RetryClass != RetryFatal {
				t.Fatalf("waiting for server %s reason=%s at %v: %v", code, reason, at, err)
			}
			return progressCount
		}
		switch e := event.(type) {
		case *ProgressEvent:
			if e.Position != at {
				t.Fatalf("waiting for %s: unexpected progress %#v", code, e)
			}
			progressCount++
		case *SchemaEvent:
			if e.Position.Epoch != at.Epoch || e.Position.Revision > at.Revision || e.Version != 1 {
				t.Fatalf("waiting for %s: unexpected schema %#v", code, e)
			}
		default:
			t.Fatalf("waiting for %s: unexpected event %#v", code, event)
		}
	}
}

func TestIntegrationFoundationChunksDeliveryAck(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[tls], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			// Keep periodic progress outside this bounded delivery-gate test.
			c := foundationNativeFeed(t, ctx, tls, "--feed-progress-ms", "60000")
			if err := c.CreateSlotWithOptions(ctx, "feed", "consumer", SlotOptions{Chunks: true}); err != nil {
				t.Fatal(err)
			}
			w, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			schema, err := c.Describe(ctx, "feed")
			if err != nil {
				t.Fatal(err)
			}
			chunk := NewChunk(schema)
			for y := 0; y < 2; y++ {
				for x := 0; x < 2; x++ {
					chunk.SetBlock(x, y, Record{"v": uint64(300 + y*2 + x), "label": "complete"})
				}
			}
			revision, err := c.SetChunk(ctx, "feed", 0, 0, chunk)
			if err != nil {
				t.Fatal(err)
			}
			// Receive a fully parsed native wire reply before Next returns it to
			// the application. This test-only relay uses the existing channel;
			// no decoder/socket hook or replacement server frame is installed.
			var pending Reply
			pendingChunks := false
			for !pendingChunks {
				select {
				case reply := <-w.pushes:
					event, err := w.decode(ctx, reply)
					if err != nil {
						t.Fatal("decode pending native CHUNKS", err)
					}
					switch e := event.(type) {
					case *SchemaEvent:
						if e.Version != 1 || e.Position.Epoch != w.Start().Epoch || e.Position.Revision > revision {
							t.Fatal("pending CHUNKS schema", e)
						}
					case *ChunksEvent:
						if e.Position != (Position{w.Start().Epoch, revision}) || len(e.Chunks) != 1 {
							t.Fatal("pending native CHUNKS", e)
						}
						pending = reply
						pendingChunks = true
					default:
						t.Fatalf("waiting for pending native CHUNKS: %#v", event)
					}
				case <-w.done:
					t.Fatal("pending native CHUNKS ended", w.conn.terminalError())
				case <-ctx.Done():
					t.Fatal("waiting for pending native CHUNKS", ctx.Err())
				}
			}
			if err := w.Ack(ctx, revision); !errors.Is(err, ErrProtocol) {
				t.Fatal("decoded but not application-delivered CHUNKS allowed ACK", err)
			}
			relayDone := make(chan struct{})
			go func() {
				defer close(relayDone)
				select {
				case w.pushes <- pending:
				case <-ctx.Done():
				case <-w.closing:
				}
			}()
			defer func() { cancel(); <-relayDone }()
			event := nativeFeedMutation(t, ctx, w, "chunks", revision, w.Start()).(*ChunksEvent)
			<-relayDone // The original complete reply crossed Next's input gate.
			if len(event.Chunks) != 1 || event.Txn != nil || event.Tag != nil || event.User == nil || *event.User != testAdmin {
				t.Fatal("native CHUNKS envelope", event)
			}
			frame := event.Chunks[0]
			if frame.CX != 0 || frame.CY != 0 || frame.CZ != nil || frame.Version != revision || frame.Form == nil {
				t.Fatal("native CHUNKS frame", frame)
			}
			decoded, err := DecodeChunk(schema, frame.Form)
			if err != nil || decoded.Version != revision || decoded.SchemaVersion != schema.Version {
				t.Fatal("native CHUNKS complete form", decoded, err)
			}
			for y := 0; y < 2; y++ {
				for x := 0; x < 2; x++ {
					if !reflect.DeepEqual(decoded.Block(x, y), chunk.Block(x, y)) {
						t.Fatalf("native CHUNKS block (%d,%d): %v", x, y, decoded.Block(x, y))
					}
				}
			}
			if err := w.Ack(ctx, revision); err != nil {
				t.Fatal("application-delivered native CHUNKS ACK", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal("persist native CHUNKS ACK through Close", err)
			}
			if got := nativeSlotPosition(t, ctx, c); got != event.Position {
				t.Fatal("native CHUNKS ACK persistence", got, event.Position)
			}
		})
	}
}

func TestIntegrationFoundationSlotTakeover(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[tls], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			c := foundationNativeFeed(t, ctx, tls, "--feed-progress-ms", "60000")
			if !c.ServerInfo().Supports("slot_takeover") {
				t.Fatal("native takeover capability missing")
			}
			if err := c.CreateSlot(ctx, "feed", "consumer"); err != nil {
				t.Fatal(err)
			}
			old, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			first, err := c.SetBlock(ctx, "feed", 0, 0, Record{"v": 7})
			if err != nil {
				t.Fatal(err)
			}
			delivered := nativeFeedMutation(t, ctx, old, "change", first, old.Start()).(*ChangeEvent)
			if len(delivered.Blocks) != 1 || delivered.Blocks[0].After["v"] != uint64(7) {
				t.Fatal("old owner mutation", delivered)
			}
			// The complete small frame leaves a clear output boundary. No old
			// ACK was issued, so takeover must replay this delivered mutation.
			replacement, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer", Takeover: true})
			if err != nil {
				t.Fatal("native slot takeover", err)
			}
			defer replacement.Close()
			nativeFeedTerminal(t, ctx, old, delivered.Position, CodeKilled, "slot_takeover")
			if replacement.Start() != old.Start() || nativeSlotPosition(t, ctx, c) != old.Start() {
				t.Fatal("takeover acknowledged an unacknowledged mutation", replacement.Start(), old.Start())
			}
			replay := nativeFeedMutation(t, ctx, replacement, "change", first, replacement.Start()).(*ChangeEvent)
			if !reflect.DeepEqual(replay.Blocks, delivered.Blocks) {
				t.Fatal("takeover replay body", replay, delivered)
			}
			second, err := c.SetBlock(ctx, "feed", 0, 0, Record{"v": 8})
			if err != nil {
				t.Fatal(err)
			}
			change := nativeFeedMutation(t, ctx, replacement, "change", second, replay.Position).(*ChangeEvent)
			if len(change.Blocks) != 1 || change.Blocks[0].Before["v"] != uint64(7) || change.Blocks[0].After["v"] != uint64(8) {
				t.Fatal("replacement owner mutation", change)
			}
			if err := replacement.Ack(ctx, second); err != nil {
				t.Fatal(err)
			}
			if err := replacement.Close(); err != nil {
				t.Fatal("persist replacement ACK", err)
			}
			if got := nativeSlotPosition(t, ctx, c); got != change.Position {
				t.Fatal("replacement ACK persistence", got, change.Position)
			}
		})
	}
}

func TestIntegrationFoundationAckDeadline(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[tls], func(t *testing.T) {
			for _, chunks := range []bool{false, true} {
				t.Run(map[bool]string{false: "change", true: "chunks"}[chunks], func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
					defer cancel()
					c := foundationNativeFeed(t, ctx, tls, "--feed-progress-ms", "100")
					if err := c.CreateSlotWithOptions(ctx, "feed", "consumer", SlotOptions{Area: &Area{0, 0, 0, 0}, Chunks: chunks}); err != nil {
						t.Fatal(err)
					}
					w, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer", AckTimeout: 50 * time.Millisecond})
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					outside, err := c.SetBlock(ctx, "feed", 8, 0, Record{"v": 1})
					if err != nil {
						t.Fatal(err)
					}
					progress := w.Start()
					for count := 0; count < 3; {
						event, err := w.Next(ctx)
						if err != nil {
							t.Fatal("waiting for three real progress frames without a mutation deadline", err)
						}
						switch e := event.(type) {
						case *SchemaEvent:
							if e.Version != 1 || e.Position.Epoch != progress.Epoch || e.Position.Revision > outside {
								t.Fatal("progress-only schema", e)
							}
						case *ProgressEvent:
							if e.Position.Epoch != progress.Epoch || e.Position.Revision < progress.Revision || e.Position.Revision > outside {
								t.Fatal("progress-only frontier", e)
							}
							progress = e.Position
							if progress.Revision == outside {
								count++
							}
						default:
							t.Fatalf("filtered write produced a mutation: %#v", event)
						}
					}
					// Withhold ACK through all three ticks: acknowledging each
					// tick could conceal a mutation clock wrongly started by it.
					if err := w.Ack(ctx, outside); err != nil {
						t.Fatal("application-delivered progress ACK", err)
					}
					if err := w.Close(); err != nil {
						t.Fatal("progress cannot start the mutation timeout", err)
					}
					if got := nativeSlotPosition(t, ctx, c); got != progress {
						t.Fatal("progress ACK persistence", got, progress)
					}
					kind := map[bool]string{false: "change", true: "chunks"}[chunks]
					open := func() *Watch {
						watch, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer", AckTimeout: 2 * time.Second})
						if err != nil {
							t.Fatal("open native deadline watch", err)
						}
						return watch
					}
					acked := open()
					defer acked.Close()
					first, err := c.SetBlock(ctx, "feed", 0, 0, Record{"v": 2})
					if err != nil {
						t.Fatal(err)
					}
					firstEvent := nativeFeedMutation(t, ctx, acked, kind, first, acked.Start())
					if err := acked.Ack(ctx, first); err != nil {
						t.Fatal("first native mutation ACK", err)
					}
					if err := acked.Close(); err != nil {
						t.Fatal("first native ACK persistence barrier", err)
					}
					if got := nativeSlotPosition(t, ctx, c); got != firstEvent.GetPosition() {
						t.Fatal("first native mutation persisted ACK", got, firstEvent)
					}
					unacked := open()
					defer unacked.Close()
					if unacked.Start() != firstEvent.GetPosition() {
						t.Fatal("native deadline watch resumed before ACK", unacked.Start(), firstEvent)
					}
					second, err := c.SetBlock(ctx, "feed", 0, 0, Record{"v": 3})
					if err != nil {
						t.Fatal(err)
					}
					secondEvent := nativeFeedMutation(t, ctx, unacked, kind, second, unacked.Start())
					if nativeFeedTerminal(t, ctx, unacked, secondEvent.GetPosition(), CodeTimeout, "ack_timeout") == 0 {
						t.Fatal("mutation deadline fixture observed no intervening progress")
					}
					if got := nativeSlotPosition(t, ctx, c); got != firstEvent.GetPosition() {
						t.Fatal("timeout acknowledged an unacknowledged mutation", got, firstEvent)
					}
					replay, err := c.Watch(ctx, "feed", WatchOptions{Slot: "consumer"})
					if err != nil {
						t.Fatal("reopen after native ACK timeout", err)
					}
					defer replay.Close()
					if replay.Start() != firstEvent.GetPosition() {
						t.Fatal("timeout replay start", replay.Start(), firstEvent)
					}
					if event := nativeFeedMutation(t, ctx, replay, kind, second, replay.Start()); !reflect.DeepEqual(event, secondEvent) {
						t.Fatal("native timeout replay body", event, secondEvent)
					}
					if err := replay.Ack(ctx, second); err != nil {
						t.Fatal(err)
					}
					if err := replay.Close(); err != nil {
						t.Fatal("persist native timeout replay ACK", err)
					}
					if got := nativeSlotPosition(t, ctx, c); got != secondEvent.GetPosition() {
						t.Fatal("native timeout replay persisted ACK", got, secondEvent)
					}
				})
			}
		})
	}
}
