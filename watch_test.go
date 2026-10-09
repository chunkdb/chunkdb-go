package chunkdb

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const watchEpoch = "0123456789abcdef0123456789abcdef"

func push(items ...string) string { return ">" + strings.TrimPrefix(respArray(items...), "*") }
func changePush(version int64, before, after string) string {
	return push(respBulk("change"), respBulk(watchEpoch), respInt(11), respInt(123), respBulk("writer"), respInt(version), respArray(respArray(respInt(-1), respArray(respInt(math.MaxInt64), respInt(3)), before, after)))
}
func fakeWatch(t *testing.T, frames string, columns []string) (*Client, *Watch) {
	t.Helper()
	server := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			writeRaw(cn, describeReply("world", 1, columns))
		case "WATCH":
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n"+frames)
		case "UNWATCH":
			writeSimple(cn, "OK")
		case "PING":
			writeSimple(cn, "PONG")
		}
	}))
	c := newTestClient(t, server, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return c, w
}
func nextWatch(t *testing.T, w *Watch) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	e, err := w.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestWatchTypedPushes(t *testing.T) {
	row := respArray(respInt(7), respInt(-2), respBool(true), ",1.25\r\n", ",2.5\r\n", respBulk("\x05"), respBulk("hello"), respBulk("\x00\xff"))
	frames := changePush(1, respNull, row) + push(respBulk("resync"), respBulk(watchEpoch), respInt(12))
	c, w := fakeWatch(t, frames, worldColumns)
	if w.Start() != (Position{watchEpoch, 10}) {
		t.Fatal(w.Start())
	}
	e := nextWatch(t, w).(*ChangeEvent)
	if e.Position.Revision != 11 || e.CommitTimeMs != 123 || *e.User != "writer" || e.SchemaVersion != 1 {
		t.Fatal(e)
	}
	b := e.Blocks[0]
	if b.X != int64(-1) || b.Y != (ChunkOffset{math.MaxInt64, 3}) || b.Before != nil {
		t.Fatal(b)
	}
	want := Record{"id": uint64(7), "temp": int64(-2), "solid": true, "h": float32(1.25), "d": float64(2.5), "mask": Bits{N: 3, Bytes: []byte{5}}, "name": "hello", "blob": []byte{0, 255}}
	if !reflect.DeepEqual(b.After, want) {
		t.Fatalf("%#v != %#v", b.After, want)
	}
	if nextWatch(t, w).(*ResyncEvent).Position.Revision != 12 {
		t.Fatal("resync")
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestWatchSchemaCacheAndCloseDrain(t *testing.T) {
	old := []string{fakeColumn(1, "v", "u8", false, false, respNull)}
	changed := []string{fakeColumn(1, "v", "text(16)", false, false, respNull)}
	frames := changePush(1, respArray(respInt(1)), respArray(respInt(2))) + push(respBulk("schema"), respBulk(watchEpoch), respInt(12), respInt(2), respArray(changed...)) + changePush(2, respArray(respBulk("a")), respArray(respBulk("b")))
	_, w := fakeWatch(t, frames, old)
	if nextWatch(t, w).(*ChangeEvent).Blocks[0].After["v"] != uint64(2) {
		t.Fatal("old schema")
	}
	schema := nextWatch(t, w).(*SchemaEvent)
	schema.Columns[0].Name = "wrong"
	e := nextWatch(t, w).(*ChangeEvent)
	if e.Blocks[0].Before["v"] != "a" || e.Blocks[0].After["v"] != "b" {
		t.Fatal(e)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Close must consume pushes even if the consumer never calls Next.
	_, w2 := fakeWatch(t, frames+frames, old)
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestWatchUnknownSchemaFetch(t *testing.T) {
	col := []string{fakeColumn(1, "v", "i8", false, false, respNull)}
	var mu sync.Mutex
	describes := 0
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			mu.Lock()
			describes++
			n := describes
			mu.Unlock()
			writeRaw(cn, describeReply("world", int64(n), col))
		case "WATCH":
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n"+changePush(2, respNull, respArray(respInt(-4))))
		case "UNWATCH":
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, s, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if nextWatch(t, w).(*ChangeEvent).Blocks[0].After["v"] != int64(-4) {
		t.Fatal("schema lookup")
	}
	mu.Lock()
	defer mu.Unlock()
	if describes != 2 {
		t.Fatal(describes)
	}
}
func TestWatchErrors(t *testing.T) {
	for _, code := range []string{CodeNoTable, CodeBusy, CodeProtocol} {
		t.Run(code, func(t *testing.T) {
			s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
				if verbOf(command) == "DESCRIBE" {
					writeRaw(cn, describeReply("world", 1, worldColumns))
				} else {
					writeServerError(cn, "ERR "+code+" refused")
				}
			}))
			c := newTestClient(t, s, nil)
			_, err := c.Watch(t.Context(), "world", WatchOptions{})
			var typed *Error
			if !errors.As(err, &typed) || typed.ServerCode != code {
				t.Fatalf("%v", err)
			}
		})
	}
	for _, frames := range []string{">3\r\n" + respBulk("resync") + respBulk("bad") + respInt(1), changePush(1, respNull, respArray(respInt(1))), push(respBulk("unknown"), respBulk(watchEpoch), respInt(1))} {
		_, w := fakeWatch(t, frames, worldColumns)
		_, err := w.Next(t.Context())
		if !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
}
func TestWatchCancellationAndConcurrentClose(t *testing.T) {
	_, w := fakeWatch(t, "", worldColumns)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := w.Next(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := w.Next(t.Context()); result <- err }()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestWatchLostConnection(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		if verbOf(command) == "DESCRIBE" {
			writeRaw(cn, describeReply("world", 1, worldColumns))
		} else if verbOf(command) == "WATCH" {
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n")
			cn.Close()
		}
	}))
	c := newTestClient(t, s, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Next(t.Context()); !errors.Is(err, ErrConnection) {
		t.Fatal(err)
	}
}

func TestWatchOptionsAndStartValidation(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		if verbOf(command) == "DESCRIBE" {
			writeRaw(cn, describeReply("world", 1, worldColumns))
		} else {
			writeRaw(cn, "+OK bad 1\r\n")
		}
	}))
	c := newTestClient(t, s, nil)
	for _, opts := range []WatchOptions{{Area: &Area{1, 0, 0, 0}}, {After: &Position{Epoch: "bad", Revision: 1}}} {
		if _, err := c.Watch(t.Context(), "world", opts); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
	if _, err := c.Watch(t.Context(), "world", WatchOptions{Area: &Area{-1, -2, 3, 4}, After: &Position{watchEpoch, math.MaxUint64}}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if got := s.lastCommand(t); got != "WATCH world AREA -1 -2 TO 3 4 AFTER "+watchEpoch+" 18446744073709551615" {
		t.Fatal(got)
	}
}
func TestWatchHistoricalSchemaUnavailable(t *testing.T) {
	col := []string{fakeColumn(1, "v", "i8", false, false, respNull)}
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		if verbOf(command) == "DESCRIBE" {
			writeRaw(cn, describeReply("world", 2, col))
		} else if verbOf(command) == "WATCH" {
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n"+changePush(1, respNull, respArray(respInt(1))))
		}
	}))
	c := newTestClient(t, s, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Next(t.Context()); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "schema version 1 unavailable") {
		t.Fatal(err)
	}
	if _, err := w.Next(t.Context()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
func TestWatchNextCancellationThenResume(t *testing.T) {
	ready := make(chan net.Conn, 1)
	server := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			writeRaw(cn, describeReply("world", 1, worldColumns))
		case "WATCH":
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n")
			ready <- cn
		case "UNWATCH":
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, server, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	cn := <-ready
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := w.Next(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	if err := w.conn.terminalError(); err != nil {
		t.Fatal(err)
	}
	writeRaw(cn, push(respBulk("resync"), respBulk(watchEpoch), respInt(11)))
	if nextWatch(t, w).(*ResyncEvent).Position.Revision != 11 {
		t.Fatal("cancelled wait consumed event")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestPushOutsideWatchRejected(t *testing.T) {
	s := newFakeServer(t, respondWith(push(respBulk("resync"), respBulk(watchEpoch), respInt(11))))
	c := newTestClient(t, s, nil)
	if err := c.Ping(t.Context()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestWatchInvalidSchemaColumns(t *testing.T) {
	for name, columns := range map[string][]string{
		"empty":          {},
		"duplicate_name": {fakeColumn(1, "v", "u8", false, false, respNull), fakeColumn(2, "v", "u8", false, false, respNull)},
		"duplicate_id":   {fakeColumn(1, "a", "u8", false, false, respNull), fakeColumn(1, "b", "u8", false, false, respNull)},
	} {
		t.Run(name, func(t *testing.T) {
			_, w := fakeWatch(t, push(respBulk("schema"), respBulk(watchEpoch), respInt(11), respInt(2), respArray(columns...)), worldColumns)
			if _, err := w.Next(t.Context()); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}
func TestWatchRejectsNestedPush(t *testing.T) {
	_, w := fakeWatch(t, push(respBulk("resync"), respBulk(watchEpoch), push(respInt(11))), worldColumns)
	if _, err := w.Next(t.Context()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
