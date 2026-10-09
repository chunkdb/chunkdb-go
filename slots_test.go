package chunkdb

import (
	"bufio"
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

func slotReply(acked, retained string) string {
	return respMap("table", respBulk("world"), "name", respBulk("consumer"), "epoch", respBulk(watchEpoch), "acked", acked, "retained_bytes", retained, "lost", respBool(true))
}
func TestSlotsStatementsAndTypes(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		if strings.HasPrefix(command, "SHOW SLOTS") {
			writeRaw(cn, respArray(slotReply(":18446744073709551615\r\n", ":18446744073709551615\r\n")))
		} else {
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, s, func(o *Options) { o.Table = "world" })
	if err := c.CreateSlot(t.Context(), "", "consumer"); err != nil {
		t.Fatal(err)
	}
	if s.lastCommand(t) != "CREATE SLOT 'consumer' ON world" {
		t.Fatal(s.commands())
	}
	if err := c.DropSlot(t.Context(), "world", "consumer"); err != nil {
		t.Fatal(err)
	}
	if s.lastCommand(t) != "DROP SLOT 'consumer' ON world" {
		t.Fatal(s.commands())
	}
	for _, table := range []string{"", "world"} {
		got, err := c.Slots(t.Context(), table)
		if err != nil || !reflect.DeepEqual(got, []Slot{{Table: "world", Name: "consumer", Epoch: watchEpoch, Acked: math.MaxUint64, RetainedBytes: math.MaxUint64, Lost: true}}) {
			t.Fatal(got, err)
		}
		want := "SHOW SLOTS"
		if table != "" {
			want += " ON world"
		}
		if s.lastCommand(t) != want {
			t.Fatal(s.commands())
		}
	}
	p, err := NewPool(PoolOptions{Options: Options{URI: s.uri("")}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.CreateSlot(t.Context(), "world", "consumer"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Slots(t.Context(), "world"); err != nil {
		t.Fatal(err)
	}
	if err := p.DropSlot(t.Context(), "world", "consumer"); err != nil {
		t.Fatal(err)
	}
}
func TestSlotsInvalidInputAndReplies(t *testing.T) {
	for _, name := range []string{"", "bad'name", "Upper", strings.Repeat("a", 64)} {
		c, _ := NewClient(Options{})
		if err := c.CreateSlot(t.Context(), "world", name); !errors.Is(err, ErrProtocol) {
			t.Fatal(name, err)
		}
		if err := c.DropSlot(t.Context(), "world", name); !errors.Is(err, ErrProtocol) {
			t.Fatal(name, err)
		}
		if name != "" {
			if _, err := c.Watch(t.Context(), "world", WatchOptions{Slot: name}); !errors.Is(err, ErrProtocol) {
				t.Fatal(name, err)
			}
		}
	}
	for _, frame := range []string{respNull, respArray(respInt(1)), respArray(respMap()), respArray(slotReply(respInt(-1), respInt(1))), respArray(slotReply(respBulk("1"), respInt(1))), respArray(slotReply(respInt(1), respInt(-1))), respArray(respMap("table", respBulk("world"), "table", respBulk("world")))} {
		t.Run(frame, func(t *testing.T) {
			s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, _ string) { writeRaw(cn, frame) }))
			c := newTestClient(t, s, nil)
			if _, err := c.Slots(t.Context(), ""); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}
func TestSlotWatchAckNoReplyAndRecoverableError(t *testing.T) {
	commands := make(chan string, 8)
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			writeRaw(cn, describeReply("world", 1, []string{fakeColumn(1, "v", "u8", false, false, respNull)}))
		case "WATCH":
			commands <- command
			writeRaw(cn, "+OK "+watchEpoch+" 10\r\n"+changePush(1, respNull, respArray(respInt(2))))
		case "ACK":
			commands <- command
			if command == "ACK 11" {
				writeServerError(cn, "ERR INVALID_ARGUMENT rejected")
				writeRaw(cn, push(respBulk("resync"), respBulk(watchEpoch), respInt(12)))
			}
		case "UNWATCH":
			commands <- command
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, s, nil)
	w, err := c.Watch(t.Context(), "world", WatchOptions{Slot: "consumer", Area: &Area{0, 0, 1, 1}, After: &Position{watchEpoch, 9}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := <-commands; got != "WATCH world SLOT 'consumer' AREA 0 0 TO 1 1 AFTER "+watchEpoch+" 9" {
		t.Fatal(got)
	}
	if err := w.Ack(t.Context(), 12); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if err := w.Ack(t.Context(), 10); err != nil {
		t.Fatal(err)
	} // no reply; must return
	if got := <-commands; got != "ACK 10" {
		t.Fatal(got)
	}
	nextWatch(t, w)
	if err := w.Ack(t.Context(), 11); err != nil {
		t.Fatal(err)
	}
	if got := <-commands; got != "ACK 11" {
		t.Fatal(got)
	}
	if _, err := w.Next(t.Context()); !isServerCode(err, CodeInvalidArgument) {
		t.Fatal(err)
	}
	if nextWatch(t, w).GetPosition().Revision != 12 {
		t.Fatal("stream ended after ACK rejection")
	}
	if err := w.Ack(t.Context(), 10); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := <-commands; got != "UNWATCH" {
		t.Fatal(got)
	}
	if err := w.Ack(t.Context(), 12); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	_, ordinary := fakeWatch(t, "", worldColumns)
	if err := ordinary.Ack(t.Context(), 10); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
func TestSlotWatchStartupInvalidArgument(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		if verbOf(command) == "DESCRIBE" {
			writeRaw(cn, describeReply("world", 1, worldColumns))
		} else {
			writeServerError(cn, "ERR INVALID_ARGUMENT denied")
		}
	}))
	c := newTestClient(t, s, nil)
	if _, err := c.Watch(t.Context(), "world", WatchOptions{Slot: "consumer"}); !isServerCode(err, CodeInvalidArgument) {
		t.Fatal(err)
	}
}

type ackBlockingConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *ackBlockingConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(b)
}
func TestWatchAckInterruptedWrite(t *testing.T) {
	for _, closeWatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "context", true: "close"}[closeWatch], func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			observed := &ackBlockingConn{Conn: left, entered: make(chan struct{})}
			c, _ := NewClient(Options{CommandTimeout: -1})
			w := &Watch{client: c, slot: "consumer", start: Position{watchEpoch, 10}, delivered: Position{watchEpoch, 10}, acknowledged: 10, controlGate: make(chan struct{}, 1), closing: make(chan struct{}), done: make(chan struct{})}
			w.ackContext, w.ackCancel = context.WithCancel(context.Background())
			defer w.ackCancel()
			w.conn = &conn{client: c, netConn: observed, writer: bufio.NewWriter(observed), watch: w}
			c.active = w.conn
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- w.Ack(ctx, 10) }()
			<-observed.entered
			if closeWatch {
				if err := w.Close(); !errors.Is(err, ErrTimeout) {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, ErrTimeout) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("ACK write did not stop")
			}
			if err := w.conn.terminalError(); !errors.Is(err, ErrTimeout) {
				t.Fatal(err)
			}
		})
	}
}
