package chunkdb

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// stalledServer answers HELLO and PING, then stops reading a connection at
// any other statement, like a peer that no longer drains its socket. A large
// request to it blocks in the client's write once the socket buffers fill.
type stalledServer struct {
	listener net.Listener
	// stalled is closed once the first other statement arrived, so the
	// client is inside its write.
	stalled chan struct{}
	once    sync.Once

	mu       sync.Mutex
	conns    []net.Conn
	accepted int
}

func newStalledServer(t *testing.T) *stalledServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &stalledServer{listener: listener, stalled: make(chan struct{})}
	go server.acceptLoop()
	t.Cleanup(func() {
		_ = listener.Close()
		server.mu.Lock()
		defer server.mu.Unlock()
		for _, conn := range server.conns {
			_ = conn.Close()
		}
	})
	return server
}

func (s *stalledServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.accepted++
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.serve(conn)
	}
}

func (s *stalledServer) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		switch verbOf(strings.TrimRight(line, "\r\n")) {
		case "HELLO":
			writeRaw(conn, helloReply)
		case "PING":
			writeSimple(conn, "PONG")
		default:
			s.once.Do(func() { close(s.stalled) })
			return // The connection stays open and is never read again.
		}
	}
}

func (s *stalledServer) acceptedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

func (s *stalledServer) client(t *testing.T, configure func(*Options)) *Client {
	t.Helper()
	opts := Options{URI: "chunk://" + s.listener.Addr().String() + "/", ConnectTimeout: 2 * time.Second}
	configure(&opts)
	client, err := Connect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// stalledWritePayload is far larger than the socket buffers on both ends.
const stalledWritePayload = 64 << 20

func sendStalledChunk(ctx context.Context, client *Client) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := client.Do(ctx, "SET CHUNK 0 0 IN world $1", make([]byte, stalledWritePayload))
		result <- err
	}()
	return result
}

// awaitResult fails the test when a call is still blocked long after its
// deadline or cancellation.
func awaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the call is still blocked in its write")
		return nil
	}
}

func TestClientCommandTimeoutInterruptsBlockedWrite(t *testing.T) {
	server := newStalledServer(t)
	client := server.client(t, func(o *Options) { o.CommandTimeout = 100 * time.Millisecond })

	err := awaitResult(t, sendStalledChunk(t.Context(), client))
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want a command timeout", err)
	}
	if !strings.Contains(err.Error(), "command timeout") {
		t.Fatalf("got %v, want the command timeout named", err)
	}

	// The request was cut off mid-frame, so the connection is dropped and
	// the next call dials a fresh one.
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after the interrupted write: %v", err)
	}
	if got := server.acceptedConns(); got != 2 {
		t.Fatalf("got %d connections, want 2", got)
	}
}

func TestClientCancellationInterruptsBlockedWrite(t *testing.T) {
	server := newStalledServer(t)
	client := server.client(t, func(o *Options) { o.CommandTimeout = -1 })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := sendStalledChunk(ctx, client)
	<-server.stalled
	cancel()

	err := awaitResult(t, result)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want it to wrap context.Canceled", err)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after the interrupted write: %v", err)
	}
	if got := server.acceptedConns(); got != 2 {
		t.Fatalf("got %d connections, want 2", got)
	}
}

func TestClientCallWaitingBehindBlockedWriteHonorsItsContext(t *testing.T) {
	server := newStalledServer(t)
	client := server.client(t, func(o *Options) {
		o.CommandTimeout = -1
		o.PipelineDepth = 2
	})

	// The first request has no deadline and stays blocked in its write.
	blocked := sendStalledChunk(t.Context(), client)
	<-server.stalled

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	waiting := make(chan error, 1)
	go func() { waiting <- client.Ping(ctx) }()
	err := awaitResult(t, waiting)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the waiting call's own deadline", err)
	}

	// Close ends the blocked write.
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := awaitResult(t, blocked); err == nil {
		t.Fatal("the blocked request succeeded after Close")
	}
}

// lateCancelConn ends a context right after a write went through, before the
// client's write call returns, and remembers the last write deadline set.
type lateCancelConn struct {
	net.Conn

	mu            sync.Mutex
	cancel        context.CancelFunc
	writeDeadline time.Time
}

func (c *lateCancelConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return n, err
}

func (c *lateCancelConn) SetWriteDeadline(at time.Time) error {
	c.mu.Lock()
	c.writeDeadline = at
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(at)
}

func (c *lateCancelConn) lastWriteDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadline
}

func TestClientClearsAWriteDeadlineSetAfterTheWrite(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, func(o *Options) { o.CommandTimeout = -1 })
	netConn, err := net.Dial("tcp", server.addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hooked := &lateCancelConn{Conn: netConn, cancel: cancel}
	established := &conn{client: client, netConn: hooked, reader: bufio.NewReader(hooked),
		writer: bufio.NewWriter(hooked), writeTurn: make(chan struct{}, 1)}
	go established.readLoop()
	defer func() { _ = established.shutdown(ErrClosed) }()

	// The context ends once the request is written, so its end expires the
	// write deadline too late to interrupt anything. When the reply then wins
	// over the cancellation, the connection stays in use, so the deadline
	// must be cleared before the call returns.
	_, _ = client.execOn(ctx, established, "PING", "PING", nil)
	if at := hooked.lastWriteDeadline(); !at.IsZero() {
		t.Fatalf("the connection keeps the write deadline %v", at)
	}
}

func TestClientRequestWithEndedContextOnAnIdleConnection(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, func(o *Options) { o.CommandTimeout = -1 })
	ended, cancel := context.WithCancel(t.Context())
	cancel()

	// A request whose context already ended takes a free write turn, so it
	// fails only as an interrupted request, which drops the connection;
	// transactions rely on it for ROLLBACK. Its reply may still win, which
	// leaves the connection in step. Waiting for the turn and the ended
	// context would race, so the check repeats.
	const requests = 16
	for range requests {
		established, err := client.connection(t.Context())
		if err != nil {
			t.Fatalf("connection: %v", err)
		}
		_, err = client.execOn(ended, established, "PING", "PING", nil)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want it to wrap context.Canceled", err)
		}
		if established.terminalError() == nil {
			t.Fatal("the connection was kept after a request with an ended context failed")
		}
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
