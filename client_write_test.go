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
