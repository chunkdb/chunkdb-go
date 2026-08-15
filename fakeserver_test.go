package chunkdb

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
)

// fakeServer is an in-process endpoint speaking the chunkdb wire protocol. It
// lets the client tests drive framing, error, and connection-lifecycle paths
// that a real server cannot be asked for on demand.
//
// Commands are dispatched to handle one at a time per connection, in arrival
// order. A handler that wants to keep a request in flight records the
// connection and writes its response later.
type fakeServer struct {
	listener net.Listener
	handle   func(*fakeServer, net.Conn, string)
	// closeOnAccept makes the server hang up immediately, without reading a
	// single command.
	closeOnAccept bool

	mu       sync.Mutex
	received []string
	conns    []net.Conn
	accepted int
	stopped  bool
}

func newFakeServer(t *testing.T, handle func(*fakeServer, net.Conn, string)) *fakeServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &fakeServer{listener: listener, handle: handle}
	go server.acceptLoop()
	t.Cleanup(server.stop)
	return server
}

// newClosingFakeServer accepts connections and hangs up immediately.
func newClosingFakeServer(t *testing.T) *fakeServer {
	t.Helper()

	server := newFakeServer(t, func(*fakeServer, net.Conn, string) {})
	server.mu.Lock()
	server.closeOnAccept = true
	server.mu.Unlock()
	return server
}

func (s *fakeServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.accepted++
		if s.closeOnAccept {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		go s.serve(conn)
	}
}

func (s *fakeServer) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		command := strings.TrimRight(line, "\r\n")

		s.mu.Lock()
		s.received = append(s.received, command)
		s.mu.Unlock()

		s.handle(s, conn, command)
	}
}

func (s *fakeServer) stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()

	_ = s.listener.Close()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// dropConnections closes every accepted connection, simulating a server that
// drops its clients.
func (s *fakeServer) dropConnections() {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (s *fakeServer) addr() string { return s.listener.Addr().String() }

func (s *fakeServer) uri(token string) string {
	if token == "" {
		return "chunk://" + s.addr() + "/"
	}
	return "chunk://" + token + "@" + s.addr() + "/"
}

func (s *fakeServer) commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

// lastCommand returns the most recently received command line.
func (s *fakeServer) lastCommand(t *testing.T) string {
	t.Helper()
	commands := s.commands()
	if len(commands) == 0 {
		t.Fatal("server received no commands")
	}
	return commands[len(commands)-1]
}

func (s *fakeServer) acceptedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// withAuth answers AUTH with +OK and forwards everything else to handle.
func withAuth(handle func(*fakeServer, net.Conn, string)) func(*fakeServer, net.Conn, string) {
	return func(s *fakeServer, conn net.Conn, command string) {
		if strings.HasPrefix(command, "AUTH ") {
			writeSimple(conn, "OK")
			return
		}
		handle(s, conn, command)
	}
}

// respondWith answers AUTH with +OK and every other command with the same
// canned response bytes.
func respondWith(response string) func(*fakeServer, net.Conn, string) {
	return withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		_, _ = conn.Write([]byte(response))
	})
}

func verbOf(command string) string {
	verb, _, _ := strings.Cut(command, " ")
	return verb
}

func writeSimple(conn net.Conn, text string) {
	_, _ = conn.Write([]byte("+" + text + "\r\n"))
}

func writeServerError(conn net.Conn, text string) {
	_, _ = conn.Write([]byte("-" + text + "\r\n"))
}

func writeBulk(conn net.Conn, payload []byte) {
	_, _ = fmt.Fprintf(conn, "$%d\r\n", len(payload))
	_, _ = conn.Write(payload)
	_, _ = conn.Write([]byte("\r\n"))
}

func writeBulkString(conn net.Conn, payload string) {
	writeBulk(conn, []byte(payload))
}

func writeArray(conn net.Conn, items ...string) {
	_, _ = fmt.Fprintf(conn, "*%d\r\n", len(items))
	for _, item := range items {
		writeBulkString(conn, item)
	}
}

// infoPayload is the INFO response used by tests that need chunk geometry.
// Two 2x2-block chunks of 4-bit blocks keep the derived sizes small:
// 16 payload bits (2 bytes) and 4 presence bits (1 byte).
const infoPayload = "chunkdb_version=1\nblock_bits=4\nchunk_width_blocks=2\nchunk_height_blocks=2\n"

const (
	testChunkPayloadBits  = 16
	testChunkBlockCount   = 4
	testChunkPayloadBytes = 2
	testPresenceBytes     = 1
)
