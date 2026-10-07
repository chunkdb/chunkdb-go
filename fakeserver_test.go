package chunkdb

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
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
	// puts holds every CHUNKPUT request exactly as it arrived: the request
	// line, the payload, and the empty line after it.
	puts     [][]byte
	conns    []net.Conn
	accepted int
	// finished counts connections the client closed or the server dropped.
	finished int
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
	defer func() {
		s.mu.Lock()
		s.finished++
		s.mu.Unlock()
	}()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(strings.ToUpper(line), "CHUNKPUT ") {
			// The payload and its empty-line terminator follow the header;
			// read them so the next iteration sees the next request line.
			fields := strings.Fields(line)
			n, convErr := strconv.Atoi(fields[len(fields)-1])
			if convErr != nil || n < 0 {
				return
			}
			body := make([]byte, n+2)
			if _, readErr := io.ReadFull(reader, body); readErr != nil {
				return
			}
			s.mu.Lock()
			s.puts = append(s.puts, append([]byte(line), body...))
			s.mu.Unlock()
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

func (s *fakeServer) finishedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

// lastPut returns the most recent CHUNKPUT request as it arrived on the wire.
func (s *fakeServer) lastPut(t *testing.T) []byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.puts) == 0 {
		t.Fatal("server received no CHUNKPUT")
	}
	return s.puts[len(s.puts)-1]
}

// helloLimits is the part of every fake HELLO reply before the table lines.
const helloLimits = "protocol=2\nserver_version=test\ncapabilities=zrle\nmax_line_bytes=65536\n" +
	"max_area_chunks=256\nmax_response_bytes=67108864\nmax_scan_limit=1024\nmax_batch_ops=1024\n"

// defaultInfo is the fake server's default table. Chunks of 2x2 blocks of 4
// bits keep the derived sizes small: 2 payload bytes and 1 presence byte.
const defaultInfo = "table=default\nstore_id=ffeeddccbbaa99887766554433221100\nblock_bits=4\n" +
	"chunk_width_blocks=2\nchunk_height_blocks=2\nlarge_chunk_width_chunks=8\n" +
	"large_chunk_height_chunks=8\ndurability_mode=relaxed\ncheckpoint_updates=256\n" +
	"checkpoint_wal_bytes=1048576\nwal_group_commit_updates=8\ncheckpoint_compression=none\n"

const (
	testChunkPayloadBytes = 2
	testPresenceBytes     = 1
)

// fakeTables are the tables the fake HELLO and USE know.
var fakeTables = map[string]string{"default": defaultInfo, "terrain": terrainInfo}

// answerHello answers a HELLO line like a protocol 2 server holding
// fakeTables, ignoring the token.
func answerHello(conn net.Conn, command string) {
	args := strings.Fields(command)
	table := "default"
	for i := 2; i+1 < len(args); i += 2 {
		if args[i] == "TABLE" {
			table = args[i+1]
		}
	}
	info, ok := fakeTables[table]
	if !ok {
		writeServerError(conn, "ERR NO_TABLE table '"+table+"' does not exist")
		return
	}
	writeBulkString(conn, helloLimits+info)
}

// withHello answers HELLO and forwards everything else to handle.
func withHello(handle func(*fakeServer, net.Conn, string)) func(*fakeServer, net.Conn, string) {
	return func(s *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "HELLO" {
			answerHello(conn, command)
			return
		}
		handle(s, conn, command)
	}
}

// respondWith answers HELLO and every other command with the same canned
// response bytes.
func respondWith(response string) func(*fakeServer, net.Conn, string) {
	return withHello(func(_ *fakeServer, conn net.Conn, _ string) {
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

func writeNull(conn net.Conn) {
	_, _ = conn.Write([]byte("$-1\r\n"))
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
