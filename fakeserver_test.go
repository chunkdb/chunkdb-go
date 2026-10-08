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
// Statements are dispatched to handle one at a time per connection, in
// arrival order, after their parameter frames were read. A handler that wants
// to keep a request in flight records the connection and writes its reply
// later.
type fakeServer struct {
	listener net.Listener
	handle   func(*fakeServer, net.Conn, string)
	// closeOnAccept makes the server hang up immediately, without reading a
	// single statement.
	closeOnAccept bool

	mu       sync.Mutex
	received []string
	// params holds the parameter frames of each received statement; nil is
	// NULL.
	params   [][][]byte
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

// parameterCount is the highest $n of a statement, outside quotes.
func parameterCount(statement string) int {
	count := 0
	quoted := false
	for i := 0; i < len(statement); i++ {
		switch {
		case statement[i] == '\'':
			quoted = !quoted
		case statement[i] == '$' && !quoted:
			end := i + 1
			for end < len(statement) && statement[end] >= '0' && statement[end] <= '9' {
				end++
			}
			if n, err := strconv.Atoi(statement[i+1 : end]); err == nil && n > count {
				count = n
			}
		}
	}
	return count
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
		statement := strings.TrimRight(line, "\r\n")
		params := make([][]byte, 0)
		for range parameterCount(statement) {
			header, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			header = strings.TrimRight(header, "\r\n")
			if header == "$-1" {
				params = append(params, nil)
				continue
			}
			n, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
			if err != nil || n < 0 {
				return
			}
			body := make([]byte, n+2)
			if _, err := io.ReadFull(reader, body); err != nil {
				return
			}
			params = append(params, body[:n:n])
		}

		s.mu.Lock()
		s.received = append(s.received, statement)
		s.params = append(s.params, params)
		s.mu.Unlock()

		s.handle(s, conn, statement)
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

// lastCommand returns the most recently received statement.
func (s *fakeServer) lastCommand(t *testing.T) string {
	t.Helper()
	commands := s.commands()
	if len(commands) == 0 {
		t.Fatal("server received no statements")
	}
	return commands[len(commands)-1]
}

// lastParams returns the parameter frames of the most recent statement.
func (s *fakeServer) lastParams(t *testing.T) [][]byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.params) == 0 {
		t.Fatal("server received no statements")
	}
	return s.params[len(s.params)-1]
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

// RESP3 reply builders.

func respBulk(payload string) string {
	return "$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n"
}
func respInt(n int64) string { return ":" + strconv.FormatInt(n, 10) + "\r\n" }
func respArray(items ...string) string {
	return "*" + strconv.Itoa(len(items)) + "\r\n" + strings.Join(items, "")
}

// respMap takes alternating keys (written as bulk strings) and values.
func respMap(pairs ...string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "%%%d\r\n", len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out.WriteString(respBulk(pairs[i]) + pairs[i+1])
	}
	return out.String()
}

func respBool(v bool) string {
	if v {
		return "#t\r\n"
	}
	return "#f\r\n"
}

const respNull = "_\r\n"

// helloReply is the fake server's HELLO 3 reply.
var helloReply = respMap(
	"protocol", respInt(3),
	"server_version", respBulk("test"),
	"max_line_bytes", respInt(65536),
	"max_parameters", respInt(65535),
	"max_area_chunks", respInt(256),
	"max_response_bytes", respInt(64<<20),
	"max_scan_limit", respInt(1024),
)

func fakeColumn(id int64, name, typ string, null, required bool, def string) string {
	return respMap("id", respInt(id), "name", respBulk(name), "type", respBulk(typ), "null", respBool(null),
		"required", respBool(required), "default", def)
}

// worldColumns are the columns of the fake server's tables: a row of every
// type. Chunks are 2x2 blocks.
var worldColumns = []string{
	fakeColumn(1, "id", "u10", false, true, respNull),
	fakeColumn(2, "temp", "i8", true, false, respNull),
	fakeColumn(3, "solid", "bool", false, false, respNull),
	fakeColumn(4, "h", "f32", false, false, ",1.5\r\n"),
	fakeColumn(5, "d", "f64", false, false, respNull),
	fakeColumn(6, "mask", "bits(3)", true, false, respNull),
	fakeColumn(7, "name", "text(16)", true, false, respNull),
	fakeColumn(8, "blob", "bytes(4)", false, false, respNull),
}

// describeReply answers DESCRIBE of a table with columns.
func describeReply(table string, version int64, columns []string) string {
	return respMap(
		"table", respBulk(table),
		"version", respInt(version),
		"columns", respArray(columns...),
		"chunk", respArray(respInt(2), respInt(2)),
		"large", respArray(respInt(8), respInt(8)),
		"options", respMap(
			"durability_mode", respBulk("relaxed"),
			"checkpoint_updates", respInt(256),
			"checkpoint_wal_bytes", respInt(1<<20),
			"wal_group_commit_updates", respInt(8),
			"checkpoint_compression", respBulk("none"),
			"var_max_chunk_bytes", respInt(1<<20),
		),
	)
}

// worldFormBytes is the size of an empty chunk form of the fake tables:
// chunk and schema version, presence, then the values (and validity bits) of
// id, temp, solid, h, d and mask.
const worldFormBytes = 16 + 1 + 5 + (4 + 1) + 1 + 16 + 32 + (2 + 1)

// emptyWorldForm is an empty chunk form of size bytes at chunk version 7 and
// schema version 1.
func emptyWorldForm(size int) string {
	form := make([]byte, size)
	form[0], form[8] = 7, 1
	return string(form)
}

// worldSectionBytes is the part of an empty chunk form each column takes.
var worldSectionBytes = map[string]int{"id": 5, "temp": 4 + 1, "solid": 1, "h": 16, "d": 32, "mask": 2 + 1}

// answerHello answers a HELLO line like a protocol 3 server, ignoring the
// token.
func answerHello(conn net.Conn, _ string) {
	writeRaw(conn, helloReply)
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

// respondWith answers HELLO and every other statement with the same canned
// reply bytes.
func respondWith(response string) func(*fakeServer, net.Conn, string) {
	return withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		writeRaw(conn, response)
	})
}

// respondWithSchema answers DESCRIBE with the fake world table and every
// other statement with the same canned reply bytes.
func respondWithSchema(response string) func(*fakeServer, net.Conn, string) {
	return withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "DESCRIBE" {
			writeRaw(conn, describeReply(strings.Fields(command)[1], 1, worldColumns))
			return
		}
		writeRaw(conn, response)
	})
}

func verbOf(command string) string {
	verb, _, _ := strings.Cut(command, " ")
	return strings.ToUpper(verb)
}

func writeRaw(conn net.Conn, text string) {
	_, _ = conn.Write([]byte(text))
}

func writeSimple(conn net.Conn, text string) {
	writeRaw(conn, "+"+text+"\r\n")
}

func writeServerError(conn net.Conn, text string) {
	writeRaw(conn, "-"+text+"\r\n")
}
