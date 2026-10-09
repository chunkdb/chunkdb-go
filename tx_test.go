package chunkdb

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// txServer is a fake server with transactions: BEGIN, COMMIT and ROLLBACK
// keep a per-connection state, writes inside a transaction answer _, and
// everything else is answered like genericHandler. After a CONFLICT from a
// statement, every statement answers the same CONFLICT until ROLLBACK (+OK)
// or COMMIT (the CONFLICT again) closes the transaction. It logs every
// statement after HELLO as "<connection>:<statement>".
type txServer struct {
	mu    sync.Mutex
	conns map[net.Conn]int
	inTx  map[net.Conn]bool
	// conflicted holds the CONFLICT a statement got, per connection.
	conflicted map[net.Conn]string
	log        []string
	// commits are the replies to the next COMMITs; :42 once they run out.
	commits []string
	// replies answer the next statements of the given verb ("GET BLOCK")
	// inside a transaction instead of the default.
	replies map[string][]string
}

func newTxServer(t *testing.T) (*fakeServer, *txServer) {
	t.Helper()
	state := &txServer{
		conns: make(map[net.Conn]int), inTx: make(map[net.Conn]bool), conflicted: make(map[net.Conn]string),
		replies: make(map[string][]string),
	}
	return newFakeServer(t, withHello(state.handle)), state
}

func (s *txServer) handle(server *fakeServer, conn net.Conn, statement string) {
	s.mu.Lock()
	id, ok := s.conns[conn]
	if !ok {
		id = len(s.conns) + 1
		s.conns[conn] = id
	}
	s.log = append(s.log, strconv.Itoa(id)+":"+statement)
	command := commandOf(statement)
	inTx := s.inTx[conn]
	var reply string
	switch conflict := s.conflicted[conn]; {
	case conflict != "" && command == "ROLLBACK":
		delete(s.conflicted, conn)
		delete(s.inTx, conn)
		reply = "+OK\r\n"
	case conflict != "":
		if command == "COMMIT" {
			delete(s.conflicted, conn)
			delete(s.inTx, conn)
		}
		reply = conflict
	case command == "BEGIN":
		s.inTx[conn] = true
		reply = "+OK\r\n"
	case command == "ROLLBACK":
		delete(s.inTx, conn)
		reply = "+OK\r\n"
	case command == "COMMIT":
		delete(s.inTx, conn)
		reply = respInt(42)
		if len(s.commits) > 0 {
			reply, s.commits = s.commits[0], s.commits[1:]
		}
	case inTx && len(s.replies[command]) > 0:
		reply, s.replies[command] = s.replies[command][0], s.replies[command][1:]
		if strings.HasPrefix(reply, "-ERR CONFLICT") {
			s.conflicted[conn] = reply
		}
	case inTx && (command == "SET BLOCK" || command == "DELETE BLOCK" || command == "SET CHUNK"):
		reply = respNull
	}
	s.mu.Unlock()

	if reply == "" {
		genericHandler(server, conn, statement)
		return
	}
	writeRaw(conn, reply)
}

// statements returns the log without connection numbers.
func (s *txServer) statements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.log))
	for i, entry := range s.log {
		_, out[i], _ = strings.Cut(entry, ":")
	}
	return out
}

func (s *txServer) entries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

const conflictReply = "-ERR CONFLICT chunk_changed chunk (0, 0) changed after the snapshot\r\n"

func TestTransactionDoesNotReconnectAfterAClosingErrorReply(t *testing.T) {
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, statement string) {
		switch commandOf(statement) {
		case "BEGIN":
			writeSimple(conn, "OK")
		case "SET BLOCK":
			writeServerError(conn, "ERR INVALID_ARGUMENT the table has no column nope")
			_ = conn.Close()
		default:
			genericHandler(s, conn, statement)
		}
	}))
	client := newTestClient(t, server, nil)
	attempts := 0
	_, err := client.Transaction(t.Context(), func(tx *Tx) error {
		attempts++
		if _, err := tx.call(t.Context(), "SET BLOCK 0 0 IN world nope = $1", [][]byte{{1}}); !isServerCode(err, CodeInvalidArgument) {
			t.Fatalf("got %v, want INVALID_ARGUMENT", err)
		}
		err := tx.DeleteBlock(t.Context(), "world", 1, 1)
		if !errors.Is(err, ErrConnection) || server.acceptedConns() != 1 {
			t.Fatalf("statement after closing error: %v on %d connections", err, server.acceptedConns())
		}
		return err
	})
	if !errors.Is(err, ErrConnection) || attempts != 1 {
		t.Fatalf("got %v after %d attempts, want a connection error without retry", err, attempts)
	}
	if err := client.Ping(t.Context()); err != nil || server.acceptedConns() != 2 {
		t.Fatalf("Ping after transaction: %v on %d connections", err, server.acceptedConns())
	}
}

func TestTransactionRetriesAfterAConflict(t *testing.T) {
	server, state := newTxServer(t)
	state.commits = []string{conflictReply, respInt(42)}
	client := newTestClient(t, server, nil)

	attempts := 0
	version, err := client.Transaction(t.Context(), func(tx *Tx) error {
		attempts++
		return tx.SetBlock(t.Context(), "world", 0, 0, Record{"id": 1})
	})
	if err != nil || version != 42 || attempts != 2 {
		t.Fatalf("got %d, %v after %d attempts; want 42 after 2", version, err, attempts)
	}
	// The cold schema is fetched inside the transaction, on its connection.
	want := []string{
		"BEGIN", "DESCRIBE world", "SET BLOCK 0 0 IN world id = $1", "COMMIT",
		"BEGIN", "SET BLOCK 0 0 IN world id = $1", "COMMIT",
	}
	if got := state.statements(); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestTransactionRollsBackOnACallbackError(t *testing.T) {
	server, state := newTxServer(t)
	client := newTestClient(t, server, nil)

	errStop := errors.New("stop")
	attempts := 0
	_, err := client.Transaction(t.Context(), func(tx *Tx) error {
		attempts++
		if err := tx.DeleteBlock(t.Context(), "world", 0, 0); err != nil {
			return err
		}
		return errStop
	})
	if !errors.Is(err, errStop) || attempts != 1 {
		t.Fatalf("got %v after %d attempts, want errStop after 1", err, attempts)
	}
	want := []string{"BEGIN", "DELETE BLOCK 0 0 FROM world", "ROLLBACK"}
	if got := state.statements(); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	// The client goes on outside a transaction on the same connection.
	if err := client.Ping(t.Context()); err != nil || server.acceptedConns() != 1 {
		t.Fatalf("Ping: %v on %d connections", err, server.acceptedConns())
	}
}

func TestTransactionRetryLimit(t *testing.T) {
	server, state := newTxServer(t)
	state.commits = slices.Repeat([]string{conflictReply}, 10)
	client := newTestClient(t, server, nil)

	for _, tc := range []struct {
		opts     []TxOption
		attempts int
	}{
		{[]TxOption{TxRetries(2)}, 3},
		{[]TxOption{TxRetries(0)}, 1},
		{nil, DefaultTxRetries + 1},
	} {
		state.commits = slices.Repeat([]string{conflictReply}, 10)
		attempts := 0
		_, err := client.Transaction(t.Context(), func(tx *Tx) error {
			attempts++
			return tx.SetBlock(t.Context(), "world", 0, 0, Record{"id": 1})
		}, tc.opts...)
		var conflict *ConflictError
		if !errors.As(err, &conflict) || conflict.Reason != ConflictChunkChanged || !errors.Is(err, ErrConflict) ||
			!errors.Is(err, ErrServer) || errors.Is(err, ErrVersionMismatch) {
			t.Fatalf("got %v, want a chunk_changed conflict", err)
		}
		if attempts != tc.attempts {
			t.Fatalf("got %d attempts, want %d", attempts, tc.attempts)
		}
		if !strings.Contains(err.Error(), "COMMIT: transaction conflict, chunk_changed") {
			t.Fatalf("got message %q", err.Error())
		}
	}
}

func TestTransactionConflictAtAStatement(t *testing.T) {
	server, state := newTxServer(t)
	state.replies["GET BLOCK"] = []string{"-ERR CONFLICT duration the transaction was open longer than 5000 ms\r\n"}
	client := newTestClient(t, server, nil)

	attempts := 0
	version, err := client.Transaction(t.Context(), func(tx *Tx) error {
		attempts++
		_, err := tx.GetBlock(t.Context(), "world", 0, 0)
		if attempts == 1 {
			var conflict *ConflictError
			if !errors.As(err, &conflict) || conflict.Reason != ConflictDuration {
				t.Errorf("got %v, want a duration conflict", err)
			}
			// Nothing is sent after the conflict: the server would answer the
			// same CONFLICT.
			if err := tx.SetBlock(t.Context(), "world", 0, 0, Record{"id": 1}); !errors.Is(err, ErrConflict) {
				t.Errorf("got %v, want the conflict again", err)
			}
			// A callback that drops the conflict still runs again.
			return nil
		}
		return err
	})
	if err != nil || version != 42 || attempts != 2 {
		t.Fatalf("got %d, %v after %d attempts", version, err, attempts)
	}
	// ROLLBACK closes the ended transaction before the next attempt.
	want := []string{
		"BEGIN", "DESCRIBE world", "GET BLOCK 0 0 FROM world COLUMNS id, temp, solid, h, d, mask, name, blob", "ROLLBACK",
		"BEGIN", "GET BLOCK 0 0 FROM world COLUMNS id, temp, solid, h, d, mask, name, blob", "COMMIT",
	}
	if got := state.statements(); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTransactionWithoutWritesHasNoVersion(t *testing.T) {
	server, state := newTxServer(t)
	state.commits = []string{respNull}
	client := newTestClient(t, server, nil)

	version, err := client.Transaction(t.Context(), func(tx *Tx) error {
		_, err := tx.GetChunk(t.Context(), "world", 0, 0)
		return err
	})
	if err != nil || version != 0 {
		t.Fatalf("got %d, %v", version, err)
	}

	// A write inside a transaction must answer _, not a version.
	state.replies["SET BLOCK"] = []string{respInt(7)}
	_, err = client.Transaction(t.Context(), func(tx *Tx) error {
		return tx.SetBlock(t.Context(), "world", 0, 0, Record{"id": 1})
	})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a protocol error", err)
	}

	// A malformed CONFLICT reply is a protocol error.
	if _, err := client.Do(t.Context(), "COMMIT"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	state.commits = []string{"-ERR CONFLICT\r\n"}
	if _, err := client.Do(t.Context(), "COMMIT"); !errors.Is(err, ErrProtocol) || errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want a protocol error", err)
	}
}

func TestTransactionHoldsTheConnection(t *testing.T) {
	server, state := newTxServer(t)
	client := newTestClient(t, server, func(o *Options) { o.PipelineDepth = 4 })
	ctx := t.Context()

	pinged := make(chan error, 1)
	_, err := client.Transaction(ctx, func(tx *Tx) error {
		if err := tx.SetBlock(ctx, "world", 0, 0, Record{"id": 1}); err != nil {
			return err
		}
		go func() { pinged <- client.Ping(ctx) }()
		// The plain request waits for the transaction to end.
		select {
		case err := <-pinged:
			t.Errorf("Ping finished inside the transaction: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		return tx.SetBlock(ctx, "world", 1, 1, Record{"id": 2})
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if err := <-pinged; err != nil {
		t.Fatalf("Ping: %v", err)
	}
	got := state.statements()
	if got[len(got)-1] != "PING" || slices.Index(got, "COMMIT") != len(got)-2 {
		t.Fatalf("got %q, want PING after COMMIT", got)
	}

	// Two transactions on one client run one after the other.
	var group sync.WaitGroup
	for i := range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := client.Transaction(ctx, func(tx *Tx) error {
				if err := tx.SetBlock(ctx, "world", int64(i), 0, Record{"id": i}); err != nil {
					return err
				}
				return tx.SetBlock(ctx, "world", int64(i), 1, Record{"id": i})
			})
			if err != nil {
				t.Errorf("Transaction: %v", err)
			}
		}()
	}
	group.Wait()
	open := false
	for _, statement := range state.statements() {
		switch statement {
		case "BEGIN":
			if open {
				t.Fatalf("a transaction began inside another: %q", state.statements())
			}
			open = true
		case "COMMIT":
			open = false
		}
	}
}

func TestPoolTransactionUsesItsOwnConnection(t *testing.T) {
	server, state := newTxServer(t)
	pool, err := NewPool(PoolOptions{
		Options:        Options{URI: server.uri(""), ConnectTimeout: 2 * time.Second, CommandTimeout: 2 * time.Second},
		MaxConnections: 2,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close() }()
	ctx := t.Context()

	version, err := pool.Transaction(ctx, func(tx *Tx) error {
		if err := tx.SetBlock(ctx, "world", 0, 0, Record{"id": 1}); err != nil {
			return err
		}
		// Another operation of the pool goes to another connection while
		// the transaction is open.
		return pool.Ping(ctx)
	})
	if err != nil || version != 42 {
		t.Fatalf("Transaction: %d, %v", version, err)
	}
	entries := state.entries()
	begin := entries[slices.IndexFunc(entries, func(e string) bool { return strings.HasSuffix(e, ":BEGIN") })]
	ping := entries[slices.IndexFunc(entries, func(e string) bool { return strings.HasSuffix(e, ":PING") })]
	if strings.Split(begin, ":")[0] == strings.Split(ping, ":")[0] {
		t.Fatalf("PING ran on the transaction's connection: %q", entries)
	}
}

func TestTransactionPanicDropsTheConnection(t *testing.T) {
	server, state := newTxServer(t)
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Fatalf("recovered %v, want boom", r)
			}
		}()
		_, _ = client.Transaction(ctx, func(tx *Tx) error {
			if err := tx.SetBlock(ctx, "world", 0, 0, Record{"id": 1}); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	// The dropped connection rolled the transaction back on the server; the
	// client reconnects outside any transaction.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 2 {
		t.Fatalf("got %d connections, want 2", got)
	}
	if slices.Contains(state.statements(), "COMMIT") {
		t.Fatalf("got %q, want no COMMIT", state.statements())
	}
}

func TestTransactionAfterItsFunctionReturned(t *testing.T) {
	server, state := newTxServer(t)
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	var escaped *Tx
	if _, err := client.Transaction(ctx, func(tx *Tx) error { escaped = tx; return nil }); err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if err := escaped.DeleteBlock(ctx, "world", 0, 0); err == nil || !strings.Contains(err.Error(), "ended") {
		t.Fatalf("got %v, want a refusal", err)
	}
	want := []string{"BEGIN", "COMMIT"}
	if got := state.statements(); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	// A cancelled context ends the wait for the connection.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client.Transaction(cancelled, func(*Tx) error { return nil }); !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want a timeout", err)
	}
}

func TestTransactionClosesAStatementConflict(t *testing.T) {
	server, state := newTxServer(t)
	conflict := "-ERR CONFLICT history_limit the table kept too many earlier chunk states\r\n"
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	// A callback that returns another error after the conflict: ROLLBACK,
	// no retry.
	state.replies["SET BLOCK"] = []string{conflict}
	errStop := errors.New("stop")
	attempts := 0
	_, err := client.Transaction(ctx, func(tx *Tx) error {
		attempts++
		_ = tx.SetBlock(ctx, "world", 0, 0, Record{"id": 1})
		return errStop
	})
	if !errors.Is(err, errStop) || attempts != 1 {
		t.Fatalf("got %v after %d attempts, want errStop after 1", err, attempts)
	}

	// Past the retry limit: ROLLBACK after every attempt, then the conflict.
	state.replies["SET BLOCK"] = []string{conflict, conflict}
	_, err = client.Transaction(ctx, func(tx *Tx) error {
		return tx.SetBlock(ctx, "world", 0, 0, Record{"id": 1})
	}, TxRetries(1))
	var typed *ConflictError
	if !errors.As(err, &typed) || typed.Reason != ConflictHistoryLimit {
		t.Fatalf("got %v, want a history_limit conflict", err)
	}

	want := []string{
		"BEGIN", "DESCRIBE world", "SET BLOCK 0 0 IN world id = $1", "ROLLBACK",
		"BEGIN", "SET BLOCK 0 0 IN world id = $1", "ROLLBACK",
		"BEGIN", "SET BLOCK 0 0 IN world id = $1", "ROLLBACK",
		"PING",
	}
	// The connection is out of the transaction afterwards.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := state.statements(); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	// A ROLLBACK that does not answer +OK drops the connection.
	broken := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, statement string) {
		switch commandOf(statement) {
		case "BEGIN":
			writeSimple(conn, "OK")
		case "ROLLBACK":
			writeServerError(conn, "ERR INTERNAL rollback failed")
		default:
			writeRaw(conn, conflict)
		}
	}))
	other := newTestClient(t, broken, nil)
	if _, err := other.Transaction(ctx, func(tx *Tx) error { return tx.DeleteBlock(ctx, "world", 0, 0) }, TxRetries(0)); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want a conflict", err)
	}
	waitForFinishedConns(t, broken, 1)
}
