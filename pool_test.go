package chunkdb

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestPool(t *testing.T, server *fakeServer, configure func(*PoolOptions)) *Pool {
	t.Helper()

	opts := PoolOptions{
		Options: Options{
			URI:            server.uri("tok"),
			ConnectTimeout: 2 * time.Second,
			CommandTimeout: 2 * time.Second,
		},
		MaxConnections: 2,
		AcquireTimeout: 2 * time.Second,
	}
	if configure != nil {
		configure(&opts)
	}

	pool, err := ConnectPool(t.Context(), opts)
	if err != nil {
		t.Fatalf("ConnectPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func TestPoolReusesOneConnectionWhenSequential(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, nil)

	for range 5 {
		if err := pool.Ping(t.Context()); err != nil {
			t.Fatalf("Ping: %v", err)
		}
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestPoolWarmsMinConnections(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	newTestPool(t, server, func(o *PoolOptions) {
		o.MaxConnections = 4
		o.MinConnections = 3
	})

	if got := server.acceptedConns(); got != 3 {
		t.Fatalf("got %d connections, want 3 warm connections", got)
	}
}

func TestPoolWarmFailureIsReported(t *testing.T) {
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR AUTH_FAILED invalid token")
	})

	_, err := ConnectPool(t.Context(), PoolOptions{
		Options:        Options{URI: server.uri("wrong")},
		MaxConnections: 2,
		MinConnections: 1,
	})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
}

func TestPoolRunsUpToMaxConnectionsConcurrently(t *testing.T) {
	const maxConnections = 3

	requests := make(chan net.Conn, maxConnections*2)
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		requests <- conn
	}))
	pool := newTestPool(t, server, func(o *PoolOptions) { o.MaxConnections = maxConnections })

	results := make(chan error, maxConnections)
	for range maxConnections {
		go func() { results <- pool.Ping(t.Context()) }()
	}

	conns := make([]net.Conn, 0, maxConnections)
	for range maxConnections {
		select {
		case conn := <-requests:
			conns = append(conns, conn)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d requests reached the server", len(conns), maxConnections)
		}
	}
	if got := server.acceptedConns(); got != maxConnections {
		t.Fatalf("got %d connections, want %d", got, maxConnections)
	}

	for _, conn := range conns {
		writeSimple(conn, "PONG")
	}
	for range maxConnections {
		if err := <-results; err != nil {
			t.Fatalf("Ping: %v", err)
		}
	}
}

func TestPoolAcquireTimesOutWhenSaturated(t *testing.T) {
	release := make(chan struct{})
	leased := make(chan struct{})
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, func(o *PoolOptions) {
		o.MaxConnections = 1
		o.AcquireTimeout = 100 * time.Millisecond
	})

	go func() {
		_ = pool.WithClient(t.Context(), func(context.Context, *Client) error {
			close(leased)
			<-release
			return nil
		})
	}()
	<-leased

	err := pool.Ping(t.Context())
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	if !strings.Contains(err.Error(), "pool acquire timeout") {
		t.Fatalf("got %q, want a pool acquire timeout message", err)
	}

	close(release)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after the lease was returned: %v", err)
	}
}

func TestPoolRecoversCapacityAfterTransportFailure(t *testing.T) {
	// The server drops the connection instead of answering PING, then behaves
	// normally, so the pool must discard the broken client and still be able to
	// open a replacement within its one-connection ceiling.
	var dropped bool
	var mu sync.Mutex
	server := newFakeServer(t, withAuth(func(s *fakeServer, conn net.Conn, _ string) {
		mu.Lock()
		first := !dropped
		dropped = true
		mu.Unlock()

		if first {
			_ = conn.Close()
			return
		}
		writeSimple(conn, "PONG")
	}))
	pool := newTestPool(t, server, func(o *PoolOptions) {
		o.MaxConnections = 1
		o.AcquireTimeout = 500 * time.Millisecond
	})

	if err := pool.Ping(t.Context()); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after the failure: %v", err)
	}
}

func TestPoolWithClientPropagatesCallbackError(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, nil)

	sentinel := errors.New("callback failed")
	if err := pool.WithClient(t.Context(), func(context.Context, *Client) error {
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the callback error", err)
	}

	// A non-transport failure must not cost the pool its connection.
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want the client to have been reused", got)
	}
}

func TestPoolReturnsLeaseWhenTheCallbackPanics(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, func(o *PoolOptions) {
		o.MaxConnections = 1
		o.AcquireTimeout = 500 * time.Millisecond
	})

	func() {
		defer func() { _ = recover() }()
		_ = pool.WithClient(t.Context(), func(context.Context, *Client) error {
			panic("callback panicked")
		})
	}()

	// The lease must have been returned, or this acquire would time out.
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after a panicking callback: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPoolCloseWaitsForLeases(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, nil)

	leased := make(chan struct{})
	release := make(chan struct{})
	leaseDone := make(chan struct{})
	go func() {
		defer close(leaseDone)
		_ = pool.WithClient(context.Background(), func(context.Context, *Client) error {
			close(leased)
			<-release
			return nil
		})
	}()
	<-leased

	closed := make(chan error, 1)
	go func() { closed <- pool.Close() }()

	select {
	case <-closed:
		t.Fatal("Close returned while a client was still leased")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-leaseDone

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the lease was released")
	}
}

func TestPoolClosedRejectsOperations(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, nil)

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := pool.Ping(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestPoolConcurrentMixedOperations(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, func(o *PoolOptions) { o.MaxConnections = 4 })

	var group sync.WaitGroup
	for i := range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := pool.Set(t.Context(), int64(i), 0, "1010"); err != nil {
				t.Errorf("Set: %v", err)
				return
			}
			if _, err := pool.ReadBlock(t.Context(), int64(i), 0); err != nil {
				t.Errorf("ReadBlock: %v", err)
			}
		}()
	}
	group.Wait()

	if got := server.acceptedConns(); got > 4 {
		t.Fatalf("got %d connections, want at most 4", got)
	}
}

func TestNewPoolRejectsBadOptions(t *testing.T) {
	cases := map[string]PoolOptions{
		"zero max":         {MaxConnections: 0},
		"negative max":     {MaxConnections: -1},
		"negative min":     {MaxConnections: 2, MinConnections: -1},
		"min above max":    {MaxConnections: 2, MinConnections: 3},
		"invalid endpoint": {Options: Options{URI: "http://host/"}, MaxConnections: 1},
	}

	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPool(opts); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestPoolMirrorsClientCommands(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	pool := newTestPool(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{"ping", func() error { return pool.Ping(ctx) }, "PING"},
		{"info", func() error { _, err := pool.Info(ctx); return err }, "INFO"},
		{"get", func() error { _, err := pool.Get(ctx, 1, 2); return err }, "GET 1 2"},
		{"read block", func() error { _, err := pool.ReadBlock(ctx, 1, 2); return err }, "EXISTS 1 2"},
		{"exists", func() error { _, err := pool.Exists(ctx, 1, 2); return err }, "EXISTS 1 2"},
		{"set", func() error { return pool.Set(ctx, 1, 2, "1010") }, "SET 1 2 1010"},
		{"unset", func() error { return pool.Unset(ctx, 1, 2) }, "UNSET 1 2"},
		{"mset", func() error { return pool.MSet(ctx, []Block{{X: 1, Y: 2, Bits: "1010"}}) }, "MSET 1 2 1010"},
		{"mget", func() error { _, err := pool.MGet(ctx, []BlockRef{{X: 1, Y: 2}}); return err }, "MGET 1 2"},
		{"chunk exists", func() error { _, err := pool.ChunkExists(ctx, 1, 2); return err }, "CHUNKEXISTS 1 2"},
		{"read chunk", func() error { _, err := pool.ReadChunk(ctx, 1, 2); return err }, "CHUNK 1 2 STATE"},
		{"chunk", func() error { _, err := pool.Chunk(ctx, 1, 2); return err }, "CHUNK 1 2"},
		{"set chunk", func() error { return pool.SetChunk(ctx, 1, 2, "1010") }, "CHUNKSET 1 2 1010"},
		{
			"set chunk state",
			func() error {
				return pool.SetChunkState(ctx, 1, 2, ChunkStateInput{
					Bits:     strings.Repeat("1", testChunkPayloadBits),
					Presence: strings.Repeat("1", testChunkBlockCount),
				})
			},
			"CHUNKSET 1 2 STATE 1111111111111111|1111",
		},
		{"chunkbin", func() error { _, err := pool.ChunkBin(ctx, 1, 2); return err }, "CHUNKBIN 1 2"},
		{"chunkbin state", func() error { _, err := pool.ChunkBinState(ctx, 1, 2); return err }, "CHUNKBIN 1 2 STATE"},
		{"chunkbinc", func() error { _, err := pool.ChunkBinCompressed(ctx, 1, 2); return err }, "CHUNKBINC 1 2"},
		{
			"chunkbinc state",
			func() error { _, err := pool.ChunkBinStateCompressed(ctx, 1, 2); return err },
			"CHUNKBINC 1 2 STATE",
		},
		{"scan", func() error { _, err := pool.ChunkScan(ctx, 10, nil); return err }, "CHUNKSCAN 10"},
		{"range", func() error { _, err := pool.ChunkRange(ctx, 1, 2, 3, 4); return err }, "CHUNKRANGE 1 2 3 4"},
		{"radius", func() error { _, err := pool.ChunkRadius(ctx, 1, 2, 3); return err }, "CHUNKRADIUS 1 2 3"},
		{"version", func() error { _, err := pool.ChunkVersion(ctx, 1, 2); return err }, "CHUNKVER 1 2"},
		{
			"compare and set",
			func() error {
				_, err := pool.ChunkCompareAndSet(ctx, 1, 2, 5, ChunkStateInput{Bits: "1111", Presence: "1"})
				return err
			},
			"CHUNKCAS 1 2 5 STATE 1111|1",
		},
		{
			"batch",
			func() error {
				_, err := pool.ChunkBatch(ctx, 1, 2, []BatchOperation{UnsetOp(3, 4)})
				return err
			},
			"CHUNKBATCH 1 2 - UNSET 3 4",
		},
		{
			"batch with version",
			func() error {
				_, err := pool.ChunkBatchIfVersion(ctx, 1, 2, 9, []BatchOperation{UnsetOp(3, 4)})
				return err
			},
			"CHUNKBATCH 1 2 9 UNSET 3 4",
		},
		{"wal flush", func() error { return pool.WALFlush(ctx) }, "WALFLUSH"},
		{"metrics", func() error { _, err := pool.Metrics(ctx); return err }, "METRICS"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			if got := server.lastCommand(t); got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}
}
