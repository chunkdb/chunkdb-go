package chunkdb

import (
	"context"
	"errors"
	"sync"
	"time"
)

// PoolOptions configure a [Pool]. The embedded [Options] apply to every client
// the pool creates.
type PoolOptions struct {
	Options

	// MaxConnections is the upper bound on clients the pool keeps open. It
	// must be greater than zero.
	MaxConnections int
	// MinConnections is the number of warm connections [ConnectPool] opens up
	// front. It must not exceed MaxConnections.
	MinConnections int
	// AcquireTimeout bounds waiting for a free client. Zero falls back to
	// [Options.CommandTimeout] and then [DefaultTimeout]; a negative value
	// disables the wait deadline.
	AcquireTimeout time.Duration
}

// Pool is a fixed-size set of [Client] connections leased per operation.
//
// It is the recommended way to run concurrent workloads: the protocol has no
// request multiplexing, so parallelism comes from multiple sockets. A Pool is
// safe for concurrent use.
type Pool struct {
	clientOpts     Options
	maxConnections int
	acquireTimeout time.Duration

	// idle holds ready-to-lease clients. capacity holds one token per client
	// the pool may still open; a token is consumed when a client is created
	// and returned when it is closed.
	idle     chan *Client
	capacity chan struct{}

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	leased int
}

// NewPool builds a pool without opening any connection.
func NewPool(opts PoolOptions) (*Pool, error) {
	if opts.MaxConnections <= 0 {
		return nil, requestErrorf("", "MaxConnections must be > 0")
	}
	if opts.MinConnections < 0 {
		return nil, requestErrorf("", "MinConnections must be >= 0")
	}
	if opts.MinConnections > opts.MaxConnections {
		return nil, requestErrorf("", "MinConnections must be <= MaxConnections")
	}
	// Surface malformed URIs and options now rather than on first acquire.
	if _, err := resolveOptions(opts.Options); err != nil {
		return nil, err
	}

	acquireTimeout := opts.AcquireTimeout
	if acquireTimeout == 0 {
		acquireTimeout = opts.CommandTimeout
	}

	pool := &Pool{
		clientOpts:     opts.Options,
		maxConnections: opts.MaxConnections,
		acquireTimeout: resolveTimeout(acquireTimeout),
		idle:           make(chan *Client, opts.MaxConnections),
		capacity:       make(chan struct{}, opts.MaxConnections),
	}
	pool.cond = sync.NewCond(&pool.mu)
	for range opts.MaxConnections {
		pool.capacity <- struct{}{}
	}
	return pool, nil
}

// ConnectPool builds a pool and opens [PoolOptions.MinConnections] warm
// connections. If any warm connection fails, the pool is closed and the error
// is returned.
func ConnectPool(ctx context.Context, opts PoolOptions) (*Pool, error) {
	pool, err := NewPool(opts)
	if err != nil {
		return nil, err
	}
	if err := pool.warm(ctx, opts.MinConnections); err != nil {
		_ = pool.Close()
		return nil, err
	}
	return pool, nil
}

func (p *Pool) warm(ctx context.Context, count int) error {
	warmed := make([]*Client, 0, count)
	for range count {
		client, err := NewClient(p.clientOpts)
		if err != nil {
			p.releaseWarm(warmed)
			return err
		}
		<-p.capacity
		if err := client.Connect(ctx); err != nil {
			_ = client.Close()
			p.capacity <- struct{}{}
			p.releaseWarm(warmed)
			return err
		}
		warmed = append(warmed, client)
	}
	for _, client := range warmed {
		p.idle <- client
	}
	return nil
}

func (p *Pool) releaseWarm(clients []*Client) {
	for _, client := range clients {
		p.closeClient(client)
	}
}

// Close shuts the pool down. It waits for leased clients to be returned, then
// closes every connection. A closed pool reports [ErrClosed].
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	for p.leased > 0 {
		p.cond.Wait()
	}
	p.mu.Unlock()

	// Every release has completed by now, so one drain collects both the
	// clients that were already idle and those just returned.
	var errs []error
	for {
		select {
		case client := <-p.idle:
			if err := client.Close(); err != nil {
				errs = append(errs, err)
			}
			p.capacity <- struct{}{}
		default:
			return errors.Join(errs...)
		}
	}
}

// WithClient leases one client for the duration of fn.
//
// The client is returned to the pool afterwards, or discarded when fn reports a
// transport failure ([ErrConnection], [ErrTimeout], or [ErrTLS]).
func (p *Pool) WithClient(ctx context.Context, fn func(context.Context, *Client) error) error {
	_, err := withPooledClient(ctx, p, func(ctx context.Context, client *Client) (struct{}, error) {
		return struct{}{}, fn(ctx, client)
	})
	return err
}

func withPooledClient[T any](ctx context.Context, p *Pool, fn func(context.Context, *Client) (T, error)) (value T, err error) {
	client, acquireErr := p.acquire(ctx)
	if acquireErr != nil {
		return value, acquireErr
	}

	// Deferred so a panic in fn still returns the lease; otherwise Close would
	// wait on it forever.
	defer func() { p.release(client, isTransportFailure(err)) }()
	return fn(ctx, client)
}

func (p *Pool) acquire(ctx context.Context) (*Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, closedError("")
	}
	p.leased++
	p.mu.Unlock()

	client, err := p.take(ctx)
	if err != nil {
		p.finishLease()
		return nil, err
	}

	// A pooled client may have lost its socket while idle; Connect restores it
	// and reports a server that has become unreachable.
	if err := client.Connect(ctx); err != nil {
		p.closeClient(client)
		p.finishLease()
		return nil, err
	}
	return client, nil
}

// take returns an idle client, or creates one while the pool is below its
// connection ceiling.
func (p *Pool) take(ctx context.Context) (*Client, error) {
	select {
	case client := <-p.idle:
		return client, nil
	default:
	}

	waitCtx := ctx
	if p.acquireTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, p.acquireTimeout)
		defer cancel()
	}

	select {
	case client := <-p.idle:
		return client, nil
	case <-p.capacity:
		client, err := NewClient(p.clientOpts)
		if err != nil {
			p.capacity <- struct{}{}
			return nil, err
		}
		return client, nil
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return nil, timeoutErrorf("ACQUIRE", ctx.Err(), "%s", ctx.Err())
		}
		return nil, timeoutErrorf("ACQUIRE", waitCtx.Err(), "pool acquire timeout after %s", p.acquireTimeout)
	}
}

func (p *Pool) release(client *Client, discard bool) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if discard || closed {
		p.closeClient(client)
	} else {
		select {
		case p.idle <- client:
		default:
			// idle is sized to MaxConnections, so this is unreachable in
			// practice; closing keeps the capacity accounting honest anyway.
			p.closeClient(client)
		}
	}
	p.finishLease()
}

func (p *Pool) finishLease() {
	p.mu.Lock()
	p.leased--
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *Pool) closeClient(client *Client) {
	_ = client.Close()
	p.capacity <- struct{}{}
}

func isTransportFailure(err error) bool {
	return errors.Is(err, ErrConnection) || errors.Is(err, ErrTimeout) || errors.Is(err, ErrTLS)
}

// Ping runs [Client.Ping] on a leased client.
func (p *Pool) Ping(ctx context.Context) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.Ping(ctx) })
}

// Info runs [Client.Info] on a leased client.
func (p *Pool) Info(ctx context.Context) (Info, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (Info, error) { return c.Info(ctx) })
}

// Get runs [Client.Get] on a leased client.
func (p *Pool) Get(ctx context.Context, x, y int64) (BlockState, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (BlockState, error) { return c.Get(ctx, x, y) })
}

// Set runs [Client.Set] on a leased client.
func (p *Pool) Set(ctx context.Context, x, y int64, bits string) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.Set(ctx, x, y, bits) })
}

// Unset runs [Client.Unset] on a leased client.
func (p *Pool) Unset(ctx context.Context, x, y int64) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.Unset(ctx, x, y) })
}

// MSet runs [Client.MSet] on a leased client.
func (p *Pool) MSet(ctx context.Context, blocks []Block) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.MSet(ctx, blocks) })
}

// MGet runs [Client.MGet] on a leased client.
func (p *Pool) MGet(ctx context.Context, blocks []BlockRef) ([]BlockState, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]BlockState, error) { return c.MGet(ctx, blocks) })
}

// ChunkExists runs [Client.ChunkExists] on a leased client.
func (p *Pool) ChunkExists(ctx context.Context, cx, cy int64) (bool, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (bool, error) { return c.ChunkExists(ctx, cx, cy) })
}

// GetChunk runs [Client.GetChunk] on a leased client.
func (p *Pool) GetChunk(ctx context.Context, cx, cy int64, opts GetOptions) ([]byte, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]byte, error) {
		return c.GetChunk(ctx, cx, cy, opts)
	})
}

// GetChunkState runs [Client.GetChunkState] on a leased client.
func (p *Pool) GetChunkState(ctx context.Context, cx, cy int64, opts GetOptions) (ChunkState, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (ChunkState, error) {
		return c.GetChunkState(ctx, cx, cy, opts)
	})
}

// PutChunk runs [Client.PutChunk] on a leased client.
func (p *Pool) PutChunk(ctx context.Context, cx, cy int64, payload []byte, opts PutOptions) (MutationResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (MutationResult, error) {
		return c.PutChunk(ctx, cx, cy, payload, opts)
	})
}

// PutChunkState runs [Client.PutChunkState] on a leased client.
func (p *Pool) PutChunkState(ctx context.Context, cx, cy int64, state ChunkStateInput, opts PutOptions) (MutationResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (MutationResult, error) {
		return c.PutChunkState(ctx, cx, cy, state, opts)
	})
}

// ChunkScan runs [Client.ChunkScan] on a leased client.
func (p *Pool) ChunkScan(ctx context.Context, limit int, cursor *CoordPair) (ScanResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (ScanResult, error) {
		return c.ChunkScan(ctx, limit, cursor)
	})
}

// ChunkRange runs [Client.ChunkRange] on a leased client.
func (p *Pool) ChunkRange(ctx context.Context, cx0, cy0, cx1, cy1 int64, opts GetOptions) ([]RangeEntry, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]RangeEntry, error) {
		return c.ChunkRange(ctx, cx0, cy0, cx1, cy1, opts)
	})
}

// ChunkRadius runs [Client.ChunkRadius] on a leased client.
func (p *Pool) ChunkRadius(ctx context.Context, cx, cy int64, radiusChunks int, opts GetOptions) ([]RangeEntry, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]RangeEntry, error) {
		return c.ChunkRadius(ctx, cx, cy, radiusChunks, opts)
	})
}

// ChunkVersion runs [Client.ChunkVersion] on a leased client.
func (p *Pool) ChunkVersion(ctx context.Context, cx, cy int64) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) { return c.ChunkVersion(ctx, cx, cy) })
}

// ChunkBatch runs [Client.ChunkBatch] on a leased client.
func (p *Pool) ChunkBatch(ctx context.Context, cx, cy int64, operations []BatchOperation) (MutationResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (MutationResult, error) {
		return c.ChunkBatch(ctx, cx, cy, operations)
	})
}

// ChunkBatchIfVersion runs [Client.ChunkBatchIfVersion] on a leased client.
func (p *Pool) ChunkBatchIfVersion(ctx context.Context, cx, cy int64, expectedVersion uint64, operations []BatchOperation) (MutationResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (MutationResult, error) {
		return c.ChunkBatchIfVersion(ctx, cx, cy, expectedVersion, operations)
	})
}

// WALFlush runs [Client.WALFlush] on a leased client.
func (p *Pool) WALFlush(ctx context.Context) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.WALFlush(ctx) })
}

// Metrics runs [Client.Metrics] on a leased client.
func (p *Pool) Metrics(ctx context.Context) (string, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (string, error) { return c.Metrics(ctx) })
}
