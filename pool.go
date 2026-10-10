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
// request multiplexing, so parallelism comes from multiple sockets. Each
// client keeps its own schema cache. A Pool is safe for concurrent use.
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

// Transaction runs [Client.Transaction] on a leased client, which serves no
// other operation until the transaction ends.
func (p *Pool) Transaction(ctx context.Context, fn func(tx *Tx) error, opts ...TxOption) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) {
		return c.Transaction(ctx, fn, opts...)
	})
}

// Do runs [Client.Do] on a leased client.
func (p *Pool) Do(ctx context.Context, statement string, params ...[]byte) (Reply, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (Reply, error) {
		return c.Do(ctx, statement, params...)
	})
}

// Ping runs [Client.Ping] on a leased client.
func (p *Pool) Ping(ctx context.Context) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.Ping(ctx) })
}

// GetBlock runs [Client.GetBlock] on a leased client.
func (p *Pool) GetBlock(ctx context.Context, table string, x, y int64, columns ...string) (Record, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (Record, error) {
		return c.GetBlock(ctx, table, x, y, columns...)
	})
}

// SetBlock runs [Client.SetBlock] on a leased client.
func (p *Pool) SetBlock(ctx context.Context, table string, x, y int64, values Record, opts ...WriteOption) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetBlock(ctx, table, x, y, values, opts...)
	})
}

// DeleteBlock runs [Client.DeleteBlock] on a leased client.
func (p *Pool) DeleteBlock(ctx context.Context, table string, x, y int64, opts ...WriteOption) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) {
		return c.DeleteBlock(ctx, table, x, y, opts...)
	})
}

// GetChunk runs [Client.GetChunk] on a leased client.
func (p *Pool) GetChunk(ctx context.Context, table string, cx, cy int64, columns ...string) (*Chunk, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (*Chunk, error) {
		return c.GetChunk(ctx, table, cx, cy, columns...)
	})
}

// GetChunkRaw runs [Client.GetChunkRaw] on a leased client.
func (p *Pool) GetChunkRaw(ctx context.Context, table string, cx, cy int64, columns ...string) ([]byte, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]byte, error) {
		return c.GetChunkRaw(ctx, table, cx, cy, columns...)
	})
}

// SetChunk runs [Client.SetChunk] on a leased client.
func (p *Pool) SetChunk(ctx context.Context, table string, cx, cy int64, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunk(ctx, table, cx, cy, chunk, opts...)
	})
}

// SetChunkRaw runs [Client.SetChunkRaw] on a leased client.
func (p *Pool) SetChunkRaw(ctx context.Context, table string, cx, cy int64, form []byte, opts ...WriteOption) (uint64, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkRaw(ctx, table, cx, cy, form, opts...)
	})
}

// GetArea runs [Client.GetArea] on a leased client.
func (p *Pool) GetArea(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) ([]AreaChunk, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]AreaChunk, error) {
		return c.GetArea(ctx, table, cx0, cy0, cx1, cy1, columns...)
	})
}

// GetAreaAround runs [Client.GetAreaAround] on a leased client.
func (p *Pool) GetAreaAround(ctx context.Context, table string, cx, cy, radius int64, columns ...string) ([]AreaChunk, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]AreaChunk, error) {
		return c.GetAreaAround(ctx, table, cx, cy, radius, columns...)
	})
}

// ScanChunks runs [Client.ScanChunks] on a leased client.
func (p *Pool) ScanChunks(ctx context.Context, table string, after *ChunkCoord, limit int) (ScanPage, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (ScanPage, error) {
		return c.ScanChunks(ctx, table, after, limit)
	})
}

// Describe runs [Client.Describe] on a leased client.
func (p *Pool) Describe(ctx context.Context, table string) (*Schema, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (*Schema, error) { return c.Describe(ctx, table) })
}

// Tables runs [Client.Tables] on a leased client.
func (p *Pool) Tables(ctx context.Context) ([]string, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]string, error) { return c.Tables(ctx) })
}

// FlushWAL runs [Client.FlushWAL] on a leased client.
func (p *Pool) FlushWAL(ctx context.Context) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.FlushWAL(ctx) })
}

// Metrics runs [Client.Metrics] on a leased client.
func (p *Pool) Metrics(ctx context.Context) (string, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (string, error) { return c.Metrics(ctx) })
}
