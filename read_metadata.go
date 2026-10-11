package chunkdb

import "context"

// ReadOptions selects metadata captured with a block's row.
type ReadOptions struct{ WithVersion, WithSchema bool }

func (o ReadOptions) clauses() string {
	clause := ""
	if o.WithVersion {
		clause += " WITH VERSION"
	}
	if o.WithSchema {
		clause += " WITH SCHEMA"
	}
	return clause
}

// BlockRead holds a row and its requested metadata. Record is nil for an
// absent block. Version is nil for a chunk without any retained commit or a transaction-private
// modified chunk; an absent block with a retained commit still has its version.
type BlockRead struct {
	Record        Record
	Version       *uint64
	SchemaVersion uint64
}

// AreaRead holds area chunks and their schema version, including an empty area.
type AreaRead struct {
	Chunks        []AreaChunk
	SchemaVersion uint64
}

func parseBlockEnvelope(reply Reply, opts ReadOptions) (BlockRead, Reply, error) {
	var result BlockRead
	if !opts.WithVersion && !opts.WithSchema {
		return result, reply, nil
	}
	fields := 1
	if opts.WithVersion {
		fields++
	}
	if opts.WithSchema {
		fields++
	}
	bad := func() (BlockRead, Reply, error) {
		return BlockRead{}, Reply{}, protocolErrorf("GET BLOCK", "invalid metadata envelope")
	}
	if reply.Kind != ReplyArray || len(reply.Array) < fields {
		return bad()
	}
	index := 0
	if opts.WithVersion {
		if reply.Array[index].Kind != ReplyNull {
			n, ok := reply.Array[index].Uint64()
			if !ok {
				return bad()
			}
			result.Version = &n
		}
		index++
	}
	if opts.WithSchema {
		n, ok := reply.Array[index].Uint64()
		if !ok {
			return bad()
		}
		result.SchemaVersion = n
		index++
	}
	return result, reply.Array[index], nil
}

// GetBlockWithOptions reads a block with individually selected metadata.
func (c *Client) GetBlockWithOptions(ctx context.Context, table string, x, y int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	return getBlockWithOptions(ctx, c, table, x, y, opts, columns)
}

// GetBlockWithMetadata reads a block with both chunk and schema versions.
func (c *Client) GetBlockWithMetadata(ctx context.Context, table string, x, y int64, columns ...string) (BlockRead, error) {
	return c.GetBlockWithOptions(ctx, table, x, y, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// GetBlockWithOptions reads snapshot or private rows with selected metadata.
func (tx *Tx) GetBlockWithOptions(ctx context.Context, table string, x, y int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getBlockWithOptions(ctx, tx, table, x, y, opts, columns)
}

// GetBlockWithMetadata reads a transaction's row and both versions.
func (tx *Tx) GetBlockWithMetadata(ctx context.Context, table string, x, y int64, columns ...string) (BlockRead, error) {
	return tx.GetBlockWithOptions(ctx, table, x, y, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// GetBlockWithOptions runs Client.GetBlockWithOptions on a leased client.
func (p *Pool) GetBlockWithOptions(ctx context.Context, table string, x, y int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (BlockRead, error) {
		return c.GetBlockWithOptions(ctx, table, x, y, opts, columns...)
	})
}

// GetBlockWithMetadata runs Client.GetBlockWithMetadata on a leased client.
func (p *Pool) GetBlockWithMetadata(ctx context.Context, table string, x, y int64, columns ...string) (BlockRead, error) {
	return p.GetBlockWithOptions(ctx, table, x, y, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// GetAreaWithSchema reads an area and the schema version used for its forms.
func (c *Client) GetAreaWithSchema(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) (AreaRead, error) {
	return getAreaWithSchema(ctx, c, table, areaRange(cx0, cy0, cx1, cy1), true, columns)
}

// GetAreaWithSchema reads transaction snapshot or private chunks and schema.
func (tx *Tx) GetAreaWithSchema(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) (AreaRead, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getAreaWithSchema(ctx, tx, table, areaRange(cx0, cy0, cx1, cy1), true, columns)
}

// GetAreaWithSchema runs Client.GetAreaWithSchema on a leased client.
func (p *Pool) GetAreaWithSchema(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) (AreaRead, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaRead, error) {
		return c.GetAreaWithSchema(ctx, table, cx0, cy0, cx1, cy1, columns...)
	})
}
