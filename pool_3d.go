package chunkdb

import "context"

// GetBlock3D reads a three-dimensional block on a leased client.
func (p *Pool) GetBlock3D(ctx context.Context, table string, x, y, z int64, columns ...string) (Record, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (Record, error) {
		return c.GetBlock3D(ctx, table, x, y, z, columns...)
	})
}

// GetBlock3DWithOptions reads a block and selected metadata on a leased client.
func (p *Pool) GetBlock3DWithOptions(ctx context.Context, table string, x, y, z int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (BlockRead, error) {
		return c.GetBlock3DWithOptions(ctx, table, x, y, z, opts, columns...)
	})
}

// GetBlock3DWithMetadata reads a block and both versions on a leased client.
func (p *Pool) GetBlock3DWithMetadata(ctx context.Context, table string, x, y, z int64, columns ...string) (BlockRead, error) {
	return p.GetBlock3DWithOptions(ctx, table, x, y, z, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// SetBlock3D updates a three-dimensional block on a leased client.
func (p *Pool) SetBlock3D(ctx context.Context, table string, x, y, z int64, values Record, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetBlock3D(ctx, table, x, y, z, values, opts...)
	})
}

// DeleteBlock3D removes one three-dimensional block on a leased client.
func (p *Pool) DeleteBlock3D(ctx context.Context, table string, x, y, z int64, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.DeleteBlock3D(ctx, table, x, y, z, opts...)
	})
}

// GetChunk3D reads a three-dimensional chunk on a leased client.
func (p *Pool) GetChunk3D(ctx context.Context, table string, x, y, z int64, columns ...string) (*Chunk, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (*Chunk, error) {
		return c.GetChunk3D(ctx, table, x, y, z, columns...)
	})
}

// GetChunkRaw3D reads an opaque three-dimensional chunk on a leased client.
func (p *Pool) GetChunkRaw3D(ctx context.Context, table string, x, y, z int64, columns ...string) ([]byte, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) ([]byte, error) {
		return c.GetChunkRaw3D(ctx, table, x, y, z, columns...)
	})
}

// SetChunk3D replaces a three-dimensional chunk on a leased client.
func (p *Pool) SetChunk3D(ctx context.Context, table string, x, y, z int64, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunk3D(ctx, table, x, y, z, chunk, opts...)
	})
}

// SetChunkRaw3D replaces an opaque three-dimensional chunk on a leased client.
func (p *Pool) SetChunkRaw3D(ctx context.Context, table string, x, y, z int64, form []byte, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkRaw3D(ctx, table, x, y, z, form, opts...)
	})
}

// GetArea3D reads an inclusive three-dimensional box on a leased client.
func (p *Pool) GetArea3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) ([]AreaChunk3D, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) ([]AreaChunk3D, error) {
		return c.GetArea3D(ctx, table, x0, y0, z0, x1, y1, z1, columns...)
	})
}

// GetArea3DWithSchema reads a box and its schema version on a leased client.
func (p *Pool) GetArea3DWithSchema(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) (AreaRead3D, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaRead3D, error) {
		return c.GetArea3DWithSchema(ctx, table, x0, y0, z0, x1, y1, z1, columns...)
	})
}

// GetAreaAround3D reads a three-dimensional ball on a leased client.
func (p *Pool) GetAreaAround3D(ctx context.Context, table string, x, y, z, radius int64, columns ...string) ([]AreaChunk3D, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) ([]AreaChunk3D, error) {
		return c.GetAreaAround3D(ctx, table, x, y, z, radius, columns...)
	})
}
