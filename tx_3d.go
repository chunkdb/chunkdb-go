package chunkdb

import "context"

// GetBlock3D reads a three-dimensional block from the transaction snapshot.
func (tx *Tx) GetBlock3D(ctx context.Context, table string, x, y, z int64, columns ...string) (Record, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getBlock(ctx, tx, table, x, y, columns, z)
}

// GetBlock3DWithOptions reads a snapshot block with selected version metadata.
func (tx *Tx) GetBlock3DWithOptions(ctx context.Context, table string, x, y, z int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getBlockWithOptions(ctx, tx, table, x, y, opts, columns, z)
}

// GetBlock3DWithMetadata reads a snapshot block and both version fields.
func (tx *Tx) GetBlock3DWithMetadata(ctx context.Context, table string, x, y, z int64, columns ...string) (BlockRead, error) {
	return tx.GetBlock3DWithOptions(ctx, table, x, y, z, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// SetBlock3D stages a three-dimensional block update for COMMIT.
func (tx *Tx) SetBlock3D(ctx context.Context, table string, x, y, z int64, values Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setBlock(ctx, tx, table, x, y, values, "", z)
	return expectNoVersion(reply, err, "SET BLOCK")
}

// DeleteBlock3D stages removal of one three-dimensional block.
func (tx *Tx) DeleteBlock3D(ctx context.Context, table string, x, y, z int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := deleteBlock(ctx, tx, table, x, y, "", z)
	return expectNoVersion(reply, err, "DELETE BLOCK")
}

// GetChunk3D reads one three-dimensional chunk from the snapshot.
func (tx *Tx) GetChunk3D(ctx context.Context, table string, x, y, z int64, columns ...string) (*Chunk, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getChunk(ctx, tx, table, x, y, columns, z)
}

// GetChunkRaw3D reads one snapshot chunk as an opaque form.
func (tx *Tx) GetChunkRaw3D(ctx context.Context, table string, x, y, z int64, columns ...string) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getChunkRaw(ctx, tx, table, x, y, columns, z)
}

// SetChunk3D stages a complete chunk replacement for COMMIT.
func (tx *Tx) SetChunk3D(ctx context.Context, table string, x, y, z int64, chunk *Chunk) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunk(ctx, tx, table, x, y, chunk, "", z)
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkRaw3D stages a complete opaque chunk form for COMMIT.
func (tx *Tx) SetChunkRaw3D(ctx context.Context, table string, x, y, z int64, form []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkRaw(ctx, tx, table, x, y, form, "", z)
	return expectNoVersion(reply, err, "SET CHUNK")
}

// GetArea3D reads an inclusive chunk box from the transaction snapshot.
func (tx *Tx) GetArea3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) ([]AreaChunk3D, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	result, err := getArea3D(ctx, tx, table, areaRange3D(x0, y0, z0, x1, y1, z1), false, columns)
	return result.Chunks, err
}

// GetArea3DWithSchema reads a snapshot box and its schema version.
func (tx *Tx) GetArea3DWithSchema(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) (AreaRead3D, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getArea3D(ctx, tx, table, areaRange3D(x0, y0, z0, x1, y1, z1), true, columns)
}

// GetAreaAround3D reads a ball of chunk coordinates from the snapshot.
func (tx *Tx) GetAreaAround3D(ctx context.Context, table string, x, y, z, radius int64, columns ...string) ([]AreaChunk3D, error) {
	area, err := areaAround3D(x, y, z, radius)
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	result, err := getArea3D(ctx, tx, table, area, false, columns)
	return result.Chunks, err
}
