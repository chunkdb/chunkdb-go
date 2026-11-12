package chunkdb

import (
	"context"
	"iter"
	"strconv"
	"strings"
)

// ScanOptions selects a two-dimensional page and optional retained changes.
type ScanOptions struct {
	After    *ChunkCoord
	Limit    int
	Since    *uint64
	WithData bool
	Columns  []string
	// Epoch binds a resumed scan to a previously returned table lifetime.
	Epoch string
}

// ScanOptions3D selects a three-dimensional page and optional retained changes.
type ScanOptions3D struct {
	After    *ChunkCoord3D
	Limit    int
	Since    *uint64
	WithData bool
	Columns  []string
	Epoch    string
}

// ScannedChunk contains a retained version and optional decoded data.
// Deleted is nil without WITH DATA and otherwise identifies a tombstone.
type ScannedChunk struct {
	CX, CY  int64
	Version uint64
	Chunk   *Chunk
	Deleted *bool
}

// ScannedChunk3D contains a retained three-dimensional chunk identity.
type ScannedChunk3D struct {
	CX, CY, CZ int64
	Version    uint64
	Chunk      *Chunk
	Deleted    *bool
}

// ChunkScanPage includes the table lifetime and completed frontier of a page.
// Position is nil when a read-only server cannot establish its frontier.
type ChunkScanPage struct {
	Chunks                []ScannedChunk
	More                  bool
	Epoch                 string
	Position              *uint64
	RetainedDeletionFloor uint64
}

// ChunkScanPage3D is a page of retained three-dimensional chunk identities.
type ChunkScanPage3D struct {
	Chunks                []ScannedChunk3D
	More                  bool
	Epoch                 string
	Position              *uint64
	RetainedDeletionFloor uint64
}

// ScanPage3D is a page of populated three-dimensional chunk coordinates.
type ScanPage3D struct {
	Chunks []ChunkCoord3D
	More   bool
}

// ScanChunksWithOptions reads a two-dimensional page with retained versions.
// WITH DATA keeps complete chunk metadata even with a block-column projection.
func (c *Client) ScanChunksWithOptions(ctx context.Context, table string, opts ScanOptions) (ChunkScanPage, error) {
	var after []int64
	if opts.After != nil {
		after = []int64{opts.After.CX, opts.After.CY}
	}
	page, err := scanChunksChecked(ctx, c, table, 2, after, opts.Limit, opts.Since, opts.WithData, opts.Columns, opts.Epoch)
	if err != nil {
		return ChunkScanPage{}, err
	}
	result := ChunkScanPage{More: page.More, Epoch: page.Epoch, Position: page.Position, RetainedDeletionFloor: page.RetainedDeletionFloor,
		Chunks: make([]ScannedChunk, 0, len(page.Chunks))}
	for _, chunk := range page.Chunks {
		result.Chunks = append(result.Chunks, ScannedChunk{CX: chunk.CX, CY: chunk.CY, Version: chunk.Version, Chunk: chunk.Chunk, Deleted: chunk.Deleted})
	}
	return result, nil
}

// ScanChunks3DWithOptions reads a three-dimensional page with retained versions.
func (c *Client) ScanChunks3DWithOptions(ctx context.Context, table string, opts ScanOptions3D) (ChunkScanPage3D, error) {
	var after []int64
	if opts.After != nil {
		after = []int64{opts.After.CX, opts.After.CY, opts.After.CZ}
	}
	return scanChunksChecked(ctx, c, table, 3, after, opts.Limit, opts.Since, opts.WithData, opts.Columns, opts.Epoch)
}

// ScanChunks3D lists populated chunks in ascending x, then y, then z order.
func (c *Client) ScanChunks3D(ctx context.Context, table string, after *ChunkCoord3D, limit int) (ScanPage3D, error) {
	page, err := c.ScanChunks3DWithOptions(ctx, table, ScanOptions3D{After: after, Limit: limit})
	if err != nil {
		return ScanPage3D{}, err
	}
	result := ScanPage3D{More: page.More, Chunks: make([]ChunkCoord3D, 0, len(page.Chunks))}
	for _, chunk := range page.Chunks {
		result.Chunks = append(result.Chunks, ChunkCoord3D{CX: chunk.CX, CY: chunk.CY, CZ: chunk.CZ})
	}
	return result, nil
}

func scanChunksChecked(ctx context.Context, s session, table string, dims int, after []int64, limit int, since *uint64, data bool, columns []string, expectedEpoch string) (ChunkScanPage3D, error) {
	const command = "SCAN CHUNKS"
	name, err := s.tableName(command, table)
	if err != nil {
		return ChunkScanPage3D{}, err
	}
	if limit < 0 || (len(columns) != 0 && !data) {
		return ChunkScanPage3D{}, requestErrorf(command, "use a nonnegative limit and WITH DATA for column projections")
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return ChunkScanPage3D{}, err
	}
	if expectedEpoch != "" && !validEpoch(expectedEpoch) {
		return ChunkScanPage3D{}, requestErrorf(command, "invalid table epoch")
	}
	expectedEpoch = strings.ToLower(expectedEpoch)
	page, err := withSchema(ctx, s, name, func(schema *Schema) (ChunkScanPage3D, error) {
		if schema.Dims != dims {
			return ChunkScanPage3D{}, requestErrorf(command, "table %s needs %d coordinates", name, schema.Dims)
		}
		if expectedEpoch != "" && strings.ToLower(schema.Epoch) != expectedEpoch {
			return ChunkScanPage3D{}, requestErrorf(command, "scan position belongs to another table epoch")
		}
		if _, err := formColumns(schema, columns); err != nil {
			return ChunkScanPage3D{}, &staleSchemaError{err: err, local: true}
		}
		statement := "SCAN CHUNKS FROM " + name
		if after != nil {
			statement += " AFTER"
			for _, axis := range after {
				statement += " " + coord(axis)
			}
		}
		if since != nil {
			statement += " SINCE " + strconv.FormatUint(*since, 10)
		}
		if limit > 0 {
			statement += " LIMIT " + strconv.Itoa(limit)
		}
		if data {
			statement += " WITH DATA" + columnsClause(columns)
		}
		reply, err := s.call(ctx, statement, nil)
		if err != nil {
			return ChunkScanPage3D{}, err
		}
		return decodeScanPage(schema, reply, dims, after, limit, since, data, columns, expectedEpoch)
	})
	return page, annotate(command, err)
}

func decodeScanPage(schema *Schema, reply Reply, dims int, after []int64, limit int, since *uint64, data bool, columns []string, expectedEpoch string) (ChunkScanPage3D, error) {
	bad := func(what string) (ChunkScanPage3D, error) {
		return ChunkScanPage3D{}, protocolErrorf("SCAN CHUNKS", "%s", what)
	}
	chunks, haveChunks := reply.Lookup("chunks")
	more, haveMore := reply.Lookup("more")
	epoch, haveEpoch := reply.Lookup("epoch")
	position, havePosition := reply.Lookup("position")
	floor, haveFloor := reply.Lookup("retained_deletion_floor")
	if reply.Kind != ReplyMap || !haveChunks || chunks.Kind != ReplyArray || !haveMore || more.Kind != ReplyBoolean || !haveEpoch || epoch.Kind != ReplyBulk || !validEpoch(string(epoch.Bulk)) || !havePosition || !haveFloor {
		return bad("expected chunks, more, epoch, position and retained_deletion_floor")
	}
	page := ChunkScanPage3D{More: more.Bool, Epoch: strings.ToLower(string(epoch.Bulk)), Chunks: make([]ScannedChunk3D, 0, len(chunks.Array))}
	if (expectedEpoch != "" && page.Epoch != expectedEpoch) || (schema.Epoch != "" && page.Epoch != strings.ToLower(schema.Epoch)) {
		return bad("table epoch changed during scan")
	}
	var ok bool
	page.RetainedDeletionFloor, ok = floor.Uint64()
	if !ok {
		return bad("invalid retained deletion floor")
	}
	if since != nil && *since < page.RetainedDeletionFloor {
		return bad("scan history precedes the retained deletion floor")
	}
	if position.Kind != ReplyNull {
		value, valid := position.Uint64()
		if !valid {
			return bad("invalid completed frontier")
		}
		page.Position = &value
	}
	if limit > 0 && len(chunks.Array) > limit {
		return bad("scan page exceeds the requested limit")
	}
	previous := after
	for _, item := range chunks.Array {
		fields := dims + 1
		if data {
			fields++
		}
		if item.Kind != ReplyArray || len(item.Array) < fields {
			return bad("invalid scan entry arity")
		}
		point := make([]int64, dims)
		for i := range point {
			point[i], ok = item.Array[i].Int64()
			if !ok {
				return bad("invalid scan coordinate")
			}
		}
		if previous != nil && !scanPointLess(previous, point) {
			return bad("scan coordinates do not advance in order")
		}
		previous = point
		version, valid := item.Array[dims].Uint64()
		if !valid || version == 0 || (since != nil && version <= *since) {
			return bad("invalid retained chunk version")
		}
		entry := ScannedChunk3D{CX: point[0], CY: point[1], Version: version}
		if dims == 3 {
			entry.CZ = point[2]
		}
		if data {
			deleted := item.Array[dims+1].Kind == ReplyNull
			entry.Deleted = &deleted
			if deleted {
				if since == nil {
					return bad("ordinary scan contains a tombstone")
				}
			} else {
				form, err := expectBulk(item.Array[dims+1], "SCAN CHUNKS")
				if err != nil {
					return ChunkScanPage3D{}, err
				}
				entry.Chunk, err = decodeSectionedChunk(schema, form, columns...)
				if err != nil {
					return ChunkScanPage3D{}, &staleSchemaError{err: err}
				}
				if entry.Chunk.Version != version {
					return bad("chunk form and scan version differ")
				}
			}
		}
		page.Chunks = append(page.Chunks, entry)
	}
	if page.More && len(page.Chunks) == 0 {
		return bad("empty page reports more")
	}
	return page, nil
}

func scanPointLess(left, right []int64) bool {
	for i := range left {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return false
}

// ScanChunksWithOptions reads a two-dimensional page on a leased client.
func (p *Pool) ScanChunksWithOptions(ctx context.Context, table string, opts ScanOptions) (ChunkScanPage, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (ChunkScanPage, error) {
		return c.ScanChunksWithOptions(ctx, table, opts)
	})
}

// ScanChunks3DWithOptions reads a three-dimensional page on a leased client.
func (p *Pool) ScanChunks3DWithOptions(ctx context.Context, table string, opts ScanOptions3D) (ChunkScanPage3D, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (ChunkScanPage3D, error) {
		return c.ScanChunks3DWithOptions(ctx, table, opts)
	})
}

// ScanChunks3D lists populated three-dimensional chunks on a leased client.
func (p *Pool) ScanChunks3D(ctx context.Context, table string, after *ChunkCoord3D, limit int) (ScanPage3D, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (ScanPage3D, error) {
		return c.ScanChunks3D(ctx, table, after, limit)
	})
}

func allChunks3D(ctx context.Context, table string, limit int, scan func(context.Context, string, ScanOptions3D) (ChunkScanPage3D, error)) iter.Seq2[ChunkCoord3D, error] {
	return func(yield func(ChunkCoord3D, error) bool) {
		options := ScanOptions3D{Limit: limit}
		for {
			page, err := scan(ctx, table, options)
			if err != nil {
				yield(ChunkCoord3D{}, err)
				return
			}
			if options.Epoch == "" {
				options.Epoch = page.Epoch
			}
			for _, chunk := range page.Chunks {
				point := ChunkCoord3D{CX: chunk.CX, CY: chunk.CY, CZ: chunk.CZ}
				if !yield(point, nil) {
					return
				}
				options.After = &point
			}
			if !page.More {
				return
			}
		}
	}
}

// AllChunks3D walks every populated chunk while retaining one table lifetime.
func (c *Client) AllChunks3D(ctx context.Context, table string, limit int) iter.Seq2[ChunkCoord3D, error] {
	return allChunks3D(ctx, table, limit, c.ScanChunks3DWithOptions)
}

// AllChunks3D walks every three-dimensional page using leased clients.
func (p *Pool) AllChunks3D(ctx context.Context, table string, limit int) iter.Seq2[ChunkCoord3D, error] {
	return allChunks3D(ctx, table, limit, p.ScanChunks3DWithOptions)
}
