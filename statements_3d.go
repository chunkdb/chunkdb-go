package chunkdb

import "context"

// ChunkCoord3D addresses a chunk in a three-dimensional table.
type ChunkCoord3D struct{ CX, CY, CZ int64 }

// AreaChunk3D contains one populated chunk of a three-dimensional area.
type AreaChunk3D struct {
	CX, CY, CZ int64
	Chunk      *Chunk
}

// AreaRead3D contains area chunks and the schema version used for their forms.
type AreaRead3D struct {
	Chunks        []AreaChunk3D
	SchemaVersion uint64
}

// GetBlock3D reads one block with explicit three-axis coordinates.
func (c *Client) GetBlock3D(ctx context.Context, table string, x, y, z int64, columns ...string) (Record, error) {
	return getBlock(ctx, c, table, x, y, columns, z)
}

// GetBlock3DWithOptions reads one three-dimensional block and selected metadata.
func (c *Client) GetBlock3DWithOptions(ctx context.Context, table string, x, y, z int64, opts ReadOptions, columns ...string) (BlockRead, error) {
	return getBlockWithOptions(ctx, c, table, x, y, opts, columns, z)
}

// GetBlock3DWithMetadata reads one block with its chunk and schema versions.
func (c *Client) GetBlock3DWithMetadata(ctx context.Context, table string, x, y, z int64, columns ...string) (BlockRead, error) {
	return c.GetBlock3DWithOptions(ctx, table, x, y, z, ReadOptions{WithVersion: true, WithSchema: true}, columns...)
}

// SetBlock3D updates one block and returns its producing chunk version.
func (c *Client) SetBlock3D(ctx context.Context, table string, x, y, z int64, values Record, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET BLOCK", opts, false)
	if err != nil {
		return 0, err
	}
	reply, err := setBlock(ctx, c, table, x, y, values, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET BLOCK")
}

// DeleteBlock3D removes one block and returns its producing chunk version.
func (c *Client) DeleteBlock3D(ctx context.Context, table string, x, y, z int64, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("DELETE BLOCK", opts, false)
	if err != nil {
		return 0, err
	}
	reply, err := deleteBlock(ctx, c, table, x, y, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "DELETE BLOCK")
}

// GetChunk3D reads a chunk and its complete chunk-column row.
func (c *Client) GetChunk3D(ctx context.Context, table string, x, y, z int64, columns ...string) (*Chunk, error) {
	return getChunk(ctx, c, table, x, y, columns, z)
}

// GetChunkRaw3D reads an opaque chunk form at explicit three-axis coordinates.
func (c *Client) GetChunkRaw3D(ctx context.Context, table string, x, y, z int64, columns ...string) ([]byte, error) {
	return getChunkRaw(ctx, c, table, x, y, columns, z)
}

// SetChunk3D replaces a chunk's block values and complete metadata row.
func (c *Client) SetChunk3D(ctx context.Context, table string, x, y, z int64, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunk(ctx, c, table, x, y, chunk, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

// SetChunkRaw3D replaces a chunk with a complete opaque form.
func (c *Client) SetChunkRaw3D(ctx context.Context, table string, x, y, z int64, form []byte, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunkRaw(ctx, c, table, x, y, form, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

// GetArea3D reads an inclusive box of three-dimensional chunk coordinates.
func (c *Client) GetArea3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) ([]AreaChunk3D, error) {
	result, err := getArea3D(ctx, c, table, areaRange3D(x0, y0, z0, x1, y1, z1), false, columns)
	return result.Chunks, err
}

// GetArea3DWithSchema reads a box with the schema version used by its forms.
func (c *Client) GetArea3DWithSchema(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, columns ...string) (AreaRead3D, error) {
	return getArea3D(ctx, c, table, areaRange3D(x0, y0, z0, x1, y1, z1), true, columns)
}

// GetAreaAround3D reads the ball of chunk coordinates within radius of a center.
func (c *Client) GetAreaAround3D(ctx context.Context, table string, x, y, z, radius int64, columns ...string) ([]AreaChunk3D, error) {
	area, err := areaAround3D(x, y, z, radius)
	if err != nil {
		return nil, err
	}
	result, err := getArea3D(ctx, c, table, area, false, columns)
	return result.Chunks, err
}

func areaRange3D(x0, y0, z0, x1, y1, z1 int64) string {
	return coordinatePoint(x0, y0, z0) + " TO " + coordinatePoint(x1, y1, z1)
}

func areaAround3D(x, y, z, radius int64) (string, error) {
	if radius < 0 {
		return "", requestErrorf("GET AREA", "the radius must not be negative")
	}
	return "AROUND " + coordinatePoint(x, y, z) + " RADIUS " + coord(radius), nil
}

func getArea3D(ctx context.Context, s session, table, area string, withSchemaVersion bool, columns []string) (AreaRead3D, error) {
	const command = "GET AREA"
	name, err := s.tableName(command, table)
	if err != nil {
		return AreaRead3D{}, err
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return AreaRead3D{}, err
	}
	result, err := withSchema(ctx, s, name, func(schema *Schema) (AreaRead3D, error) {
		if schema.Dims != 3 {
			return AreaRead3D{}, requestErrorf(command, "table %s is not three-dimensional", name)
		}
		indexes, err := formColumns(schema, columns)
		if err != nil {
			return AreaRead3D{}, &staleSchemaError{err: err, local: true}
		}
		names := columnNames(schema, indexes)
		clause := ""
		if withSchemaVersion {
			clause = " WITH SCHEMA"
		}
		var responseConnection *conn
		reply, err := s.call(ctx, "GET AREA "+area+" FROM "+name+columnsClause(names)+clause, nil, areaReplyOptions{knownShape: true, withSchema: withSchemaVersion, connection: &responseConnection})
		if err != nil {
			if isNoColumn(err) {
				return AreaRead3D{}, &staleSchemaError{err: err}
			}
			return AreaRead3D{}, err
		}
		return decodeAreaResponse(responseConnection, func() (AreaRead3D, error) {
			var result AreaRead3D
			if withSchemaVersion {
				if reply.Kind != ReplyArray || len(reply.Array) < 2 {
					return AreaRead3D{}, protocolErrorf(command, "invalid schema envelope")
				}
				version, ok := reply.Array[0].Uint64()
				if !ok {
					return AreaRead3D{}, protocolErrorf(command, "invalid schema version")
				}
				if version != schema.Version {
					return AreaRead3D{}, &staleSchemaError{err: protocolErrorf(command, "schema version changed during read")}
				}
				result.SchemaVersion = version
				reply = reply.Array[1]
			}
			if reply.Kind != ReplyArray {
				return AreaRead3D{}, protocolErrorf(command, "expected an array of chunks")
			}
			if err := checkAreaDecodedCells(schema, indexes, len(reply.Array)); err != nil {
				return AreaRead3D{}, err
			}
			result.Chunks = make([]AreaChunk3D, 0, len(reply.Array))
			for _, item := range reply.Array {
				if item.Kind != ReplyArray || len(item.Array) < 4 {
					return AreaRead3D{}, protocolErrorf(command, "expected [cx, cy, cz, chunk] entries")
				}
				x, okX := item.Array[0].Int64()
				y, okY := item.Array[1].Int64()
				z, okZ := item.Array[2].Int64()
				form, err := expectBulk(item.Array[3], command)
				if !okX || !okY || !okZ || err != nil {
					return AreaRead3D{}, protocolErrorf(command, "expected [cx, cy, cz, chunk] entries")
				}
				chunk, err := decodeAreaChunk(schema, form, names)
				if err != nil {
					return AreaRead3D{}, err
				}
				result.Chunks = append(result.Chunks, AreaChunk3D{CX: x, CY: y, CZ: z, Chunk: chunk})
			}
			return result, nil
		})
	})
	return result, annotate(command, err)
}
