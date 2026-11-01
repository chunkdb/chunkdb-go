package chunkdb

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// SetChunkColumns replaces selected block columns and preserves chunk metadata.
// Presence comes from chunk, including removal of absent blocks.
func (c *Client) SetChunkColumns(ctx context.Context, table string, x, y int64, columns []string, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunkColumns(ctx, c, table, x, y, columns, chunk, clause)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

// SetChunkColumns3D replaces selected block columns at three-axis coordinates.
func (c *Client) SetChunkColumns3D(ctx context.Context, table string, x, y, z int64, columns []string, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunkColumns(ctx, c, table, x, y, columns, chunk, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

func setChunkColumns(ctx context.Context, s session, table string, x, y int64, columns []string, chunk *Chunk, clause string, z ...int64) (Reply, error) {
	const command = "SET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	if len(columns) == 0 {
		return Reply{}, requestErrorf(command, "a block-column mutation needs columns")
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return Reply{}, err
	}
	reply, err := withSchema(ctx, s, name, func(schema *Schema) (Reply, error) {
		if _, err := formColumns(schema, columns); err != nil {
			return Reply{}, &staleSchemaError{err: err, local: true}
		}
		writeSchema := *schema
		writeSchema.Options.VarMaxChunkBytes = maxSectionedVariableBytes
		form, err := encodeSectionedBlockMutation(&writeSchema, chunk, columns...)
		if err != nil {
			return Reply{}, err
		}
		reply, err := s.call(ctx, "SET CHUNK "+coordinatePoint(x, y, z...)+" IN "+name+columnsClause(columns)+" $1"+clause, [][]byte{form})
		if errors.Is(err, ErrSchemaMismatch) || isNoColumn(err) {
			return Reply{}, &staleSchemaError{err: err}
		}
		return reply, err
	})
	return reply, annotate(command, err)
}

// SetChunkMetadata updates named chunk columns without changing block values.
func (c *Client) SetChunkMetadata(ctx context.Context, table string, x, y int64, values Record, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunkMetadata(ctx, c, table, x, y, values, clause)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

// SetChunkMetadata3D updates chunk columns at explicit three-axis coordinates.
func (c *Client) SetChunkMetadata3D(ctx context.Context, table string, x, y, z int64, values Record, opts ...WriteOption) (uint64, error) {
	clause, err := writeClause("SET CHUNK", opts, true)
	if err != nil {
		return 0, err
	}
	reply, err := setChunkMetadata(ctx, c, table, x, y, values, clause, z)
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

func setChunkMetadata(ctx context.Context, s session, table string, x, y int64, values Record, clause string, z ...int64) (Reply, error) {
	const command = "SET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	if len(values) == 0 {
		return Reply{}, requestErrorf(command, "META needs at least one chunk column")
	}
	names := slices.Sorted(maps.Keys(values))
	if err := checkNames(command, "chunk column", names...); err != nil {
		return Reply{}, err
	}
	reply, err := withSchema(ctx, s, name, func(schema *Schema) (Reply, error) {
		var statement strings.Builder
		statement.WriteString("SET CHUNK " + coordinatePoint(x, y, z...) + " IN " + name + " META ")
		parameters := make([][]byte, 0, len(names))
		for i, name := range names {
			column, found := schema.ChunkColumn(name)
			if !found {
				return Reply{}, &staleSchemaError{err: requestErrorf(command, "no chunk column %s", name), local: true}
			}
			if values[name] == nil && !column.Null {
				return Reply{}, requestErrorf(command, "chunk column %s cannot be NULL", name)
			}
			value, err := EncodeValue(column.Type, values[name])
			if err != nil {
				return Reply{}, annotate(command, err)
			}
			parameters = append(parameters, value)
			if i != 0 {
				statement.WriteString(", ")
			}
			statement.WriteString(name + " = $" + strconv.Itoa(i+1))
		}
		statement.WriteString(clause)
		reply, err := s.call(ctx, statement.String(), parameters)
		if isWrongSize(err) || isNoColumn(err) || errors.Is(err, ErrSchemaMismatch) {
			return Reply{}, &staleSchemaError{err: err}
		}
		if isServerCode(err, CodeBadRequest) {
			s.forgetSchema(name)
		}
		return reply, err
	})
	return reply, annotate(command, err)
}

// SetChunkColumns stages selected block columns while preserving metadata.
func (tx *Tx) SetChunkColumns(ctx context.Context, table string, x, y int64, columns []string, chunk *Chunk) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkColumns(ctx, tx, table, x, y, columns, chunk, "")
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkColumns3D stages selected three-dimensional block columns.
func (tx *Tx) SetChunkColumns3D(ctx context.Context, table string, x, y, z int64, columns []string, chunk *Chunk) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkColumns(ctx, tx, table, x, y, columns, chunk, "", z)
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkMetadata stages chunk-column updates for COMMIT.
func (tx *Tx) SetChunkMetadata(ctx context.Context, table string, x, y int64, values Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkMetadata(ctx, tx, table, x, y, values, "")
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkMetadata3D stages three-dimensional chunk-column updates.
func (tx *Tx) SetChunkMetadata3D(ctx context.Context, table string, x, y, z int64, values Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkMetadata(ctx, tx, table, x, y, values, "", z)
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkColumns updates selected block columns on a leased client.
func (p *Pool) SetChunkColumns(ctx context.Context, table string, x, y int64, columns []string, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkColumns(ctx, table, x, y, columns, chunk, opts...)
	})
}

// SetChunkColumns3D updates selected three-dimensional block columns on a leased client.
func (p *Pool) SetChunkColumns3D(ctx context.Context, table string, x, y, z int64, columns []string, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkColumns3D(ctx, table, x, y, z, columns, chunk, opts...)
	})
}

// SetChunkMetadata updates chunk columns on a leased client.
func (p *Pool) SetChunkMetadata(ctx context.Context, table string, x, y int64, values Record, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkMetadata(ctx, table, x, y, values, opts...)
	})
}

// SetChunkMetadata3D updates three-dimensional chunk columns on a leased client.
func (p *Pool) SetChunkMetadata3D(ctx context.Context, table string, x, y, z int64, values Record, opts ...WriteOption) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.SetChunkMetadata3D(ctx, table, x, y, z, values, opts...)
	})
}
