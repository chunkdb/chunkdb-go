package chunkdb

import (
	"context"
	"strings"
)

// Returning asks a block write for the block (RETURNING): Old returns the
// block before the write, else the block after it; DELETE always returns the
// old block. Columns are the returned columns in order; empty returns every
// column in schema order.
type Returning struct {
	Old     bool
	Columns []string
}

// ReturnedRow is one block a RETURNING write returns. Z is zero in a
// two-dimensional table.
type ReturnedRow struct {
	X, Y, Z int64
	Values  Record
}

// WriteResult is the reply of a write with RETURNING. Applied is false when
// [IfVersion] or [If] refused the write, which then changed nothing and
// returned no rows. Version is the chunk's version after the write, or its
// current version when refused; nil for a never-written chunk.
type WriteResult struct {
	Applied bool
	Version *uint64
	Rows    []ReturnedRow
}

func (r Returning) clause(command string, isDelete bool) (string, error) {
	text := " RETURNING "
	if r.Old {
		text += "OLD "
	} else if isDelete {
		return "", requestErrorf(command, "DELETE BLOCK returns the old block; set Returning.Old")
	}
	if len(r.Columns) == 0 {
		return text + "*", nil
	}
	if err := checkNames(command, "column", r.Columns...); err != nil {
		return "", err
	}
	return text + strings.Join(r.Columns, ", "), nil
}

// SetBlockReturning is [Client.SetBlock] with RETURNING: a refused condition
// or version returns Applied false instead of an error.
func (c *Client) SetBlockReturning(ctx context.Context, table string, x, y int64, values Record, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return c.writeReturning(ctx, "SET BLOCK", table, x, y, nil, values, returning, opts)
}

// SetBlock3DReturning is [Client.SetBlockReturning] at three-axis coordinates.
func (c *Client) SetBlock3DReturning(ctx context.Context, table string, x, y, z int64, values Record, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return c.writeReturning(ctx, "SET BLOCK", table, x, y, []int64{z}, values, returning, opts)
}

// DeleteBlockReturning is [Client.DeleteBlock] returning the deleted block;
// returning.Old must be set.
func (c *Client) DeleteBlockReturning(ctx context.Context, table string, x, y int64, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return c.writeReturning(ctx, "DELETE BLOCK", table, x, y, nil, nil, returning, opts)
}

// DeleteBlock3DReturning is [Client.DeleteBlockReturning] at three-axis coordinates.
func (c *Client) DeleteBlock3DReturning(ctx context.Context, table string, x, y, z int64, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return c.writeReturning(ctx, "DELETE BLOCK", table, x, y, []int64{z}, nil, returning, opts)
}

func (c *Client) writeReturning(ctx context.Context, command, table string, x, y int64, z []int64, values Record, returning Returning, opts []WriteOption) (WriteResult, error) {
	clause, err := blockWriteClause(command, opts)
	if err != nil {
		return WriteResult{}, err
	}
	text, err := returning.clause(command, command == "DELETE BLOCK")
	if err != nil {
		return WriteResult{}, err
	}
	clause.text += text
	var reply Reply
	if command == "SET BLOCK" {
		reply, err = setBlock(ctx, c, table, x, y, values, clause, z...)
	} else {
		reply, err = deleteBlock(ctx, c, table, x, y, clause, z...)
	}
	if err != nil {
		return WriteResult{}, err
	}
	name, err := c.tableName(command, table)
	if err != nil {
		return WriteResult{}, err
	}
	schema, err := c.Schema(ctx, name)
	if err != nil {
		return WriteResult{}, err
	}
	return parseWriteResult(command, schema, returning.Columns, reply)
}

func parseWriteResult(command string, schema *Schema, names []string, reply Reply) (WriteResult, error) {
	bad := func(what string) (WriteResult, error) {
		return WriteResult{}, protocolErrorf(command, "RETURNING reply: %s", what)
	}
	applied, okApplied := reply.Lookup("applied")
	version, okVersion := reply.Lookup("version")
	rows, okRows := reply.Lookup("rows")
	if reply.Kind != ReplyMap || !okApplied || applied.Kind != ReplyBoolean || !okVersion || !okRows || rows.Kind != ReplyArray {
		return bad("expected applied, version and rows")
	}
	result := WriteResult{Applied: applied.Bool, Rows: make([]ReturnedRow, 0, len(rows.Array))}
	if version.Kind != ReplyNull {
		n, ok := version.Uint64()
		if !ok {
			return bad("invalid version")
		}
		result.Version = &n
	}
	if !result.Applied && len(rows.Array) != 0 {
		return bad("a refused write returned rows")
	}
	columns := schema.Columns
	if len(names) != 0 {
		columns = make([]Column, 0, len(names))
		for _, name := range names {
			column, ok := schema.Column(name)
			if !ok {
				return bad("unknown column " + name)
			}
			columns = append(columns, column)
		}
	}
	dims := schema.Dims
	if dims == 0 {
		dims = 2
	}
	for _, item := range rows.Array {
		if item.Kind != ReplyArray || len(item.Array) != dims+len(columns) {
			return bad("a row does not match the columns")
		}
		var axes [3]int64
		for i := range dims {
			n, ok := item.Array[i].Int64()
			if !ok {
				return bad("invalid coordinate")
			}
			axes[i] = n
		}
		row := ReturnedRow{X: axes[0], Y: axes[1], Z: axes[2], Values: make(Record, len(columns))}
		for i, column := range columns {
			value, err := decodeValue(column.Type, item.Array[dims+i])
			if err != nil {
				return WriteResult{}, annotate(command, err)
			}
			row.Values[column.Name] = value
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

// SetBlockReturning runs [Client.SetBlockReturning] on a leased client.
func (p *Pool) SetBlockReturning(ctx context.Context, table string, x, y int64, values Record, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (WriteResult, error) {
		return c.SetBlockReturning(ctx, table, x, y, values, returning, opts...)
	})
}

// DeleteBlockReturning runs [Client.DeleteBlockReturning] on a leased client.
func (p *Pool) DeleteBlockReturning(ctx context.Context, table string, x, y int64, returning Returning, opts ...WriteOption) (WriteResult, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (WriteResult, error) {
		return c.DeleteBlockReturning(ctx, table, x, y, returning, opts...)
	})
}
