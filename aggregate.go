package chunkdb

import (
	"context"
	"math/big"
	"strings"
)

// AggregateOp is the function of one AGGREGATE result.
type AggregateOp string

// AGGREGATE functions.
const (
	AggregateCount AggregateOp = "COUNT"
	AggregateSum   AggregateOp = "SUM"
	AggregateMin   AggregateOp = "MIN"
	AggregateMax   AggregateOp = "MAX"
	AggregateAvg   AggregateOp = "AVG"
)

// AggregateFunction is one function of an AGGREGATE statement. Column is
// empty for COUNT(*) and names an integer or float block column otherwise.
type AggregateFunction struct {
	Op     AggregateOp
	Column string
}

// CountAll counts the present blocks: COUNT(*).
func CountAll() AggregateFunction { return AggregateFunction{Op: AggregateCount} }

// SumOf sums column, skipping NULL values.
func SumOf(column string) AggregateFunction { return AggregateFunction{AggregateSum, column} }

// MinOf returns the smallest value of column.
func MinOf(column string) AggregateFunction { return AggregateFunction{AggregateMin, column} }

// MaxOf returns the largest value of column.
func MaxOf(column string) AggregateFunction { return AggregateFunction{AggregateMax, column} }

// AvgOf averages column as a float64.
func AvgOf(column string) AggregateFunction { return AggregateFunction{AggregateAvg, column} }

// Name returns the lowercase result name the server uses, such as "count(*)" or "sum(height)".
func (f AggregateFunction) Name() string {
	if f.Op == AggregateCount {
		return "count(*)"
	}
	return strings.ToLower(string(f.Op)) + "(" + f.Column + ")"
}

// Around is a two-dimensional ball of chunks: every chunk within Radius of CX, CY.
type Around struct{ CX, CY, Radius int64 }

// Around3D is a three-dimensional ball of chunks.
type Around3D struct{ CX, CY, CZ, Radius int64 }

// ChunkSelector names the chunks of an AGGREGATE statement. Set exactly one field.
type ChunkSelector struct {
	Chunk    *ChunkCoord
	Chunk3D  *ChunkCoord3D
	Area     *Area
	Area3D   *Area3D
	Around   *Around
	Around3D *Around3D
}

func (s ChunkSelector) clause(command string) (string, error) {
	var clauses []string
	if c := s.Chunk; c != nil {
		clauses = append(clauses, "CHUNK "+coord(c.CX)+" "+coord(c.CY))
	}
	if c := s.Chunk3D; c != nil {
		clauses = append(clauses, "CHUNK "+coord(c.CX)+" "+coord(c.CY)+" "+coord(c.CZ))
	}
	if a := s.Area; a != nil {
		clauses = append(clauses, "AREA "+areaRange(a.CX0, a.CY0, a.CX1, a.CY1))
	}
	if a := s.Area3D; a != nil {
		clauses = append(clauses, "AREA "+areaRange3D(a.CX0, a.CY0, a.CZ0, a.CX1, a.CY1, a.CZ1))
	}
	if a := s.Around; a != nil {
		if a.Radius < 0 {
			return "", requestErrorf(command, "the radius must not be negative")
		}
		clauses = append(clauses, "AREA AROUND "+coord(a.CX)+" "+coord(a.CY)+" RADIUS "+coord(a.Radius))
	}
	if a := s.Around3D; a != nil {
		if a.Radius < 0 {
			return "", requestErrorf(command, "the radius must not be negative")
		}
		clauses = append(clauses, "AREA AROUND "+coord(a.CX)+" "+coord(a.CY)+" "+coord(a.CZ)+" RADIUS "+coord(a.Radius))
	}
	if len(clauses) != 1 {
		return "", requestErrorf(command, "select exactly one chunk, area or ball")
	}
	return clauses[0], nil
}

// AggregateResult holds one AGGREGATE result per function, in request order.
// A value is nil (no value), int64, *big.Int (an integer outside the int64
// range), or float64.
type AggregateResult struct {
	Names  []string
	Values []any
}

// Value returns the result named like [AggregateFunction.Name].
func (r AggregateResult) Value(name string) (any, bool) {
	for i, n := range r.Names {
		if n == name {
			return r.Values[i], true
		}
	}
	return nil, false
}

// AggregateRow is the result of one chunk of AGGREGATE ... BY CHUNK. CZ is
// zero for two-dimensional tables. Values follow the request order and use
// the types of [AggregateResult].
type AggregateRow struct {
	CX, CY, CZ int64
	Values     []any
}

func aggregateStatement(table string, selector ChunkSelector, functions []AggregateFunction, where *Condition, byChunk bool) (string, error) {
	const command = "AGGREGATE"
	selectorText, err := selector.clause(command)
	if err != nil {
		return "", err
	}
	if len(functions) == 0 {
		return "", requestErrorf(command, "name at least one function")
	}
	parts := make([]string, len(functions))
	for i, f := range functions {
		switch f.Op {
		case AggregateCount:
			if f.Column != "" {
				return "", requestErrorf(command, "COUNT takes no column")
			}
			parts[i] = "COUNT(*)"
		case AggregateSum, AggregateMin, AggregateMax, AggregateAvg:
			if err := checkNames(command, "column", f.Column); err != nil {
				return "", err
			}
			parts[i] = string(f.Op) + "(" + f.Column + ")"
		default:
			return "", requestErrorf(command, "unknown function %q", f.Op)
		}
	}
	filter, err := conditionText(command, where)
	if err != nil {
		return "", err
	}
	statement := command + " " + selectorText + " FROM " + table + " " + strings.Join(parts, ", ") + filter
	if byChunk {
		statement += " BY CHUNK"
	}
	return statement, nil
}

func aggregateValue(r Reply) (any, bool) {
	switch r.Kind {
	case ReplyNull:
		return nil, true
	case ReplyDouble:
		return r.Double, true
	case ReplyBigNumber:
		if r.BigNumber == nil {
			return nil, false
		}
		return new(big.Int).Set(r.BigNumber), true
	case ReplyInteger:
		if n, ok := r.Int64(); ok {
			return n, true
		}
		return r.BigInt()
	}
	return nil, false
}

func parseAggregate(reply Reply, functions []AggregateFunction) (AggregateResult, error) {
	const command = "AGGREGATE"
	if reply.Kind != ReplyMap || len(reply.Map) != len(functions) {
		return AggregateResult{}, protocolErrorf(command, "expected a map with one entry per function")
	}
	result := AggregateResult{Names: make([]string, len(functions)), Values: make([]any, len(functions))}
	for i, entry := range reply.Map {
		name := functions[i].Name()
		if entry.Key.Kind != ReplyBulk || string(entry.Key.Bulk) != name {
			return AggregateResult{}, protocolErrorf(command, "expected result %s", name)
		}
		value, ok := aggregateValue(entry.Value)
		if !ok {
			return AggregateResult{}, protocolErrorf(command, "invalid value of %s", name)
		}
		result.Names[i], result.Values[i] = name, value
	}
	return result, nil
}

func parseAggregateRows(reply Reply, dims, functions int) ([]AggregateRow, error) {
	const command = "AGGREGATE"
	if reply.Kind != ReplyArray {
		return nil, protocolErrorf(command, "expected an array of rows")
	}
	rows := make([]AggregateRow, 0, len(reply.Array))
	for _, item := range reply.Array {
		if item.Kind != ReplyArray || len(item.Array) != dims+functions {
			return nil, protocolErrorf(command, "invalid row")
		}
		var axes [3]int64
		for i := range dims {
			n, ok := item.Array[i].Int64()
			if !ok || item.Array[i].Kind != ReplyInteger {
				return nil, protocolErrorf(command, "invalid chunk coordinate")
			}
			axes[i] = n
		}
		row := AggregateRow{CX: axes[0], CY: axes[1], CZ: axes[2], Values: make([]any, functions)}
		for i := range functions {
			value, ok := aggregateValue(item.Array[dims+i])
			if !ok {
				return nil, protocolErrorf(command, "invalid row value")
			}
			row.Values[i] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s ChunkSelector) dims() int {
	if s.Chunk3D != nil || s.Area3D != nil || s.Around3D != nil {
		return 3
	}
	return 2
}

// Aggregate counts and summarizes the present blocks of the selected chunks
// with AGGREGATE. Each function appears at most once; it requires READ.
func (c *Client) Aggregate(ctx context.Context, table string, selector ChunkSelector, functions ...AggregateFunction) (AggregateResult, error) {
	return c.AggregateWhere(ctx, table, selector, nil, functions...)
}

// AggregateWhere is [Client.Aggregate] over only the present blocks for which
// where is true; nil aggregates every present block.
func (c *Client) AggregateWhere(ctx context.Context, table string, selector ChunkSelector, where *Condition, functions ...AggregateFunction) (AggregateResult, error) {
	reply, err := aggregate(ctx, c, table, selector, functions, where, false)
	if err != nil {
		return AggregateResult{}, err
	}
	return parseAggregate(reply, functions)
}

// AggregateByChunk runs AGGREGATE ... BY CHUNK: one row for each selected
// chunk with a present block, in coordinate order.
func (c *Client) AggregateByChunk(ctx context.Context, table string, selector ChunkSelector, functions ...AggregateFunction) ([]AggregateRow, error) {
	return c.AggregateByChunkWhere(ctx, table, selector, nil, functions...)
}

// AggregateByChunkWhere is [Client.AggregateByChunk] over only the present
// blocks for which where is true: a chunk without a matching block has no row.
func (c *Client) AggregateByChunkWhere(ctx context.Context, table string, selector ChunkSelector, where *Condition, functions ...AggregateFunction) ([]AggregateRow, error) {
	reply, err := aggregate(ctx, c, table, selector, functions, where, true)
	if err != nil {
		return nil, err
	}
	return parseAggregateRows(reply, selector.dims(), len(functions))
}

func aggregate(ctx context.Context, c *Client, table string, selector ChunkSelector, functions []AggregateFunction, where *Condition, byChunk bool) (Reply, error) {
	const command = "AGGREGATE"
	name, err := c.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	statement, err := aggregateStatement(name, selector, functions, where, byChunk)
	if err != nil {
		return Reply{}, err
	}
	args := conditionArgs(where)
	if len(args) == 0 {
		return c.call(ctx, statement, nil)
	}
	reply, err := withSchema(ctx, c, name, func(schema *Schema) (Reply, error) {
		params, err := encodeArgs(command, schema, args)
		if err != nil {
			return Reply{}, err
		}
		reply, err := c.call(ctx, statement, params)
		if isWrongSize(err) || isNoColumn(err) {
			return Reply{}, &staleSchemaError{err: err}
		}
		return reply, err
	})
	return reply, annotate(command, err)
}

// AggregateWhere runs [Client.AggregateWhere] on a leased client.
func (p *Pool) AggregateWhere(ctx context.Context, table string, selector ChunkSelector, where *Condition, functions ...AggregateFunction) (AggregateResult, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AggregateResult, error) {
		return c.AggregateWhere(ctx, table, selector, where, functions...)
	})
}

// AggregateByChunkWhere runs [Client.AggregateByChunkWhere] on a leased client.
func (p *Pool) AggregateByChunkWhere(ctx context.Context, table string, selector ChunkSelector, where *Condition, functions ...AggregateFunction) ([]AggregateRow, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) ([]AggregateRow, error) {
		return c.AggregateByChunkWhere(ctx, table, selector, where, functions...)
	})
}

// Aggregate runs [Client.Aggregate] on a leased client.
func (p *Pool) Aggregate(ctx context.Context, table string, selector ChunkSelector, functions ...AggregateFunction) (AggregateResult, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AggregateResult, error) {
		return c.Aggregate(ctx, table, selector, functions...)
	})
}

// AggregateByChunk runs [Client.AggregateByChunk] on a leased client.
func (p *Pool) AggregateByChunk(ctx context.Context, table string, selector ChunkSelector, functions ...AggregateFunction) ([]AggregateRow, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) ([]AggregateRow, error) {
		return c.AggregateByChunk(ctx, table, selector, functions...)
	})
}
