package chunkdb

import (
	"context"
	"errors"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
)

// AreaPageOptions selects one bounded page of requested two-dimensional cells.
// Limit zero uses the negotiated maximum; After is the previous page's cursor.
type AreaPageOptions struct {
	Limit uint64
	After *ChunkCoord
}

// AreaPageOptions3D selects one bounded three-dimensional area page.
type AreaPageOptions3D struct {
	Limit uint64
	After *ChunkCoord3D
}

// AreaPage contains populated chunks and an unfinished page's continuation cursor.
// An unfinished page can be empty; its cursor still advances over empty cells.
type AreaPage struct {
	Chunks        []AreaChunk
	More          bool
	After         *ChunkCoord
	SchemaVersion uint64
}

// AreaPage3D is a three-dimensional area page with an exact cursor.
type AreaPage3D struct {
	Chunks        []AreaChunk3D
	More          bool
	After         *ChunkCoord3D
	SchemaVersion uint64
}

type areaPageRequest struct {
	dims        int
	first, last [3]int64
	radius      *uint64
	after       *[3]int64
	limit       uint64
}

type areaResponseControl struct {
	connection *conn
	deadline   callDeadline
}

func (c *areaResponseControl) checkpoint() error {
	if c.deadline.ctx.Err() != nil {
		return c.deadline.err()
	}
	if end, exists := c.deadline.ctx.Deadline(); exists && !time.Now().Before(end) {
		return c.deadline.err()
	}
	return nil
}

func decodeAreaResponse[T any](control *areaResponseControl, decode func() (T, error)) (T, error) {
	var value T
	if control == nil {
		return value, protocolErrorf("GET AREA", "response has no validation deadline")
	}
	defer control.deadline.cancel()
	err := control.checkpoint()
	if err == nil {
		value, err = decode()
	}
	if expired := control.checkpoint(); expired != nil {
		var zero T
		value = zero
		err = expired
	}
	var typed *Error
	var stale *staleSchemaError
	var limit *areaDecodeLimitError
	if err != nil && errors.As(err, &typed) && (typed.Phase == PhaseTimeout || typed.Phase == PhaseProtocol && !errors.As(err, &stale) && !errors.As(err, &limit)) {
		_ = control.connection.shutdown(err)
	}
	return value, err
}

func decodeAreaChunk(schema *Schema, data []byte, names []string) (*Chunk, error) {
	form, err := parseSectionedForm(data, math.MaxUint32, false)
	if err != nil {
		return nil, err
	}
	if form.header.schema != schema.Version {
		return nil, &staleSchemaError{err: protocolErrorf("GET AREA", "chunk form schema version changed during read")}
	}
	return decodeSectionedChunk(schema, data, names...)
}

func copyAreaPageOptions(options AreaPageOptions) AreaPageOptions {
	if options.After != nil {
		point := *options.After
		options.After = &point
	}
	return options
}
func copyAreaPageOptions3D(options AreaPageOptions3D) AreaPageOptions3D {
	if options.After != nil {
		point := *options.After
		options.After = &point
	}
	return options
}

func areaPage2D(x0, y0, x1, y1 int64, radius *int64, options AreaPageOptions) areaPageRequest {
	request := areaPageRequest{dims: 2, first: [3]int64{x0, y0}, last: [3]int64{x1, y1}, limit: options.Limit}
	if radius != nil {
		r := uint64(*radius)
		request.radius = &r
	}
	if options.After != nil {
		point := [3]int64{options.After.CX, options.After.CY}
		request.after = &point
	}
	return request
}
func areaPage3D(x0, y0, z0, x1, y1, z1 int64, radius *int64, options AreaPageOptions3D) areaPageRequest {
	request := areaPageRequest{dims: 3, first: [3]int64{x0, y0, z0}, last: [3]int64{x1, y1, z1}, limit: options.Limit}
	if radius != nil {
		r := uint64(*radius)
		request.radius = &r
	}
	if options.After != nil {
		point := [3]int64{options.After.CX, options.After.CY, options.After.CZ}
		request.after = &point
	}
	return request
}

func (r areaPageRequest) valid() error {
	if r.radius != nil && *r.radius > math.MaxInt64 {
		return requestErrorf("GET AREA", "radius must be nonnegative and fit int64")
	}
	for i := range r.dims {
		if r.first[i] > r.last[i] {
			return requestErrorf("GET AREA", "endpoints must be ordered")
		}
	}
	if r.after != nil && !r.contains(*r.after) {
		return requestErrorf("GET AREA", "cursor is outside the selector")
	}
	return nil
}

func areaPointLess(left, right [3]int64, dims int) bool {
	for i := range dims {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return false
}

func (r areaPageRequest) contains(point [3]int64) bool {
	if r.radius == nil {
		for i := range r.dims {
			if point[i] < r.first[i] || point[i] > r.last[i] {
				return false
			}
		}
		return true
	}
	var sum, delta, center, square, radius big.Int
	for i := range r.dims {
		delta.SetInt64(point[i])
		center.SetInt64(r.first[i])
		delta.Sub(&delta, &center)
		square.Mul(&delta, &delta)
		sum.Add(&sum, &square)
	}
	radius.SetUint64(*r.radius)
	radius.Mul(&radius, &radius)
	return sum.Cmp(&radius) <= 0
}

func (r areaPageRequest) bounds(point [3]int64, axis int) (int64, int64) {
	if r.radius == nil {
		return r.first[axis], r.last[axis]
	}
	var remaining, delta, center, square big.Int
	remaining.SetUint64(*r.radius)
	remaining.Mul(&remaining, &remaining)
	for i := range axis {
		delta.SetInt64(point[i])
		center.SetInt64(r.first[i])
		delta.Sub(&delta, &center)
		square.Mul(&delta, &delta)
		remaining.Sub(&remaining, &square)
	}
	remaining.Sqrt(&remaining)
	center.SetInt64(r.first[axis])
	var low, high big.Int
	low.Sub(&center, &remaining)
	high.Add(&center, &remaining)
	min, max := big.NewInt(math.MinInt64), big.NewInt(math.MaxInt64)
	if low.Cmp(min) < 0 {
		low.Set(min)
	}
	if high.Cmp(max) > 0 {
		high.Set(max)
	}
	return low.Int64(), high.Int64()
}

func (r areaPageRequest) firstPoint() [3]int64 {
	var point [3]int64
	for i := range r.dims {
		point[i], _ = r.bounds(point, i)
	}
	return point
}

func (r areaPageRequest) next(point [3]int64) ([3]int64, bool) {
	for i := r.dims - 1; i >= 0; i-- {
		_, high := r.bounds(point, i)
		if point[i] < high {
			point[i]++
			for j := i + 1; j < r.dims; j++ {
				point[j], _ = r.bounds(point, j)
			}
			return point, true
		}
	}
	return [3]int64{}, false
}

func areaPointText(point [3]int64, dims int) string {
	values := make([]string, dims)
	for i := range dims {
		values[i] = coord(point[i])
	}
	return strings.Join(values, " ")
}

func (r areaPageRequest) clause() string {
	if r.radius != nil {
		return "AROUND " + areaPointText(r.first, r.dims) + " RADIUS " + strconv.FormatUint(*r.radius, 10)
	}
	return areaPointText(r.first, r.dims) + " TO " + areaPointText(r.last, r.dims)
}

func areaReplyPoint(reply Reply, dims int) ([3]int64, error) {
	var point [3]int64
	if reply.Kind != ReplyArray || len(reply.Array) != dims {
		return point, protocolErrorf("GET AREA", "cursor has the wrong coordinate arity")
	}
	for i := range dims {
		value, ok := reply.Array[i].Int64()
		if !ok {
			return point, protocolErrorf("GET AREA", "coordinate is outside int64")
		}
		point[i] = value
	}
	return point, nil
}

func validateAreaPage(reply Reply, request areaPageRequest, maxChunks uint64, checkpoints ...func() error) ([]Reply, bool, *[3]int64, error) {
	var check func() error
	if len(checkpoints) != 0 {
		check = checkpoints[0]
	}
	if err := areaCheckpoint(check); err != nil {
		return nil, false, nil, err
	}
	bad := func(message string) ([]Reply, bool, *[3]int64, error) {
		return nil, false, nil, protocolErrorf("GET AREA", "%s", message)
	}
	if reply.Kind != ReplyMap {
		return bad("expected an area page map")
	}
	fields := make(map[string]Reply, 3)
	for _, entry := range reply.Map {
		if err := areaCheckpoint(check); err != nil {
			return nil, false, nil, err
		}
		name := ""
		if entry.Key.Kind == ReplyBulk {
			name = string(entry.Key.Bulk)
		} else if entry.Key.Kind == ReplySimple {
			name = entry.Key.Text
		}
		if name == "chunks" || name == "more" || name == "after" {
			if _, exists := fields[name]; exists {
				return bad("duplicate page field")
			}
			fields[name] = entry.Value
		}
	}
	chunks, more, cursor := fields["chunks"], fields["more"], fields["after"]
	if chunks.Kind != ReplyArray || more.Kind != ReplyBoolean || cursor.Kind == 0 {
		return bad("missing page fields")
	}
	limit := request.limit
	if limit == 0 {
		limit = maxChunks
	}
	if limit > maxChunks {
		return bad("page limit exceeds negotiated bound")
	}
	if uint64(len(chunks.Array)) > limit || uint64(len(chunks.Array)) > maxChunks {
		return bad("too many returned chunks")
	}
	var after *[3]int64
	if cursor.Kind != ReplyNull {
		point, err := areaReplyPoint(cursor, request.dims)
		if err != nil {
			return nil, false, nil, err
		}
		if !request.contains(point) || (request.after != nil && !areaPointLess(*request.after, point, request.dims)) {
			return bad("cursor does not advance within selector")
		}
		after = &point
	}
	if more.Bool == (after == nil) {
		return bad("only unfinished pages have a continuation cursor")
	}
	valid, err := request.validProgress(after, limit, check)
	if err != nil {
		return nil, false, nil, err
	}
	if !valid {
		return bad("continuation or completion exceeds requested cell limit")
	}
	previous := request.after
	for _, row := range chunks.Array {
		if err := areaCheckpoint(check); err != nil {
			return nil, false, nil, err
		}
		if row.Kind != ReplyArray || len(row.Array) < request.dims+1 {
			return bad("chunk row is missing fields")
		}
		point, err := areaReplyPoint(Reply{Kind: ReplyArray, Array: row.Array[:request.dims]}, request.dims)
		if err != nil {
			return nil, false, nil, err
		}
		if !request.contains(point) || (previous != nil && !areaPointLess(*previous, point, request.dims)) || (after != nil && areaPointLess(*after, point, request.dims)) {
			return bad("chunk rows are outside completed page or unordered")
		}
		if row.Array[request.dims].Kind != ReplyBulk {
			return bad("chunk form must be a bulk string")
		}
		previous = &point
	}
	return chunks.Array, more.Bool, after, nil
}

func (c *Client) areaInfo(ctx context.Context) (*ServerInfo, error) {
	cn, err := c.connection(ctx)
	if err != nil {
		return nil, err
	}
	return cn.info, nil
}
func (tx *Tx) areaInfo(context.Context) (*ServerInfo, error) {
	if tx.done {
		return nil, requestErrorf("GET AREA", "the transaction has ended")
	}
	if tx.conflict != nil {
		return nil, tx.conflict
	}
	if err := tx.conn.terminalError(); err != nil {
		return nil, err
	}
	return tx.conn.info, nil
}

func getAreaPage(ctx context.Context, s session, table string, request areaPageRequest, columns []string) (AreaPage3D, error) {
	if err := request.valid(); err != nil {
		return AreaPage3D{}, err
	}
	name, err := s.tableName("GET AREA", table)
	if err != nil {
		return AreaPage3D{}, err
	}
	if err := checkNames("GET AREA", "column", columns...); err != nil {
		return AreaPage3D{}, err
	}
	info, err := s.areaInfo(ctx)
	if err != nil {
		return AreaPage3D{}, err
	}
	if !info.Supports("area_paging") {
		return AreaPage3D{}, requestErrorf("GET AREA", "the server does not support area_paging")
	}
	limits, err := limitsForArea(info, nil)
	if err != nil {
		return AreaPage3D{}, err
	}
	if request.limit > limits.maxChunks {
		return AreaPage3D{}, requestErrorf("GET AREA", "limit exceeds MaxAreaChunks")
	}
	limit := request.limit
	if limit == 0 {
		limit = limits.maxChunks
	}
	result, err := withSchema(ctx, s, name, func(schema *Schema) (AreaPage3D, error) {
		dims := schema.Dims
		if dims == 0 {
			dims = 2
		}
		if dims != request.dims {
			return AreaPage3D{}, requestErrorf("GET AREA", "selector has wrong table dimensionality")
		}
		indexes, err := formColumns(schema, columns)
		if err != nil {
			return AreaPage3D{}, &staleSchemaError{err: err, local: true}
		}
		names := columnNames(schema, indexes)
		statement := "GET AREA " + request.clause() + " FROM " + name + columnsClause(names) + " LIMIT " + strconv.FormatUint(limit, 10)
		if request.after != nil {
			statement += " AFTER " + areaPointText(*request.after, request.dims)
		}
		statement += " WITH SCHEMA"
		var responseControl *areaResponseControl
		reply, err := s.call(ctx, statement, nil, areaReplyOptions{knownShape: true, page: true, withSchema: true, limit: limit, response: &responseControl})
		if err != nil {
			if isNoColumn(err) {
				return AreaPage3D{}, &staleSchemaError{err: err}
			}
			return AreaPage3D{}, err
		}
		return decodeAreaResponse(responseControl, func() (AreaPage3D, error) {
			if reply.Kind != ReplyArray || len(reply.Array) < 2 {
				return AreaPage3D{}, protocolErrorf("GET AREA", "invalid schema envelope")
			}
			version, ok := reply.Array[0].Uint64()
			if !ok {
				return AreaPage3D{}, protocolErrorf("GET AREA", "invalid schema version")
			}
			if version != schema.Version {
				return AreaPage3D{}, &staleSchemaError{err: protocolErrorf("GET AREA", "schema version changed during read")}
			}
			rows, more, after, err := validateAreaPage(reply.Array[1], request, limits.maxChunks, responseControl.checkpoint)
			if err != nil {
				return AreaPage3D{}, err
			}
			if err := checkAreaDecodedCells(schema, indexes, len(rows)); err != nil {
				return AreaPage3D{}, err
			}
			page := AreaPage3D{More: more, SchemaVersion: version, Chunks: make([]AreaChunk3D, 0, len(rows))}
			if after != nil {
				page.After = &ChunkCoord3D{CX: after[0], CY: after[1], CZ: after[2]}
			}
			for _, row := range rows {
				if err := responseControl.checkpoint(); err != nil {
					return AreaPage3D{}, err
				}
				point, _ := areaReplyPoint(Reply{Kind: ReplyArray, Array: row.Array[:request.dims]}, request.dims)
				chunk, err := decodeAreaChunk(schema, row.Array[request.dims].Bulk, names)
				if err != nil {
					return AreaPage3D{}, err
				}
				page.Chunks = append(page.Chunks, AreaChunk3D{CX: point[0], CY: point[1], CZ: point[2], Chunk: chunk})
			}
			return page, nil
		})
	})
	return result, annotate("GET AREA", err)
}

func areaPageAs2D(page AreaPage3D, err error) (AreaPage, error) {
	if err != nil {
		return AreaPage{}, annotate("GET AREA", err)
	}
	result := AreaPage{More: page.More, SchemaVersion: page.SchemaVersion, Chunks: make([]AreaChunk, 0, len(page.Chunks))}
	if page.After != nil {
		result.After = &ChunkCoord{CX: page.After.CX, CY: page.After.CY}
	}
	for _, chunk := range page.Chunks {
		result.Chunks = append(result.Chunks, AreaChunk{CX: chunk.CX, CY: chunk.CY, Chunk: chunk.Chunk})
	}
	return result, nil
}

// GetAreaPage reads one rectangular page, including schema metadata and empty-cell progress.
func (c *Client) GetAreaPage(ctx context.Context, table string, x0, y0, x1, y1 int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	return areaPageAs2D(getAreaPage(ctx, c, table, areaPage2D(x0, y0, x1, y1, nil, options), columns))
}

// GetAreaAroundPage reads one page from an inclusive Euclidean disc.
func (c *Client) GetAreaAroundPage(ctx context.Context, table string, x, y, radius int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	return areaPageAs2D(getAreaPage(ctx, c, table, areaPage2D(x, y, x, y, &radius, options), columns))
}

// GetAreaPage3D reads one three-dimensional rectangular page.
func (c *Client) GetAreaPage3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	return getAreaPage(ctx, c, table, areaPage3D(x0, y0, z0, x1, y1, z1, nil, options), columns)
}

// GetAreaAroundPage3D reads one page from an inclusive Euclidean ball.
func (c *Client) GetAreaAroundPage3D(ctx context.Context, table string, x, y, z, radius int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	return getAreaPage(ctx, c, table, areaPage3D(x, y, z, x, y, z, &radius, options), columns)
}

// GetAreaPage reads a page within the transaction's snapshot and private writes.
func (tx *Tx) GetAreaPage(ctx context.Context, table string, x0, y0, x1, y1 int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return areaPageAs2D(getAreaPage(ctx, tx, table, areaPage2D(x0, y0, x1, y1, nil, options), columns))
}

// GetAreaAroundPage reads a disc page within the transaction.
func (tx *Tx) GetAreaAroundPage(ctx context.Context, table string, x, y, radius int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return areaPageAs2D(getAreaPage(ctx, tx, table, areaPage2D(x, y, x, y, &radius, options), columns))
}

// GetAreaPage3D reads a three-dimensional page within the transaction.
func (tx *Tx) GetAreaPage3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getAreaPage(ctx, tx, table, areaPage3D(x0, y0, z0, x1, y1, z1, nil, options), columns)
}

// GetAreaAroundPage3D reads a ball page within the transaction.
func (tx *Tx) GetAreaAroundPage3D(ctx context.Context, table string, x, y, z, radius int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getAreaPage(ctx, tx, table, areaPage3D(x, y, z, x, y, z, &radius, options), columns)
}

// GetAreaPage reads a page on a leased connection.
func (p *Pool) GetAreaPage(ctx context.Context, table string, x0, y0, x1, y1 int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	options = copyAreaPageOptions(options)
	columns = slices.Clone(columns)
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaPage, error) {
		return c.GetAreaPage(ctx, table, x0, y0, x1, y1, options, columns...)
	})
}

// GetAreaAroundPage reads a disc page on a leased connection.
func (p *Pool) GetAreaAroundPage(ctx context.Context, table string, x, y, radius int64, options AreaPageOptions, columns ...string) (AreaPage, error) {
	options = copyAreaPageOptions(options)
	columns = slices.Clone(columns)
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaPage, error) {
		return c.GetAreaAroundPage(ctx, table, x, y, radius, options, columns...)
	})
}

// GetAreaPage3D reads a three-dimensional page on a leased connection.
func (p *Pool) GetAreaPage3D(ctx context.Context, table string, x0, y0, z0, x1, y1, z1 int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	options = copyAreaPageOptions3D(options)
	columns = slices.Clone(columns)
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaPage3D, error) {
		return c.GetAreaPage3D(ctx, table, x0, y0, z0, x1, y1, z1, options, columns...)
	})
}

// GetAreaAroundPage3D reads a ball page on a leased connection.
func (p *Pool) GetAreaAroundPage3D(ctx context.Context, table string, x, y, z, radius int64, options AreaPageOptions3D, columns ...string) (AreaPage3D, error) {
	options = copyAreaPageOptions3D(options)
	columns = slices.Clone(columns)
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (AreaPage3D, error) {
		return c.GetAreaAroundPage3D(ctx, table, x, y, z, radius, options, columns...)
	})
}
