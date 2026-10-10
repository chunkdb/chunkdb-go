package chunkdb

import (
	"context"
	"errors"
	"iter"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Do sends one CQL statement and its parameter frames, and returns the
// decoded reply. A parameter is the binary form of a value ([EncodeValue]);
// nil is NULL. Statements are single lines: one containing CR or LF is refused
// before anything is sent. An error reply is returned as an error: an
// [*Error] or a typed permission, version, schema, or conflict error.
// BACKUP is available through Do and uses paths on the server filesystem.
//
// A CREATE TABLE, ALTER TABLE or DROP TABLE sent through Do, including inside
// MIGRATE, drops the client's cached schema of that table.
func (c *Client) Do(ctx context.Context, statement string, params ...[]byte) (Reply, error) {
	if table, ok := tableStatementTarget(statement); ok {
		defer c.forgetSchema(table)
	}
	return c.call(ctx, statement, params)
}

// call runs one statement: it reserves a pipeline slot, sends the statement
// with its parameters, and returns the reply.
func (c *Client) call(ctx context.Context, statement string, params [][]byte) (Reply, error) {
	command := commandOf(statement)
	release, err := c.acquireSlot(ctx, command)
	if err != nil {
		return Reply{}, err
	}
	defer release()
	return c.exec(ctx, command, statement, params)
}

// commandOf names a statement for errors: its verb, and the object for verbs
// that take one ("SET BLOCK").
func commandOf(statement string) string {
	fields := strings.Fields(strings.ToUpper(statement))
	if len(fields) == 0 {
		return ""
	}
	switch fields[0] {
	case "GET", "SET", "DELETE", "CREATE", "ALTER", "DROP", "SHOW", "FLUSH", "SCAN":
		if len(fields) > 1 {
			return fields[0] + " " + fields[1]
		}
	}
	return fields[0]
}

// tableStatementTarget returns the table a CREATE, ALTER or DROP TABLE
// statement names.
func tableStatementTarget(statement string) (string, bool) {
	fields := strings.Fields(statement)
	if len(fields) >= 5 && strings.EqualFold(fields[0], "MIGRATE") {
		fields = fields[2:]
	}
	if len(fields) < 3 || !strings.EqualFold(fields[1], "TABLE") {
		return "", false
	}
	switch strings.ToUpper(fields[0]) {
	case "CREATE", "ALTER", "DROP":
		name, _, _ := strings.Cut(fields[2], "(")
		return name, name != ""
	}
	return "", false
}

// annotate names the command in a client-side error that has none.
func annotate(command string, err error) error {
	var typed *Error
	if errors.As(err, &typed) && typed.Command == "" {
		typed.Command = command
	}
	return err
}

// tableName resolves a table argument: "" is the default table.
func (c *Client) tableName(command, table string) (string, error) {
	if table == "" {
		return c.DefaultTable(), nil
	}
	if err := checkNames(command, "table", table); err != nil {
		return "", err
	}
	return table, nil
}

func ifVersionClause(opts []WriteOption) string {
	if v := resolveWriteOptions(opts).ifVersion; v != nil {
		return " IF VERSION " + strconv.FormatUint(*v, 10)
	}
	return ""
}

func columnsClause(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return " COLUMNS " + strings.Join(names, ", ")
}

func coord(v int64) string { return strconv.FormatInt(v, 10) }

// versionOf reads the chunk version a write answers.
func versionOf(reply Reply, command string) (uint64, error) {
	version, ok := reply.Uint64()
	if !ok {
		return 0, protocolErrorf(command, "expected a chunk version, got a reply of kind %d", reply.Kind)
	}
	return version, nil
}

func expectSimple(reply Reply, command, want string) error {
	if reply.Kind != ReplySimple || reply.Text != want {
		return protocolErrorf(command, "expected +%s, got a reply of kind %d %q", want, reply.Kind, reply.Text)
	}
	return nil
}

func expectBulk(reply Reply, command string) ([]byte, error) {
	if reply.Kind != ReplyBulk {
		return nil, protocolErrorf(command, "expected a bulk string, got a reply of kind %d", reply.Kind)
	}
	return reply.Bulk, nil
}

// wrongSizeMessage is the server's message for a parameter frame whose size
// does not fit its column, which a cached schema older than the table causes.
var wrongSizeMessage = regexp.MustCompile(`^\$\d+ for column \S+ \(\S+\) must be \d+ bytes, got \d+$`)

func isWrongSize(err error) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.ServerCode == CodeInvalidArgument && wrongSizeMessage.MatchString(typed.ServerMessage)
}

func isNoColumn(err error) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.ServerCode == CodeInvalidArgument &&
		strings.HasPrefix(typed.ServerMessage, "the table has no column ")
}

// isMalformed reports a reply that does not fit the schema it was decoded
// with.
func isMalformed(err error) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.Kind == KindProtocol && typed.Phase == PhaseProtocol
}

// staleSchemaError marks a failure that a schema newer than the cached one
// may avoid.
type staleSchemaError struct {
	err error
	// local is set when the client found the mismatch without asking the
	// server; a schema fetched for this very call cannot be newer then.
	local bool
}

func (e *staleSchemaError) Error() string { return e.err.Error() }
func (e *staleSchemaError) Unwrap() error { return e.err }

// session runs the block, chunk and area statements and supplies the schemas
// they encode and decode values with: a [Client], or a [Tx], which sends on
// its transaction's connection and cannot ask DESCRIBE inside it.
type session interface {
	call(ctx context.Context, statement string, params [][]byte) (Reply, error)
	tableName(command, table string) (string, error)
	// schemaOf returns the schema of table, and whether it was fetched from
	// the server for this call.
	schemaOf(ctx context.Context, table string) (schema *Schema, fresh bool, err error)
	// refetchSchema returns a newer schema of table after fn reported stale,
	// the error the stale schema caused, or fails.
	refetchSchema(ctx context.Context, table string, stale error) (*Schema, error)
	forgetSchema(table string)
}

func (c *Client) schemaOf(ctx context.Context, table string) (*Schema, bool, error) {
	if schema := c.cachedSchema(table); schema != nil {
		return schema, false, nil
	}
	schema, err := c.fetchSchema(ctx, table)
	return schema, true, err
}

func (c *Client) refetchSchema(ctx context.Context, table string, _ error) (*Schema, error) {
	return c.fetchSchema(ctx, table)
}

// withSchema runs fn with the schema of table, fetching it when none is
// cached. When fn reports a stale schema, it fetches the schema again and runs
// fn once more.
func withSchema[T any](ctx context.Context, s session, table string, fn func(*Schema) (T, error)) (T, error) {
	schema, fresh, err := s.schemaOf(ctx, table)
	if err != nil {
		var zero T
		return zero, err
	}
	value, err := fn(schema)
	var stale *staleSchemaError
	if !errors.As(err, &stale) {
		return value, err
	}
	if fresh && stale.local {
		return value, stale.err
	}
	if schema, err = s.refetchSchema(ctx, table, stale.err); err != nil {
		return value, err
	}
	value, err = fn(schema)
	if errors.As(err, &stale) {
		return value, stale.err
	}
	return value, err
}

// Schema returns the client's cached schema of table ("" is the default
// table), asking the server with DESCRIBE when none is cached. The client
// refreshes a table's schema with [Client.Describe], after a table statement it
// sent for that table, and when a statement shows the cached one is out of
// date. The result is a copy.
func (c *Client) Schema(ctx context.Context, table string) (*Schema, error) {
	name, err := c.tableName("DESCRIBE", table)
	if err != nil {
		return nil, err
	}
	if schema := c.cachedSchema(name); schema != nil {
		return schema.clone(), nil
	}
	schema, err := c.fetchSchema(ctx, name)
	return schema.clone(), err
}

// Describe asks the server for table's schema ("" is the default table) and
// replaces the client's cached one with it. The result is a copy.
func (c *Client) Describe(ctx context.Context, table string) (*Schema, error) {
	name, err := c.tableName("DESCRIBE", table)
	if err != nil {
		return nil, err
	}
	schema, err := c.fetchSchema(ctx, name)
	return schema.clone(), err
}

func (c *Client) fetchSchema(ctx context.Context, table string) (*Schema, error) {
	reply, err := c.call(ctx, "DESCRIBE "+table, nil)
	return c.keepSchema(table, reply, err)
}

// keepSchema parses the reply to DESCRIBE table and caches the schema.
func (c *Client) keepSchema(table string, reply Reply, err error) (*Schema, error) {
	if err != nil {
		if isServerCode(err, CodeNoTable) {
			c.forgetSchema(table)
		}
		return nil, err
	}
	schema, err := parseSchema(reply)
	if err != nil {
		return nil, err
	}
	c.schemaMu.Lock()
	c.schemas[table] = schema
	c.schemaMu.Unlock()
	return schema, nil
}

func (c *Client) cachedSchema(table string) *Schema {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	return c.schemas[table]
}

func (c *Client) forgetSchema(table string) {
	c.schemaMu.Lock()
	delete(c.schemas, table)
	c.schemaMu.Unlock()
}

func columnNames(schema *Schema, indexes []int) []string {
	names := make([]string, len(indexes))
	for i, index := range indexes {
		names[i] = schema.Columns[index].Name
	}
	return names
}

// GetBlock reads block (x, y) of table ("" is the default table): the named
// columns, or every column of the client's cached schema. An absent block
// returns a nil Record and no error.
func (c *Client) GetBlock(ctx context.Context, table string, x, y int64, columns ...string) (Record, error) {
	return getBlock(ctx, c, table, x, y, columns)
}

func getBlock(ctx context.Context, s session, table string, x, y int64, columns []string) (Record, error) {
	const command = "GET BLOCK"
	name, err := s.tableName(command, table)
	if err != nil {
		return nil, err
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return nil, err
	}
	record, err := withSchema(ctx, s, name, func(schema *Schema) (Record, error) {
		indexes, err := formColumns(schema, columns)
		if err != nil {
			return nil, &staleSchemaError{err: err, local: true}
		}
		names := columnNames(schema, indexes)
		reply, err := s.call(ctx, "GET BLOCK "+coord(x)+" "+coord(y)+" FROM "+name+columnsClause(names), nil)
		if err != nil {
			if isNoColumn(err) {
				return nil, &staleSchemaError{err: err}
			}
			return nil, err
		}
		if reply.Kind == ReplyNull {
			return nil, nil
		}
		if reply.Kind != ReplyArray || len(reply.Array) != len(indexes) {
			return nil, &staleSchemaError{err: protocolErrorf(command, "expected %d values", len(indexes))}
		}
		record := make(Record, len(indexes))
		for i, index := range indexes {
			column := schema.Columns[index]
			value, err := decodeValue(column.Type, reply.Array[i])
			if err != nil {
				return nil, &staleSchemaError{err: err}
			}
			record[column.Name] = value
		}
		return record, nil
	})
	return record, annotate(command, err)
}

// SetBlock writes the given columns of block (x, y) in table ("" is the
// default table) and returns the chunk's new version. Values travel as
// parameters, encoded by the column types of the client's cached schema
// ([EncodeValue] lists the Go types); nil is NULL. A new block takes, for
// each column not given, its default, else NULL for a NULL column, else zero
// or empty.
//
// When the server refuses a value because its size does not fit the column,
// the cached schema is out of date: the client fetches the schema once and
// sends the write once more. The refused write changed nothing. A value longer
// than the column now holds (another client narrowed it) makes the server
// answer BAD_REQUEST and close the connection; that write fails, and the next
// statement uses a fresh schema.
func (c *Client) SetBlock(ctx context.Context, table string, x, y int64, values Record, opts ...WriteOption) (uint64, error) {
	reply, err := setBlock(ctx, c, table, x, y, values, ifVersionClause(opts))
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET BLOCK")
}

// setBlock sends SET BLOCK with the clause after the assignments, and returns
// its reply.
func setBlock(ctx context.Context, s session, table string, x, y int64, values Record, clause string) (Reply, error) {
	const command = "SET BLOCK"
	name, err := s.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	if len(values) == 0 {
		return Reply{}, requestErrorf(command, "SET BLOCK needs at least one column")
	}
	names := slices.Sorted(maps.Keys(values))
	if err := checkNames(command, "column", names...); err != nil {
		return Reply{}, err
	}
	reply, err := withSchema(ctx, s, name, func(schema *Schema) (Reply, error) {
		var statement strings.Builder
		statement.WriteString("SET BLOCK " + coord(x) + " " + coord(y) + " IN " + name + " ")
		params := make([][]byte, 0, len(names))
		for i, columnName := range names {
			column, ok := schema.Column(columnName)
			if !ok {
				return Reply{}, &staleSchemaError{err: requestErrorf(command, "table %s has no column %s", name, columnName), local: true}
			}
			value, err := normalize(columnName, column.Type, values[columnName])
			if err != nil {
				return Reply{}, err
			}
			if value == nil && !column.Null {
				return Reply{}, requestErrorf(command, "column %s cannot be NULL", columnName)
			}
			var param []byte
			if value != nil {
				param = encodeNormalized(value)
			}
			params = append(params, param)
			if i > 0 {
				statement.WriteString(", ")
			}
			statement.WriteString(columnName + " = $" + strconv.Itoa(i+1))
		}
		statement.WriteString(clause)
		reply, err := s.call(ctx, statement.String(), params)
		if err != nil {
			if isWrongSize(err) || isNoColumn(err) {
				return Reply{}, &staleSchemaError{err: err}
			}
			if isServerCode(err, CodeBadRequest) {
				// A frame longer than its column holds: the connection is
				// closed, so the write is not sent again, but the next
				// statement uses a fresh schema.
				s.forgetSchema(name)
			}
			return Reply{}, err
		}
		return reply, nil
	})
	return reply, annotate(command, err)
}

// DeleteBlock deletes block (x, y) of table ("" is the default table) and
// returns the chunk's version after it.
func (c *Client) DeleteBlock(ctx context.Context, table string, x, y int64, opts ...WriteOption) (uint64, error) {
	reply, err := deleteBlock(ctx, c, table, x, y, ifVersionClause(opts))
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "DELETE BLOCK")
}

func deleteBlock(ctx context.Context, s session, table string, x, y int64, clause string) (Reply, error) {
	name, err := s.tableName("DELETE BLOCK", table)
	if err != nil {
		return Reply{}, err
	}
	return s.call(ctx, "DELETE BLOCK "+coord(x)+" "+coord(y)+" FROM "+name+clause, nil)
}

// GetChunk reads chunk (cx, cy) of table ("" is the default table), decoded
// with the client's cached schema ([DecodeChunk]): the named columns, or every
// column. A chunk without blocks has no present block and its version.
func (c *Client) GetChunk(ctx context.Context, table string, cx, cy int64, columns ...string) (*Chunk, error) {
	return getChunk(ctx, c, table, cx, cy, columns)
}

func getChunk(ctx context.Context, s session, table string, cx, cy int64, columns []string) (*Chunk, error) {
	const command = "GET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return nil, err
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return nil, err
	}
	chunk, err := withSchema(ctx, s, name, func(schema *Schema) (*Chunk, error) {
		indexes, err := formColumns(schema, columns)
		if err != nil {
			return nil, &staleSchemaError{err: err, local: true}
		}
		names := columnNames(schema, indexes)
		reply, err := s.call(ctx, "GET CHUNK "+coord(cx)+" "+coord(cy)+" FROM "+name+columnsClause(names), nil)
		if err != nil {
			if isNoColumn(err) {
				return nil, &staleSchemaError{err: err}
			}
			return nil, err
		}
		form, err := expectBulk(reply, command)
		if err != nil {
			return nil, err
		}
		return decodeOrStale(schema, form, names)
	})
	return chunk, annotate(command, err)
}

func decodeOrStale(schema *Schema, form []byte, names []string) (*Chunk, error) {
	chunk, err := DecodeChunk(schema, form, names...)
	if isMalformed(err) {
		return nil, &staleSchemaError{err: err}
	}
	return chunk, err
}

// GetChunkRaw reads chunk (cx, cy) of table ("" is the default table) as its
// chunk form, the named columns or every column, without decoding it.
// [Client.SetChunkRaw] writes a form of every column back, to this or another
// chunk.
func (c *Client) GetChunkRaw(ctx context.Context, table string, cx, cy int64, columns ...string) ([]byte, error) {
	return getChunkRaw(ctx, c, table, cx, cy, columns)
}

func getChunkRaw(ctx context.Context, s session, table string, cx, cy int64, columns []string) ([]byte, error) {
	const command = "GET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return nil, err
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return nil, err
	}
	reply, err := s.call(ctx, "GET CHUNK "+coord(cx)+" "+coord(cy)+" FROM "+name+columnsClause(columns), nil)
	if err != nil {
		return nil, err
	}
	return expectBulk(reply, command)
}

// SetChunk replaces every column of chunk (cx, cy) in table ("" is the default
// table) with chunk, encoded by the client's cached schema ([EncodeChunk]), and
// returns the chunk's new version. chunk.Version is not read; pass
// [IfVersion] to make the write conditional.
//
// When the table's schema version differs from the cached one, the server
// refuses the form and changes nothing; the client fetches the schema, encodes
// chunk again and sends it once more. A chunk whose columns no longer match the
// table's then fails to encode.
func (c *Client) SetChunk(ctx context.Context, table string, cx, cy int64, chunk *Chunk, opts ...WriteOption) (uint64, error) {
	reply, err := setChunk(ctx, c, table, cx, cy, chunk, ifVersionClause(opts))
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

func setChunk(ctx context.Context, s session, table string, cx, cy int64, chunk *Chunk, clause string) (Reply, error) {
	const command = "SET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	reply, err := withSchema(ctx, s, name, func(schema *Schema) (Reply, error) {
		form, err := EncodeChunk(schema, chunk)
		if err != nil {
			return Reply{}, err
		}
		reply, err := setChunkForm(ctx, s, name, cx, cy, form, clause)
		if errors.Is(err, ErrSchemaMismatch) {
			return Reply{}, &staleSchemaError{err: err}
		}
		return reply, err
	})
	return reply, annotate(command, err)
}

// SetChunkRaw replaces every column of chunk (cx, cy) in table ("" is the
// default table) with a chunk form of every column, as [Client.GetChunkRaw]
// reads it, and returns the chunk's new version. The chunk version in the form
// is not read; a form of another schema version than the table's fails with a
// [*SchemaMismatchError].
func (c *Client) SetChunkRaw(ctx context.Context, table string, cx, cy int64, form []byte, opts ...WriteOption) (uint64, error) {
	reply, err := setChunkRaw(ctx, c, table, cx, cy, form, ifVersionClause(opts))
	if err != nil {
		return 0, err
	}
	return versionOf(reply, "SET CHUNK")
}

func setChunkRaw(ctx context.Context, s session, table string, cx, cy int64, form []byte, clause string) (Reply, error) {
	const command = "SET CHUNK"
	name, err := s.tableName(command, table)
	if err != nil {
		return Reply{}, err
	}
	if form == nil {
		return Reply{}, requestErrorf(command, "the chunk form is nil")
	}
	return setChunkForm(ctx, s, name, cx, cy, form, clause)
}

func setChunkForm(ctx context.Context, s session, table string, cx, cy int64, form []byte, clause string) (Reply, error) {
	return s.call(ctx, "SET CHUNK "+coord(cx)+" "+coord(cy)+" IN "+table+" $1"+clause, [][]byte{form})
}

// GetArea reads the chunks from (cx0, cy0) to (cx1, cy1) of table ("" is the
// default table), decoded like [Client.GetChunk]. It returns only chunks with a
// present block, in ascending CX then CY. One read covers at most
// [ServerInfo.MaxAreaChunks] chunks.
func (c *Client) GetArea(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) ([]AreaChunk, error) {
	return getArea(ctx, c, table, areaRange(cx0, cy0, cx1, cy1), columns)
}

// GetAreaAround reads the chunks within radius chunks of (cx, cy), like
// [Client.GetArea].
func (c *Client) GetAreaAround(ctx context.Context, table string, cx, cy, radius int64, columns ...string) ([]AreaChunk, error) {
	area, err := areaAround(cx, cy, radius)
	if err != nil {
		return nil, err
	}
	return getArea(ctx, c, table, area, columns)
}

func areaRange(cx0, cy0, cx1, cy1 int64) string {
	return coord(cx0) + " " + coord(cy0) + " TO " + coord(cx1) + " " + coord(cy1)
}

func areaAround(cx, cy, radius int64) (string, error) {
	if radius < 0 {
		return "", requestErrorf("GET AREA", "the radius must not be negative")
	}
	return "AROUND " + coord(cx) + " " + coord(cy) + " RADIUS " + coord(radius), nil
}

func getArea(ctx context.Context, s session, table, area string, columns []string) ([]AreaChunk, error) {
	const command = "GET AREA"
	name, err := s.tableName(command, table)
	if err != nil {
		return nil, err
	}
	if err := checkNames(command, "column", columns...); err != nil {
		return nil, err
	}
	chunks, err := withSchema(ctx, s, name, func(schema *Schema) ([]AreaChunk, error) {
		indexes, err := formColumns(schema, columns)
		if err != nil {
			return nil, &staleSchemaError{err: err, local: true}
		}
		names := columnNames(schema, indexes)
		reply, err := s.call(ctx, "GET AREA "+area+" FROM "+name+columnsClause(names), nil)
		if err != nil {
			if isNoColumn(err) {
				return nil, &staleSchemaError{err: err}
			}
			return nil, err
		}
		if reply.Kind != ReplyArray {
			return nil, protocolErrorf(command, "expected an array, got a reply of kind %d", reply.Kind)
		}
		chunks := make([]AreaChunk, 0, len(reply.Array))
		for _, item := range reply.Array {
			if item.Kind != ReplyArray || len(item.Array) != 3 {
				return nil, protocolErrorf(command, "expected [cx, cy, chunk] entries")
			}
			cx, okX := item.Array[0].Int64()
			cy, okY := item.Array[1].Int64()
			form, err := expectBulk(item.Array[2], command)
			if !okX || !okY || err != nil {
				return nil, protocolErrorf(command, "expected [cx, cy, chunk] entries")
			}
			chunk, err := decodeOrStale(schema, form, names)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, AreaChunk{CX: cx, CY: cy, Chunk: chunk})
		}
		return chunks, nil
	})
	return chunks, annotate(command, err)
}

// ScanChunks lists chunks of table ("" is the default table) that have a
// present block, in ascending CX then CY: the first ones, or those after
// after. limit 0 takes the server's default and maximum,
// [ServerInfo.MaxScanLimit]. [Client.AllChunks] walks every page.
func (c *Client) ScanChunks(ctx context.Context, table string, after *ChunkCoord, limit int) (ScanPage, error) {
	const command = "SCAN CHUNKS"
	name, err := c.tableName(command, table)
	if err != nil {
		return ScanPage{}, err
	}
	if limit < 0 {
		return ScanPage{}, requestErrorf(command, "the limit must not be negative")
	}
	statement := "SCAN CHUNKS FROM " + name
	if after != nil {
		statement += " AFTER " + coord(after.CX) + " " + coord(after.CY)
	}
	if limit > 0 {
		statement += " LIMIT " + strconv.Itoa(limit)
	}
	reply, err := c.call(ctx, statement, nil)
	if err != nil {
		return ScanPage{}, err
	}
	chunks, okChunks := reply.Lookup("chunks")
	more, okMore := reply.Lookup("more")
	if reply.Kind != ReplyMap || !okChunks || chunks.Kind != ReplyArray || !okMore || more.Kind != ReplyBoolean {
		return ScanPage{}, protocolErrorf(command, "expected a map of chunks and more")
	}
	page := ScanPage{Chunks: make([]ChunkCoord, 0, len(chunks.Array)), More: more.Bool}
	for _, item := range chunks.Array {
		if item.Kind != ReplyArray || len(item.Array) != 2 {
			return ScanPage{}, protocolErrorf(command, "expected [cx, cy] entries")
		}
		cx, okX := item.Array[0].Int64()
		cy, okY := item.Array[1].Int64()
		if !okX || !okY {
			return ScanPage{}, protocolErrorf(command, "expected [cx, cy] entries")
		}
		page.Chunks = append(page.Chunks, ChunkCoord{CX: cx, CY: cy})
	}
	if page.More && len(page.Chunks) == 0 {
		return ScanPage{}, protocolErrorf(command, "a page without chunks reports more")
	}
	return page, nil
}

// AllChunks walks every page of [Client.ScanChunks], limit chunks at a time
// (0 takes the server's default). It stops at the first error, which it
// yields.
//
//	for coord, err := range client.AllChunks(ctx, "", 0) {
//		if err != nil {
//			return err
//		}
//		...
//	}
func (c *Client) AllChunks(ctx context.Context, table string, limit int) iter.Seq2[ChunkCoord, error] {
	return func(yield func(ChunkCoord, error) bool) {
		var after *ChunkCoord
		for {
			page, err := c.ScanChunks(ctx, table, after, limit)
			if err != nil {
				yield(ChunkCoord{}, err)
				return
			}
			for _, chunk := range page.Chunks {
				if !yield(chunk, nil) {
					return
				}
			}
			if !page.More {
				return
			}
			last := page.Chunks[len(page.Chunks)-1]
			after = &last
		}
	}
}

// Ping checks that the connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	reply, err := c.call(ctx, "PING", nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, "PING", "PONG")
}

// FlushWAL returns once every write acknowledged before it is durable, on
// every table.
func (c *Client) FlushWAL(ctx context.Context) error {
	reply, err := c.call(ctx, "FLUSH WAL", nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, "FLUSH WAL", "OK")
}

// Metrics returns the server's metrics in the Prometheus text format.
func (c *Client) Metrics(ctx context.Context) (string, error) {
	reply, err := c.call(ctx, "SHOW METRICS", nil)
	if err != nil {
		return "", err
	}
	text, err := expectBulk(reply, "SHOW METRICS")
	return string(text), err
}
