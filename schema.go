package chunkdb

import (
	"bytes"
	"math"
	"slices"
)

// Column is one column of a [Schema].
type Column struct {
	// ID is the column's id, which text and bytes values in a chunk form
	// carry. It is never reused within a table.
	ID   uint32
	Name string
	Type ColumnType
	// Null reports whether the column holds NULL.
	Null bool
	// Required reports whether a new block must give the column a value.
	Required bool
	// Default is the value a new block takes when it gives none, in the Go
	// type of the column's values, or nil for none.
	Default any
}

// Schema is a table as DESCRIBE reports it.
type Schema struct {
	Table string
	// Version is the schema version; every column change raises it.
	Version uint64
	// Columns are in schema order.
	Columns []Column
	// ChunkWidth and ChunkHeight are the blocks of a chunk.
	ChunkWidth  int
	ChunkHeight int
	// LargeWidth and LargeHeight are the chunks of a large chunk.
	LargeWidth  int
	LargeHeight int
	Options     TableOptions
}

// Column returns the column called name.
func (s *Schema) Column(name string) (Column, bool) {
	if i := s.columnIndex(name); i >= 0 {
		return s.Columns[i], true
	}
	return Column{}, false
}

func (s *Schema) columnIndex(name string) int {
	return slices.IndexFunc(s.Columns, func(c Column) bool { return c.Name == name })
}

// BlockCount is the blocks of one chunk.
func (s *Schema) BlockCount() int { return s.ChunkWidth * s.ChunkHeight }

// Locate returns the chunk holding block (x, y) and the block's local
// coordinates in it, as [Chunk.Index] takes them.
func (s *Schema) Locate(x, y int64) (cx, cy int64, lx, ly int) {
	cx, lx = floorDivMod(x, int64(s.ChunkWidth))
	cy, ly = floorDivMod(y, int64(s.ChunkHeight))
	return cx, cy, lx, ly
}

func floorDivMod(v, size int64) (int64, int) {
	q, r := v/size, v%size
	if r < 0 {
		q--
		r += size
	}
	return q, int(r)
}

func (s *Schema) clone() *Schema {
	if s == nil {
		return nil
	}
	out := *s
	out.Columns = make([]Column, len(s.Columns))
	for i, column := range s.Columns {
		column.Default = cloneValue(column.Default)
		out.Columns[i] = column
	}
	return &out
}

func cloneValue(v any) any {
	switch value := v.(type) {
	case []byte:
		return bytes.Clone(value)
	case Bits:
		return Bits{N: value.N, Bytes: bytes.Clone(value.Bytes)}
	}
	return v
}

// parseSchema reads a DESCRIBE reply.
func parseSchema(reply Reply) (*Schema, error) {
	const command = "DESCRIBE"
	bad := func(what string) (*Schema, error) {
		return nil, protocolErrorf(command, "malformed DESCRIBE reply: %s", what)
	}
	if reply.Kind != ReplyMap {
		return bad("not a map")
	}
	schema := &Schema{}

	table, ok := reply.Lookup("table")
	if !ok || table.Kind != ReplyBulk {
		return bad("table")
	}
	schema.Table = string(table.Bulk)
	version, ok := reply.Lookup("version")
	if schema.Version, ok = version.Uint64(); !ok {
		return bad("version")
	}

	columns, ok := reply.Lookup("columns")
	if !ok || columns.Kind != ReplyArray || len(columns.Array) == 0 {
		return bad("columns")
	}
	for _, item := range columns.Array {
		column, err := parseColumn(item)
		if err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, column)
	}

	var err error
	if schema.ChunkWidth, schema.ChunkHeight, err = parsePair(reply, "chunk"); err != nil {
		return nil, err
	}
	if schema.LargeWidth, schema.LargeHeight, err = parsePair(reply, "large"); err != nil {
		return nil, err
	}

	options, ok := reply.Lookup("options")
	if !ok || options.Kind != ReplyMap {
		return bad("options")
	}
	for _, entry := range options.Map {
		if entry.Key.Kind != ReplyBulk {
			return bad("option name")
		}
		value := entry.Value
		number, _ := value.Uint64()
		text := string(value.Bulk)
		switch string(entry.Key.Bulk) {
		case "durability_mode":
			schema.Options.DurabilityMode = text
		case "checkpoint_compression":
			schema.Options.CheckpointCompression = text
		case "checkpoint_updates":
			schema.Options.CheckpointUpdates = number
		case "checkpoint_wal_bytes":
			schema.Options.CheckpointWalBytes = number
		case "wal_group_commit_updates":
			schema.Options.WalGroupCommitUpdates = number
		case "var_max_chunk_bytes":
			schema.Options.VarMaxChunkBytes = number
		case "feed_buffer_bytes":
			schema.Options.FeedBufferBytes = number
		case "slot_max_bytes":
			schema.Options.SlotMaxBytes = number
		}
	}
	return schema, nil
}

func parseColumn(item Reply) (Column, error) {
	bad := func(what string) (Column, error) {
		return Column{}, protocolErrorf("DESCRIBE", "malformed DESCRIBE column: %s", what)
	}
	if item.Kind != ReplyMap {
		return bad("not a map")
	}
	var column Column
	id, ok := item.Lookup("id")
	value, okID := id.Uint64()
	if !ok || !okID || value == 0 || value > math.MaxUint32 {
		return bad("id")
	}
	column.ID = uint32(value)
	name, ok := item.Lookup("name")
	if !ok || name.Kind != ReplyBulk {
		return bad("name")
	}
	column.Name = string(name.Bulk)
	typeName, ok := item.Lookup("type")
	if !ok || typeName.Kind != ReplyBulk {
		return bad("type")
	}
	t, err := ParseColumnType(string(typeName.Bulk))
	if err != nil {
		return bad("type " + string(typeName.Bulk))
	}
	column.Type = t
	null, ok := item.Lookup("null")
	if !ok || null.Kind != ReplyBoolean {
		return bad("null")
	}
	column.Null = null.Bool
	required, ok := item.Lookup("required")
	if !ok || required.Kind != ReplyBoolean {
		return bad("required")
	}
	column.Required = required.Bool
	def, ok := item.Lookup("default")
	if !ok {
		return bad("default")
	}
	if column.Default, err = decodeValue(t, def); err != nil {
		return bad("default of " + column.Name)
	}
	return column, nil
}

func parsePair(reply Reply, key string) (int, int, error) {
	pair, ok := reply.Lookup(key)
	if !ok || pair.Kind != ReplyArray || len(pair.Array) != 2 {
		return 0, 0, protocolErrorf("DESCRIBE", "malformed DESCRIBE reply: %s", key)
	}
	width, okWidth := pair.Array[0].Int64()
	height, okHeight := pair.Array[1].Int64()
	if !okWidth || !okHeight || width <= 0 || height <= 0 || width > 1<<31 || height > 1<<31 {
		return 0, 0, protocolErrorf("DESCRIBE", "malformed DESCRIBE reply: %s", key)
	}
	return int(width), int(height), nil
}

// isName reports whether name is a CQL table, column or option name:
// [a-z_][a-z0-9_]*.
func isName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if c == '_' || (c >= 'a' && c <= 'z') || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

func checkNames(command, what string, names ...string) error {
	for _, name := range names {
		if !isName(name) {
			return requestErrorf(command, "invalid %s name %q: names are [a-z_][a-z0-9_]*", what, name)
		}
	}
	return nil
}
