package chunkdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
)

// varEntryHeaderBytes is the column id, block index and length before each
// text or bytes value of a chunk form.
const varEntryHeaderBytes = 12

// formHeaderBytes is the chunk version and the schema version that start a
// chunk form.
const formHeaderBytes = 16

// Chunk is a decoded chunk form: its version, the schema version it was
// encoded for, which blocks are present, and the values of its columns.
//
// Block i of the chunk, at local coordinates (lx, ly), is i = ly*Width + lx
// ([Schema.Locate] finds them for a block coordinate).
type Chunk struct {
	// Version is the chunk version the form was read at. A write ignores it;
	// pass [IfVersion] to make a write conditional.
	Version uint64
	// SchemaVersion is the table's schema version the form was encoded for.
	// [EncodeChunk] writes the version of the schema it encodes with instead.
	SchemaVersion uint64
	Width         int
	Height        int
	// Present has one entry per block.
	Present []bool
	// Columns maps a column name to one value per block, in the Go types
	// [EncodeValue] lists. An absent block has nil in every column, and so
	// has a NULL value.
	Columns map[string][]any
}

// NewChunk returns a chunk of schema's size with no present block and every
// column.
func NewChunk(schema *Schema) *Chunk {
	count := schema.BlockCount()
	chunk := &Chunk{
		Width:   schema.ChunkWidth,
		Height:  schema.ChunkHeight,
		Present: make([]bool, count),
		Columns: make(map[string][]any, len(schema.Columns)),
	}
	for _, column := range schema.Columns {
		chunk.Columns[column.Name] = make([]any, count)
	}
	return chunk
}

// Index returns the block index of local coordinates (lx, ly).
func (c *Chunk) Index(lx, ly int) int { return ly*c.Width + lx }

// Block returns the values of block (lx, ly), or nil when it is absent.
func (c *Chunk) Block(lx, ly int) Record {
	i := c.Index(lx, ly)
	if !c.Present[i] {
		return nil
	}
	record := make(Record, len(c.Columns))
	for name, values := range c.Columns {
		record[name] = values[i]
	}
	return record
}

// SetBlock makes block (lx, ly) present and sets the given columns. A column
// that is not given keeps its value; see [EncodeChunk] for nil values.
func (c *Chunk) SetBlock(lx, ly int, values Record) {
	i := c.Index(lx, ly)
	c.Present[i] = true
	for name, value := range values {
		column, ok := c.Columns[name]
		if !ok {
			column = make([]any, len(c.Present))
			c.Columns[name] = column
		}
		column[i] = value
	}
}

// DeleteBlock makes block (lx, ly) absent and clears its values.
func (c *Chunk) DeleteBlock(lx, ly int) {
	i := c.Index(lx, ly)
	c.Present[i] = false
	for _, values := range c.Columns {
		values[i] = nil
	}
}

// formColumns resolves the columns a chunk form covers: names in their
// order, or every column of schema.
func formColumns(schema *Schema, names []string) ([]int, error) {
	if len(names) == 0 {
		indexes := make([]int, len(schema.Columns))
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, nil
	}
	indexes := make([]int, 0, len(names))
	for _, name := range names {
		i := schema.columnIndex(name)
		if i < 0 {
			return nil, requestErrorf("", "table %s has no column %s", schema.Table, name)
		}
		indexes = append(indexes, i)
	}
	return indexes, nil
}

// DecodeChunk decodes a chunk form of a table with schema: the chunk version
// and the schema version (u64 little-endian each), the presence bitmap, per
// fixed-width column its values and, for a NULL column, its validity bits,
// then the text and bytes values. columns names the columns the form was read
// with (GET CHUNK ... COLUMNS), in that order; none means every column. A form
// of another schema version than schema's is an error.
// Text and bytes values are mapped to columns by [Column.ID].
func DecodeChunk(schema *Schema, form []byte, columns ...string) (*Chunk, error) {
	indexes, err := formColumns(schema, columns)
	if err != nil {
		return nil, err
	}
	bad := func(format string, args ...any) (*Chunk, error) {
		return nil, protocolErrorf("", "chunk form of table "+schema.Table+": "+format, args...)
	}
	count := schema.BlockCount()
	presenceBytes := (count + 7) / 8
	if len(form) < formHeaderBytes+presenceBytes {
		return bad("%d bytes is shorter than its versions and presence", len(form))
	}
	chunk := &Chunk{
		Version:       binary.LittleEndian.Uint64(form),
		SchemaVersion: binary.LittleEndian.Uint64(form[8:]),
		Width:         schema.ChunkWidth,
		Height:        schema.ChunkHeight,
		Present:       make([]bool, count),
		Columns:       make(map[string][]any, len(indexes)),
	}
	if chunk.SchemaVersion != schema.Version {
		return bad("encoded for schema version %d, not %d", chunk.SchemaVersion, schema.Version)
	}
	presence := form[formHeaderBytes : formHeaderBytes+presenceBytes]
	for i := range count {
		chunk.Present[i] = bitAt(presence, i)
	}
	offset := formHeaderBytes + presenceBytes

	var varColumns []int
	for _, index := range indexes {
		column := schema.Columns[index]
		values := make([]any, count)
		chunk.Columns[column.Name] = values
		width, fixed := column.Type.fixedWidth()
		if !fixed {
			varColumns = append(varColumns, index)
			for i := range count {
				if chunk.Present[i] && !column.Null {
					values[i] = emptyValue(column.Type)
				}
			}
			continue
		}
		valueBytes := (count*width + 7) / 8
		validityBytes := 0
		if column.Null {
			validityBytes = presenceBytes
		}
		if len(form) < offset+valueBytes+validityBytes {
			return bad("%d bytes ends inside column %s", len(form), column.Name)
		}
		section := form[offset : offset+valueBytes]
		validity := form[offset+valueBytes : offset+valueBytes+validityBytes]
		for i := range count {
			if !chunk.Present[i] || (column.Null && !bitAt(validity, i)) {
				continue
			}
			values[i] = readFixed(column.Type, section, i*width, width)
		}
		offset += valueBytes + validityBytes
	}

	for offset < len(form) {
		if len(form)-offset < varEntryHeaderBytes {
			return bad("a text or bytes value is cut off")
		}
		id := binary.LittleEndian.Uint32(form[offset:])
		block := int(binary.LittleEndian.Uint32(form[offset+4:]))
		length := int(binary.LittleEndian.Uint32(form[offset+8:]))
		offset += varEntryHeaderBytes
		if length > len(form)-offset {
			return bad("a text or bytes value is cut off")
		}
		value := form[offset : offset+length]
		offset += length
		index, err := varColumnOf(schema, varColumns, id)
		if err != nil {
			return nil, err
		}
		if block >= count || !chunk.Present[block] {
			return bad("a value of column %s belongs to an absent block %d", schema.Columns[index].Name, block)
		}
		column := schema.Columns[index]
		if column.Type.Kind == ColumnText {
			chunk.Columns[column.Name][block] = string(value)
		} else {
			chunk.Columns[column.Name][block] = bytes.Clone(value)
		}
	}
	return chunk, nil
}

// varColumnOf maps the column id of a text or bytes value to a schema index
// among the form's text and bytes columns.
func varColumnOf(schema *Schema, varColumns []int, id uint32) (int, error) {
	for _, index := range varColumns {
		if schema.Columns[index].ID == id {
			return index, nil
		}
	}
	return 0, protocolErrorf("", "chunk form of table %s: a text or bytes value of column id %d, which it was not read with", schema.Table, id)
}

func emptyValue(t ColumnType) any {
	if t.Kind == ColumnText {
		return ""
	}
	return []byte{}
}

func bitAt(data []byte, i int) bool { return data[i/8]>>(i%8)&1 == 1 }

// readBits reads n <= 64 bits at bit offset off, lowest bit first.
func readBits(data []byte, off, n int) uint64 {
	var value uint64
	for got := 0; got < n; {
		at := off + got
		shift := at % 8
		take := min(8-shift, n-got)
		value |= uint64(data[at/8]>>shift&(1<<take-1)) << got
		got += take
	}
	return value
}

// writeBits ORs the low n <= 64 bits of value into data at bit offset off.
func writeBits(data []byte, off, n int, value uint64) {
	for put := 0; put < n; {
		at := off + put
		shift := at % 8
		take := min(8-shift, n-put)
		data[at/8] |= byte(value>>put&(1<<take-1)) << shift
		put += take
	}
}

// readFixed decodes value i of a fixed-width column from its section.
func readFixed(t ColumnType, section []byte, off, width int) any {
	if t.Kind == ColumnBits {
		b := NewBits(t.Size)
		for done := 0; done < t.Size; done += 8 {
			b.Bytes[done/8] = byte(readBits(section, off+done, min(8, t.Size-done)))
		}
		return b
	}
	raw := readBits(section, off, width)
	switch t.Kind {
	case ColumnUnsigned:
		return raw
	case ColumnSigned:
		if width < 64 && raw>>(width-1)&1 == 1 {
			raw |= ^uint64(0) << width
		}
		return int64(raw)
	case ColumnBool:
		return raw == 1
	case ColumnF32:
		return math.Float32frombits(uint32(raw))
	default:
		return math.Float64frombits(raw)
	}
}

// writeFixed encodes a normalized value of a fixed-width column into its
// section at bit offset off.
func writeFixed(t ColumnType, section []byte, off, width int, value any) {
	switch v := value.(type) {
	case Bits:
		for done := 0; done < t.Size; done += 8 {
			writeBits(section, off+done, min(8, t.Size-done), uint64(v.Bytes[done/8]))
		}
	case uint64:
		writeBits(section, off, width, v)
	case int64:
		writeBits(section, off, width, uint64(v))
	case bool:
		if v {
			writeBits(section, off, 1, 1)
		}
	case float32:
		writeBits(section, off, 32, uint64(math.Float32bits(v)))
	case float64:
		writeBits(section, off, 64, math.Float64bits(v))
	}
}

// EncodeChunk encodes chunk as the full chunk form of a table with schema,
// every column included, as SET CHUNK takes it. The form carries schema's
// version, which SET CHUNK checks against the table's. The chunk must have schema's
// size and a value slice for every column; absent blocks are written empty.
//
// A nil value of a present block is NULL in a NULL column; in any other column
// it takes the column's default, else zero or empty, and is an error in a
// REQUIRED column without a default.
func EncodeChunk(schema *Schema, chunk *Chunk) ([]byte, error) {
	if chunk == nil {
		return nil, requestErrorf("", "the chunk is nil")
	}
	count := schema.BlockCount()
	if chunk.Width != schema.ChunkWidth || chunk.Height != schema.ChunkHeight || len(chunk.Present) != count {
		return nil, requestErrorf("", "the chunk is %dx%d blocks with %d presence entries; table %s has %dx%d blocks",
			chunk.Width, chunk.Height, len(chunk.Present), schema.Table, schema.ChunkWidth, schema.ChunkHeight)
	}
	for name := range chunk.Columns {
		if schema.columnIndex(name) < 0 {
			return nil, requestErrorf("", "table %s has no column %s", schema.Table, name)
		}
	}
	presenceBytes := (count + 7) / 8
	form := make([]byte, formHeaderBytes+presenceBytes, formHeaderBytes+presenceBytes+count)
	binary.LittleEndian.PutUint64(form, chunk.Version)
	binary.LittleEndian.PutUint64(form[8:], schema.Version)
	for i, present := range chunk.Present {
		if present {
			form[formHeaderBytes+i/8] |= 1 << (i % 8)
		}
	}

	type entry struct {
		id    uint32
		block int
		value []byte
	}
	var entries []entry
	varBytes := uint64(0)
	for _, column := range schema.Columns {
		values, ok := chunk.Columns[column.Name]
		if !ok || len(values) != count {
			return nil, requestErrorf("", "the chunk needs %d values of column %s, got %d", count, column.Name, len(values))
		}
		width, fixed := column.Type.fixedWidth()
		var section, validity []byte
		if fixed {
			section = make([]byte, (count*width+7)/8)
			if column.Null {
				validity = make([]byte, presenceBytes)
			}
		}
		for i, raw := range values {
			if !chunk.Present[i] {
				continue
			}
			value, err := normalize(column.Name, column.Type, raw)
			if err != nil {
				return nil, err
			}
			if value == nil && !column.Null {
				if column.Required && column.Default == nil {
					return nil, requestErrorf("", "column %s is REQUIRED: block %d has no value", column.Name, i)
				}
				value = column.Default
				if value == nil {
					value = zeroValue(column.Type)
				}
			}
			if value == nil {
				continue
			}
			if fixed {
				writeFixed(column.Type, section, i*width, width, value)
				if column.Null {
					validity[i/8] |= 1 << (i % 8)
				}
				continue
			}
			data := encodeNormalized(value)
			if len(data) == 0 && !column.Null {
				// The empty value of a column that cannot be NULL has no entry.
				continue
			}
			entries = append(entries, entry{id: column.ID, block: i, value: data})
			varBytes += uint64(varEntryHeaderBytes + len(data))
		}
		form = append(form, section...)
		form = append(form, validity...)
	}
	if limit := schema.Options.VarMaxChunkBytes; limit > 0 && varBytes > limit {
		return nil, requestErrorf("", "the chunk's text and bytes values take %d bytes; table %s holds at most %d (var_max_chunk_bytes)",
			varBytes, schema.Table, limit)
	}
	slices.SortStableFunc(entries, func(a, b entry) int {
		if a.id != b.id {
			if a.id < b.id {
				return -1
			}
			return 1
		}
		return a.block - b.block
	})
	for _, e := range entries {
		form = binary.LittleEndian.AppendUint32(form, e.id)
		form = binary.LittleEndian.AppendUint32(form, uint32(e.block))
		form = binary.LittleEndian.AppendUint32(form, uint32(len(e.value)))
		form = append(form, e.value...)
	}
	return form, nil
}

func zeroValue(t ColumnType) any {
	switch t.Kind {
	case ColumnUnsigned:
		return uint64(0)
	case ColumnSigned:
		return int64(0)
	case ColumnBool:
		return false
	case ColumnF32:
		return float32(0)
	case ColumnF64:
		return float64(0)
	case ColumnBits:
		return NewBits(t.Size)
	}
	return emptyValue(t)
}
