package chunkdb

import "math"

// varEntryHeaderBytes is the column id, block index and length before each
// text or bytes value of a chunk form.
const varEntryHeaderBytes = 12

// chunkFormLimit bounds a checked chunk mutation before sending its frame.
func chunkFormLimit(schema *Schema) (uint64, error) {
	return sectionedChunkFormLimit(schema)
}

// Chunk is a decoded chunk form: its version, the schema version it was
// encoded for, which blocks are present, and the values of its columns.
//
// Block i of the chunk, at local coordinates (lx, ly), is i = ly*Width + lx
// ([Schema.Locate] finds them for a block coordinate).
type Chunk struct {
	// Version identifies the chunk state when read; compare it for equality.
	// A write ignores this field; pass [IfVersion] for a conditional write.
	Version uint64
	// SchemaVersion is the table's schema version the form was encoded for.
	// [EncodeChunk] writes the version of the schema it encodes with instead.
	SchemaVersion uint64
	Width         int
	Height        int
	Depth         int
	// CommitTimeMS is the producing commit's Unix time in milliseconds.
	CommitTimeMS uint64
	// Projected names a block-column projection; metadata is always complete.
	Projected bool
	// Private marks transaction-local state without a producing commit.
	Private bool
	// Metadata holds the values of the table's chunk columns.
	Metadata Record
	// UnknownSections retains opaque transport sections from a read.
	// EncodeChunk refuses them instead of silently republishing unknown data.
	UnknownSections map[uint16][]byte
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
	if schema.Dims == 3 {
		chunk.Depth = schema.ChunkDepth
	} else {
		chunk.Depth = 1
	}
	for _, column := range schema.Columns {
		chunk.Columns[column.Name] = make([]any, count)
	}
	return chunk
}

// Index3D returns x + Width*(y + Height*z), with x varying fastest.
func (c *Chunk) Index3D(lx, ly, lz int) int { return lx + c.Width*(ly+c.Height*lz) }

// Index returns the block index of local coordinates (lx, ly).
func (c *Chunk) Index(lx, ly int) int { return ly*c.Width + lx }

// Block returns the values of block (lx, ly), or nil when it is absent.
func (c *Chunk) Block(lx, ly int) Record {
	return c.blockAt(c.Index(lx, ly))
}

// Block3D returns one local block, or nil when absent.
func (c *Chunk) Block3D(lx, ly, lz int) Record { return c.blockAt(c.Index3D(lx, ly, lz)) }

func (c *Chunk) blockAt(i int) Record {
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
	c.setBlockAt(c.Index(lx, ly), values)
}

// SetBlock3D creates or updates one local block.
func (c *Chunk) SetBlock3D(lx, ly, lz int, values Record) {
	c.setBlockAt(c.Index3D(lx, ly, lz), values)
}

func (c *Chunk) setBlockAt(i int, values Record) {
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
	c.deleteBlockAt(c.Index(lx, ly))
}

// DeleteBlock3D clears one local block and all of its column values.
func (c *Chunk) DeleteBlock3D(lx, ly, lz int) { c.deleteBlockAt(c.Index3D(lx, ly, lz)) }

func (c *Chunk) deleteBlockAt(i int) {
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

// DecodeChunk decodes a checked chunk form with complete chunk metadata.
// columns names its block-column projection; none means every block column.
// The form must match the supplied schema version and active dimensions.
func DecodeChunk(schema *Schema, form []byte, columns ...string) (*Chunk, error) {
	return decodeSectionedChunk(schema, form, columns...)
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
	return encodeSectionedChunk(schema, chunk)
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
