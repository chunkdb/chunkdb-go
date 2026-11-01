package chunkdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

// worldSchema has a column of every type, with ids that are not 1..n as after
// ALTER, and chunks of 2x2 blocks.
func worldSchema() *Schema {
	return &Schema{
		Table: "world", Version: 3, ChunkWidth: 2, ChunkHeight: 2,
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeUint(10), Required: true},
			{ID: 3, Name: "temp", Type: TypeInt(8), Null: true},
			{ID: 4, Name: "solid", Type: TypeBool()},
			{ID: 5, Name: "h", Type: TypeF32(), Default: float32(1.5)},
			{ID: 6, Name: "d", Type: TypeF64()},
			{ID: 7, Name: "mask", Type: TypeBits(3), Null: true},
			{ID: 9, Name: "name", Type: TypeText(16), Null: true},
			{ID: 12, Name: "blob", Type: TypeBytes(4)},
		},
		Options: TableOptions{VarMaxChunkBytes: 1 << 20},
	}
}

func TestChunkFormLayout(t *testing.T) {
	schema := &Schema{
		Table: "t", Version: 1, ChunkWidth: 2, ChunkHeight: 2,
		Columns: []Column{
			{ID: 1, Name: "a", Type: TypeUint(64)},
			{ID: 2, Name: "b", Type: TypeInt(8), Null: true},
			{ID: 3, Name: "c", Type: TypeText(10), Null: true},
			{ID: 4, Name: "d", Type: TypeBits(5)},
		},
	}
	chunk := NewChunk(schema)
	d, _ := ParseBits("11001")
	chunk.SetBlock(0, 0, Record{"a": uint64(math.MaxUint64), "b": -3, "c": "hi", "d": d})
	chunk.Version = 2

	form, err := EncodeChunk(schema, chunk)
	if err != nil {
		t.Fatalf("EncodeChunk: %v", err)
	}
	// The public encoder emits the checked envelope and stable descriptors.
	parsed, err := parseSectionedForm(form, uint64(len(form)), true)
	if err != nil || parsed.header.schema != 1 || parsed.header.version != 2 ||
		!bytes.Equal(parsed.section(sectionPresence), []byte{1}) {
		t.Fatalf("checked form: %+v, %v", parsed, err)
	}
	spans, err := parseFormBlockSpans(parsed, 4)
	if err != nil || len(spans) != 3 || spans[0].id != 1 || spans[1].id != 2 || spans[2].id != 4 {
		t.Fatalf("stable descriptors: %+v, %v", spans, err)
	}

	decoded, err := DecodeChunk(schema, form)
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	if decoded.Version != 2 || decoded.SchemaVersion != 1 || !reflect.DeepEqual(decoded.Present, []bool{true, false, false, false}) {
		t.Fatalf("got %+v", decoded)
	}
	block := decoded.Block(0, 0)
	if block["a"] != uint64(math.MaxUint64) || block["b"] != int64(-3) || block["c"] != "hi" || !block["d"].(Bits).Equal(d) {
		t.Fatalf("got %v", block)
	}
	if decoded.Block(1, 0) != nil {
		t.Fatal("an absent block has values")
	}
}

func TestChunkRoundTripEveryType(t *testing.T) {
	schema := worldSchema()
	chunk := NewChunk(schema)
	mask, _ := ParseBits("101")
	chunk.SetBlock(0, 0, Record{
		"id": 1023, "temp": int8(-128), "solid": true, "h": float32(-0.25), "d": math.Inf(1),
		"mask": mask, "name": "it's\r\n", "blob": []byte{0, '\r', '\n', 0xff},
	})
	// Unset columns take the default, NULL, or zero.
	chunk.SetBlock(1, 1, Record{"id": 7, "name": ""})
	chunk.SetBlock(0, 1, Record{"id": 0, "temp": 127, "d": math.NaN()})
	chunk.SetBlock(1, 0, Record{"id": 1})
	chunk.DeleteBlock(1, 0)

	form, err := EncodeChunk(schema, chunk)
	if err != nil {
		t.Fatalf("EncodeChunk: %v", err)
	}
	decoded, err := DecodeChunk(schema, form)
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	first := decoded.Block(0, 0)
	if first["id"] != uint64(1023) || first["temp"] != int64(-128) || first["solid"] != true || first["h"] != float32(-0.25) ||
		first["d"] != math.Inf(1) || !first["mask"].(Bits).Equal(mask) || first["name"] != "it's\r\n" ||
		!bytes.Equal(first["blob"].([]byte), []byte{0, '\r', '\n', 0xff}) {
		t.Fatalf("got %v", first)
	}
	last := decoded.Block(1, 1)
	if last["id"] != uint64(7) || last["temp"] != nil || last["solid"] != false || last["h"] != float32(1.5) ||
		last["mask"] != nil || last["name"] != "" || !bytes.Equal(last["blob"].([]byte), []byte{}) {
		t.Fatalf("got %v", last)
	}
	if third := decoded.Block(0, 1); third["temp"] != int64(127) || !math.IsNaN(third["d"].(float64)) {
		t.Fatalf("got %v", third)
	}
	if decoded.Block(1, 0) != nil {
		t.Fatal("a deleted block is present")
	}
	// Encoding the decoded chunk gives the same form.
	again, err := EncodeChunk(schema, decoded)
	if err != nil || !bytes.Equal(again, form) {
		t.Fatalf("re-encoding: %v; the form changed", err)
	}
}

// A form read with COLUMNS holds the sections of the named columns in that
// order, then their text and bytes values.
func TestDecodeChunkWithColumns(t *testing.T) {
	schema := worldSchema()
	chunk := NewChunk(schema)
	chunk.SetBlock(1, 0, Record{"id": 5, "name": "x", "blob": []byte{9}, "temp": -1})
	form, err := encodeSectionedChunk(schema, chunk, "name", "temp")
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeChunk(schema, form, "name", "temp")
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	if len(decoded.Columns) != 2 || decoded.Columns["temp"][1] != int64(-1) || decoded.Columns["name"][1] != "x" {
		t.Fatalf("got %v", decoded.Columns)
	}
}

func TestDecodeChunkRejectsBadForms(t *testing.T) {
	schema := worldSchema()
	chunk := NewChunk(schema)
	chunk.SetBlock(0, 0, Record{"id": 1, "name": "abc"})
	form, _ := EncodeChunk(schema, chunk)
	var variableOffset int
	for i := 0; i < int(binary.LittleEndian.Uint16(form[32:])); i++ {
		entry := form[36+12*i:]
		if binary.LittleEndian.Uint16(entry) == sectionVariable {
			variableOffset = int(binary.LittleEndian.Uint32(entry[4:]))
			break
		}
	}
	if variableOffset == 0 {
		t.Fatal("missing variable section")
	}
	unknownID := bytes.Clone(form)
	binary.LittleEndian.PutUint32(unknownID[variableOffset:], 99)
	absentBlock := bytes.Clone(form)
	binary.LittleEndian.PutUint32(absentBlock[variableOffset+4:], 3)
	otherSchema := bytes.Clone(form)
	binary.LittleEndian.PutUint64(otherSchema[16:], 2)
	cases := map[string][]byte{
		"too short": form[:16], "other schema": otherSchema,
		"cut in a column": form[:100], "cut in a header": form[:variableOffset+6],
		"cut in a value":    form[:variableOffset+12+2],
		"unknown column id": unknownID, "absent block": absentBlock,
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeChunk(schema, bad); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
	if _, err := DecodeChunk(schema, form, "nope"); err == nil {
		t.Fatal("DecodeChunk accepted an unknown column")
	}
}

func TestEncodeChunkRejects(t *testing.T) {
	schema := worldSchema()
	cases := map[string]func(*Chunk){
		"required without value": func(c *Chunk) { c.SetBlock(0, 0, Record{"temp": 1}) },
		"value out of range":     func(c *Chunk) { c.SetBlock(0, 0, Record{"id": 1024}) },
		"unknown column":         func(c *Chunk) { c.SetBlock(0, 0, Record{"id": 1, "nope": 1}) },
		"missing column":         func(c *Chunk) { delete(c.Columns, "blob") },
		"wrong size":             func(c *Chunk) { c.Width = 4 },
		"short column":           func(c *Chunk) { c.Columns["id"] = c.Columns["id"][:2] },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			chunk := NewChunk(schema)
			change(chunk)
			if _, err := EncodeChunk(schema, chunk); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want a request error", err)
			}
		})
	}

	small := worldSchema()
	small.Options.VarMaxChunkBytes = 20
	chunk := NewChunk(small)
	chunk.SetBlock(0, 0, Record{"id": 1, "name": "0123456789"})
	if _, err := EncodeChunk(small, chunk); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want an error over var_max_chunk_bytes", err)
	}
	if _, err := EncodeChunk(small, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want an error for a nil chunk", err)
	}
}

func TestSchemaLocate(t *testing.T) {
	schema := &Schema{ChunkWidth: 16, ChunkHeight: 8}
	cases := []struct {
		x, y   int64
		cx, cy int64
		lx, ly int
	}{
		{0, 0, 0, 0, 0, 0},
		{17, 9, 1, 1, 1, 1},
		{-1, -1, -1, -1, 15, 7},
		{-16, -9, -1, -2, 0, 7},
		{math.MinInt64, math.MaxInt64, math.MinInt64 / 16, math.MaxInt64 / 8, 0, 7},
	}
	for _, c := range cases {
		cx, cy, lx, ly := schema.Locate(c.x, c.y)
		if cx != c.cx || cy != c.cy || lx != c.lx || ly != c.ly {
			t.Fatalf("Locate(%d, %d) = %d %d %d %d", c.x, c.y, cx, cy, lx, ly)
		}
	}
}
