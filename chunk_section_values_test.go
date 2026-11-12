package chunkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

func typedSectionFixture() (*Schema, []chunkFormSection) {
	schema := &Schema{Table: "t", Dims: 2, Version: 7, ChunkWidth: 3, ChunkHeight: 1,
		Columns: []Column{{ID: 20, Name: "left", Type: TypeUint(3)},
			{ID: 17, Name: "future", Type: ColumnType{Kind: ColumnUnknown, RawType: "future"}, Null: true},
			{ID: 4, Name: "right", Type: TypeUint(8)}}}
	spans := make([]byte, 76)
	binary.LittleEndian.PutUint32(spans, 3)
	for i, fields := range [][]uint32{{20, 3, 0, 2, 0, 0}, {17, 9, 2, 4, 0, 1}, {4, 8, 6, 3, 0, 0}} {
		for j, v := range fields {
			binary.LittleEndian.PutUint32(spans[4+24*i+4*j:], v)
		}
	}
	return schema, []chunkFormSection{{1, []byte{7}}, {2, []byte{0xe9, 1, 0xff, 1, 6, 0, 11, 22, 33}},
		{3, []byte{5}}, {6, spans}, {99, []byte{0xad}}}
}

func TestTypedSectionedUnknownColumnKeepsKnownNeighbours(t *testing.T) {
	schema, sections := typedSectionFixture()
	data := sectionFixture(sections)
	chunk, err := decodeSectionedChunk(schema, data)
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Version != 42 || chunk.SchemaVersion != 7 || chunk.CommitTimeMS != 11 || chunk.Depth != 1 {
		t.Fatalf("identity %+v", chunk)
	}
	if !reflect.DeepEqual(chunk.Columns["left"], []any{uint64(1), uint64(5), uint64(7)}) ||
		!reflect.DeepEqual(chunk.Columns["right"], []any{uint64(11), uint64(22), uint64(33)}) {
		t.Fatalf("known columns: %+v", chunk.Columns)
	}
	unknown := chunk.Columns["future"]
	if !bytes.Equal(unknown[0].([]byte), []byte{0xff, 1}) || unknown[1] != nil || !bytes.Equal(unknown[2].([]byte), []byte{1, 0}) {
		t.Fatalf("opaque values: %+v", unknown)
	}
	if !bytes.Equal(chunk.UnknownSections[99], []byte{0xad}) {
		t.Fatal("unknown section lost")
	}
	data[len(data)-1] = 0
	if !bytes.Equal(chunk.UnknownSections[99], []byte{0xad}) {
		t.Fatal("opaque section aliases input")
	}
	binary.LittleEndian.PutUint32(sections[3].body[8:], 4)
	if _, err := decodeSectionedChunk(schema, sectionFixture(sections)); err == nil {
		t.Fatal("schema width mismatch accepted")
	}
}

func metadataSection(emptyText bool) []byte {
	body := make([]byte, 4+3*16)
	binary.LittleEndian.PutUint32(body, 3)
	for i, id := range []uint32{2, 7, 8} {
		binary.LittleEndian.PutUint32(body[4+16*i:], id)
	}
	binary.LittleEndian.PutUint32(body[12:], 52)
	binary.LittleEndian.PutUint32(body[16:], 2)
	if !emptyText {
		binary.LittleEndian.PutUint16(body[24:], 1)
	}
	binary.LittleEndian.PutUint32(body[28:], 54)
	binary.LittleEndian.PutUint32(body[44:], 54)
	return append(body, 3, 0)
}

func TestTypedSectionedMetadataAndCombinedVariableBudget(t *testing.T) {
	schema, sections := typedSectionFixture()
	schema.Columns = append(schema.Columns, Column{ID: 9, Name: "label", Type: TypeText(10), Null: true})
	schema.ChunkColumns = []Column{{ID: 2, Name: "status", Type: TypeUint(10)},
		{ID: 7, Name: "title", Type: TypeText(10), Null: true}, {ID: 8, Name: "empty", Type: TypeBytes(3)}}
	vars := binary.LittleEndian.AppendUint32(nil, 9)
	vars = binary.LittleEndian.AppendUint32(vars, 0)
	vars = binary.LittleEndian.AppendUint32(vars, 2)
	vars = append(vars, 'h', 'i')
	sections = []chunkFormSection{sections[0], sections[1], sections[2], {4, vars}, {5, metadataSection(true)}, sections[3]}
	schema.Options.VarMaxChunkBytes = 26
	chunk, err := decodeSectionedChunk(schema, sectionFixture(sections))
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Metadata["status"] != uint64(3) || chunk.Metadata["title"] != "" || !reflect.DeepEqual(chunk.Metadata["empty"], []byte{}) || chunk.Columns["label"][0] != "hi" {
		t.Fatalf("metadata %+v / columns %+v", chunk.Metadata, chunk.Columns)
	}
	schema.Options.VarMaxChunkBytes = 25
	if _, err := decodeSectionedChunk(schema, sectionFixture(sections)); err != nil {
		t.Fatalf("stored values became unreadable after lowering the write quota: %v", err)
	}
	sections[4].body = metadataSection(false)
	schema.Options.VarMaxChunkBytes = 14
	chunk, err = decodeSectionedChunk(schema, sectionFixture(sections))
	if err != nil || chunk.Metadata["title"] != nil {
		t.Fatalf("NULL metadata: %+v %v", chunk, err)
	}
	sections[3].body[len(vars)-1] = 0xff
	if _, err := decodeSectionedChunk(schema, sectionFixture(sections)); err == nil {
		t.Fatal("invalid text accepted")
	}
}

func TestChunkLocalThreeDimensionalOrder(t *testing.T) {
	schema := &Schema{Dims: 3, ChunkWidth: 2, ChunkHeight: 2, ChunkDepth: 2, Columns: []Column{{ID: 1, Name: "n", Type: TypeUint(8)}}}
	chunk := NewChunk(schema)
	if schema.BlockCount() != 8 || chunk.Index3D(1, 0, 1) != 5 || chunk.Depth != 2 {
		t.Fatalf("geometry %+v", chunk)
	}
	chunk.SetBlock3D(1, 0, 1, Record{"n": uint64(9)})
	if chunk.Block3D(1, 0, 1)["n"] != uint64(9) || chunk.Block(1, 0) != nil {
		t.Fatal("z addressing changed another layer")
	}
	chunk.DeleteBlock3D(1, 0, 1)
	if chunk.Block3D(1, 0, 1) != nil {
		t.Fatal("3D delete retained a block")
	}
	cx, cy, cz, x, y, z := schema.Locate3D(-1, -3, -5)
	if cx != -1 || cy != -2 || cz != -3 || x != 1 || y != 1 || z != 1 {
		t.Fatalf("negative address: %d,%d,%d / %d,%d,%d", cx, cy, cz, x, y, z)
	}
}

func TestSectionedReadUsesCombinedHardVariableLimit(t *testing.T) {
	const hard = 64 * 1024 * 1024
	schema := &Schema{Table: "t", Dims: 2, Version: 7, ChunkWidth: 1, ChunkHeight: 1,
		Columns:      []Column{{ID: 1, Name: "n", Type: TypeUint(1)}, {ID: 2, Name: "data", Type: TypeBytes(hard)}},
		ChunkColumns: []Column{{ID: 3, Name: "title", Type: TypeBytes(1), Null: true}}}
	schema.Options.VarMaxChunkBytes = 25
	spans := make([]byte, 28)
	for i, field := range []uint32{1, 1, 1, 0, 1, 0, 0} {
		binary.LittleEndian.PutUint32(spans[4*i:], field)
	}
	vars := make([]byte, hard-12)
	binary.LittleEndian.PutUint32(vars, 2)
	binary.LittleEndian.PutUint32(vars[8:], uint32(len(vars)-12))
	row := make([]byte, 20)
	binary.LittleEndian.PutUint32(row, 1)
	binary.LittleEndian.PutUint32(row[4:], 3)
	binary.LittleEndian.PutUint32(row[12:], 20)
	sections := []chunkFormSection{{1, []byte{1}}, {2, []byte{0}}, {4, vars}, {5, row}, {6, spans}}
	chunk, err := decodeSectionedChunk(schema, sectionFixture(sections))
	if err != nil || len(chunk.Columns["data"][0].([]byte)) != hard-24 {
		t.Fatalf("exact combined hard limit rejected: %v", err)
	}
	// A nullable empty value costs 12 bytes, and one byte makes the sum overflow.
	row = append(row, 0)
	binary.LittleEndian.PutUint32(row[16:], 1)
	sections[3].body = row
	if _, err := decodeSectionedChunk(schema, sectionFixture(sections)); err == nil || !strings.Contains(err.Error(), "hard byte limit") {
		t.Fatalf("combined hard limit ignored: %v", err)
	}
	sections[2].body = make([]byte, hard+1)
	if _, err := decodeSectionedChunk(schema, sectionFixture(sections)); err == nil || !strings.Contains(err.Error(), "hard byte limit") {
		t.Fatalf("raw variable hard limit ignored: %v", err)
	}
}

func TestSectionedEncoderThreeDimensionalGolden(t *testing.T) {
	schema := &Schema{Table: "t", Dims: 3, Version: 7, ChunkWidth: 2, ChunkHeight: 2, ChunkDepth: 2,
		Columns: []Column{{ID: 1, Name: "n", Type: TypeUint(8)}}}
	chunk := NewChunk(schema)
	chunk.Version, chunk.CommitTimeMS = 42, 11
	chunk.SetBlock3D(1, 0, 1, Record{"n": uint64(9)})
	data, err := encodeSectionedChunk(schema, chunk)
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := hex.DecodeString("01000000010000000800000000000000080000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	want := sectionFixture([]chunkFormSection{{1, []byte{0x20}}, {2, []byte{0, 0, 0, 0, 0, 9, 0, 0}}, {6, descriptors}})
	if !bytes.Equal(data, want) {
		t.Fatalf("3D form got %x want %x", data, want)
	}
	decoded, err := decodeSectionedChunk(schema, data)
	if err != nil || decoded.Block3D(1, 0, 1)["n"] != uint64(9) {
		t.Fatalf("3D read %+v %v", decoded, err)
	}
}

func TestSectionedEncoderValuesMetadataAndProjection(t *testing.T) {
	schema := worldSchema()
	schema.ChunkColumns = []Column{{ID: 3, Name: "status", Type: TypeUint(10), Default: 3},
		{ID: 5, Name: "title", Type: TypeText(10), Null: true}, {ID: 9, Name: "empty", Type: TypeBytes(3)}}
	chunk := NewChunk(schema)
	chunk.SetBlock(0, 0, Record{"id": uint64(23), "temp": int8(-3), "solid": true, "name": "hi"})
	chunk.Metadata = Record{"title": ""}
	data, err := encodeSectionedChunk(schema, chunk)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSectionedChunk(schema, data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Block(0, 0)["id"] != uint64(23) || decoded.Block(0, 0)["temp"] != int64(-3) || decoded.Block(0, 0)["h"] != float32(1.5) || decoded.Block(0, 0)["name"] != "hi" {
		t.Fatalf("values %+v", decoded.Block(0, 0))
	}
	if decoded.Metadata["status"] != uint64(3) || decoded.Metadata["title"] != "" || !reflect.DeepEqual(decoded.Metadata["empty"], []byte{}) {
		t.Fatalf("metadata %+v", decoded.Metadata)
	}
	form, err := parseSectionedForm(data, uint64(len(data)), false)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := parseFormChunkColumns(form)
	if err != nil || !bytes.Equal(rows[0].value, []byte{3, 0}) {
		t.Fatalf("packed metadata: %+v %v", rows, err)
	}
	projected, err := encodeSectionedChunk(schema, chunk, "name")
	if err != nil {
		t.Fatal(err)
	}
	partial, err := decodeSectionedChunk(schema, projected, "name")
	if err != nil || !partial.Projected || len(partial.Columns) != 1 || partial.Columns["name"][0] != "hi" || !reflect.DeepEqual(partial.Metadata, decoded.Metadata) {
		t.Fatalf("projection %+v %v", partial, err)
	}
	if _, err := decodeSectionedChunk(schema, projected); err == nil {
		t.Fatal("projection accepted as a full form")
	}
	schema.Options.VarMaxChunkBytes = 25 // block hi:14; nullable empty title:12.
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("combined variable budget ignored")
	}
	schema.Options.VarMaxChunkBytes = 26
	if _, err := encodeSectionedChunk(schema, chunk); err != nil {
		t.Fatal(err)
	}
	chunk.UnknownSections = map[uint16][]byte{99: {1}}
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("unknown section republished")
	}
	chunk.UnknownSections = nil
	chunk.Private = true
	chunk.Version = 1
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("private producing version accepted")
	}
	chunk.Version = 0
	chunk.CommitTimeMS = 0
	if _, err := encodeSectionedChunk(schema, chunk); err != nil {
		t.Fatal(err)
	}
	chunk.Metadata["status"] = 1024
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("out-of-range metadata accepted")
	}
}

func TestSectionedMetadataDefaultsAndExplicitNull(t *testing.T) {
	schema := &Schema{Table: "t", Dims: 2, Version: 7, ChunkWidth: 1, ChunkHeight: 1,
		Columns:      []Column{{ID: 1, Name: "n", Type: TypeUint(8)}},
		ChunkColumns: []Column{{ID: 2, Name: "title", Type: TypeText(10), Null: true, Default: "base"}}}
	chunk := NewChunk(schema)
	read := func() *Chunk {
		t.Helper()
		data, err := encodeSectionedChunk(schema, chunk)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeSectionedChunk(schema, data)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if read().Metadata["title"] != "base" {
		t.Fatal("missing nullable metadata did not use its default")
	}
	chunk.Metadata = Record{"title": nil}
	if read().Metadata["title"] != nil {
		t.Fatal("explicit NULL did not override the default")
	}
	schema.ChunkColumns[0].Required = true
	schema.ChunkColumns[0].Default = nil
	chunk.Metadata = nil
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("missing REQUIRED nullable metadata accepted")
	}
	chunk.Metadata = Record{"title": nil}
	if read().Metadata["title"] != nil {
		t.Fatal("explicit nullable value was treated as missing")
	}
}

func TestSectionedBlockMutationPreservesRequiredMetadata(t *testing.T) {
	schema := &Schema{Table: "t", Dims: 2, Version: 7, ChunkWidth: 1, ChunkHeight: 1,
		Columns:      []Column{{ID: 1, Name: "n", Type: TypeUint(8)}},
		ChunkColumns: []Column{{ID: 2, Name: "title", Type: TypeText(10), Required: true}}}
	chunk := NewChunk(schema)
	chunk.SetBlock(0, 0, Record{"n": 9})
	data, err := encodeSectionedBlockMutation(schema, chunk, "n")
	if err != nil {
		t.Fatal(err)
	}
	form, err := parseSectionedForm(data, uint64(len(data)), true)
	if err != nil {
		t.Fatal(err)
	}
	if form.header.flags&chunkFormProjected == 0 || form.section(sectionChunkColumns) != nil {
		t.Fatal("block-only mutation replaces metadata")
	}
	spans, err := parseFormBlockSpans(form, 1)
	if err != nil || len(spans) != 1 || !bytes.Equal(spans[0].values, []byte{9}) {
		t.Fatalf("spans: %+v %v", spans, err)
	}
	if _, err := decodeSectionedChunk(schema, data, "n"); err == nil {
		t.Fatal("incomplete mutation row accepted as a read")
	}
}
