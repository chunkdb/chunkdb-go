package chunkdb

import (
	"encoding/binary"
	"errors"
	"testing"
)

// withScaleSection appends section 7 to a form, which the client's own
// serializer refuses to write.
func withScaleSection(t *testing.T, data []byte, scale uint32, reduction uint16) []byte {
	t.Helper()
	form, err := parseSectionedForm(data, 1<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	body := binary.LittleEndian.AppendUint32(nil, scale)
	body = binary.LittleEndian.AppendUint16(body, reduction)
	body = binary.LittleEndian.AppendUint16(body, 0)
	form.sections = append(form.sections, chunkFormSection{kind: sectionScale, body: body})
	out := append([]byte{}, data[:sectionedHeaderBytes]...)
	binary.LittleEndian.PutUint16(out[32:], uint16(len(form.sections)))
	offset := sectionedHeaderBytes + len(form.sections)*sectionDirectoryBytes
	for _, section := range form.sections {
		out = binary.LittleEndian.AppendUint16(out, section.kind)
		out = binary.LittleEndian.AppendUint16(out, 0)
		out = binary.LittleEndian.AppendUint32(out, uint32(offset))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(section.body)))
		offset += len(section.body)
	}
	for _, section := range form.sections {
		out = append(out, section.body...)
	}
	return out
}

func TestDownsampledForm(t *testing.T) {
	columns := []Column{{ID: 1, Name: "h", Type: TypeUint(8)}}
	samples := &Schema{Table: "t", Dims: 2, Version: 3, ChunkWidth: 2, ChunkHeight: 2, ChunkDepth: 1, Columns: columns}
	full := &Schema{Table: "t", Dims: 2, Version: 3, ChunkWidth: 4, ChunkHeight: 4, ChunkDepth: 1, Columns: columns}
	chunk := NewChunk(samples)
	chunk.SetBlock(1, 0, Record{"h": 9})
	data, err := encodeSectionedChunk(samples, chunk)
	if err != nil {
		t.Fatal(err)
	}
	scaled := withScaleSection(t, data, 2, 1)
	read, err := decodeAreaChunk(full, scaled, nil, 2, ReduceMax)
	if err != nil || read.Scale != 2 || read.Reduction != ReduceMax || read.Width != 2 || read.Height != 2 || read.Block(1, 0)["h"] != uint64(9) {
		t.Fatalf("%+v %v", read, err)
	}
	if _, err := decodeAreaChunk(full, scaled, nil, 2, ReduceMin); !errors.Is(err, ErrProtocol) {
		t.Fatalf("accepted another reduction: %v", err)
	}
	if _, err := decodeSectionedChunk(full, scaled); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a full read accepted a downsampled form: %v", err)
	}
	for _, broken := range [][]byte{withScaleSection(t, data, 3, 1), withScaleSection(t, data, 2, 9), withScaleSection(t, data, 0, 1)} {
		if _, err := decodeAreaChunk(full, broken, nil, 2, ReduceMax); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted a broken scale section: %v", err)
		}
	}
	if _, err := EncodeChunk(full, read); !errors.Is(err, ErrProtocol) {
		t.Fatalf("encoded a downsampled chunk: %v", err)
	}
	for _, bad := range []struct {
		scale     int
		reduction Reduction
	}{{2, ""}, {0, ReduceMax}, {-1, ReduceMax}, {2, "MEDIAN"}} {
		if _, err := scaleClause(bad.scale, bad.reduction); !errors.Is(err, ErrProtocol) {
			t.Fatal(bad, err)
		}
	}
	if clause, err := scaleClause(4, ReduceMode); err != nil || clause != " SCALE 4 USING MODE" {
		t.Fatal(clause, err)
	}
}
