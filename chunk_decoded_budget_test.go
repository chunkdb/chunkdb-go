package chunkdb

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestSectionedDenseDecodedCellsAreBounded(t *testing.T) {
	schema := &Schema{Dims: 2, Version: 1, ChunkWidth: 4096, ChunkHeight: 2}
	for i := range 513 {
		schema.Columns = append(schema.Columns, Column{ID: uint32(i + 1), Name: fmt.Sprintf("v%d", i), Type: TypeText(8), Null: true})
	}
	form := sectionedForm{header: chunkFormHeader{schema: 1, version: 1}, sections: []chunkFormSection{{sectionPresence, make([]byte, 1024)}, {sectionBlockSpans, make([]byte, 4)}}}
	data, err := serializeSectionedForm(form, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSectionedChunk(schema, data); err == nil || !strings.Contains(err.Error(), "column values") {
		t.Fatal("unbounded dense decode", err)
	}
	form.header.flags = chunkFormProjected
	data, err = serializeSectionedForm(form, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := decodeSectionedChunk(schema, data, "v0")
	if err != nil || len(chunk.Columns) != 1 || len(chunk.Columns["v0"]) != 8192 {
		t.Fatal(chunk, err)
	}
}
