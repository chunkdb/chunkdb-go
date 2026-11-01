package chunkdb

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func TestSectionedMetadataRefusalBoundsAllocation(t *testing.T) {
	schema := &Schema{Table: "space", Version: 1, Dims: 3, ChunkWidth: 1, ChunkHeight: 1, ChunkDepth: 1,
		Columns: []Column{{ID: 1, Name: "n", Type: TypeUint(8)}}, Options: TableOptions{VarMaxChunkBytes: 13}}
	metadata := Record{}
	shared := make([]byte, 64*1024)
	for i := 0; i < 128; i++ {
		name := fmt.Sprintf("v%d", i)
		schema.ChunkColumns = append(schema.ChunkColumns, Column{ID: uint32(i + 1), Name: name, Type: TypeBytes(len(shared)), Null: true})
		metadata[name] = shared
	}
	chunk := NewChunk(schema)
	chunk.Metadata = metadata
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := encodeSectionedChunk(schema, chunk)
	runtime.ReadMemStats(&after)
	if err == nil || !strings.Contains(err.Error(), "variable data") {
		t.Fatal(err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4*1024*1024 {
		t.Fatalf("refused metadata expanded repeated payloads: allocated %d bytes", allocated)
	}
}
