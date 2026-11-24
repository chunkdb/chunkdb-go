package chunkdb

import "testing"

// A downsampled 3D area read with a projection keeps the chunk columns and
// numbers samples like blocks with each side divided by the factor. Area
// pages always name their columns, so every form is projected.
func TestIntegrationDownsampledThreeDimensionalArea(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, client, "volume", TableSpec{ChunkWidth: 4, ChunkHeight: 4, ChunkDepth: 4,
		Columns:      []ColumnDef{{Name: "h", Type: TypeUint(8)}, {Name: "kind", Type: TypeUint(8), Null: true}},
		ChunkColumns: []ColumnDef{{Name: "biome", Type: TypeUint(8), Default: 0}}})
	if _, err := client.SetChunkMetadata3D(ctx, "volume", 0, 0, 0, Record{"biome": 6}); err != nil {
		t.Fatal(err)
	}
	for _, block := range []struct{ x, y, z, h int64 }{{0, 0, 0, 3}, {1, 1, 1, 7}, {2, 0, 2, 5}, {3, 3, 3, 1}} {
		if _, err := client.SetBlock3D(ctx, "volume", block.x, block.y, block.z, Record{"h": block.h, "kind": 1}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := client.GetAreaPage3D(ctx, "volume", 0, 0, 0, 0, 0, 0, AreaPageOptions3D{Scale: 2, Reduction: ReduceMax}, "h")
	if err != nil || len(page.Chunks) != 1 {
		t.Fatalf("%+v %v", page, err)
	}
	chunk := page.Chunks[0].Chunk
	if chunk.Scale != 2 || chunk.Reduction != ReduceMax || chunk.Width != 2 || chunk.Height != 2 || chunk.Depth != 2 || !chunk.Projected {
		t.Fatalf("%+v", chunk)
	}
	// Samples (0,0,0), (1,0,1) and (1,1,1) hold the largest h of their groups.
	for _, sample := range []struct{ x, y, z, h int }{{0, 0, 0, 7}, {1, 0, 1, 5}, {1, 1, 1, 1}} {
		if block := chunk.Block3D(sample.x, sample.y, sample.z); len(block) != 1 || block["h"] != uint64(sample.h) {
			t.Fatalf("sample %d %d %d: %+v", sample.x, sample.y, sample.z, block)
		}
	}
	if chunk.Block3D(1, 1, 0) != nil {
		t.Fatal("an empty group has a sample")
	}
	if chunk.Metadata["biome"] != uint64(6) {
		t.Fatalf("metadata %+v", chunk.Metadata)
	}
	first, err := client.GetAreaPage3D(ctx, "volume", 0, 0, 0, 0, 0, 0, AreaPageOptions3D{Scale: 4, Reduction: ReduceFirst})
	if err != nil || len(first.Chunks) != 1 {
		t.Fatalf("%+v %v", first, err)
	}
	if whole := first.Chunks[0].Chunk; whole.Width != 1 || whole.Depth != 1 || whole.Block3D(0, 0, 0)["h"] != uint64(3) || whole.Block3D(0, 0, 0)["kind"] != uint64(1) {
		t.Fatalf("%+v", whole)
	}
}
