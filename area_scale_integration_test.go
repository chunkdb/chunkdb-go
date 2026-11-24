package chunkdb

import "testing"

func TestIntegrationDownsampledAreaPages(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, client, "terrain", TableSpec{ChunkWidth: 4, ChunkHeight: 4, Columns: []ColumnDef{{Name: "h", Type: TypeUint(8)}, {Name: "kind", Type: TypeUint(8), Null: true}}})
	for _, block := range []struct{ x, y, h int64 }{{0, 0, 3}, {1, 1, 7}, {2, 0, 5}, {5, 0, 1}} {
		if _, err := client.SetBlock(ctx, "terrain", block.x, block.y, Record{"h": block.h}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := client.GetAreaPage(ctx, "terrain", 0, 0, 1, 0, AreaPageOptions{Scale: 2, Reduction: ReduceMax}, "h")
	if err != nil || len(page.Chunks) != 2 {
		t.Fatalf("%+v %v", page, err)
	}
	first := page.Chunks[0].Chunk
	if first.Scale != 2 || first.Reduction != ReduceMax || first.Width != 2 || first.Height != 2 || first.Block(0, 0)["h"] != uint64(7) || first.Block(1, 0)["h"] != uint64(5) || first.Block(0, 1) != nil {
		t.Fatalf("%+v", first)
	}
	if second := page.Chunks[1].Chunk; second.Block(0, 0)["h"] != uint64(1) {
		t.Fatalf("%+v", second)
	}
	minimum, err := client.GetAreaPage(ctx, "terrain", 0, 0, 0, 0, AreaPageOptions{Scale: 4, Reduction: ReduceMin})
	if err != nil || len(minimum.Chunks) != 1 || minimum.Chunks[0].Chunk.Width != 1 || minimum.Chunks[0].Chunk.Block(0, 0)["h"] != uint64(3) {
		t.Fatalf("%+v %v", minimum, err)
	}
	if _, err := client.GetAreaPage(ctx, "terrain", 0, 0, 0, 0, AreaPageOptions{Scale: 3, Reduction: ReduceMax}); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("a factor that does not divide the chunk: %v", err)
	}
	if _, err := client.SetChunk(ctx, "terrain", 0, 0, first); err == nil {
		t.Fatal("wrote a downsampled chunk")
	}
}
