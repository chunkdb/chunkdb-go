package chunkdb

import (
	"strings"
	"testing"
)

func TestIntegrationChunkModelTwoDimensions(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plain"
		if tlsEnabled {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			server := startServer(t, serverConfig{tls: tlsEnabled})
			c := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled; o.Table = "space" })
			createTable(t, c, "space", TableSpec{ChunkWidth: 2, ChunkHeight: 2,
				Columns:      []ColumnDef{{Name: "n", Type: TypeUint(8)}},
				ChunkColumns: []ColumnDef{{Name: "owner", Type: TypeUint(8), Required: true}, {Name: "label", Type: TypeText(16), Null: true, Default: "seed"}}})
			initial, err := c.SetChunkMetadata(t.Context(), "", 0, 0, Record{"owner": 1}, IfAbsent())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.SetBlock(t.Context(), "", 0, 0, Record{"n": 7}, IfVersion(initial)); err != nil {
				t.Fatal(err)
			}
			chunk, err := c.GetChunk(t.Context(), "", 0, 0, "n")
			if err != nil || chunk == nil || chunk.Metadata["owner"] != uint64(1) || chunk.Metadata["label"] != "seed" || chunk.CommitTimeMS == 0 {
				t.Fatal(chunk, err)
			}
			chunk.SetBlock(0, 0, Record{"n": 9})
			chunk.Metadata = Record{"owner": 99}
			if _, err = c.SetChunkColumns(t.Context(), "", 0, 0, []string{"n"}, chunk); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Transaction(t.Context(), func(tx *Tx) error {
				if err := tx.SetChunkMetadata(t.Context(), "", 0, 0, Record{"label": nil}); err != nil {
					return err
				}
				return tx.SetBlock(t.Context(), "", 0, 0, Record{"n": 10})
			}); err != nil {
				t.Fatal(err)
			}
			chunk, err = c.GetChunk(t.Context(), "", 0, 0)
			if err != nil || chunk.Metadata["owner"] != uint64(1) || chunk.Metadata["label"] != nil || chunk.Block(0, 0)["n"] != uint64(10) {
				t.Fatal(chunk, err)
			}
			// Full writes replace metadata while partial writes preserve it.
			chunk.Metadata = Record{"owner": 3}
			if _, err = c.SetChunk(t.Context(), "", 0, 0, chunk); err != nil {
				t.Fatal(err)
			}
			zero := uint64(0)
			page, err := c.ScanChunksWithOptions(t.Context(), "", ScanOptions{Since: &zero, WithData: true, Columns: []string{"n"}})
			if err != nil || len(page.Chunks) != 1 || page.Chunks[0].Chunk.Metadata["owner"] != uint64(3) || page.Position == nil {
				t.Fatal(page, err)
			}
			raw, err := c.GetChunkRaw(t.Context(), "", 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.SetChunkRaw(t.Context(), "", 1, 0, raw); err != nil {
				t.Fatal(err)
			}
			area, err := c.GetAreaWithSchema(t.Context(), "", 0, 0, 1, 0, "n")
			if err != nil || len(area.Chunks) != 2 || area.Chunks[1].Chunk.Metadata["owner"] != uint64(3) {
				t.Fatal(area, err)
			}
			createTable(t, c, "quota", TableSpec{ChunkWidth: 1, ChunkHeight: 1,
				Columns:      []ColumnDef{{Name: "n", Type: TypeUint(8)}, {Name: "text", Type: TypeText(1024)}},
				ChunkColumns: []ColumnDef{{Name: "label", Type: TypeText(1024), Null: true}}})
			quotaSchema, err := c.Schema(t.Context(), "quota")
			if err != nil {
				t.Fatal(err)
			}
			large := NewChunk(quotaSchema)
			large.SetBlock(0, 0, Record{"text": strings.Repeat("x", 512)})
			large.Metadata = Record{"label": strings.Repeat("y", 512)}
			if _, err = c.SetChunk(t.Context(), "quota", 0, 0, large); err != nil {
				t.Fatal(err)
			}
			if err = c.SetTableOption(t.Context(), "quota", "var_max_chunk_bytes", 13); err != nil {
				t.Fatal(err)
			}
			large, err = c.GetChunk(t.Context(), "quota", 0, 0)
			if err != nil || large.Metadata["label"] != strings.Repeat("y", 512) {
				t.Fatal(large, err)
			}
			if _, err = c.SetChunk(t.Context(), "quota", 0, 0, large); err != nil {
				t.Fatal("non-growing full replacement", err)
			}
			large.Columns["text"][0] = strings.Repeat("x", 400)
			if _, err = c.SetChunkColumns(t.Context(), "quota", 0, 0, []string{"text"}, large); err != nil {
				t.Fatal("shrinking partial replacement", err)
			}
			shrunk, err := c.GetChunk(t.Context(), "quota", 0, 0)
			if err != nil || shrunk.Block(0, 0)["text"] != strings.Repeat("x", 400) || shrunk.Metadata["label"] != strings.Repeat("y", 512) {
				t.Fatal(shrunk, err)
			}
			raw, err = c.GetChunkRaw(t.Context(), "quota", 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.SetChunkRaw(t.Context(), "quota", 0, 0, raw); err != nil {
				t.Fatal("non-growing raw replacement", err)
			}
		})
	}
}
