package chunkdb

import (
	"bytes"
	"errors"
	"testing"
)

func TestIntegrationChunkModelThreeDimensions(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plain"
		if tlsEnabled {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			server := startServer(t, serverConfig{tls: tlsEnabled, workers: 4})
			c := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled; o.Table = "space" })
			spec := TableSpec{Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}}, ChunkColumns: []ColumnDef{{Name: "owner", Type: TypeUint(8), Required: true}, {Name: "label", Type: TypeText(16), Null: true, Default: "seed"}}, ChunkWidth: 2, ChunkHeight: 2, ChunkDepth: 2}
			createTable(t, c, "space", spec)
			schema, err := c.Describe(t.Context(), "")
			if err != nil || schema.Dims != 3 {
				t.Fatal(schema, err)
			}
			if chunk, err := c.GetChunk3D(t.Context(), "", 0, 0, 0); err != nil || chunk != nil {
				t.Fatal(chunk, err)
			}
			if _, err := c.SetBlock3D(t.Context(), "", 0, 0, 0, Record{"n": 7}); !isServerCode(err, CodeInvalidArgument) {
				t.Fatal("first block write lacks required metadata", err)
			}
			initial, err := c.SetChunkMetadata3D(t.Context(), "", 0, 0, 0, Record{"owner": 1, "label": "base"}, IfAbsent())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.SetBlock3D(t.Context(), "", 0, 0, 0, Record{"n": 7}, IfVersion(initial)); err != nil {
				t.Fatal(err)
			}
			chunk, err := c.GetChunk3D(t.Context(), "", 0, 0, 0, "n")
			if err != nil || chunk == nil || chunk.Metadata["owner"] != uint64(1) || chunk.Metadata["label"] != "base" {
				t.Fatal(chunk, err)
			}
			chunk.SetBlock3D(0, 0, 0, Record{"n": 9})
			chunk.Metadata = Record{"owner": 99}
			if _, err := c.SetChunkColumns3D(t.Context(), "", 0, 0, 0, []string{"n"}, chunk); err != nil {
				t.Fatal(err)
			}
			if _, err := c.SetChunkMetadata3D(t.Context(), "", 0, 0, 0, Record{"label": nil}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Transaction(t.Context(), func(tx *Tx) error {
				if err := tx.SetChunkMetadata3D(t.Context(), "", 0, 0, 0, Record{"owner": 2}); err != nil {
					return err
				}
				return tx.SetBlock3D(t.Context(), "", 0, 0, 0, Record{"n": 10})
			}); err != nil {
				t.Fatal(err)
			}
			chunk, err = c.GetChunk3D(t.Context(), "", 0, 0, 0)
			if err != nil || chunk.Metadata["owner"] != uint64(2) || chunk.Metadata["label"] != nil || chunk.Block3D(0, 0, 0)["n"] != uint64(10) {
				t.Fatal(chunk, err)
			}
			raw, err := c.GetChunkRaw3D(t.Context(), "", 0, 0, 0)
			if err != nil || !bytes.HasPrefix(raw, []byte("CKCF")) {
				t.Fatal(raw, err)
			}
			if _, err := c.SetChunkMetadata3D(t.Context(), "", 1, 0, 0, Record{"owner": 5}); err != nil {
				t.Fatal(err)
			}
			empty := NewChunk(schema)
			empty.Metadata = Record{"owner": 0}
			deleted, err := c.SetChunk3D(t.Context(), "", 1, 0, 0, empty)
			if err != nil {
				t.Fatal(err)
			}
			tombstone, err := c.GetChunk3D(t.Context(), "", 1, 0, 0)
			if err != nil || tombstone == nil || tombstone.Version != deleted || tombstone.Metadata["label"] != "seed" {
				t.Fatal(tombstone, err)
			}
			zero := uint64(0)
			page, err := c.ScanChunks3DWithOptions(t.Context(), "", ScanOptions3D{Since: &zero, WithData: true, Columns: []string{"n"}})
			if err != nil || page.Epoch != schema.Epoch || page.Position == nil || *page.Position < deleted || len(page.Chunks) != 2 {
				t.Fatal(page, err)
			}
			if page.Chunks[0].Deleted == nil || *page.Chunks[0].Deleted || page.Chunks[0].Chunk.Metadata["owner"] != uint64(2) {
				t.Fatal(page)
			}
			if page.Chunks[1].Deleted == nil || !*page.Chunks[1].Deleted || page.Chunks[1].Version != deleted || page.Chunks[1].Chunk != nil {
				t.Fatal(page)
			}
			live, err := c.ScanChunks3D(t.Context(), "", nil, 1)
			if err != nil || len(live.Chunks) != 1 || live.More {
				t.Fatal(live, err)
			}
			area, err := c.GetArea3DWithSchema(t.Context(), "", 0, 0, 0, 1, 0, 0, "n")
			if err != nil || area.SchemaVersion != schema.Version || len(area.Chunks) != 1 || area.Chunks[0].Chunk.Metadata["owner"] != uint64(2) {
				t.Fatal(area, err)
			}
			ball, err := c.GetAreaAround3D(t.Context(), "", 0, 0, 0, 1)
			if err != nil || len(ball) != 1 || ball[0].CX != 0 || ball[0].CY != 0 || ball[0].CZ != 0 {
				t.Fatal(ball, err)
			}
			_, err = c.SetChunkMetadata3D(t.Context(), "", 0, 0, 0, Record{"owner": 3}, IfAbsent())
			var mismatch *VersionMismatchError
			if !errors.As(err, &mismatch) || mismatch.Current != chunk.Version || mismatch.Current == 0 {
				t.Fatal("occupied IF ABSENT must return current producing version", err)
			}
			unchanged, err := c.GetChunkRaw3D(t.Context(), "", 0, 0, 0)
			if err != nil || !bytes.Equal(raw, unchanged) {
				t.Fatal("failed condition changed chunk state", err)
			}
			quiet, err := c.ScanChunks3DWithOptions(t.Context(), "", ScanOptions3D{Since: page.Position})
			if err != nil || len(quiet.Chunks) != 0 || quiet.Position == nil || *quiet.Position != *page.Position {
				t.Fatal("failed condition advanced the completed frontier", quiet, err)
			}
			if _, err := c.SetChunkMetadata3D(t.Context(), "", 1, 0, 0, Record{"owner": 6}, IfAbsent()); err != nil {
				t.Fatal(err)
			}
			pool, err := ConnectPool(t.Context(), PoolOptions{Options: Options{URI: server.uri + "space", TLSInsecure: tlsEnabled}, MaxConnections: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			row, err := pool.GetBlock3D(t.Context(), "", 0, 0, 0)
			if err != nil || row["n"] != uint64(10) {
				t.Fatal(row, err)
			}
			if err := c.DropTable(t.Context(), "space"); err != nil {
				t.Fatal(err)
			}
			createTable(t, c, "space", spec)
			if _, err := c.ScanChunks3DWithOptions(t.Context(), "", ScanOptions3D{Epoch: page.Epoch}); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}
