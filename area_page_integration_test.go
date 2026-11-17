package chunkdb

import (
	"errors"
	"fmt"
	"testing"
)

func TestIntegrationConfiguredAreaPages(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		for _, dims := range []int{2, 3} {
			t.Run(fmt.Sprintf("tls=%v/dims=%d", tlsEnabled, dims), func(t *testing.T) {
				server := startServer(t, serverConfig{tls: tlsEnabled, args: []string{"--max-area-chunks", "2", "--max-area-response-bytes", "256", "--txn-max-written-chunks", "1", "--txn-max-read-chunks", "2"}})
				client := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled; o.Table = "space" })
				info := client.ServerInfo()
				if info.MaxAreaChunks != 2 || info.MaxResponseBytes != 256 || info.Limits.TxnMaxWrittenChunks != 1 || info.Limits.TxnMaxReadChunks != 2 {
					t.Fatal(info)
				}
				spec := TableSpec{ChunkWidth: 1, ChunkHeight: 1, Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}}, ChunkColumns: []ColumnDef{{Name: "tag", Type: TypeUint(8), Default: 7}}}
				if dims == 3 {
					spec.ChunkDepth = 1
				}
				createTable(t, client, "space", spec)
				if dims == 2 {
					if _, err := client.SetBlock(t.Context(), "", 5, 0, Record{"n": 9}); err != nil {
						t.Fatal(err)
					}
					first, err := client.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{Limit: 2})
					if err != nil || len(first.Chunks) != 0 || !first.More || first.After == nil || *first.After != (ChunkCoord{1, 0}) {
						t.Fatal(first, err)
					}
					last, err := client.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{Limit: 2, After: &ChunkCoord{3, 0}}, "n")
					if err != nil || last.More || len(last.Chunks) != 1 || last.Chunks[0].CX != 5 || last.Chunks[0].Chunk.Metadata["tag"] != uint64(7) || last.After != nil {
						t.Fatal(last, err)
					}
					if raw, err := client.Do(t.Context(), `GET AREA 0 0TO 5 0FROM space COLUMNS n LIMIT 1AFTER 4 0WITH SCHEMA`); err != nil || raw.Kind != ReplyArray {
						t.Fatal("compressed area tokens", raw, err)
					}
					if raw, err := client.Do(t.Context(), "GET AREA-1-2 TO 0 0 FROM space LIMIT 1"); err != nil || raw.Kind != ReplyMap {
						t.Fatal("compressed area prefix", raw, err)
					}
					done, err := client.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{After: &ChunkCoord{5, 0}})
					if err != nil || done.After != nil || done.More {
						t.Fatal(done, err)
					}
					if _, err := client.Transaction(t.Context(), func(tx *Tx) error {
						_, err := tx.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{Limit: 2})
						return err
					}); err != nil {
						t.Fatal(err)
					}
					for x := range int64(2) {
						if _, err := client.SetBlock(t.Context(), "", x, 0, Record{"n": x + 1}); err != nil {
							t.Fatal(err)
						}
					}
					short, err := client.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{Limit: 2})
					if err != nil || len(short.Chunks) != 1 || short.After == nil || short.After.CX != 0 || !short.More {
						t.Fatal(short, err)
					}
					next, err := client.GetAreaPage(t.Context(), "", 0, 0, 5, 0, AreaPageOptions{Limit: 2, After: short.After})
					if err != nil || len(next.Chunks) != 1 || next.Chunks[0].CX != 1 || next.After.CX != 2 {
						t.Fatal(next, err)
					}
					if _, err := client.GetArea(t.Context(), "", 0, 0, 5, 0); !areaInvalidArgument(err) {
						t.Fatal("unpaged requested-count bound", err)
					}
				} else {
					if _, err := client.SetBlock3D(t.Context(), "", 5, 0, 0, Record{"n": 9}); err != nil {
						t.Fatal(err)
					}
					first, err := client.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{Limit: 2})
					if err != nil || len(first.Chunks) != 0 || !first.More || first.After == nil || *first.After != (ChunkCoord3D{1, 0, 0}) {
						t.Fatal(first, err)
					}
					last, err := client.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{Limit: 2, After: &ChunkCoord3D{3, 0, 0}}, "n")
					if err != nil || last.More || last.After != nil || len(last.Chunks) != 1 || last.Chunks[0].CZ != 0 || last.Chunks[0].Chunk.Metadata["tag"] != uint64(7) {
						t.Fatal(last, err)
					}
					if raw, err := client.Do(t.Context(), `GET AREA 0 0 0TO 5 0 0FROM space COLUMNS n LIMIT 1AFTER 4 0 0WITH SCHEMA`); err != nil || raw.Kind != ReplyArray {
						t.Fatal("compressed area tokens", raw, err)
					}
					if raw, err := client.Do(t.Context(), "GET AREA-1-2-3 TO 0 0 0 FROM space LIMIT 1"); err != nil || raw.Kind != ReplyMap {
						t.Fatal("compressed area prefix", raw, err)
					}
					done, err := client.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{After: &ChunkCoord3D{5, 0, 0}})
					if err != nil || done.After != nil {
						t.Fatal(done, err)
					}
					if _, err := client.Transaction(t.Context(), func(tx *Tx) error {
						_, err := tx.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{Limit: 2})
						return err
					}); err != nil {
						t.Fatal(err)
					}
					for x := range int64(2) {
						if _, err := client.SetBlock3D(t.Context(), "", x, 0, 0, Record{"n": x + 1}); err != nil {
							t.Fatal(err)
						}
					}
					short, err := client.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{Limit: 2})
					if err != nil || len(short.Chunks) != 1 || short.After == nil || short.After.CX != 0 || short.After.CZ != 0 || !short.More {
						t.Fatal(short, err)
					}
					next, err := client.GetAreaPage3D(t.Context(), "", 0, 0, 0, 5, 0, 0, AreaPageOptions3D{Limit: 2, After: short.After})
					if err != nil || next.After.CX != 2 {
						t.Fatal(next, err)
					}
					if _, err := client.GetArea3D(t.Context(), "", 0, 0, 0, 5, 0, 0); !areaInvalidArgument(err) {
						t.Fatal("unpaged requested-count bound", err)
					}
				}
			})
		}
	}
}

func areaInvalidArgument(err error) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.ServerCode == CodeInvalidArgument
}
