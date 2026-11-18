package chunkdb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestIntegrationChunkModelFeedForms(t *testing.T) {
	modelReadCases(t, func(t *testing.T, tlsEnabled bool, dims int) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		server := startServer(t, serverConfig{tls: tlsEnabled, workers: 4})
		c := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled })
		spec := TableSpec{ChunkWidth: 2, ChunkHeight: 2,
			Columns:      []ColumnDef{{Name: "n", Type: TypeUint(8)}, {Name: "ignored", Type: TypeUint(8)}, {Name: "text", Type: TypeText(16)}},
			ChunkColumns: []ColumnDef{{Name: "label", Type: TypeText(16), Null: true}}}
		if dims == 3 {
			spec.ChunkDepth = 2
		}
		createTable(t, c, "feed_forms", spec)
		schema, err := c.Describe(ctx, "feed_forms")
		if err != nil {
			t.Fatal(err)
		}
		var unknown *Error
		if _, err := c.Watch(ctx, "feed_forms", WatchOptions{Columns: []string{"text", "missing"}}); !errors.As(err, &unknown) || unknown.Phase != PhaseRequest {
			t.Fatal("projection naming no column", err)
		}
		changes, err := c.Watch(ctx, "feed_forms", WatchOptions{Columns: []string{"text", "n"}})
		if err != nil {
			t.Fatal(err)
		}
		defer changes.Close()
		chunks, err := c.Watch(ctx, "feed_forms", WatchOptions{Columns: []string{"text", "n"}, Chunks: true})
		if err != nil {
			t.Fatal(err)
		}
		defer chunks.Close()
		point := ChunkCoord3D{CX: -1}
		localZ := 0
		if dims == 3 {
			point.CZ, localZ = -1, 1
		}
		a := modelReadAPI{c, "feed_forms", dims}
		data := NewChunk(schema)
		data.Metadata = Record{"label": "before"}
		data.SetBlock3D(1, 0, localZ, Record{"n": 7, "ignored": 9, "text": "selected"})
		first := a.put(t, point, data)
		committed, err := a.get(t, point)
		if err != nil || committed == nil {
			t.Fatal(committed, err)
		}
		deleted := a.put(t, point, NewChunk(schema))
		empty, err := a.get(t, point)
		if err != nil || empty == nil {
			t.Fatal(empty, err)
		}
		// Both commits are queued before reading, so a later deletion cannot
		// replace the earlier complete form or its metadata and timestamp.
		for i, revision := range []uint64{first, deleted} {
			observed, err := waitWatchEvent(ctx, func(ctx context.Context) (Event, error) {
				event, err := changes.Next(ctx)
				if announced, ok := event.(*SchemaEvent); ok && (announced.Dims != dims || len(announced.Columns) != 2 || !reflect.DeepEqual(announced.ChunkColumns, schema.ChunkColumns)) {
					t.Fatalf("projected schema: %+v", announced)
				}
				return event, err
			}, changes.Start(), "change", revision, schema.Version)
			if err != nil {
				t.Fatal(err)
			}
			event := observed.(*ChangeEvent)
			wantTime := committed.CommitTimeMS
			if i == 1 {
				wantTime = empty.CommitTimeMS
			}
			if event.CommitTimeMs != wantTime || event.Tag != nil || event.Txn != nil || len(event.Blocks) != 1 || len(event.ChunkChanges) != 1 {
				t.Fatalf("change envelope: %+v", event)
			}
			block := event.Blocks[0]
			if block.X != int64(-1) || block.Y != int64(0) || (dims == 3 && block.Z != int64(-1)) || (dims == 2 && block.Z != nil) {
				t.Fatalf("block coordinates: %+v", block)
			}
			row := event.ChunkChanges[0]
			if row.CX != point.CX || row.CY != point.CY || (dims == 3 && (row.CZ == nil || *row.CZ != point.CZ)) || (dims == 2 && row.CZ != nil) {
				t.Fatalf("metadata coordinates: %+v", row)
			}
			if i == 0 {
				if block.Before != nil || !reflect.DeepEqual(block.After, Record{"text": "selected", "n": uint64(7)}) || row.Before != nil || !reflect.DeepEqual(row.After, data.Metadata) {
					t.Fatalf("first identity: %+v, %+v", block, row)
				}
			} else if !reflect.DeepEqual(block.Before, Record{"text": "selected", "n": uint64(7)}) || block.After != nil || !reflect.DeepEqual(row.Before, data.Metadata) || !reflect.DeepEqual(row.After, Record{"label": nil}) {
				t.Fatalf("deleted identity: %+v, %+v", block, row)
			}
			whole := nativeFeedMutation(t, ctx, chunks, "chunks", revision, chunks.Start()).(*ChunksEvent)
			if whole.CommitTimeMs != wantTime || whole.Tag != nil || whole.Txn != nil || len(whole.Chunks) != 1 {
				t.Fatalf("chunks envelope: %+v", whole)
			}
			frame := whole.Chunks[0]
			if frame.Version != revision || frame.CX != point.CX || frame.CY != point.CY || (dims == 3 && (frame.CZ == nil || *frame.CZ != point.CZ)) || (dims == 2 && frame.CZ != nil) {
				t.Fatalf("chunk identity: %+v", frame)
			}
			if i == 1 {
				if frame.Form != nil {
					t.Fatal("deleted CHUNKS frame retained a form")
				}
				continue
			}
			decoded, err := DecodeChunk(schema, frame.Form, "text", "n")
			if err != nil || decoded.Version != first || decoded.CommitTimeMS != committed.CommitTimeMS || !decoded.Projected || decoded.Private ||
				!reflect.DeepEqual(decoded.Metadata, data.Metadata) || !reflect.DeepEqual(decoded.Block3D(1, 0, localZ), Record{"text": "selected", "n": uint64(7)}) {
				t.Fatalf("queued projected form: %+v, %v", decoded, err)
			}
		}
		// Schema pushes carry only the projected block columns, in requested
		// order, and keep their stable IDs across a rename.
		for _, statement := range []string{"ALTER TABLE feed_forms ADD COLUMN extra u8 DEFAULT 0", "ALTER TABLE feed_forms RENAME COLUMN n TO count"} {
			if _, err := c.Do(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		var written uint64
		if dims == 3 {
			written, err = c.SetBlock3D(ctx, "feed_forms", 10, 10, 10, Record{"count": 5, "text": "renamed"})
		} else {
			written, err = c.SetBlock(ctx, "feed_forms", 10, 10, Record{"count": 5, "text": "renamed"})
		}
		if err != nil {
			t.Fatal(err)
		}
		textColumn, _ := schema.Column("text")
		countColumn, _ := schema.Column("n")
		var versions []uint64
		for {
			event, err := changes.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if announced, ok := event.(*SchemaEvent); ok {
				last := "n"
				if len(versions) == 1 {
					last = "count"
				}
				if announced.Dims != dims || len(announced.Columns) != 2 || announced.Columns[0].ID != textColumn.ID || announced.Columns[0].Name != "text" ||
					announced.Columns[1].ID != countColumn.ID || announced.Columns[1].Name != last || !reflect.DeepEqual(announced.ChunkColumns, schema.ChunkColumns) {
					t.Fatalf("projected altered schema: %+v", announced)
				}
				versions = append(versions, announced.Version)
				continue
			}
			change, ok := event.(*ChangeEvent)
			if !ok {
				continue
			}
			if change.Position.Revision != written || change.SchemaVersion != schema.Version+2 || len(change.Blocks) != 1 ||
				!reflect.DeepEqual(change.Blocks[0].After, Record{"text": "renamed", "count": uint64(5)}) {
				t.Fatalf("change after rename: %+v", change)
			}
			break
		}
		if !reflect.DeepEqual(versions, []uint64{schema.Version + 1, schema.Version + 2}) {
			t.Fatalf("schema pushes: %v", versions)
		}
	})
}
