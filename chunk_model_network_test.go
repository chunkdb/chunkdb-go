package chunkdb

import (
	"bytes"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
)

func modelDescription(dims int, required bool) string {
	chunk, large := respArray(respInt(2), respInt(2)), respArray(respInt(8), respInt(8))
	if dims == 3 {
		chunk = respArray(respInt(2), respInt(2), respInt(2))
		large = respArray(respInt(8), respInt(8), respInt(8))
	}
	return respMap("table", respBulk("space"), "version", respInt(7), "dims", respInt(int64(dims)),
		"epoch", respBulk(strings.Repeat("a", 32)),
		"columns", respArray(fakeColumn(1, "n", "u8", false, false, respInt(0))),
		"chunk_columns", respArray(fakeColumn(1, "title", "text(8)", true, required, respNull)),
		"chunk", chunk, "large", large,
		"options", respMap("var_max_chunk_bytes", respInt(1<<20)))
}

func newModelServer(t *testing.T, dims int, required bool) *fakeServer {
	t.Helper()
	var mu sync.Mutex
	inTx := make(map[net.Conn]bool)
	return newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, line string) {
		mu.Lock()
		defer mu.Unlock()
		switch commandOf(line) {
		case "DESCRIBE":
			writeRaw(conn, modelDescription(dims, required))
		case "GET BLOCK":
			row := respArray(respInt(3))
			var envelope []string
			if strings.Contains(line, " WITH VERSION") {
				envelope = append(envelope, respInt(9))
			}
			if strings.Contains(line, " WITH SCHEMA") {
				envelope = append(envelope, respInt(7))
			}
			if len(envelope) != 0 {
				row = respArray(append(envelope, row)...)
			}
			writeRaw(conn, row)
		case "GET CHUNK":
			writeRaw(conn, respNull)
		case "GET AREA":
			if strings.Contains(line, " WITH SCHEMA") {
				writeRaw(conn, respArray(respInt(7), respArray()))
			} else {
				writeRaw(conn, respArray())
			}
		case "SET BLOCK", "DELETE BLOCK", "SET CHUNK":
			if inTx[conn] {
				writeRaw(conn, respNull)
			} else {
				writeRaw(conn, respInt(8))
			}
		case "BEGIN":
			inTx[conn] = true
			writeSimple(conn, "OK")
		case "COMMIT":
			inTx[conn] = false
			writeRaw(conn, respInt(42))
		case "ROLLBACK":
			inTx[conn] = false
			writeSimple(conn, "OK")
		default:
			writeSimple(conn, "OK")
		}
	}))
}

func TestChunkModelThreeDimensionalWire(t *testing.T) {
	server := newModelServer(t, 3, true)
	client := newTestClient(t, server, nil)
	schema, err := client.Describe(t.Context(), "space")
	if err != nil {
		t.Fatal(err)
	}
	chunk := NewChunk(schema)
	chunk.SetBlock3D(1, 0, 1, Record{"n": 9})
	chunk.Metadata = Record{"title": "base"}
	form, err := encodeSectionedChunk(schema, chunk)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, want string
		run        func() error
	}{
		{"get", "GET BLOCK -1 2 0 FROM space COLUMNS n", func() error { _, err := client.GetBlock3D(t.Context(), "space", -1, 2, 0); return err }},
		{"metadata", "GET BLOCK -1 2 -9223372036854775808 FROM space COLUMNS n WITH VERSION WITH SCHEMA", func() error {
			read, err := client.GetBlock3DWithMetadata(t.Context(), "space", -1, 2, math.MinInt64)
			if err == nil && (read.Version == nil || *read.Version != 9 || read.SchemaVersion != 7) {
				t.Fatal(read)
			}
			return err
		}},
		{"set", "SET BLOCK 1 2 0 IN space n = $1 IF VERSION 9", func() error {
			_, err := client.SetBlock3D(t.Context(), "space", 1, 2, 0, Record{"n": 7}, IfVersion(9))
			return err
		}},
		{"delete", "DELETE BLOCK 1 2 -3 FROM space IF VERSION 9", func() error { _, err := client.DeleteBlock3D(t.Context(), "space", 1, 2, -3, IfVersion(9)); return err }},
		{"chunk", "GET CHUNK 1 2 0 FROM space COLUMNS n", func() error { _, err := client.GetChunk3D(t.Context(), "space", 1, 2, 0); return err }},
		{"raw", "GET CHUNK 1 2 -3 FROM space", func() error { _, err := client.GetChunkRaw3D(t.Context(), "space", 1, 2, -3); return err }},
		{"replace", "SET CHUNK 1 2 0 IN space $1 IF ABSENT", func() error {
			_, err := client.SetChunk3D(t.Context(), "space", 1, 2, 0, chunk, IfAbsent())
			return err
		}},
		{"raw replace", "SET CHUNK 1 2 3 IN space $1 IF VERSION 9", func() error {
			_, err := client.SetChunkRaw3D(t.Context(), "space", 1, 2, 3, form, IfVersion(9))
			return err
		}},
		{"box", "GET AREA -1 -2 -3 TO 4 5 0 FROM space COLUMNS n", func() error { _, err := client.GetArea3D(t.Context(), "space", -1, -2, -3, 4, 5, 0); return err }},
		{"box schema", "GET AREA 0 0 0 TO 1 2 3 FROM space COLUMNS n WITH SCHEMA", func() error {
			read, err := client.GetArea3DWithSchema(t.Context(), "space", 0, 0, 0, 1, 2, 3)
			if err == nil && read.SchemaVersion != 7 {
				t.Fatal(read)
			}
			return err
		}},
		{"ball", "GET AREA AROUND 1 2 0 RADIUS 4 FROM space COLUMNS n", func() error { _, err := client.GetAreaAround3D(t.Context(), "space", 1, 2, 0, 4); return err }},
		{"meta", "SET CHUNK 1 2 0 IN space META title = $1 IF ABSENT", func() error {
			_, err := client.SetChunkMetadata3D(t.Context(), "space", 1, 2, 0, Record{"title": nil}, IfAbsent())
			if err == nil {
				params := server.lastParams(t)
				if len(params) != 1 || params[0] != nil {
					t.Fatal(params)
				}
			}
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err != nil {
				t.Fatal(err)
			}
			if got := server.lastCommand(t); got != test.want {
				t.Fatalf("wire %q, want %q", got, test.want)
			}
		})
	}
	// A block-only form must not synthesize or overwrite required metadata.
	chunk.Metadata = nil
	if _, err := client.SetChunkColumns3D(t.Context(), "space", 1, 2, 0, []string{"n"}, chunk, IfVersion(9)); err != nil {
		t.Fatal(err)
	}
	if got := server.lastCommand(t); got != "SET CHUNK 1 2 0 IN space COLUMNS n $1 IF VERSION 9" {
		t.Fatal(got)
	}
	params := server.lastParams(t)
	partial, err := parseSectionedForm(params[0], uint64(len(params[0])), true)
	if err != nil || partial.header.flags&chunkFormProjected == 0 || partial.section(sectionChunkColumns) != nil {
		t.Fatalf("metadata was replaced: %+v %v", partial, err)
	}
	spans, err := parseFormBlockSpans(partial, 8)
	if err != nil || len(spans) != 1 || !bytes.Equal(spans[0].values, []byte{0, 0, 0, 0, 0, 9, 0, 0}) {
		t.Fatalf("projection: %+v %v", spans, err)
	}
	before := len(server.commands())
	if _, err := client.SetChunk3D(t.Context(), "space", 0, 0, 0, chunk, IfAbsent(), IfVersion(1)); err == nil {
		t.Fatal("conflicting conditions accepted")
	}
	if _, err := client.SetBlock3D(t.Context(), "space", 0, 0, 0, Record{"n": 1}, IfAbsent()); err == nil {
		t.Fatal("block IF ABSENT ignored")
	}
	if _, err := client.SetBlock(t.Context(), "space", 0, 0, Record{"n": 1}, IfAbsent()); err == nil {
		t.Fatal("two-axis block IF ABSENT ignored")
	}
	if len(server.commands()) != before {
		t.Fatal("invalid mutation reached the wire")
	}
}

func TestChunkModelPoolAndTransactionThreeDimensionalWire(t *testing.T) {
	server := newModelServer(t, 3, true)
	pool := newTestPool(t, server, nil)
	if _, err := pool.SetBlock3D(t.Context(), "space", 1, 2, 0, Record{"n": 4}); err != nil {
		t.Fatal(err)
	}
	if server.lastCommand(t) != "SET BLOCK 1 2 0 IN space n = $1" {
		t.Fatal(server.lastCommand(t))
	}
	if _, err := pool.SetChunkMetadata3D(t.Context(), "space", 1, 2, 0, Record{"title": "base"}); err != nil {
		t.Fatal(err)
	}
	if server.lastCommand(t) != "SET CHUNK 1 2 0 IN space META title = $1" {
		t.Fatal(server.lastCommand(t))
	}
	_, err := pool.Transaction(t.Context(), func(tx *Tx) error {
		if err := tx.SetBlock3D(t.Context(), "space", 3, 4, 0, Record{"n": 4}); err != nil {
			return err
		}
		if _, err := tx.GetBlock3DWithMetadata(t.Context(), "space", 3, 4, 0); err != nil {
			return err
		}
		if err := tx.SetChunkMetadata3D(t.Context(), "space", 3, 4, 0, Record{"title": nil}); err != nil {
			return err
		}
		_, err := tx.GetArea3DWithSchema(t.Context(), "space", 0, 0, 0, 1, 1, 1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	commands := strings.Join(server.commands(), "\n")
	for _, expected := range []string{"BEGIN", "SET BLOCK 3 4 0 IN space n = $1", "GET BLOCK 3 4 0 FROM space COLUMNS n WITH VERSION WITH SCHEMA", "SET CHUNK 3 4 0 IN space META title = $1", "GET AREA 0 0 0 TO 1 1 1 FROM space COLUMNS n WITH SCHEMA", "COMMIT"} {
		if !strings.Contains(commands, expected) {
			t.Fatalf("missing %q in %s", expected, commands)
		}
	}
}

func TestChunkModelThreeDimensionalFeedFilters(t *testing.T) {
	epoch := strings.Repeat("a", 32)
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, line string) {
		switch commandOf(line) {
		case "DESCRIBE":
			writeRaw(conn, modelDescription(3, false))
		case "WATCH":
			writeSimple(conn, "OK "+epoch+" 0")
		default:
			writeSimple(conn, "OK")
		}
	}))
	client := newTestClient(t, server, nil)
	area := &Area3D{CX0: -1, CY0: -2, CZ0: 0, CX1: 3, CY1: 4, CZ1: 0}
	if err := client.CreateSlotWithOptions(t.Context(), "space", "reader", SlotOptions{Area3D: area, Columns: []string{"n"}, NoBefore: true, Chunks: true}); err != nil {
		t.Fatal(err)
	}
	if got := server.lastCommand(t); got != "CREATE SLOT 'reader' ON space AREA -1 -2 0 TO 3 4 0 COLUMNS (n) NO BEFORE CHUNKS" {
		t.Fatal(got)
	}
	watch, err := client.Watch(t.Context(), "space", WatchOptions{Area3D: area, Columns: []string{"n"}, Chunks: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := server.lastCommand(t); got != "WATCH space AREA -1 -2 0 TO 3 4 0 COLUMNS (n) CHUNKS" {
		t.Fatal(got)
	}
	if err := watch.Close(); err != nil {
		t.Fatal(err)
	}
	before := len(server.commands())
	if _, err := client.Watch(t.Context(), "space", WatchOptions{Area: &Area{}, Area3D: area}); err == nil {
		t.Fatal("ambiguous dimensions accepted")
	}
	if err := client.CreateSlotWithOptions(t.Context(), "space", "bad", SlotOptions{Area3D: &Area3D{CZ0: 1, CZ1: 0}}); err == nil {
		t.Fatal("inverted z accepted")
	}
	if len(server.commands()) != before {
		t.Fatal("invalid feed filters reached the wire")
	}
	filters, err := parseSlotFilters(readOne(t, respMap("area", respArray(respInt(-1), respInt(-2), respInt(0), respInt(3), respInt(4), respInt(0)),
		"columns", respNull, "column_ids", respNull, "no_before", respBool(false), "chunks", respBool(false))))
	if err != nil || len(filters.Area) != 6 || filters.Area[2] != 0 || filters.Area[5] != 0 {
		t.Fatalf("filters %+v %v", filters, err)
	}
}
