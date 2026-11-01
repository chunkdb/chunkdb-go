package chunkdb

import (
	"net"
	"strings"
	"testing"
)

func TestSectionedRawBoundAfterQuotaReduction(t *testing.T) {
	schema := &Schema{Table: "space", Version: 1, Dims: 3, ChunkWidth: 1, ChunkHeight: 1, ChunkDepth: 1,
		Columns:      []Column{{ID: 1, Name: "n", Type: TypeUint(8)}},
		ChunkColumns: []Column{{ID: 1, Name: "title", Type: TypeText(1024), Null: true}},
		Options:      TableOptions{VarMaxChunkBytes: 1 << 20}}
	chunk := NewChunk(schema)
	chunk.Metadata = Record{"title": strings.Repeat("x", 512)}
	form, err := encodeSectionedChunk(schema, chunk)
	if err != nil {
		t.Fatal(err)
	}
	schema.Options.VarMaxChunkBytes = 1
	limit, err := sectionedChunkFormLimit(schema)
	if err != nil || limit < uint64(len(form)) {
		t.Fatal(limit, len(form), err)
	}
	if _, err := decodeSectionedChunk(schema, form); err != nil {
		t.Fatal(err)
	}
}

func TestChunkModelTypedRequestsDeferLoweredQuota(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, line string) {
		switch commandOf(line) {
		case "DESCRIBE":
			description := strings.Replace(modelDescription(3, false), fakeColumn(1, "n", "u8", false, false, respInt(0)), fakeColumn(1, "n", "text(1024)", false, false, respBulk("")), 1)
			description = strings.ReplaceAll(description, respBulk("text(8)"), respBulk("text(1024)"))
			description = strings.Replace(description, respInt(1<<20), respInt(1), 1)
			writeRaw(conn, description)
		case "SET CHUNK":
			writeRaw(conn, respInt(8))
		default:
			writeSimple(conn, "OK")
		}
	}))
	client := newTestClient(t, server, nil)
	schema, err := client.Describe(t.Context(), "space")
	if err != nil {
		t.Fatal(err)
	}
	chunk := NewChunk(schema)
	chunk.SetBlock3D(0, 0, 0, Record{"n": strings.Repeat("x", 512)})
	chunk.Metadata = Record{"title": strings.Repeat("y", 512)}
	if _, err := encodeSectionedChunk(schema, chunk); err == nil {
		t.Fatal("standalone configured budget ignored")
	}
	for _, partial := range []bool{false, true} {
		var err error
		if partial {
			_, err = client.SetChunkColumns3D(t.Context(), "space", 0, 0, 0, []string{"n"}, chunk)
		} else {
			_, err = client.SetChunk3D(t.Context(), "space", 0, 0, 0, chunk)
		}
		if err != nil {
			t.Fatal(partial, err)
		}
		frames := server.lastParams(t)
		if len(frames) != 1 || len(frames[0]) <= 512 {
			t.Fatal("typed request not sent intact")
		}
	}
	if schema.Options.VarMaxChunkBytes != 1 {
		t.Fatal("cached quota changed")
	}
}
