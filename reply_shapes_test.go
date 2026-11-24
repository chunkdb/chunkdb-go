package chunkdb

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

// withMapEntry adds one key and value to an encoded RESP3 map.
func withMapEntry(t *testing.T, m, key, value string) string {
	t.Helper()
	header, rest, ok := strings.Cut(m, "\r\n")
	count, err := strconv.Atoi(strings.TrimPrefix(header, "%"))
	if !ok || err != nil || !strings.HasPrefix(header, "%") {
		t.Fatalf("not a map: %q", header)
	}
	return "%" + strconv.Itoa(count+1) + "\r\n" + rest + respBulk(key) + value
}

func TestDescribeFencedReason(t *testing.T) {
	wire := withMapEntry(t, withMapEntry(t, describeReply("world", 1, worldColumns), "revision", respNull), "fenced", respBulk("recovery_failed"))
	schema, err := parseSchema(readOne(t, wire))
	if err != nil || schema.Fenced == nil || *schema.Fenced != "recovery_failed" || schema.Revision != nil {
		t.Fatalf("%+v %v", schema, err)
	}
	cloned := schema.clone()
	*cloned.Fenced = "changed"
	if *schema.Fenced != "recovery_failed" {
		t.Fatal("a clone shares the fence reason")
	}
	if schema, err := parseSchema(readOne(t, describeReply("world", 1, worldColumns))); err != nil || schema.Fenced != nil {
		t.Fatalf("a table in service: %+v %v", schema, err)
	}
	if _, err := parseSchema(readOne(t, withMapEntry(t, describeReply("world", 1, worldColumns), "fenced", respInt(1)))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a numeric fence reason: %v", err)
	}
}

func TestConnectionRestoreProgress(t *testing.T) {
	progress := respMap("files_done", respInt(2), "files_total", respInt(5), "bytes_done", respInt(4096), "bytes_total", "(18446744073709551615\r\n")
	restoring := withMapEntry(t, connectionRow(respInt(1)), "progress", progress)
	idle := withMapEntry(t, connectionRow(respInt(2)), "progress", respNull)
	rows, err := parseConnections(readOne(t, respArray(restoring, idle, connectionRow(respInt(3)))))
	if err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	if got := rows[0].Progress; got == nil || *got != (RestoreProgress{FilesDone: 2, FilesTotal: 5, BytesDone: 4096, BytesTotal: 1<<64 - 1}) {
		t.Fatalf("%+v", got)
	}
	if rows[1].Progress != nil || rows[2].Progress != nil {
		t.Fatal("progress without a running restore")
	}
	for _, bad := range []string{
		respInt(1),
		respMap("files_done", respInt(2), "files_total", respInt(5), "bytes_done", respInt(4096)),
		respMap("files_done", respInt(-1), "files_total", respInt(5), "bytes_done", respInt(0), "bytes_total", respInt(0)),
	} {
		if _, err := parseConnections(readOne(t, respArray(withMapEntry(t, connectionRow(respInt(1)), "progress", bad)))); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted progress %q: %v", bad, err)
		}
	}
}

// A downsampled read of a three-dimensional table with chunk columns and a
// block-column projection: samples divide every axis, the metadata stays
// complete, and only the projected column is decoded.
func TestDownsampledThreeDimensionalProjectedForm(t *testing.T) {
	columns := []Column{{ID: 1, Name: "h", Type: TypeUint(8)}, {ID: 2, Name: "kind", Type: TypeUint(8), Null: true}}
	metadata := []Column{{ID: 3, Name: "biome", Type: TypeUint(8)}}
	samples := &Schema{Table: "v", Dims: 3, Version: 2, ChunkWidth: 2, ChunkHeight: 2, ChunkDepth: 2, Columns: columns, ChunkColumns: metadata}
	full := &Schema{Table: "v", Dims: 3, Version: 2, ChunkWidth: 4, ChunkHeight: 4, ChunkDepth: 4, Columns: columns, ChunkColumns: metadata}
	chunk := NewChunk(samples)
	chunk.SetBlock3D(1, 0, 1, Record{"h": uint64(9), "kind": uint64(3)})
	chunk.Metadata = Record{"biome": uint64(5)}
	data, err := encodeSectionedChunk(samples, chunk, "h")
	if err != nil {
		t.Fatal(err)
	}
	scaled := withScaleSection(t, data, 2, 4)
	read, err := decodeAreaChunk(full, scaled, []string{"h"}, 2, ReduceFirst)
	if err != nil {
		t.Fatal(err)
	}
	if read.Scale != 2 || read.Reduction != ReduceFirst || read.Width != 2 || read.Height != 2 || read.Depth != 2 || !read.Projected {
		t.Fatalf("%+v", read)
	}
	if block := read.Block3D(1, 0, 1); len(block) != 1 || block["h"] != uint64(9) || read.Block3D(0, 0, 0) != nil {
		t.Fatalf("sample %+v", block)
	}
	if read.Metadata["biome"] != uint64(5) {
		t.Fatalf("metadata %+v", read.Metadata)
	}
	if _, err := decodeAreaChunk(full, scaled, nil, 2, ReduceFirst); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a projected form read as a full one: %v", err)
	}
	uneven := *full
	uneven.ChunkDepth = 3
	if _, err := decodeAreaChunk(&uneven, scaled, []string{"h"}, 2, ReduceFirst); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a scale that does not divide the depth: %v", err)
	}
	if _, err := EncodeChunk(full, read); !errors.Is(err, ErrProtocol) {
		t.Fatalf("encoded a downsampled chunk: %v", err)
	}
}
