package chunkdb

import (
	"errors"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"testing"
)

func scanReply(rows string, more bool, position string, floor int64) string {
	return respMap("chunks", rows, "more", respBool(more), "epoch", respBulk(strings.Repeat("a", 32)),
		"position", position, "retained_deletion_floor", respInt(floor))
}

func TestChunkModelScanRetainsZeroZAndTombstoneVersion(t *testing.T) {
	schema, err := parseSchema(readOne(t, modelDescription(3, false)))
	if err != nil {
		t.Fatal(err)
	}
	chunk := NewChunk(schema)
	chunk.Version, chunk.CommitTimeMS = 42, 11
	chunk.SetBlock3D(1, 0, 1, Record{"n": 9})
	chunk.Metadata = Record{"title": "base"}
	form, err := encodeSectionedChunk(schema, chunk, "n")
	if err != nil {
		t.Fatal(err)
	}
	var mode atomic.Int32
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, line string) {
		if commandOf(line) == "DESCRIBE" {
			writeRaw(conn, modelDescription(3, false))
			return
		}
		switch mode.Load() {
		case 0:
			rows := respArray(respArray(respInt(math.MinInt64), respInt(0), respInt(0), respInt(41), respNull),
				respArray(respInt(-1), respInt(2), respInt(0), respInt(42), respBulk(string(form))))
			writeRaw(conn, scanReply(rows, false, respInt(43), 12))
		case 1:
			writeRaw(conn, scanReply(respArray(respArray(respInt(1), respInt(2), respInt(0), "(18446744073709551615\r\n")), false, respNull, 0))
		case 2:
			writeRaw(conn, "-ERR INVALID_ARGUMENT reason=tombstone_history_lost requested history has been compacted\r\n")
		}
	}))
	client := newTestClient(t, server, nil)
	since := uint64(40)
	options := ScanOptions3D{After: &ChunkCoord3D{CX: math.MinInt64, CY: -1}, Since: &since,
		Limit: 2, WithData: true, Columns: []string{"n"}, Epoch: strings.Repeat("a", 32)}
	page, err := client.ScanChunks3DWithOptions(t.Context(), "space", options)
	if err != nil {
		t.Fatal(err)
	}
	if got := server.lastCommand(t); got != "SCAN CHUNKS FROM space AFTER -9223372036854775808 -1 0 SINCE 40 LIMIT 2 WITH DATA COLUMNS n" {
		t.Fatal(got)
	}
	if page.Position == nil || *page.Position != 43 || page.RetainedDeletionFloor != 12 || len(page.Chunks) != 2 {
		t.Fatalf("page %+v", page)
	}
	deleted, live := page.Chunks[0], page.Chunks[1]
	if deleted.CZ != 0 || deleted.Version != 41 || deleted.Chunk != nil || deleted.Deleted == nil || !*deleted.Deleted {
		t.Fatalf("deleted %+v", deleted)
	}
	if live.CZ != 0 || live.Version != 42 || live.Deleted == nil || *live.Deleted || live.Chunk.Depth != 2 || live.Chunk.Metadata["title"] != "base" || live.Chunk.Block3D(1, 0, 1)["n"] != uint64(9) {
		t.Fatalf("live %+v", live)
	}
	mode.Store(1)
	readOnly, err := client.ScanChunks3DWithOptions(t.Context(), "space", ScanOptions3D{})
	if err != nil || readOnly.Position != nil || readOnly.Chunks[0].Version != math.MaxUint64 || readOnly.Chunks[0].Deleted != nil {
		t.Fatalf("read-only %+v %v", readOnly, err)
	}
	mode.Store(2)
	_, err = client.ScanChunks3DWithOptions(t.Context(), "space", options)
	var serverError *Error
	if !errors.As(err, &serverError) || serverError.ServerCode != CodeInvalidArgument || serverError.Metadata.Reason != "tombstone_history_lost" {
		t.Fatalf("history loss hidden: %v", err)
	}
}

func TestChunkModelScanRejectsBrokenPageProgressAndIdentity(t *testing.T) {
	schema, err := parseSchema(readOne(t, modelDescription(3, false)))
	if err != nil {
		t.Fatal(err)
	}
	since := uint64(3)
	base := readOne(t, scanReply(respArray(respArray(respInt(1), respInt(2), respInt(0), respInt(4))), false, respInt(5), 2))
	for _, test := range []struct {
		name  string
		reply Reply
		after []int64
		data  bool
		epoch string
	}{
		{"missing frontier", replaceMapValue(base, "position", Reply{Kind: ReplyBulk, Bulk: []byte("bad")}), nil, false, ""},
		{"repeated coordinate", base, []int64{1, 2, 0}, false, ""},
		{"changed lifetime", replaceMapValue(base, "epoch", readOne(t, respBulk(strings.Repeat("b", 32)))), nil, false, ""},
		{"requested lifetime", base, nil, false, strings.Repeat("b", 32)},
		{"lost history", replaceMapValue(base, "retained_deletion_floor", readOne(t, respInt(4))), nil, false, ""},
		{"nonadvancing revision", replaceMapValue(base, "chunks", readOne(t, respArray(respArray(respInt(1), respInt(2), respInt(0), respInt(3))))), nil, false, ""},
		{"empty more", replaceMapValue(replaceMapValue(base, "chunks", readOne(t, respArray())), "more", readOne(t, respBool(true))), nil, false, ""},
		{"wrong arity", replaceMapValue(base, "chunks", readOne(t, respArray(respArray(respInt(1), respInt(2), respInt(4))))), nil, false, ""},
		{"ordinary tombstone", replaceMapValue(base, "chunks", readOne(t, respArray(respArray(respInt(1), respInt(2), respInt(0), respInt(4), respNull)))), nil, true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			selectedSince := &since
			if test.name == "ordinary tombstone" {
				selectedSince = nil
			}
			if _, err := decodeScanPage(schema, test.reply, 3, test.after, 1, selectedSince, test.data, nil, test.epoch); !errors.Is(err, ErrProtocol) {
				t.Fatalf("accepted broken page: %v", err)
			}
		})
	}
}
