package chunkdb

import (
	"errors"
	"math"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// terrainInfo is a USE / TABLEINFO reply for a table of 8x2 blocks of 4 bits:
// 8 payload bytes and 2 presence bytes per chunk.
const terrainInfo = "table=terrain\nstore_id=00112233445566778899aabbccddeeff\nblock_bits=4\n" +
	"chunk_width_blocks=8\nchunk_height_blocks=2\nlarge_chunk_width_chunks=8\n" +
	"large_chunk_height_chunks=8\ndurability_mode=fsync-wal\ncheckpoint_updates=256\n" +
	"checkpoint_wal_bytes=1048576\nwal_group_commit_updates=8\ncheckpoint_compression=none\n" +
	"extra_max_block_bits=4096\nextra_max_chunk_bytes=65536\nhistory=on\nhistory_start=41\n" +
	"history_start_time_ms=1791377487382\nhistory_max_age_ms=2592000000\nhistory_max_chunk_bytes=18446744073709551615\n" +
	"history_max_tag_bytes=32\n"

func tableHandler(_ *fakeServer, conn net.Conn, command string) {
	switch verbOf(command) {
	case "USE", "TABLEINFO":
		args := strings.Fields(command)
		info, ok := fakeTables[args[len(args)-1]]
		if !ok {
			writeServerError(conn, "ERR NO_TABLE table '"+args[len(args)-1]+"' does not exist")
			return
		}
		writeBulkString(conn, info)
	case "TABLES":
		writeArray(conn, "default", "terrain")
	case "TABLECREATE", "TABLESET", "TABLEDROP":
		writeSimple(conn, "OK")
	case "CHUNKGET":
		// The terrain table's state size.
		writeBulk(conn, make([]byte, 10))
	default:
		genericHandler(nil, conn, command)
	}
}

func TestURITable(t *testing.T) {
	for path, want := range map[string]string{"/": "", "": "", "/terrain": "terrain"} {
		got, err := URI{Path: path}.Table()
		if err != nil || got != want {
			t.Fatalf("Table(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
	if _, err := TableFromPath("/a/b"); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want a connection error for a two-segment path", err)
	}
	if _, err := NewClient(Options{URI: "chunk://h:1/a/b"}); err == nil {
		t.Fatal("NewClient accepted a two-segment path")
	}
}

func TestClientSelectsTableOnEveryConnection(t *testing.T) {
	server := newFakeServer(t, withHello(tableHandler))
	client := newTestClient(t, server, func(opts *Options) {
		opts.URI = strings.TrimSuffix(server.uri("tok"), "/") + "/terrain"
	})
	if client.CurrentTable() != "terrain" || client.URI().Path != "/terrain" {
		t.Fatalf("got table %q path %q", client.CurrentTable(), client.URI().Path)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	server.dropConnections()
	_ = client.Ping(t.Context())
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
	var selections []string
	for _, command := range server.commands() {
		if verbOf(command) == "HELLO" || verbOf(command) == "USE" {
			selections = append(selections, command)
		}
	}
	want := []string{"HELLO 2 AUTH tok TABLE terrain", "HELLO 2 AUTH tok TABLE terrain"}
	if !reflect.DeepEqual(selections, want) {
		t.Fatalf("got %q, want %q", selections, want)
	}
}

func TestClientOptionTableWinsOverPath(t *testing.T) {
	client, err := NewClient(Options{URI: "chunk://t@h:1/terrain", Table: "sky"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client.CurrentTable() != "sky" || client.URI().String() != "chunk://t@h:1/sky" {
		t.Fatalf("got %q, %s", client.CurrentTable(), client.URI())
	}
	defaulted, err := NewClient(Options{URI: "chunk://t@h:1/"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if defaulted.CurrentTable() != "default" {
		t.Fatalf("got %q, want default", defaulted.CurrentTable())
	}
}

func TestClientUseSwitchesTableAndGeometry(t *testing.T) {
	server := newFakeServer(t, withHello(tableHandler))
	client := newTestClient(t, server, nil)

	info, err := client.Use(t.Context(), "terrain")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}
	if info.Name != "terrain" || info.BlockBits != 4 || info.ChunkWidthBlocks != 8 ||
		info.ChunkHeightBlocks != 2 || info.Options.DurabilityMode != "fsync-wal" ||
		info.StoreID != "00112233445566778899aabbccddeeff" || info.Options.ExtraMaxBlockBits != 4096 ||
		info.Options.ExtraMaxChunkBytes != 65536 {
		t.Fatalf("got %+v", info)
	}
	if client.CurrentTable() != "terrain" {
		t.Fatalf("got %q", client.CurrentTable())
	}
	// Chunk sizes now come from the USE reply: the fake server answers 10
	// bytes, which only fits the state of 8x2 blocks of 4 bits.
	state, err := client.GetChunkState(t.Context(), 0, 0, GetOptions{})
	if err != nil || len(state.Payload) != 8 || len(state.Presence) != 2 {
		t.Fatalf("GetChunkState: %+v, %v", state, err)
	}
	// The selection is repeated in HELLO after a reconnect.
	server.dropConnections()
	_ = client.Ping(t.Context())
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
	if got := server.commands(); !slices.Contains(got, "HELLO 2 AUTH tok TABLE terrain") {
		t.Fatalf("got %q, want a HELLO naming terrain", got)
	}

	var serverErr *Error
	if _, err := client.Use(t.Context(), "missing"); !errors.As(err, &serverErr) ||
		serverErr.ServerCode != CodeNoTable {
		t.Fatalf("got %v, want %s", err, CodeNoTable)
	}
	if client.CurrentTable() != "terrain" {
		t.Fatalf("a failed Use changed the table to %q", client.CurrentTable())
	}
}

func TestClientTableCommandEncoding(t *testing.T) {
	server := newFakeServer(t, withHello(tableHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		call func() error
		want string
	}{
		{func() error {
			return client.CreateTable(ctx, "terrain", TableSpec{BlockBits: 4})
		}, "TABLECREATE terrain block_bits 4"},
		{func() error {
			return client.CreateTable(ctx, "terrain", TableSpec{
				BlockBits: 4, ChunkWidthBlocks: 32, ChunkHeightBlocks: 16,
				LargeChunkWidthChunks: 4, LargeChunkHeightChunks: 2,
				Options: TableOptions{DurabilityMode: "fsync-wal", CheckpointUpdates: 64},
			})
		}, "TABLECREATE terrain block_bits 4 chunk_width_blocks 32 chunk_height_blocks 16 " +
			"large_chunk_width_chunks 4 large_chunk_height_chunks 2 durability_mode fsync-wal " +
			"checkpoint_updates 64"},
		{func() error {
			return client.SetTableOptions(ctx, "terrain", TableOptions{
				CheckpointWalBytes: 4096, WalGroupCommitUpdates: 2, CheckpointCompression: "zrle",
			})
		}, "TABLESET terrain checkpoint_wal_bytes 4096 wal_group_commit_updates 2 " +
			"checkpoint_compression zrle"},
		{func() error {
			return client.CreateTable(ctx, "items", TableSpec{
				BlockBits: 8, Options: TableOptions{ExtraMaxBlockBits: 256, ExtraMaxChunkBytes: 1048576},
			})
		}, "TABLECREATE items block_bits 8 extra_max_block_bits 256 extra_max_chunk_bytes 1048576"},
		{func() error {
			return client.SetTableOptions(ctx, "items", TableOptions{ExtraMaxBlockBits: 512})
		}, "TABLESET items extra_max_block_bits 512"},
		{func() error {
			return client.CreateTable(ctx, "world", TableSpec{
				BlockBits: 16, Options: TableOptions{
					History: true, HistoryMaxAgeMs: 2592000000, HistoryMaxChunkBytes: math.MaxUint64 - 1, HistoryMaxTagBytes: 8,
				},
			})
		}, "TABLECREATE world block_bits 16 history on history_max_age_ms 2592000000 " +
			"history_max_chunk_bytes 18446744073709551614 history_max_tag_bytes 8"},
		{func() error {
			return client.SetTableOptions(ctx, "world", TableOptions{HistoryMaxAgeMs: HistoryNoLimit, HistoryMaxChunkBytes: HistoryNoLimit})
		}, "TABLESET world history_max_age_ms 0 history_max_chunk_bytes 0"},
		{func() error {
			return client.SetTableOptions(ctx, "terrain", TableOptions{History: true})
		}, "TABLESET terrain history on"},
		{func() error { return client.DropTable(ctx, "terrain") }, "TABLEDROP terrain"},
		{func() error {
			names, err := client.Tables(ctx)
			if err == nil && !reflect.DeepEqual(names, []string{"default", "terrain"}) {
				t.Fatalf("Tables: %q", names)
			}
			return err
		}, "TABLES"},
		{func() error {
			_, err := client.TableInfo(ctx, "terrain")
			return err
		}, "TABLEINFO terrain"},
	}
	for _, c := range cases {
		if err := c.call(); err != nil {
			t.Fatalf("%s: %v", c.want, err)
		}
		if got := server.lastCommand(t); got != c.want {
			t.Fatalf("got %q, want %q", got, c.want)
		}
	}

	before := len(server.commands())
	if err := client.SetTableOptions(ctx, "terrain", TableOptions{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a request error for no options", err)
	}
	if len(server.commands()) != before {
		t.Fatal("an empty SetTableOptions reached the server")
	}
}

func TestTableInfoHistory(t *testing.T) {
	info, err := parseTableValues(ParseInfo([]byte(terrainInfo)), "TABLEINFO")
	if err != nil || !info.Options.History || info.HistoryStart != 41 || info.HistoryStartTimeMs != 1791377487382 ||
		info.Options.HistoryMaxAgeMs != 2592000000 || info.Options.HistoryMaxChunkBytes != math.MaxUint64 ||
		info.Options.HistoryMaxTagBytes != 32 {
		t.Fatalf("got %+v, %v", info, err)
	}

	// A table without history reports off and zeros; a server without the
	// history capability omits every line.
	info, err = parseTableValues(ParseInfo([]byte(defaultInfo)), "TABLEINFO")
	if err != nil || info.Options.History || info.HistoryStart != 0 || info.Options.HistoryMaxTagBytes != 0 {
		t.Fatalf("got %+v, %v", info, err)
	}
	var kept []string
	for _, line := range strings.Split(terrainInfo, "\n") {
		if !strings.HasPrefix(line, "history") {
			kept = append(kept, line)
		}
	}
	info, err = parseTableValues(ParseInfo([]byte(strings.Join(kept, "\n"))), "TABLEINFO")
	if err != nil || info.Options.History || info.HistoryStart != 0 || info.HistoryStartTimeMs != 0 ||
		info.Options.HistoryMaxAgeMs != 0 || info.Options.HistoryMaxChunkBytes != 0 || info.Options.HistoryMaxTagBytes != 0 {
		t.Fatalf("got %+v, %v without the history lines", info, err)
	}

	for _, bad := range []string{"history=", "history=yes", "history_start=-1", "history_start=18446744073709551616",
		"history_start_time_ms=-1", "history_start_time_ms=x", "history_max_age_ms=", "history_max_chunk_bytes=-1",
		"history_max_tag_bytes=-1", "history_max_tag_bytes=lots"} {
		key, _, _ := strings.Cut(bad, "=")
		var lines []string
		for _, line := range strings.Split(terrainInfo, "\n") {
			if strings.HasPrefix(line, key+"=") {
				line = bad
			}
			lines = append(lines, line)
		}
		if _, err := parseTableValues(ParseInfo([]byte(strings.Join(lines, "\n"))), "TABLEINFO"); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: got %v, want ErrProtocol", bad, err)
		}
	}
}

func TestTableInfoExtraDataLimits(t *testing.T) {
	info, err := parseTableValues(ParseInfo([]byte(terrainInfo)), "TABLEINFO")
	if err != nil || info.Options.ExtraMaxBlockBits != 4096 || info.Options.ExtraMaxChunkBytes != 65536 {
		t.Fatalf("got %+v, %v", info.Options, err)
	}

	// A server without the extra-data capability omits both lines.
	var kept []string
	for _, line := range strings.Split(terrainInfo, "\n") {
		if !strings.HasPrefix(line, "extra_") {
			kept = append(kept, line)
		}
	}
	info, err = parseTableValues(ParseInfo([]byte(strings.Join(kept, "\n"))), "TABLEINFO")
	if err != nil || info.Options.ExtraMaxBlockBits != 0 || info.Options.ExtraMaxChunkBytes != 0 {
		t.Fatalf("got %+v, %v without the extra data lines", info.Options, err)
	}

	for _, bad := range []string{"extra_max_block_bits=4096", "extra_max_chunk_bytes=65536"} {
		key, _, _ := strings.Cut(bad, "=")
		for _, value := range []string{"", "-1", "lots"} {
			reply := strings.Replace(terrainInfo, bad, key+"="+value, 1)
			if _, err := parseTableValues(ParseInfo([]byte(reply)), "TABLEINFO"); !errors.Is(err, ErrProtocol) {
				t.Fatalf("%s=%q: got %v, want ErrProtocol", key, value, err)
			}
		}
	}
}
