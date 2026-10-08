package chunkdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestClient(t *testing.T, server *fakeServer, configure func(*Options)) *Client {
	t.Helper()

	opts := Options{
		URI:            server.uri("tok"),
		ConnectTimeout: 2 * time.Second,
		CommandTimeout: 2 * time.Second,
	}
	if configure != nil {
		configure(&opts)
	}

	client, err := Connect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// waitForFinishedConns waits until the server has seen want connections end.
func waitForFinishedConns(t *testing.T, server *fakeServer, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for server.finishedConns() < want {
		if time.Now().After(deadline) {
			t.Fatalf("got %d closed connections, want %d", server.finishedConns(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// genericHandler answers every statement with a well-formed, minimal reply so
// tests can focus on what the client sends. Every table has the fake world
// columns.
func genericHandler(_ *fakeServer, conn net.Conn, command string) {
	fields := strings.Fields(command)
	switch commandOf(command) {
	case "PING":
		writeSimple(conn, "PONG")
	case "DESCRIBE":
		writeRaw(conn, describeReply(fields[1], 1, worldColumns))
	case "GET BLOCK":
		writeRaw(conn, respNull)
	case "SET BLOCK", "DELETE BLOCK", "SET CHUNK":
		writeRaw(conn, respInt(7))
	case "GET CHUNK":
		// An empty form of the named columns, or of every column.
		size := worldFormBytes
		if _, names, ok := strings.Cut(command, " COLUMNS "); ok {
			size = 17
			for _, name := range strings.Split(names, ", ") {
				size += worldSectionBytes[name]
			}
		}
		writeRaw(conn, respBulk(emptyWorldForm(size)))
	case "GET AREA":
		writeRaw(conn, respArray())
	case "SCAN CHUNKS":
		writeRaw(conn, respMap("chunks", respArray(), "more", respBool(false)))
	case "FLUSH WAL", "CREATE TABLE", "ALTER TABLE", "DROP TABLE":
		writeSimple(conn, "OK")
	case "SHOW TABLES":
		writeRaw(conn, respArray(respBulk("default"), respBulk("world")))
	case "SHOW METRICS":
		writeRaw(conn, respBulk("# HELP chunkdb_up\n"))
	default:
		writeServerError(conn, "ERR SYNTAX column 1: unknown statement")
	}
}

func TestClientHelloAndPing(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	commands := server.commands()
	want := []string{"HELLO 3 AUTH tok", "PING"}
	if strings.Join(commands, "|") != strings.Join(want, "|") {
		t.Fatalf("got statements %q, want %q", commands, want)
	}
}

func TestClientHelloWithoutToken(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client, err := Connect(t.Context(), Options{URI: server.uri("")})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if commands := server.commands(); len(commands) != 2 || commands[0] != "HELLO 3" {
		t.Fatalf("got %q, want HELLO 3 then PING", commands)
	}
}

func TestClientServerInfo(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))

	client, err := NewClient(Options{URI: server.uri("tok")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if client.ServerInfo() != nil {
		t.Fatal("got ServerInfo before connecting")
	}
	if err := client.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	want := ServerInfo{
		Protocol: 3, ServerVersion: "test", MaxLineBytes: 65536, MaxParameters: 65535,
		MaxAreaChunks: 256, MaxResponseBytes: 64 << 20, MaxScanLimit: 1024,
	}
	info := client.ServerInfo()
	if info == nil || *info != want {
		t.Fatalf("got %+v, want %+v", info, want)
	}
	// The result is a copy.
	info.MaxLineBytes = 1
	if client.ServerInfo().MaxLineBytes != 65536 {
		t.Fatal("changing the result changed the client")
	}
}

func TestClientHelloAuthFailureIsTyped(t *testing.T) {
	// A protocol 3 server answers AUTH_REQUIRED only to a HELLO without a
	// token.
	cases := []struct {
		token   string
		reply   string
		code    string
		message string
	}{
		{"wrong", "ERR AUTH_FAILED invalid token", CodeAuthFailed, "invalid token"},
		{"", "ERR AUTH_REQUIRED use HELLO 3 AUTH <token>", CodeAuthRequired, "use HELLO 3 AUTH <token>"},
	}

	for _, testCase := range cases {
		t.Run(testCase.code, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
				writeServerError(conn, testCase.reply)
			})

			_, err := Connect(t.Context(), Options{URI: server.uri(testCase.token)})
			if !errors.Is(err, ErrAuth) || !errors.Is(err, ErrServer) {
				t.Fatalf("got %v, want ErrAuth and ErrServer", err)
			}
			var typed *Error
			if !errors.As(err, &typed) {
				t.Fatalf("got %T, want *chunkdb.Error", err)
			}
			if typed.ServerCode != testCase.code || typed.ServerMessage != testCase.message {
				t.Fatalf("got code %q message %q", typed.ServerCode, typed.ServerMessage)
			}
			if typed.Phase != PhaseAuth || typed.Command != "HELLO" {
				t.Fatalf("got phase %q command %q, want %q HELLO", typed.Phase, typed.Command, PhaseAuth)
			}
			// A failed handshake closes the socket.
			waitForFinishedConns(t, server, 1)
		})
	}
}

func TestClientHelloRefusesOlderServers(t *testing.T) {
	cases := map[string]struct {
		token string
		reply string
	}{
		"protocol 2":            {"", "ERR PROTOCOL expected HELLO 2"},
		"1.x":                   {"", "ERR UNKNOWN_COMMAND HELLO"},
		"1.x requiring a token": {"tok", "ERR AUTH_REQUIRED use AUTH <token>"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
				writeServerError(conn, testCase.reply)
			})
			_, err := Connect(t.Context(), Options{URI: server.uri(testCase.token)})
			if !errors.Is(err, ErrProtocol) || errors.Is(err, ErrServer) {
				t.Fatalf("got %v, want ErrProtocol and not ErrServer", err)
			}
			if !strings.Contains(err.Error(), "older chunkdb protocol") {
				t.Fatalf("got %q, want it to say the server speaks an older protocol", err)
			}
			waitForFinishedConns(t, server, 1)
		})
	}
}

func TestClientHelloRejectsBadReplies(t *testing.T) {
	limits := []string{
		"server_version", respBulk("test"),
		"max_line_bytes", respInt(65536),
		"max_parameters", respInt(65535),
		"max_area_chunks", respInt(256),
		"max_response_bytes", respInt(64 << 20),
		"max_scan_limit", respInt(1024),
	}
	without := func(key string) string {
		pairs := []string{"protocol", respInt(3)}
		for i := 0; i < len(limits); i += 2 {
			if limits[i] != key {
				pairs = append(pairs, limits[i], limits[i+1])
			}
		}
		return respMap(pairs...)
	}

	cases := map[string]string{
		"protocol 2":             respMap(append([]string{"protocol", respInt(2)}, limits...)...),
		"protocol missing":       respMap(limits...),
		"max_line_bytes missing": without("max_line_bytes"),
		"max_parameters missing": without("max_parameters"),
		"server_version missing": without("server_version"),
		"max_scan_limit zero":    strings.Replace(respMap(append([]string{"protocol", respInt(3)}, limits...)...), ":1024", ":0", 1),
		"not a map":              "+OK\r\n",
		"a bulk string":          respBulk("protocol=3\n"),
	}

	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
				writeRaw(conn, reply)
			})
			_, err := Connect(t.Context(), Options{URI: server.uri("tok")})
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			waitForFinishedConns(t, server, 1)
		})
	}
}

func TestClientHelloTimeout(t *testing.T) {
	// The server never answers HELLO.
	server := newFakeServer(t, func(*fakeServer, net.Conn, string) {})

	_, err := Connect(t.Context(), Options{URI: server.uri("tok"), CommandTimeout: 100 * time.Millisecond})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.Command != "HELLO" {
		t.Fatalf("got %v, want a HELLO timeout", err)
	}
	waitForFinishedConns(t, server, 1)
}

func TestClientServerErrorIsTyped(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR NO_TABLE table 'nowhere' does not exist")
	}))
	client := newTestClient(t, server, nil)

	_, err := client.DeleteBlock(t.Context(), "nowhere", 0, 0)
	if !errors.Is(err, ErrServer) || errors.Is(err, ErrAuth) || errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want ErrServer only", err)
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("got %T, want *chunkdb.Error", err)
	}
	if typed.ServerCode != CodeNoTable || typed.ServerMessage != "table 'nowhere' does not exist" {
		t.Fatalf("got code %q message %q", typed.ServerCode, typed.ServerMessage)
	}
	if typed.Command != "DELETE BLOCK" || typed.Phase != PhaseResponse {
		t.Fatalf("got command %q phase %q, want DELETE BLOCK %q", typed.Command, typed.Phase, PhaseResponse)
	}
}

func TestClientStatementEncoding(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, func(o *Options) { o.Table = "world" })
	ctx := t.Context()
	mask, _ := ParseBits("101")

	cases := []struct {
		name   string
		call   func() error
		want   string
		params [][]byte
	}{
		{"ping", func() error { return client.Ping(ctx) }, "PING", nil},
		{"get block", func() error { _, err := client.GetBlock(ctx, "", -1, 2, "name", "id"); return err },
			"GET BLOCK -1 2 FROM world COLUMNS name, id", nil},
		{"get block of every column", func() error { _, err := client.GetBlock(ctx, "land", 1, 2); return err },
			"GET BLOCK 1 2 FROM land COLUMNS id, temp, solid, h, d, mask, name, blob", nil},
		{
			"set block",
			func() error {
				_, err := client.SetBlock(ctx, "", 3, -4, Record{
					"id": 7, "temp": int8(-2), "solid": true, "h": float32(1.5), "d": 2.5,
					"mask": mask, "name": "it's\r\n", "blob": []byte{0, '\r', '\n'},
				})
				return err
			},
			"SET BLOCK 3 -4 IN world blob = $1, d = $2, h = $3, id = $4, mask = $5, name = $6, solid = $7, temp = $8",
			[][]byte{
				{0, '\r', '\n'},
				{0, 0, 0, 0, 0, 0, 4, 0x40},
				{0, 0, 0xc0, 0x3f},
				{7, 0, 0, 0, 0, 0, 0, 0},
				{5},
				[]byte("it's\r\n"),
				{1},
				{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			},
		},
		{
			"set block with NULL and IF VERSION",
			func() error {
				_, err := client.SetBlock(ctx, "", 0, 0, Record{"name": nil, "blob": []byte{}}, IfVersion(18446744073709551615))
				return err
			},
			"SET BLOCK 0 0 IN world blob = $1, name = $2 IF VERSION 18446744073709551615",
			[][]byte{{}, nil},
		},
		{"delete block", func() error { _, err := client.DeleteBlock(ctx, "", 5, 6, IfVersion(9)); return err },
			"DELETE BLOCK 5 6 FROM world IF VERSION 9", nil},
		{"get chunk", func() error { _, err := client.GetChunk(ctx, "", 1, -2, "h"); return err },
			"GET CHUNK 1 -2 FROM world COLUMNS h", nil},
		{"get chunk raw", func() error { _, err := client.GetChunkRaw(ctx, "", 1, 2); return err },
			"GET CHUNK 1 2 FROM world", nil},
		{"set chunk raw", func() error { _, err := client.SetChunkRaw(ctx, "", 1, 2, []byte("a\r\nb"), IfVersion(3)); return err },
			"SET CHUNK 1 2 IN world $1 IF VERSION 3", [][]byte{[]byte("a\r\nb")}},
		{"get area", func() error { _, err := client.GetArea(ctx, "", -1, -2, 3, 4, "id"); return err },
			"GET AREA -1 -2 TO 3 4 FROM world COLUMNS id", nil},
		{"get area around", func() error { _, err := client.GetAreaAround(ctx, "", 1, 2, 3); return err },
			"GET AREA AROUND 1 2 RADIUS 3 FROM world COLUMNS id, temp, solid, h, d, mask, name, blob", nil},
		{"scan", func() error { _, err := client.ScanChunks(ctx, "", nil, 0); return err }, "SCAN CHUNKS FROM world", nil},
		{"scan after", func() error { _, err := client.ScanChunks(ctx, "", &ChunkCoord{CX: -1, CY: 2}, 10); return err },
			"SCAN CHUNKS FROM world AFTER -1 2 LIMIT 10", nil},
		{"flush", func() error { return client.FlushWAL(ctx) }, "FLUSH WAL", nil},
		{"metrics", func() error { _, err := client.Metrics(ctx); return err }, "SHOW METRICS", nil},
		{"tables", func() error { _, err := client.Tables(ctx); return err }, "SHOW TABLES", nil},
		{"describe", func() error { _, err := client.Describe(ctx, "land"); return err }, "DESCRIBE land", nil},
		{
			"create table",
			func() error {
				return client.CreateTable(ctx, "land", TableSpec{
					Columns: []ColumnDef{
						{Name: "id", Type: TypeUint(10), Required: true},
						{Name: "light", Type: TypeUint(4), Default: 15},
						{Name: "sign", Type: TypeText(8), Null: true, Default: "it's"},
						{Name: "h", Type: TypeF32(), Default: float32(1.5)},
						{Name: "mask", Type: TypeBits(3), Default: mask},
						{Name: "blob", Type: TypeBytes(4), Default: []byte{0x0a, 0xff}},
						{Name: "far", Type: TypeF64(), Default: math.Inf(-1)},
						{Name: "flag", Type: TypeBool(), Default: true},
						{Name: "t", Type: TypeInt(8), Default: -3},
					},
					ChunkWidth: 16, ChunkHeight: 8, LargeWidth: 4, LargeHeight: 2,
					Options: TableOptions{DurabilityMode: "fsync-wal", VarMaxChunkBytes: 4096},
				})
			},
			"CREATE TABLE land (id u10 REQUIRED, light u4 DEFAULT 15, sign text(8) NULL DEFAULT 'it''s', " +
				"h f32 DEFAULT 1.5, mask bits(3) DEFAULT b'101', blob bytes(4) DEFAULT x'0aff', far f64 DEFAULT -inf, " +
				"flag bool DEFAULT TRUE, t i8 DEFAULT -3) CHUNK 16 x 8 LARGE 4 x 2 " +
				"WITH durability_mode = 'fsync-wal', var_max_chunk_bytes = 4096",
			nil,
		},
		{"add column", func() error { return client.AddColumn(ctx, "", ColumnDef{Name: "d2", Type: TypeInt(8), Null: true}) },
			"ALTER TABLE world ADD COLUMN d2 i8 NULL", nil},
		{"drop column", func() error { return client.DropColumn(ctx, "", "d2") }, "ALTER TABLE world DROP COLUMN d2", nil},
		{"rename column", func() error { return client.RenameColumn(ctx, "", "name", "label") },
			"ALTER TABLE world RENAME COLUMN name TO label", nil},
		{"alter type", func() error { return client.AlterColumnType(ctx, "", "id", TypeUint(8), ConvertNone) },
			"ALTER TABLE world ALTER COLUMN id TYPE u8", nil},
		{"alter type using", func() error { return client.AlterColumnType(ctx, "", "name", TypeText(4), ConvertTruncate) },
			"ALTER TABLE world ALTER COLUMN name TYPE text(4) USING TRUNCATE", nil},
		{"set option text", func() error { return client.SetTableOption(ctx, "", "durability_mode", "relaxed") },
			"ALTER TABLE world SET durability_mode = 'relaxed'", nil},
		{"set option number", func() error { return client.SetTableOption(ctx, "", "checkpoint_updates", 64) },
			"ALTER TABLE world SET checkpoint_updates = 64", nil},
		{"drop table", func() error { return client.DropTable(ctx, "land") }, "DROP TABLE land", nil},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			if got := server.lastCommand(t); got != testCase.want {
				t.Fatalf("got  %q\nwant %q", got, testCase.want)
			}
			params := server.lastParams(t)
			if len(params) != len(testCase.params) {
				t.Fatalf("got %d parameters, want %d", len(params), len(testCase.params))
			}
			for i, param := range params {
				if (param == nil) != (testCase.params[i] == nil) || !bytes.Equal(param, testCase.params[i]) {
					t.Fatalf("parameter %d: got %v, want %v", i+1, param, testCase.params[i])
				}
			}
		})
	}
}

func TestClientDefaultTable(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	cases := map[string]struct {
		configure func(*Options)
		want      string
	}{
		"default":      {nil, "default"},
		"uri path":     {func(o *Options) { o.URI = strings.TrimSuffix(o.URI, "/") + "/terrain" }, "terrain"},
		"option wins":  {func(o *Options) { o.URI += "terrain"; o.Table = "world" }, "world"},
		"option alone": {func(o *Options) { o.Table = "world" }, "world"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			client := newTestClient(t, server, testCase.configure)
			if got := client.DefaultTable(); got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
			if _, err := client.DeleteBlock(t.Context(), "", 0, 0); err != nil {
				t.Fatalf("DeleteBlock: %v", err)
			}
			if got := server.lastCommand(t); got != "DELETE BLOCK 0 0 FROM "+testCase.want {
				t.Fatalf("got %q", got)
			}
			// HELLO does not name the table.
			if got := server.commands()[0]; got != "HELLO 3 AUTH tok" {
				t.Fatalf("got %q", got)
			}
		})
	}
}

func TestClientValidatesBeforeSending(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	// Fetch the schema first, so every case below sends nothing.
	if _, err := client.Schema(ctx, "world"); err != nil {
		t.Fatalf("Schema: %v", err)
	}
	sent := len(server.commands())

	cases := map[string]func() error{
		"multi-line statement": func() error { _, err := client.Do(ctx, "PING\r\nPING"); return err },
		"bare LF":              func() error { _, err := client.Do(ctx, "PING\n"); return err },
		"long statement":       func() error { _, err := client.Do(ctx, "PING "+strings.Repeat("x", 65536)); return err },
		"bad table name":       func() error { _, err := client.DeleteBlock(ctx, "World", 0, 0); return err },
		"injected table name":  func() error { _, err := client.GetBlock(ctx, "world COLUMNS id", 0, 0); return err },
		"bad column name":      func() error { _, err := client.GetBlock(ctx, "world", 0, 0, "id, temp"); return err },
		"empty set":            func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{}); return err },
		"u10 too large":        func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"id": 1024}); return err },
		"u10 negative":         func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"id": -1}); return err },
		"i8 too small":         func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"temp": -129}); return err },
		"bool as int":          func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"solid": 1}); return err },
		"f32 out of range":     func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"h": 1e39}); return err },
		"bits wrong width":     func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"mask": NewBits(4)}); return err },
		"text too long": func() error {
			_, err := client.SetBlock(ctx, "world", 0, 0, Record{"name": strings.Repeat("x", 17)})
			return err
		},
		"text not UTF-8":  func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"name": "\xff"}); return err },
		"bytes as string": func() error { _, err := client.SetBlock(ctx, "world", 0, 0, Record{"blob": "ab"}); return err },
		"NULL in a non-NULL column": func() error {
			_, err := client.SetBlock(ctx, "world", 0, 0, Record{"id": nil})
			return err
		},
		"nil raw chunk":   func() error { _, err := client.SetChunkRaw(ctx, "world", 0, 0, nil); return err },
		"negative radius": func() error { _, err := client.GetAreaAround(ctx, "world", 0, 0, -1); return err },
		"negative limit":  func() error { _, err := client.ScanChunks(ctx, "world", nil, -1); return err },
		"no columns":      func() error { return client.CreateTable(ctx, "t", TableSpec{ChunkWidth: 1, ChunkHeight: 1}) },
		"bad default": func() error {
			return client.AddColumn(ctx, "world", ColumnDef{Name: "c", Type: TypeUint(4), Default: 16})
		},
		"bad type":         func() error { return client.AddColumn(ctx, "world", ColumnDef{Name: "c", Type: TypeUint(65)}) },
		"bad option value": func() error { return client.SetTableOption(ctx, "world", "checkpoint_updates", -1) },
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			err := call()
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Phase != PhaseRequest || typed.Command == "" {
				t.Fatalf("got %#v, want a request error naming its command", err)
			}
		})
	}
	if got := server.commands(); len(got) != sent {
		t.Fatalf("the client sent %q", got[sent:])
	}
}

func TestClientRefreshesTheSchemaForAnUnknownColumn(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	if _, err := client.Schema(ctx, "world"); err != nil {
		t.Fatalf("Schema: %v", err)
	}

	// The column may have been added since the schema was cached: the client
	// asks once more before it gives up.
	_, err := client.SetBlock(ctx, "world", 0, 0, Record{"nope": 1})
	var typed *Error
	if !errors.As(err, &typed) || typed.Phase != PhaseRequest || !strings.Contains(typed.Message, "no column nope") {
		t.Fatalf("got %v, want a request error", err)
	}
	if got := server.commands(); len(got) != 3 || got[2] != "DESCRIBE world" {
		t.Fatalf("got %q, want HELLO and two DESCRIBE", got)
	}
}

func TestClientRejectsBadTokenAndTable(t *testing.T) {
	for _, opts := range []Options{
		{Token: "two words"},
		{Token: "line\r\nbreak"},
		{URI: "chunk://h:1/Upper"},
		{Table: "a b"},
	} {
		if _, err := NewClient(opts); !errors.Is(err, ErrConnection) {
			t.Fatalf("NewClient(%+v): got %v, want ErrConnection", opts, err)
		}
	}
}

func TestClientDo(t *testing.T) {
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, command string) {
		switch command {
		case "SET BLOCK 1 1 IN world name = $1, blob = $2":
			writeRaw(conn, respInt(41))
		case "SET BLOCK 1 1 IN world id = 1":
			writeServerError(conn, "ERR VERSION_MISMATCH current=18446744073709551615")
		default:
			genericHandler(s, conn, command)
		}
	}))
	client := newTestClient(t, server, nil)

	reply, err := client.Do(t.Context(), "SET BLOCK 1 1 IN world name = $1, blob = $2", []byte("x"), nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if version, ok := reply.Uint64(); !ok || version != 41 {
		t.Fatalf("got %+v", reply)
	}
	if params := server.lastParams(t); len(params) != 2 || string(params[0]) != "x" || params[1] != nil {
		t.Fatalf("got parameters %q", params)
	}

	_, err = client.Do(t.Context(), "SET BLOCK 1 1 IN world id = 1")
	var mismatch *VersionMismatchError
	if !errors.As(err, &mismatch) || mismatch.Current != math.MaxUint64 {
		t.Fatalf("got %v, want a VersionMismatchError", err)
	}
}

func TestClientVersionMismatchIsTyped(t *testing.T) {
	server := newFakeServer(t, respondWithSchema("-ERR VERSION_MISMATCH current=1043\r\n"))
	client := newTestClient(t, server, nil)

	_, err := client.SetBlock(t.Context(), "world", 10, 4, Record{"id": 8}, IfVersion(1042))
	if !errors.Is(err, ErrVersionMismatch) || !errors.Is(err, ErrServer) {
		t.Fatalf("got %v, want ErrVersionMismatch and ErrServer", err)
	}
	var mismatch *VersionMismatchError
	if !errors.As(err, &mismatch) || mismatch.Current != 1043 {
		t.Fatalf("got %v, want the current version 1043", err)
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != CodeVersionMismatch || typed.Command != "SET BLOCK" {
		t.Fatalf("got %#v, want the server error underneath", typed)
	}
	if got := server.lastCommand(t); got != "SET BLOCK 10 4 IN world id = $1 IF VERSION 1042" {
		t.Fatalf("got %q", got)
	}
}

func TestClientMalformedVersionMismatchIsProtocolError(t *testing.T) {
	server := newFakeServer(t, respondWith("-ERR VERSION_MISMATCH current=soon\r\n"))
	client := newTestClient(t, server, nil)

	_, err := client.DeleteBlock(t.Context(), "world", 0, 0, IfVersion(1))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestClientGetBlockDecodesEveryType(t *testing.T) {
	values := respArray(
		respInt(1023), respInt(-128), respBool(true), ",1.5\r\n", ",-inf\r\n",
		respBulk("\x05"), respBulk("it's"), respBulk("\x00\r\n"),
	)
	server := newFakeServer(t, respondWithSchema(values))
	client := newTestClient(t, server, nil)

	record, err := client.GetBlock(t.Context(), "world", 0, 0)
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	mask, _ := ParseBits("101")
	want := Record{
		"id": uint64(1023), "temp": int64(-128), "solid": true, "h": float32(1.5), "d": math.Inf(-1),
		"mask": mask, "name": "it's", "blob": []byte{0, '\r', '\n'},
	}
	if fmt.Sprint(record) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", record, want)
	}
	if record["mask"].(Bits).String() != "101" {
		t.Fatalf("got mask %v", record["mask"])
	}
}

func TestClientGetBlockAbsentAndNull(t *testing.T) {
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, command string) {
		if strings.Contains(command, "BLOCK 1 1") {
			writeRaw(conn, respArray(respNull, respNull))
			return
		}
		genericHandler(s, conn, command)
	}))
	client := newTestClient(t, server, nil)

	record, err := client.GetBlock(t.Context(), "world", 0, 0)
	if err != nil || record != nil {
		t.Fatalf("got %v, %v; want an absent block", record, err)
	}
	record, err = client.GetBlock(t.Context(), "world", 1, 1, "temp", "name")
	if err != nil || len(record) != 2 || record["temp"] != nil || record["name"] != nil {
		t.Fatalf("got %v, %v; want two NULL values", record, err)
	}
}

func TestClientRejectsRepliesThatDoNotFitTheSchema(t *testing.T) {
	cases := map[string]string{
		"u10 out of range": respArray(respInt(1024)),
		"text as integer":  respArray(respInt(1)),
		"too few values":   respArray(),
		"not an array":     respInt(1),
	}
	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, command string) {
				if commandOf(command) == "GET BLOCK" {
					writeRaw(conn, reply)
					return
				}
				genericHandler(s, conn, command)
			}))
			client := newTestClient(t, server, nil)
			column := "id"
			if name == "text as integer" {
				column = "name"
			}
			if _, err := client.GetBlock(t.Context(), "world", 0, 0, column); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			// One retry with a fresh schema, then the error.
			describes := 0
			for _, command := range server.commands() {
				if commandOf(command) == "DESCRIBE" {
					describes++
				}
			}
			if describes != 2 {
				t.Fatalf("got %d DESCRIBE, want 2", describes)
			}
		})
	}
}

// schemaServer answers DESCRIBE with the current columns of a table whose
// column h a test can change.
type schemaServer struct {
	mu       sync.Mutex
	hType    string
	version  int64
	describe int
	sets     int
}

func (s *schemaServer) handle(_ *fakeServer, conn net.Conn, command string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch commandOf(command) {
	case "HELLO":
		answerHello(conn, command)
	case "DESCRIBE":
		s.describe++
		writeRaw(conn, describeReply("world", s.version, []string{
			fakeColumn(1, "id", "u10", false, false, respNull),
			fakeColumn(2, "h", s.hType, false, false, respNull),
		}))
	case "SET BLOCK":
		s.sets++
		if s.hType == "f64" && strings.Contains(command, "h = $") && s.describe == 1 {
			writeServerError(conn, "ERR INVALID_ARGUMENT $1 for column h (f64) must be 8 bytes, got 4")
			return
		}
		writeRaw(conn, respInt(9))
	case "ALTER TABLE":
		writeSimple(conn, "OK")
	default:
		writeServerError(conn, "ERR SYNTAX unexpected")
	}
}

func TestClientSetBlockRetriesOnceAfterAWrongSize(t *testing.T) {
	fake := &schemaServer{hType: "f32", version: 1}
	server := newFakeServer(t, fake.handle)
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	if _, err := client.SetBlock(ctx, "world", 0, 0, Record{"h": 1.5}); err != nil {
		t.Fatalf("SetBlock: %v", err)
	}
	// Another client widens h to f64; this client's schema is now stale.
	fake.mu.Lock()
	fake.hType, fake.version = "f64", 2
	fake.mu.Unlock()

	version, err := client.SetBlock(ctx, "world", 0, 0, Record{"h": 2.5})
	if err != nil || version != 9 {
		t.Fatalf("got %d, %v; want the retried write's version", version, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.describe != 2 || fake.sets != 3 {
		t.Fatalf("got %d DESCRIBE and %d SET BLOCK, want 2 and 3", fake.describe, fake.sets)
	}
	if params := server.lastParams(t); len(params) != 1 || len(params[0]) != 8 {
		t.Fatalf("got parameters %v, want one f64", params)
	}
}

func TestClientSetChunkReencodesAfterASchemaMismatch(t *testing.T) {
	var mu sync.Mutex
	schemaVersion := int64(1)
	describes, sets := 0, 0
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, command string) {
		mu.Lock()
		defer mu.Unlock()
		switch commandOf(command) {
		case "DESCRIBE":
			describes++
			writeRaw(conn, describeReply("world", schemaVersion, worldColumns))
		case "SET CHUNK":
			sets++
			params := s.params[len(s.params)-1]
			if got := int64(binary.LittleEndian.Uint64(params[0][8:])); got != schemaVersion {
				writeServerError(conn, fmt.Sprintf("ERR SCHEMA_MISMATCH current=%d the chunk was encoded for schema version %d", schemaVersion, got))
				return
			}
			writeRaw(conn, respInt(5))
		}
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	schema, err := client.Schema(ctx, "world")
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	chunk := NewChunk(schema)
	chunk.SetBlock(0, 0, Record{"id": 1, "name": "n"})
	form, _ := EncodeChunk(schema, chunk)

	// Another client changes the table: the cached schema is version 1, the
	// table's version 2.
	mu.Lock()
	schemaVersion = 2
	mu.Unlock()
	version, err := client.SetChunk(ctx, "world", 0, 0, chunk)
	if err != nil || version != 5 {
		t.Fatalf("got %d, %v; want the re-encoded write's version", version, err)
	}
	mu.Lock()
	if describes != 2 || sets != 2 {
		t.Fatalf("got %d DESCRIBE and %d SET CHUNK, want 2 and 2", describes, sets)
	}
	mu.Unlock()

	// A raw form is sent as it is.
	_, err = client.SetChunkRaw(ctx, "world", 0, 0, form)
	var mismatch *SchemaMismatchError
	if !errors.As(err, &mismatch) || mismatch.Current != 2 || !errors.Is(err, ErrSchemaMismatch) || !errors.Is(err, ErrServer) {
		t.Fatalf("got %v, want a SchemaMismatchError at version 2", err)
	}
	if errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want it not to match ErrVersionMismatch", err)
	}
}

func TestClientDropsTheConnectionWhenTheServerCloses(t *testing.T) {
	cases := []struct {
		name   string
		reply  string
		params bool
		closes bool
	}{
		{"bad request", "ERR BAD_REQUEST line too long", false, true},
		{"no table with parameters", "ERR NO_TABLE table 'x' does not exist", true, true},
		{"syntax with parameters", "ERR SYNTAX column 5: a parameter is $ followed by its number", true, true},
		{"unknown column with parameters", "ERR INVALID_ARGUMENT the table has no column x", true, true},
		{"no table without parameters", "ERR NO_TABLE table 'x' does not exist", false, false},
		{"wrong size", "ERR INVALID_ARGUMENT $1 for column a (u8) must be 8 bytes, got 1", true, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
				if command == "PING" {
					writeSimple(conn, "PONG")
					return
				}
				// The server answers without closing; the client must not
				// rely on it.
				writeServerError(conn, testCase.reply)
			}))
			client := newTestClient(t, server, nil)
			var params [][]byte
			statement := "DELETE BLOCK 0 0 FROM x"
			if testCase.params {
				statement, params = "SET BLOCK 0 0 IN x a = $1", [][]byte{{1}}
			}
			if _, err := client.Do(t.Context(), statement, params...); !errors.Is(err, ErrServer) {
				t.Fatalf("got %v, want the server error", err)
			}
			if err := client.Ping(t.Context()); err != nil {
				t.Fatalf("Ping: %v", err)
			}
			want := 1
			if testCase.closes {
				want = 2
			}
			if got := server.acceptedConns(); got != want {
				t.Fatalf("got %d connections, want %d", got, want)
			}
		})
	}
}

func TestClientForgetsSchemaAfterTableStatements(t *testing.T) {
	fake := &schemaServer{hType: "f32", version: 1}
	server := newFakeServer(t, fake.handle)
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	describes := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.describe
	}
	if _, err := client.Schema(ctx, "world"); err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if _, err := client.Schema(ctx, "world"); err != nil || describes() != 1 {
		t.Fatalf("got %v and %d DESCRIBE, want the cached schema", err, describes())
	}
	if err := client.AlterColumnType(ctx, "world", "h", TypeF64(), ConvertNone); err != nil {
		t.Fatalf("AlterColumnType: %v", err)
	}
	if _, err := client.Schema(ctx, "world"); err != nil || describes() != 2 {
		t.Fatalf("got %v and %d DESCRIBE, want a fresh schema after ALTER", err, describes())
	}
	if _, err := client.Do(ctx, "alter table world set checkpoint_updates = 1"); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if _, err := client.Schema(ctx, "world"); err != nil || describes() != 3 {
		t.Fatalf("got %v and %d DESCRIBE, want a fresh schema after ALTER through Do", err, describes())
	}
	if _, err := client.Describe(ctx, "world"); err != nil || describes() != 4 {
		t.Fatalf("got %v and %d DESCRIBE, want Describe to ask the server", err, describes())
	}
}

func TestClientSchemaIsParsedAndCopied(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	schema, err := client.Schema(t.Context(), "land")
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if schema.Table != "land" || schema.Version != 1 || len(schema.Columns) != 8 || schema.ChunkWidth != 2 ||
		schema.ChunkHeight != 2 || schema.LargeWidth != 8 || schema.LargeHeight != 8 {
		t.Fatalf("got %+v", schema)
	}
	want := TableOptions{
		DurabilityMode: "relaxed", CheckpointUpdates: 256, CheckpointWalBytes: 1 << 20,
		WalGroupCommitUpdates: 8, CheckpointCompression: "none", VarMaxChunkBytes: 1 << 20,
	}
	if schema.Options != want {
		t.Fatalf("got options %+v", schema.Options)
	}
	id, _ := schema.Column("id")
	h, _ := schema.Column("h")
	mask, _ := schema.Column("mask")
	if id.ID != 1 || mask.ID != 6 || id.Type != TypeUint(10) || !id.Required || id.Null || id.Default != nil ||
		h.Default != float32(1.5) || mask.Type != TypeBits(3) || !mask.Null {
		t.Fatalf("got columns %+v", schema.Columns)
	}
	schema.Columns[0].Name = "changed"
	again, _ := client.Schema(t.Context(), "land")
	if again.Columns[0].Name != "id" {
		t.Fatal("changing a returned schema changed the cache")
	}
}

func TestClientPipelinedRepliesKeepOrder(t *testing.T) {
	// Each GET BLOCK x is answered with x, in arrival order, while up to 8
	// requests are in flight.
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if commandOf(command) == "DESCRIBE" {
			writeRaw(conn, describeReply("world", 1, []string{fakeColumn(1, "v", "i64", false, false, respNull)}))
			return
		}
		writeRaw(conn, respArray(":"+strings.Fields(command)[2]+"\r\n"))
	}))
	client := newTestClient(t, server, func(o *Options) { o.PipelineDepth = 8 })

	var group sync.WaitGroup
	for i := range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			record, err := client.GetBlock(t.Context(), "world", int64(i), 0)
			if err != nil || record["v"] != int64(i) {
				t.Errorf("block %d: got %v, %v", i, record, err)
			}
		}()
	}
	group.Wait()
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestClientCommandTimeoutDropsConnection(t *testing.T) {
	var answer bool
	var mu sync.Mutex
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		mu.Lock()
		respond := answer
		mu.Unlock()
		if respond {
			writeSimple(conn, "PONG")
		}
	}))
	client := newTestClient(t, server, func(o *Options) { o.CommandTimeout = 100 * time.Millisecond })

	err := client.Ping(t.Context())
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want a client-side timeout rather than caller cancellation", err)
	}

	// The stream cannot be resynchronized after an abandoned request, so the
	// client drops the socket and the next call reconnects.
	mu.Lock()
	answer = true
	mu.Unlock()

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after timeout: %v", err)
	}
	if got := server.acceptedConns(); got != 2 {
		t.Fatalf("got %d connections, want 2", got)
	}
}

func TestClientContextCancellation(t *testing.T) {
	server := newFakeServer(t, withHello(func(*fakeServer, net.Conn, string) {}))
	client := newTestClient(t, server, nil)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := client.Ping(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want it to wrap context.Canceled", err)
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want it to also match ErrTimeout", err)
	}
}

func TestClientReconnectsAfterServerClosesConnection(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	server.dropConnections()

	// The first call after the drop may fail on the dead socket; the one after
	// it must succeed on a fresh connection.
	_ = client.Ping(t.Context())
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
	if got := server.acceptedConns(); got < 2 {
		t.Fatalf("got %d connections, want the client to have reconnected", got)
	}
}

func TestClientInFlightRequestsFailWhenConnectionDrops(t *testing.T) {
	server := newFakeServer(t, withHello(func(s *fakeServer, _ net.Conn, _ string) {
		go s.dropConnections()
	}))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
}

func TestClientReconnectsWhenTheConnectionDiesBeforeUse(t *testing.T) {
	// The server answers HELLO and hangs up immediately, so the connection is
	// already dead by the time the client finishes connecting and would publish
	// it. Each attempt must still dial a fresh socket instead of getting stuck
	// on the dead one.
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "HELLO" {
			answerHello(conn, command)
			_ = conn.Close()
		}
	})

	client, err := NewClient(Options{URI: server.uri("tok"), CommandTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const attempts = 3
	for range attempts {
		if err := client.Ping(t.Context()); !errors.Is(err, ErrConnection) {
			t.Fatalf("got %v, want ErrConnection", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := server.acceptedConns(); got < attempts {
		t.Fatalf("got %d connections for %d attempts, want one per attempt", got, attempts)
	}
}

func TestClientReconnectsAfterAnIdleConnectionIsDropped(t *testing.T) {
	// The server hangs up without reading anything, which is what a client sees
	// when a connection is reaped while idle.
	server := newClosingFakeServer(t)

	client, err := NewClient(Options{URI: server.uri(""), CommandTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const attempts = 3
	for range attempts {
		if err := client.Ping(t.Context()); !errors.Is(err, ErrConnection) {
			t.Fatalf("got %v, want ErrConnection", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := server.acceptedConns(); got < attempts {
		t.Fatalf("got %d connections for %d attempts, want one per attempt", got, attempts)
	}
}

func TestClientCloseIsIdempotentAndFinal(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := client.Ping(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestClientPipelinesUpToDepth(t *testing.T) {
	const depth = 4
	requests := make(chan net.Conn, depth*2)
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		requests <- conn
	}))
	client := newTestClient(t, server, func(o *Options) { o.PipelineDepth = depth })

	results := make(chan error, depth)
	for range depth {
		go func() { results <- client.Ping(t.Context()) }()
	}

	conns := make([]net.Conn, 0, depth)
	for range depth {
		select {
		case conn := <-requests:
			conns = append(conns, conn)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d requests reached the server", len(conns), depth)
		}
	}

	for _, conn := range conns {
		writeSimple(conn, "PONG")
	}
	for range depth {
		if err := <-results; err != nil {
			t.Fatalf("Ping: %v", err)
		}
	}

	// One socket carried all of them.
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestClientDefaultsToOneRequestInFlight(t *testing.T) {
	requests := make(chan net.Conn, 4)
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		requests <- conn
	}))
	client := newTestClient(t, server, nil)

	for range 3 {
		go func() { _ = client.Ping(t.Context()) }()
	}

	conn := <-requests
	select {
	case <-requests:
		t.Fatal("a second request reached the server while one was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	writeSimple(conn, "PONG")
}

func TestClientConcurrentUseIsSerialized(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	var group sync.WaitGroup
	for i := range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := client.SetBlock(t.Context(), "world", int64(i), 0, Record{"id": i}); err != nil {
				t.Errorf("SetBlock: %v", err)
			}
			if _, err := client.GetBlock(t.Context(), "world", int64(i), 0); err != nil {
				t.Errorf("GetBlock: %v", err)
			}
		}()
	}
	group.Wait()

	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestConnectReportsDialFailure(t *testing.T) {
	// Bind and immediately release a port so nothing is listening on it.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	_, err = Connect(t.Context(), Options{URI: "chunk://tok@" + address + "/"})
	if !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
}

func TestNewClientRejectsBadOptions(t *testing.T) {
	if _, err := NewClient(Options{URI: "http://host/"}); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
	if _, err := NewClient(Options{Port: 70000}); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
}

func TestClientOptionResolution(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want URI
	}{
		{
			name: "defaults",
			opts: Options{},
			want: URI{Scheme: "chunk", Host: defaultHost, Port: DefaultPort, Path: "/"},
		},
		{
			name: "uri",
			opts: Options{URI: "chunks://tok@example.test:9000/terrain"},
			want: URI{Scheme: "chunks", Secure: true, Host: "example.test", Port: 9000, Token: "tok", Path: "/terrain"},
		},
		{
			name: "explicit fields win over uri",
			opts: Options{URI: "chunk://tok@example.test:9000/", Host: "other.test", Port: 1, Token: "override", Table: "world"},
			want: URI{Scheme: "chunk", Host: "other.test", Port: 1, Token: "override", Path: "/world"},
		},
		{
			name: "tls option upgrades a chunk uri",
			opts: Options{URI: "chunk://example.test:9000/", TLS: true},
			want: URI{Scheme: "chunks", Secure: true, Host: "example.test", Port: 9000, Path: "/"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, err := NewClient(testCase.opts)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			if got := client.URI(); got != testCase.want {
				t.Fatalf("got %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestResolveTimeout(t *testing.T) {
	if got := resolveTimeout(0); got != DefaultTimeout {
		t.Fatalf("got %v, want %v", got, DefaultTimeout)
	}
	if got := resolveTimeout(-1); got != 0 {
		t.Fatalf("got %v, want a disabled deadline", got)
	}
	if got := resolveTimeout(time.Second); got != time.Second {
		t.Fatalf("got %v, want 1s", got)
	}
}
