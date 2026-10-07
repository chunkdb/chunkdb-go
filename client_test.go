package chunkdb

import (
	"bytes"
	"context"
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

// genericInfo is the fake INFO reply.
const genericInfo = "table=default\ntables=1\nloaded_chunks=0\n"

// genericHandler answers every command with a well-formed, minimal response so
// tests can focus on what the client sends. Chunk replies have the default
// table's sizes.
func genericHandler(_ *fakeServer, conn net.Conn, command string) {
	args := strings.Fields(command)
	switch verbOf(command) {
	case "PING":
		writeSimple(conn, "PONG")
	case "SET", "UNSET", "MSET", "WALFLUSH", "XPUT", "XDEL":
		writeSimple(conn, "OK")
	case "XGET":
		writeNull(conn)
	case "CHUNKEXISTS":
		writeSimple(conn, "0")
	case "GET":
		writeBulkString(conn, "0000")
	case "CHUNKGET":
		size := testChunkPayloadBytes
		if strings.Contains(command, " STATE") {
			size += testPresenceBytes
		}
		if strings.HasSuffix(command, " ZRLE") {
			writeBulk(conn, ZRLECompress(make([]byte, size)))
			return
		}
		writeBulk(conn, make([]byte, size))
	case "INFO":
		writeBulkString(conn, genericInfo)
	case "METRICS":
		writeBulkString(conn, "# HELP chunkdb_up\n")
	case "MGET":
		items := make([]string, 0, (len(args)-1)/2)
		for range (len(args) - 1) / 2 {
			items = append(items, "0000")
		}
		writeArray(conn, items...)
	case "CHUNKSCAN":
		writeArray(conn, "END")
	case "CHUNKRANGE", "CHUNKRADIUS":
		writeArray(conn)
	case "CHUNKVER":
		writeBulkString(conn, "7")
	case "CHUNKPUT", "CHUNKBATCH":
		writeBulkString(conn, "8")
	default:
		writeServerError(conn, "ERR UNKNOWN_COMMAND "+verbOf(command))
	}
}

func TestClientHelloAndPing(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	commands := server.commands()
	want := []string{"HELLO 2 AUTH tok", "PING"}
	if len(commands) != len(want) {
		t.Fatalf("got commands %q, want %q", commands, want)
	}
	for i, command := range want {
		if commands[i] != command {
			t.Fatalf("command %d: got %q, want %q", i, commands[i], command)
		}
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
	if commands := server.commands(); len(commands) != 2 || commands[0] != "HELLO 2" {
		t.Fatalf("got %q, want HELLO 2 then PING", commands)
	}
}

func TestClientServerInfo(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))

	client, err := NewClient(Options{URI: server.uri("tok")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if info := client.ServerInfo(); info != nil {
		t.Fatalf("got %+v before connecting, want nil", info)
	}
	if err := client.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	info := client.ServerInfo()
	if info == nil {
		t.Fatal("got nil ServerInfo after connecting")
	}
	if info.Protocol != 2 || info.ServerVersion != "test" || len(info.Capabilities) != 2 ||
		info.Capabilities[0] != "zrle" || info.Capabilities[1] != "extra-data" || info.MaxLineBytes != 65536 ||
		info.MaxAreaChunks != 256 || info.MaxResponseBytes != 67108864 || info.MaxScanLimit != 1024 ||
		info.MaxBatchOps != 1024 || info.MaxExtraChunkBytes != 16777216 {
		t.Fatalf("got %+v", info)
	}
	if info.Table == nil || info.Table.Name != "default" || info.Table.BlockBits != 4 ||
		info.Table.ChunkWidthBlocks != 2 || info.Table.ChunkHeightBlocks != 2 ||
		info.Table.Options.DurabilityMode != "relaxed" {
		t.Fatalf("got table %+v", info.Table)
	}
	if info.Table.Options.ExtraMaxBlockBits != 0 || info.Table.Options.ExtraMaxChunkBytes != 0 {
		t.Fatalf("got table options %+v, want no extra data", info.Table.Options)
	}
	if info.Values["server_version"] != "test" {
		t.Fatalf("got values %v", info.Values)
	}

	// The result is a copy.
	info.Capabilities[0] = "changed"
	info.Values["server_version"] = "changed"
	info.Table.Name = "changed"
	info.Table.Values["table"] = "changed"
	again := client.ServerInfo()
	if again.Capabilities[0] != "zrle" || again.Values["server_version"] != "test" ||
		again.Table.Name != "default" || again.Table.Values["table"] != "default" {
		t.Fatalf("changing a ServerInfo result changed the client: %+v", again)
	}
}

func TestClientHelloAuthFailureIsTyped(t *testing.T) {
	// A protocol 2 server answers AUTH_REQUIRED only to a HELLO without a
	// token.
	cases := []struct {
		token   string
		reply   string
		code    string
		message string
	}{
		{"wrong", "ERR AUTH_FAILED invalid token", "AUTH_FAILED", "invalid token"},
		{"", "ERR AUTH_REQUIRED use HELLO 2 AUTH <token>", "AUTH_REQUIRED", "use HELLO 2 AUTH <token>"},
	}

	for _, testCase := range cases {
		t.Run(testCase.code, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
				writeServerError(conn, testCase.reply)
			})

			_, err := Connect(t.Context(), Options{URI: server.uri(testCase.token)})
			if !errors.Is(err, ErrAuth) {
				t.Fatalf("got %v, want ErrAuth", err)
			}
			// An auth failure is a specialization of a server error, so it
			// matches both.
			if !errors.Is(err, ErrServer) {
				t.Fatalf("got %v, want it to also match ErrServer", err)
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

func TestClientHelloUnknownTable(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))

	_, err := Connect(t.Context(), Options{URI: server.uri("tok"), Table: "missing"})
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != CodeNoTable {
		t.Fatalf("got %v, want %s", err, CodeNoTable)
	}
	if errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want it not to match ErrAuth", err)
	}
	if got := server.lastCommand(t); got != "HELLO 2 AUTH tok TABLE missing" {
		t.Fatalf("got %q", got)
	}
	waitForFinishedConns(t, server, 1)
}

func TestClientHelloRefusesProtocol1Server(t *testing.T) {
	// A chunkdb 1.x server does not know HELLO.
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		writeServerError(conn, "ERR UNKNOWN_COMMAND "+verbOf(command))
	})

	_, err := Connect(t.Context(), Options{URI: server.uri("")})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
	if errors.Is(err, ErrServer) {
		t.Fatalf("got %v, want it not to match ErrServer", err)
	}
	if !strings.Contains(err.Error(), "protocol 2") || !strings.Contains(err.Error(), "chunkdb 1.x") {
		t.Fatalf("got %q, want it to name protocol 2 and chunkdb 1.x", err)
	}
	waitForFinishedConns(t, server, 1)
}

func TestClientHelloRefusesProtocol1ServerWithToken(t *testing.T) {
	// A chunkdb 1.x server that requires a token answers every command
	// before AUTH with AUTH_REQUIRED, also a HELLO that carries the token.
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR AUTH_REQUIRED use AUTH <token>")
	})

	_, err := Connect(t.Context(), Options{URI: server.uri("tok")})
	if !errors.Is(err, ErrProtocol) || errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrProtocol and not ErrAuth", err)
	}
	if !strings.Contains(err.Error(), "chunkdb 1.x") {
		t.Fatalf("got %q, want it to name chunkdb 1.x", err)
	}

	// Without a token the reply is ambiguous: it stays an auth error.
	_, err = Connect(t.Context(), Options{URI: server.uri("")})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
}

func TestClientHelloRejectsBadReplies(t *testing.T) {
	withoutLimit := func(key string) string {
		lines := strings.Split(helloLimits+defaultInfo, "\n")
		kept := lines[:0]
		for _, line := range lines {
			if !strings.HasPrefix(line, key+"=") {
				kept = append(kept, line)
			}
		}
		return strings.Join(kept, "\n")
	}

	cases := map[string]string{
		"protocol 1":             strings.Replace(helloLimits, "protocol=2", "protocol=1", 1) + defaultInfo,
		"protocol missing":       withoutLimit("protocol"),
		"max_line_bytes missing": withoutLimit("max_line_bytes"),
		"max_batch_ops missing":  withoutLimit("max_batch_ops"),
		"max_area_chunks zero":   strings.Replace(helloLimits, "max_area_chunks=256", "max_area_chunks=0", 1),
		"table lines incomplete": helloLimits + "table=default\n",
		"max_extra_chunk_bytes zero": strings.Replace(helloLimits, "max_extra_chunk_bytes=16777216",
			"max_extra_chunk_bytes=0", 1),
		"max_extra_chunk_bytes not a number": strings.Replace(helloLimits, "max_extra_chunk_bytes=16777216",
			"max_extra_chunk_bytes=lots", 1),
		"extra_max_block_bits not a number": helloLimits +
			strings.Replace(defaultInfo, "extra_max_block_bits=0", "extra_max_block_bits=x", 1),
		"extra_max_chunk_bytes negative": helloLimits +
			strings.Replace(defaultInfo, "extra_max_chunk_bytes=0", "extra_max_chunk_bytes=-1", 1),
	}

	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
				writeBulkString(conn, reply)
			})
			_, err := Connect(t.Context(), Options{URI: server.uri("tok")})
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			waitForFinishedConns(t, server, 1)
		})
	}

	t.Run("simple reply", func(t *testing.T) {
		server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
			writeSimple(conn, "OK")
		})
		if _, err := Connect(t.Context(), Options{URI: server.uri("tok")}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("got %v, want ErrProtocol", err)
		}
	})
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

func TestClientWithoutTable(t *testing.T) {
	// A server without a default table: the connection has no table until Use.
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		switch verbOf(command) {
		case "HELLO":
			writeBulkString(conn, helloLimits)
		case "USE":
			writeBulkString(conn, terrainInfo)
		case "CHUNKGET":
			writeBulk(conn, make([]byte, 10))
		default:
			genericHandler(nil, conn, command)
		}
	})
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	if info := client.ServerInfo(); info == nil || info.Table != nil {
		t.Fatalf("got %+v, want a reply without a table", info)
	}

	calls := map[string]func() error{
		"GetChunk":      func() error { _, err := client.GetChunk(ctx, 0, 0, GetOptions{}); return err },
		"GetChunkState": func() error { _, err := client.GetChunkState(ctx, 0, 0, GetOptions{}); return err },
		"PutChunk": func() error {
			_, err := client.PutChunk(ctx, 0, 0, []byte{0}, PutOptions{})
			return err
		},
		"PutChunkState": func() error {
			_, err := client.PutChunkState(ctx, 0, 0, ChunkStateInput{}, PutOptions{})
			return err
		},
		"ChunkRange":  func() error { _, err := client.ChunkRange(ctx, 0, 0, 1, 1, GetOptions{}); return err },
		"ChunkRadius": func() error { _, err := client.ChunkRadius(ctx, 0, 0, 1, GetOptions{}); return err },
		// The server sizes XPUT bytes by the table and closes a connection
		// without one.
		"XPut": func() error { return client.XPut(ctx, 0, 0, ExtraValue{BitLength: 1, Bytes: []byte{1}}) },
		"GetChunkStateExtra": func() error {
			_, err := client.GetChunkStateExtra(ctx, 0, 0, GetOptions{})
			return err
		},
		"PutChunkStateExtra": func() error {
			_, err := client.PutChunkStateExtra(ctx, 0, 0, ChunkStateExtraInput{}, PutOptions{})
			return err
		},
	}
	for name, call := range calls {
		before := len(server.commands())
		err := call()
		if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "Use") {
			t.Fatalf("%s: got %v, want a request error pointing to Use", name, err)
		}
		if len(server.commands()) != before {
			t.Fatalf("%s reached the server", name)
		}
	}

	if _, err := client.Use(ctx, "terrain"); err != nil {
		t.Fatalf("Use: %v", err)
	}
	state, err := client.GetChunkState(ctx, 0, 0, GetOptions{})
	if err != nil || len(state.Payload) != 8 || len(state.Presence) != 2 {
		t.Fatalf("GetChunkState after Use: %+v, %v", state, err)
	}
}

func TestClientServerErrorIsTyped(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR INVALID_ARGUMENT bits length mismatch")
	}))
	client := newTestClient(t, server, nil)

	err := client.Set(t.Context(), 0, 0, "1010")
	if !errors.Is(err, ErrServer) {
		t.Fatalf("got %v, want ErrServer", err)
	}
	if errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want it not to match ErrAuth", err)
	}

	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("got %T, want *chunkdb.Error", err)
	}
	if typed.ServerCode != "INVALID_ARGUMENT" || typed.ServerMessage != "bits length mismatch" {
		t.Fatalf("got code %q message %q", typed.ServerCode, typed.ServerMessage)
	}
	if typed.Command != "SET" || typed.Phase != PhaseResponse {
		t.Fatalf("got command %q phase %q, want SET %q", typed.Command, typed.Phase, PhaseResponse)
	}
}

func TestClientCommandEncoding(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "get",
			call: func() error { _, err := client.Get(ctx, 1, -2); return err },
			want: "GET 1 -2",
		},
		{
			name: "int64 extremes",
			call: func() error { return client.Set(ctx, math.MinInt64, math.MaxInt64, "1010") },
			want: "SET -9223372036854775808 9223372036854775807 1010",
		},
		{
			name: "unset",
			call: func() error { return client.Unset(ctx, 4, 5) },
			want: "UNSET 4 5",
		},
		{
			name: "mset",
			call: func() error {
				return client.MSet(ctx, []Block{{X: 1, Y: 2, Bits: "1010"}, {X: 3, Y: 4, Bits: "0101"}})
			},
			want: "MSET 1 2 1010 3 4 0101",
		},
		{
			name: "mget",
			call: func() error { _, err := client.MGet(ctx, []BlockRef{{X: 1, Y: 2}, {X: 3, Y: 4}}); return err },
			want: "MGET 1 2 3 4",
		},
		{
			name: "chunk exists",
			call: func() error { _, err := client.ChunkExists(ctx, 1, 2); return err },
			want: "CHUNKEXISTS 1 2",
		},
		{
			name: "chunk get",
			call: func() error { _, err := client.GetChunk(ctx, 1, 2, GetOptions{}); return err },
			want: "CHUNKGET 1 2",
		},
		{
			name: "chunk get zrle",
			call: func() error { _, err := client.GetChunk(ctx, 1, 2, GetOptions{ZRLE: true}); return err },
			want: "CHUNKGET 1 2 ZRLE",
		},
		{
			name: "chunk get state",
			call: func() error { _, err := client.GetChunkState(ctx, 1, 2, GetOptions{}); return err },
			want: "CHUNKGET 1 2 STATE",
		},
		{
			name: "chunk get state zrle",
			call: func() error { _, err := client.GetChunkState(ctx, 1, 2, GetOptions{ZRLE: true}); return err },
			want: "CHUNKGET 1 2 STATE ZRLE",
		},
		{
			name: "scan without cursor",
			call: func() error { _, err := client.ChunkScan(ctx, 10, nil); return err },
			want: "CHUNKSCAN 10",
		},
		{
			name: "scan with cursor",
			call: func() error { _, err := client.ChunkScan(ctx, 10, &CoordPair{CX: 3, CY: -4}); return err },
			want: "CHUNKSCAN 10 3 -4",
		},
		{
			name: "range",
			call: func() error { _, err := client.ChunkRange(ctx, -1, -2, 3, 4, GetOptions{}); return err },
			want: "CHUNKRANGE -1 -2 3 4 STATE",
		},
		{
			name: "range zrle",
			call: func() error { _, err := client.ChunkRange(ctx, -1, -2, 3, 4, GetOptions{ZRLE: true}); return err },
			want: "CHUNKRANGE -1 -2 3 4 STATE ZRLE",
		},
		{
			name: "radius",
			call: func() error { _, err := client.ChunkRadius(ctx, 1, 2, 3, GetOptions{}); return err },
			want: "CHUNKRADIUS 1 2 3 STATE",
		},
		{
			name: "radius zrle",
			call: func() error { _, err := client.ChunkRadius(ctx, 1, 2, 3, GetOptions{ZRLE: true}); return err },
			want: "CHUNKRADIUS 1 2 3 STATE ZRLE",
		},
		{
			name: "version",
			call: func() error { _, err := client.ChunkVersion(ctx, 1, 2); return err },
			want: "CHUNKVER 1 2",
		},
		{
			name: "batch without version",
			call: func() error {
				_, err := client.ChunkBatch(ctx, 1, 2, []BatchOperation{SetOp(3, 4, "1010"), UnsetOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 SET 3 4 1010 UNSET 5 6",
		},
		{
			name: "batch with version",
			call: func() error {
				_, err := client.ChunkBatchIfVersion(ctx, 1, 2, 9, []BatchOperation{UnsetOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 IF 9 UNSET 5 6",
		},
		{
			name: "batch with extra data",
			call: func() error {
				_, err := client.ChunkBatch(ctx, 1, 2, []BatchOperation{SetOp(3, 4, "1010"), XPutOp(3, 4, "1011"), XDelOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 SET 3 4 1010 XPUT 3 4 1011 XDEL 5 6",
		},
		{
			name: "batch with the largest version",
			call: func() error {
				_, err := client.ChunkBatchIfVersion(ctx, 1, 2, math.MaxUint64, []BatchOperation{UnsetOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 IF 18446744073709551615 UNSET 5 6",
		},
		{
			name: "xget",
			call: func() error { _, err := client.XGet(ctx, 1, -2); return err },
			want: "XGET 1 -2",
		},
		{
			name: "xdel",
			call: func() error { return client.XDel(ctx, math.MinInt64, math.MaxInt64) },
			want: "XDEL -9223372036854775808 9223372036854775807",
		},
		{
			name: "chunk get state extra",
			call: func() error { _, err := client.GetChunkStateExtra(ctx, 1, 2, GetOptions{}); return err },
			want: "CHUNKGET 1 2 STATE EXTRA",
		},
		{
			name: "chunk get state extra zrle",
			call: func() error { _, err := client.GetChunkStateExtra(ctx, 1, 2, GetOptions{ZRLE: true}); return err },
			want: "CHUNKGET 1 2 STATE EXTRA ZRLE",
		},
		{
			name: "wal flush",
			call: func() error { return client.WALFlush(ctx) },
			want: "WALFLUSH",
		},
		{
			name: "metrics",
			call: func() error { _, err := client.Metrics(ctx); return err },
			want: "METRICS",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			if got := server.lastCommand(t); got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}

	// Chunk sizes come from HELLO; the client never asks INFO.
	for _, command := range server.commands() {
		if verbOf(command) == "INFO" {
			t.Fatalf("the client sent %q", command)
		}
	}
}

func TestClientGet(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if command == "GET 0 0" {
			writeNull(conn)
			return
		}
		writeBulkString(conn, "0000")
	}))
	client := newTestClient(t, server, nil)

	unset, err := client.Get(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if unset != (BlockState{}) {
		t.Fatalf("got %+v, want an unset block", unset)
	}

	zero, err := client.Get(t.Context(), 1, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if zero != (BlockState{Exists: true, Bits: "0000"}) {
		t.Fatalf("got %+v, want an explicit zero block", zero)
	}
}

func TestClientMGet(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		_, _ = conn.Write([]byte("*3\r\n$4\r\n1010\r\n$-1\r\n$4\r\n0000\r\n"))
	}))
	client := newTestClient(t, server, nil)

	values, err := client.MGet(t.Context(), []BlockRef{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 2, Y: 0}})
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	want := []BlockState{{Exists: true, Bits: "1010"}, {}, {Exists: true, Bits: "0000"}}
	if len(values) != len(want) {
		t.Fatalf("got %+v, want %+v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("item %d: got %+v, want %+v", i, values[i], want[i])
		}
	}

	if _, err := client.MGet(t.Context(), []BlockRef{{X: 0, Y: 0}}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol for 3 items answering 1 block", err)
	}
}

func TestClientGetChunk(t *testing.T) {
	payload := []byte{0xab, 0xcd}
	presence := []byte{0x05}
	state := append(append([]byte{}, payload...), presence...)
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		body := payload
		if strings.Contains(command, " STATE") {
			body = state
		}
		if strings.HasSuffix(command, " ZRLE") {
			body = ZRLECompress(body)
		}
		writeBulk(conn, body)
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	for _, zrle := range []bool{false, true} {
		got, err := client.GetChunk(ctx, 0, 0, GetOptions{ZRLE: zrle})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("GetChunk zrle=%v: %x, %v", zrle, got, err)
		}
		chunk, err := client.GetChunkState(ctx, 0, 0, GetOptions{ZRLE: zrle})
		if err != nil {
			t.Fatalf("GetChunkState zrle=%v: %v", zrle, err)
		}
		if !chunk.Exists || !bytes.Equal(chunk.Payload, payload) || !bytes.Equal(chunk.Presence, presence) {
			t.Fatalf("GetChunkState zrle=%v: got %+v", zrle, chunk)
		}
	}
}

func TestClientGetChunkStateAbsent(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	chunk, err := client.GetChunkState(t.Context(), 0, 0, GetOptions{})
	if err != nil {
		t.Fatalf("GetChunkState: %v", err)
	}
	if chunk.Exists || len(chunk.Payload) != testChunkPayloadBytes || len(chunk.Presence) != testPresenceBytes {
		t.Fatalf("got %+v, want an absent chunk of zeros", chunk)
	}
}

func TestClientChunkRange(t *testing.T) {
	first := []byte{0xf0, 0x00, 0x01}
	second := []byte{0x00, 0x0f, 0x08}
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		a, b := first, second
		if strings.HasSuffix(command, " ZRLE") {
			a, b = ZRLECompress(a), ZRLECompress(b)
		}
		writeArray(conn, "-1 -2", string(a), "3 4", string(b))
	}))
	client := newTestClient(t, server, nil)

	for _, zrle := range []bool{false, true} {
		entries, err := client.ChunkRange(t.Context(), -1, -2, 3, 4, GetOptions{ZRLE: zrle})
		if err != nil {
			t.Fatalf("ChunkRange zrle=%v: %v", zrle, err)
		}
		if len(entries) != 2 {
			t.Fatalf("got %d entries, want 2", len(entries))
		}
		if entries[0].CX != -1 || entries[0].CY != -2 || !bytes.Equal(entries[0].Payload, first[:2]) ||
			!bytes.Equal(entries[0].Presence, first[2:]) {
			t.Fatalf("got %+v", entries[0])
		}
		if entries[1].CX != 3 || entries[1].CY != 4 || !bytes.Equal(entries[1].Payload, second[:2]) ||
			!bytes.Equal(entries[1].Presence, second[2:]) {
			t.Fatalf("got %+v", entries[1])
		}
	}
}

func TestClientPutChunkWireFormat(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	// terrain chunks are 8x2 blocks of 4 bits: 8 payload and 2 presence bytes,
	// large enough for zrle to pay off.
	terrain := newTestClient(t, server, func(o *Options) { o.Table = "terrain" })
	ctx := t.Context()

	version := uint64(9)
	maxVersion := uint64(math.MaxUint64)
	zeroState := make([]byte, 10)
	denseState := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	cases := []struct {
		name string
		call func() (MutationResult, error)
		want []byte
	}{
		{
			name: "payload",
			call: func() (MutationResult, error) {
				return client.PutChunk(ctx, 1, -2, []byte{0xab, 0xcd}, PutOptions{})
			},
			want: []byte("CHUNKPUT 1 -2 2\r\n\xab\xcd\r\n"),
		},
		{
			name: "state",
			call: func() (MutationResult, error) {
				return client.PutChunkState(ctx, 1, 2, ChunkStateInput{Payload: []byte{0xab, 0xcd}, Presence: []byte{0x0f}}, PutOptions{})
			},
			want: []byte("CHUNKPUT 1 2 STATE 3\r\n\xab\xcd\x0f\r\n"),
		},
		{
			name: "if version",
			call: func() (MutationResult, error) {
				return client.PutChunk(ctx, 1, 2, []byte{0x0d, 0x0a}, PutOptions{IfVersion: &version})
			},
			want: []byte("CHUNKPUT 1 2 IF 9 2\r\n\r\n\r\n"),
		},
		{
			name: "largest version and coordinates",
			call: func() (MutationResult, error) {
				return client.PutChunk(ctx, math.MinInt64, math.MaxInt64, []byte{0, 0}, PutOptions{IfVersion: &maxVersion})
			},
			want: []byte("CHUNKPUT -9223372036854775808 9223372036854775807 IF 18446744073709551615 2\r\n\x00\x00\r\n"),
		},
		{
			name: "zrle that does not shrink is sent raw",
			call: func() (MutationResult, error) {
				return client.PutChunk(ctx, 1, 2, []byte{0, 0}, PutOptions{ZRLE: true})
			},
			want: []byte("CHUNKPUT 1 2 2\r\n\x00\x00\r\n"),
		},
		{
			name: "zrle that shrinks",
			call: func() (MutationResult, error) {
				return terrain.PutChunkState(ctx, 1, 2, ChunkStateInput{Payload: zeroState[:8], Presence: zeroState[8:]},
					PutOptions{ZRLE: true, IfVersion: &version})
			},
			want: append(append([]byte("CHUNKPUT 1 2 STATE ZRLE IF 9 7\r\n"), ZRLECompress(zeroState)...), '\r', '\n'),
		},
		{
			name: "zrle of a dense chunk is sent raw",
			call: func() (MutationResult, error) {
				return terrain.PutChunkState(ctx, 1, 2, ChunkStateInput{Payload: denseState[:8], Presence: denseState[8:]},
					PutOptions{ZRLE: true})
			},
			want: append(append([]byte("CHUNKPUT 1 2 STATE 10\r\n"), denseState...), '\r', '\n'),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := testCase.call()
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if result != (MutationResult{OK: true, Version: 8}) {
				t.Fatalf("got %+v, want {OK:true Version:8}", result)
			}
			if got := server.lastPut(t); !bytes.Equal(got, testCase.want) {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}

	// The requests stayed framed: the connection still works.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestClientPutChunkValidatesSizes(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := map[string]func() error{
		"short payload": func() error {
			_, err := client.PutChunk(ctx, 0, 0, []byte{1}, PutOptions{})
			return err
		},
		"long payload": func() error {
			_, err := client.PutChunk(ctx, 0, 0, []byte{1, 2, 3}, PutOptions{ZRLE: true})
			return err
		},
		"state payload": func() error {
			_, err := client.PutChunkState(ctx, 0, 0, ChunkStateInput{Payload: []byte{1}, Presence: []byte{1}}, PutOptions{})
			return err
		},
		"state presence": func() error {
			_, err := client.PutChunkState(ctx, 0, 0, ChunkStateInput{Payload: []byte{1, 2}, Presence: []byte{}}, PutOptions{})
			return err
		},
	}

	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(server.commands())
			err := call()
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			var typed *Error
			if errors.As(err, &typed) && typed.Phase != PhaseRequest {
				t.Fatalf("got phase %q, want %q", typed.Phase, PhaseRequest)
			}
			if len(server.commands()) != before {
				t.Fatalf("client sent %q, want nothing", server.commands()[before:])
			}
		})
	}

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want the first one to stay usable", got)
	}
}

// bulkWire frames payload as a bulk response.
func bulkWire(payload []byte) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
}

func TestClientRejectsMalformedResponses(t *testing.T) {
	cases := []struct {
		name     string
		response string
		call     func(*Client) error
	}{
		{
			name:     "chunk payload of the wrong size",
			response: "$3\r\nabc\r\n",
			call:     func(c *Client) error { _, err := c.GetChunk(t.Context(), 0, 0, GetOptions{}); return err },
		},
		{
			name:     "chunk state of the wrong size",
			response: "$2\r\nab\r\n",
			call:     func(c *Client) error { _, err := c.GetChunkState(t.Context(), 0, 0, GetOptions{}); return err },
		},
		{
			name:     "zrle chunk of the wrong size",
			response: bulkWire(ZRLECompress([]byte{1})),
			call:     func(c *Client) error { _, err := c.GetChunk(t.Context(), 0, 0, GetOptions{ZRLE: true}); return err },
		},
		{
			name:     "zrle chunk that is not zrle",
			response: "$2\r\nab\r\n",
			call:     func(c *Client) error { _, err := c.GetChunk(t.Context(), 0, 0, GetOptions{ZRLE: true}); return err },
		},
		{
			name:     "null chunk",
			response: "$-1\r\n",
			call:     func(c *Client) error { _, err := c.GetChunk(t.Context(), 0, 0, GetOptions{}); return err },
		},
		{
			name:     "chunk exists out of range",
			response: "+2\r\n",
			call:     func(c *Client) error { _, err := c.ChunkExists(t.Context(), 0, 0); return err },
		},
		{
			name:     "ping with wrong text",
			response: "+PANG\r\n",
			call:     func(c *Client) error { return c.Ping(t.Context()) },
		},
		{
			name:     "set without OK",
			response: "+NOPE\r\n",
			call:     func(c *Client) error { return c.Set(t.Context(), 0, 0, "1010") },
		},
		{
			name:     "bulk where simple is expected",
			response: "$2\r\nOK\r\n",
			call:     func(c *Client) error { return c.Set(t.Context(), 0, 0, "1010") },
		},
		{
			name:     "simple where bulk is expected",
			response: "+1010\r\n",
			call:     func(c *Client) error { _, err := c.Get(t.Context(), 0, 0); return err },
		},
		{
			name:     "simple where array is expected",
			response: "+1010\r\n",
			call:     func(c *Client) error { _, err := c.MGet(t.Context(), []BlockRef{{}}); return err },
		},
		{
			name:     "empty scan response",
			response: "*0\r\n",
			call:     func(c *Client) error { _, err := c.ChunkScan(t.Context(), 1, nil); return err },
		},
		{
			name:     "scan header is not END or CURSOR",
			response: "*1\r\n$4\r\nNOPE\r\n",
			call:     func(c *Client) error { _, err := c.ChunkScan(t.Context(), 1, nil); return err },
		},
		{
			name:     "scan entry is not a coordinate pair",
			response: "*2\r\n$3\r\nEND\r\n$3\r\n0 x\r\n",
			call:     func(c *Client) error { _, err := c.ChunkScan(t.Context(), 1, nil); return err },
		},
		{
			name:     "scan entry is null",
			response: "*2\r\n$3\r\nEND\r\n$-1\r\n",
			call:     func(c *Client) error { _, err := c.ChunkScan(t.Context(), 1, nil); return err },
		},
		{
			name:     "range with an odd item count",
			response: "*1\r\n$3\r\n0 0\r\n",
			call:     func(c *Client) error { _, err := c.ChunkRange(t.Context(), 0, 0, 0, 0, GetOptions{}); return err },
		},
		{
			name:     "range with a null chunk",
			response: "*2\r\n$3\r\n0 0\r\n$-1\r\n",
			call:     func(c *Client) error { _, err := c.ChunkRange(t.Context(), 0, 0, 0, 0, GetOptions{}); return err },
		},
		{
			name:     "range chunk of the wrong size",
			response: "*2\r\n$3\r\n0 0\r\n$2\r\nab\r\n",
			call:     func(c *Client) error { _, err := c.ChunkRange(t.Context(), 0, 0, 0, 0, GetOptions{}); return err },
		},
		{
			name:     "range coordinates are not a pair",
			response: "*2\r\n$5\r\n0 0 0\r\n$3\r\nabc\r\n",
			call:     func(c *Client) error { _, err := c.ChunkRadius(t.Context(), 0, 0, 1, GetOptions{}); return err },
		},
		{
			name:     "version is not a number",
			response: "$3\r\nabc\r\n",
			call:     func(c *Client) error { _, err := c.ChunkVersion(t.Context(), 0, 0); return err },
		},
		{
			name:     "put version is not a number",
			response: "$3\r\nabc\r\n",
			call: func(c *Client) error {
				_, err := c.PutChunk(t.Context(), 0, 0, []byte{0, 0}, PutOptions{})
				return err
			},
		},
		{
			name:     "coordinate is not canonical",
			response: "*2\r\n$3\r\nEND\r\n$4\r\n01 2\r\n",
			call:     func(c *Client) error { _, err := c.ChunkScan(t.Context(), 1, nil); return err },
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newFakeServer(t, respondWith(testCase.response))
			client := newTestClient(t, server, nil)

			if err := testCase.call(client); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestClientValidatesArgumentsBeforeSending(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		name string
		call func() error
	}{
		{"set with non-bit payload", func() error { return client.Set(ctx, 0, 0, "10x0") }},
		{"set with empty payload", func() error { return client.Set(ctx, 0, 0, "") }},
		{"mset with non-bit payload", func() error { return client.MSet(ctx, []Block{{Bits: "2"}}) }},
		{"batch with no operations", func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, nil)
			return err
		}},
		{"batch with non-bit payload", func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{SetOp(0, 0, "z")})
			return err
		}},
		{"batch with unknown operation", func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{{}})
			return err
		}},
		{"batch xput with non-bit value", func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{XPutOp(0, 0, "102")})
			return err
		}},
		{"batch xput with empty value", func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{SetOp(0, 0, "1010"), XPutOp(0, 0, "")})
			return err
		}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			before := len(server.commands())
			err := testCase.call()
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
			var typed *Error
			if errors.As(err, &typed) && typed.Phase != PhaseRequest {
				t.Fatalf("got phase %q, want %q", typed.Phase, PhaseRequest)
			}
			if after := len(server.commands()); after != before {
				t.Fatalf("client sent %q, want nothing", server.commands()[before:])
			}
		})
	}
}

func TestClientEmptyBatchesSkipTheServer(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.MSet(t.Context(), nil); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	values, err := client.MGet(t.Context(), nil)
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("got %+v, want no values", values)
	}
	if commands := server.commands(); len(commands) != 1 {
		t.Fatalf("got %q, want only HELLO", commands)
	}
}

func TestClientChunkScan(t *testing.T) {
	var page int
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		page++
		if page == 1 {
			writeArray(conn, "CURSOR 2 -3", "0 0", "1 -1")
			return
		}
		writeArray(conn, "END", "2 -3")
	}))
	client := newTestClient(t, server, nil)

	first, err := client.ChunkScan(t.Context(), 2, nil)
	if err != nil {
		t.Fatalf("ChunkScan: %v", err)
	}
	if first.NextCursor == nil || *first.NextCursor != (CoordPair{CX: 2, CY: -3}) {
		t.Fatalf("got cursor %+v, want {2 -3}", first.NextCursor)
	}
	want := []CoordPair{{CX: 0, CY: 0}, {CX: 1, CY: -1}}
	if len(first.Coords) != 2 || first.Coords[0] != want[0] || first.Coords[1] != want[1] {
		t.Fatalf("got %+v, want %+v", first.Coords, want)
	}

	second, err := client.ChunkScan(t.Context(), 2, first.NextCursor)
	if err != nil {
		t.Fatalf("ChunkScan: %v", err)
	}
	if second.NextCursor != nil {
		t.Fatalf("got cursor %+v, want nil at the end of the scan", second.NextCursor)
	}
	if len(second.Coords) != 1 || second.Coords[0] != (CoordPair{CX: 2, CY: -3}) {
		t.Fatalf("got %+v", second.Coords)
	}
}

func TestClientVersionMismatchIsNotAnError(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR VERSION_MISMATCH current=42")
	}))
	client := newTestClient(t, server, nil)
	stale := uint64(41)

	result, err := client.PutChunk(t.Context(), 0, 0, []byte{1, 2}, PutOptions{IfVersion: &stale})
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if result.OK || result.Version != 42 {
		t.Fatalf("got %+v, want {OK:false Version:42}", result)
	}

	result, err = client.PutChunkState(t.Context(), 0, 0, ChunkStateInput{Payload: []byte{1, 2}, Presence: []byte{1}},
		PutOptions{IfVersion: &stale})
	if err != nil {
		t.Fatalf("PutChunkState: %v", err)
	}
	if result.OK || result.Version != 42 {
		t.Fatalf("got %+v, want {OK:false Version:42}", result)
	}

	result, err = client.ChunkBatchIfVersion(t.Context(), 0, 0, stale, []BatchOperation{UnsetOp(0, 0)})
	if err != nil {
		t.Fatalf("ChunkBatchIfVersion: %v", err)
	}
	if result.OK || result.Version != 42 {
		t.Fatalf("got %+v, want {OK:false Version:42}", result)
	}
}

func TestClientMalformedVersionMismatchIsProtocolError(t *testing.T) {
	for _, reply := range []string{
		"ERR VERSION_MISMATCH",
		"ERR VERSION_MISMATCH stale",
		"ERR VERSION_MISMATCH current=",
		"ERR VERSION_MISMATCH current=x",
		"ERR VERSION_MISMATCH current=42 extra",
		"ERR VERSION_MISMATCH current=-1",
		"ERR VERSION_MISMATCH current=18446744073709551616",
		"ERR VERSION_MISMATCH version current=42",
	} {
		t.Run(reply, func(t *testing.T) {
			server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) {
				writeServerError(conn, reply)
			}))
			client := newTestClient(t, server, nil)

			if _, err := client.PutChunk(t.Context(), 0, 0, []byte{1, 2}, PutOptions{}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("PutChunk: got %v, want ErrProtocol", err)
			}
			if _, err := client.ChunkBatchIfVersion(t.Context(), 0, 0, 1, []BatchOperation{UnsetOp(0, 0)}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("ChunkBatchIfVersion: got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestClientMutationSuccessReturnsVersion(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	result, err := client.ChunkBatch(t.Context(), 0, 0, []BatchOperation{UnsetOp(0, 0)})
	if err != nil {
		t.Fatalf("ChunkBatch: %v", err)
	}
	if !result.OK || result.Version != 8 {
		t.Fatalf("got %+v, want {OK:true Version:8}", result)
	}
}

func TestClientInfo(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	info, err := client.Info(t.Context())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Raw != genericInfo {
		t.Fatalf("got raw %q", info.Raw)
	}
	if info.Values["table"] != "default" {
		t.Fatalf("got table %q, want default", info.Values["table"])
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
			if err := client.Set(t.Context(), int64(i), 0, "1010"); err != nil {
				t.Errorf("Set: %v", err)
			}
			if _, err := client.Get(t.Context(), int64(i), 0); err != nil {
				t.Errorf("Get: %v", err)
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
			opts: Options{URI: "chunks://tok@example.test:9000/"},
			want: URI{Scheme: "chunks", Secure: true, Host: "example.test", Port: 9000, Token: "tok", Path: "/"},
		},
		{
			name: "explicit fields win over uri",
			opts: Options{URI: "chunk://tok@example.test:9000/", Host: "other.test", Port: 1, Token: "override"},
			want: URI{Scheme: "chunk", Host: "other.test", Port: 1, Token: "override", Path: "/"},
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
