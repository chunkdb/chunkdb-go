package chunkdb

import (
	"context"
	"errors"
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

// genericHandler answers every command with a well-formed, minimal response so
// tests can focus on what the client sends.
func genericHandler(_ *fakeServer, conn net.Conn, command string) {
	args := strings.Fields(command)
	switch verbOf(command) {
	case "PING":
		writeSimple(conn, "PONG")
	case "SET", "UNSET", "MSET", "CHUNKSET", "WALFLUSH":
		writeSimple(conn, "OK")
	case "EXISTS", "CHUNKEXISTS":
		writeSimple(conn, "0")
	case "GET":
		writeBulkString(conn, "0000")
	case "CHUNK":
		if strings.HasSuffix(command, " STATE") {
			writeBulkString(conn, strings.Repeat("0", testChunkPayloadBits)+"|"+strings.Repeat("0", testChunkBlockCount))
			return
		}
		writeBulkString(conn, strings.Repeat("0", testChunkPayloadBits))
	case "CHUNKBIN":
		size := testChunkPayloadBytes
		if strings.HasSuffix(command, " STATE") {
			size += testPresenceBytes
		}
		writeBulk(conn, make([]byte, size))
	case "CHUNKBINC":
		size := testChunkPayloadBytes
		if strings.HasSuffix(command, " STATE") {
			size += testPresenceBytes
		}
		writeBulk(conn, ZRLECompress(make([]byte, size)))
	case "INFO":
		writeBulkString(conn, infoPayload)
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
	case "CHUNKCAS", "CHUNKBATCH":
		writeBulkString(conn, "8")
	default:
		writeServerError(conn, "ERR UNKNOWN_COMMAND "+verbOf(command))
	}
}

func TestClientAutoAuthAndPing(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	commands := server.commands()
	want := []string{"AUTH tok", "PING"}
	if len(commands) != len(want) {
		t.Fatalf("got commands %q, want %q", commands, want)
	}
	for i, command := range want {
		if commands[i] != command {
			t.Fatalf("command %d: got %q, want %q", i, commands[i], command)
		}
	}
}

func TestClientDisableAutoAuth(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, func(o *Options) { o.DisableAutoAuth = true })

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if commands := server.commands(); len(commands) != 1 || commands[0] != "PING" {
		t.Fatalf("got %q, want only PING", commands)
	}
}

func TestClientNoTokenSkipsAuth(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client, err := Connect(t.Context(), Options{URI: server.uri("")})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if commands := server.commands(); len(commands) != 1 || commands[0] != "PING" {
		t.Fatalf("got %q, want only PING", commands)
	}
}

func TestClientExplicitAuthWithoutTokenFails(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client, err := Connect(t.Context(), Options{URI: server.uri("")})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	err = client.Auth(t.Context(), "")
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
	if len(server.commands()) != 0 {
		t.Fatalf("client sent %q, want nothing", server.commands())
	}
}

func TestClientAuthFailureIsTyped(t *testing.T) {
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR AUTH_FAILED invalid token")
	})

	_, err := Connect(t.Context(), Options{URI: server.uri("wrong")})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}
	// AUTH_FAILED is a specialization of a server error, so it matches both.
	if !errors.Is(err, ErrServer) {
		t.Fatalf("got %v, want it to also match ErrServer", err)
	}

	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("got %T, want *chunkdb.Error", err)
	}
	if typed.ServerCode != "AUTH_FAILED" || typed.ServerMessage != "invalid token" {
		t.Fatalf("got code %q message %q", typed.ServerCode, typed.ServerMessage)
	}
	if typed.Phase != PhaseAuth {
		t.Fatalf("got phase %q, want %q", typed.Phase, PhaseAuth)
	}
}

func TestClientServerErrorIsTyped(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
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
	if typed.Command != "SET" {
		t.Fatalf("got command %q, want SET", typed.Command)
	}
}

func TestClientCommandEncoding(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
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
			name: "chunk state read",
			call: func() error { _, err := client.ReadChunk(ctx, 1, 2); return err },
			want: "CHUNK 1 2 STATE",
		},
		{
			name: "chunk set",
			call: func() error { return client.SetChunk(ctx, 1, 2, "1111000011110000") },
			want: "CHUNKSET 1 2 1111000011110000",
		},
		{
			name: "chunk set state",
			call: func() error {
				return client.SetChunkState(ctx, 1, 2, ChunkStateInput{
					Bits:     strings.Repeat("1", testChunkPayloadBits),
					Presence: strings.Repeat("1", testChunkBlockCount),
				})
			},
			want: "CHUNKSET 1 2 STATE 1111111111111111|1111",
		},
		{
			name: "chunkbin state",
			call: func() error { _, err := client.ChunkBinState(ctx, 1, 2); return err },
			want: "CHUNKBIN 1 2 STATE",
		},
		{
			name: "chunkbinc",
			call: func() error { _, err := client.ChunkBinCompressed(ctx, 1, 2); return err },
			want: "CHUNKBINC 1 2",
		},
		{
			name: "chunkbinc state",
			call: func() error { _, err := client.ChunkBinStateCompressed(ctx, 1, 2); return err },
			want: "CHUNKBINC 1 2 STATE",
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
			call: func() error { _, err := client.ChunkRange(ctx, -1, -2, 3, 4); return err },
			want: "CHUNKRANGE -1 -2 3 4",
		},
		{
			name: "radius",
			call: func() error { _, err := client.ChunkRadius(ctx, 1, 2, 3); return err },
			want: "CHUNKRADIUS 1 2 3",
		},
		{
			name: "version",
			call: func() error { _, err := client.ChunkVersion(ctx, 1, 2); return err },
			want: "CHUNKVER 1 2",
		},
		{
			name: "compare and set",
			call: func() error {
				_, err := client.ChunkCompareAndSet(ctx, 1, 2, 5, ChunkStateInput{Bits: "1111", Presence: "1"})
				return err
			},
			want: "CHUNKCAS 1 2 5 STATE 1111|1",
		},
		{
			name: "batch without version",
			call: func() error {
				_, err := client.ChunkBatch(ctx, 1, 2, []BatchOperation{SetOp(3, 4, "1010"), UnsetOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 - SET 3 4 1010 UNSET 5 6",
		},
		{
			name: "batch with version",
			call: func() error {
				_, err := client.ChunkBatchIfVersion(ctx, 1, 2, 9, []BatchOperation{UnsetOp(5, 6)})
				return err
			},
			want: "CHUNKBATCH 1 2 9 UNSET 5 6",
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
}

func TestClientReadBlock(t *testing.T) {
	var present bool
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, command string) {
		switch verbOf(command) {
		case "EXISTS":
			if present {
				writeSimple(conn, "1")
				return
			}
			writeSimple(conn, "0")
		case "GET":
			writeBulkString(conn, "1011")
		}
	}))
	client := newTestClient(t, server, nil)

	state, err := client.ReadBlock(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if state.Exists || state.Bits != "" {
		t.Fatalf("got %+v, want an absent block", state)
	}
	// An absent block must not cost a GET round trip.
	if commands := server.commands(); len(commands) != 2 {
		t.Fatalf("got %q, want AUTH and EXISTS only", commands)
	}

	present = true
	state, err = client.ReadBlock(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if !state.Exists || state.Bits != "1011" {
		t.Fatalf("got %+v, want an existing block with bits 1011", state)
	}
}

func TestClientReadChunk(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeBulkString(conn, "1111000000000000|1000")
	}))
	client := newTestClient(t, server, nil)

	state, err := client.ReadChunk(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !state.Exists {
		t.Fatal("got Exists false, want true for a chunk with a present block")
	}
	if state.Bits != "1111000000000000" || state.Presence != "1000" {
		t.Fatalf("got %+v", state)
	}
}

func TestClientReadChunkAbsent(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeBulkString(conn, "0000000000000000|0000")
	}))
	client := newTestClient(t, server, nil)

	state, err := client.ReadChunk(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if state.Exists {
		t.Fatal("got Exists true, want false for an all-absent chunk")
	}
}

func TestClientMGet(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeArray(conn, "1010", "0101")
	}))
	client := newTestClient(t, server, nil)

	values, err := client.MGet(t.Context(), []BlockRef{{X: 0, Y: 0}, {X: 1, Y: 0}})
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(values) != 2 || values[0] != "1010" || values[1] != "0101" {
		t.Fatalf("got %q", values)
	}
}

func TestClientEmptyBatchesSkipTheServer(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, nil)

	if err := client.MSet(t.Context(), nil); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	values, err := client.MGet(t.Context(), nil)
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("got %q, want no values", values)
	}
	if commands := server.commands(); len(commands) != 1 {
		t.Fatalf("got %q, want only AUTH", commands)
	}
}

func TestClientChunkScan(t *testing.T) {
	var page int
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
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

func TestClientChunkRange(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeArray(conn, "-1 -2 1111000000000000|1000", "3 4 0000000000000000|0001")
	}))
	client := newTestClient(t, server, nil)

	entries, err := client.ChunkRange(t.Context(), -1, -2, 3, 4)
	if err != nil {
		t.Fatalf("ChunkRange: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0] != (RangeEntry{CX: -1, CY: -2, Bits: "1111000000000000", Presence: "1000"}) {
		t.Fatalf("got %+v", entries[0])
	}
	if entries[1] != (RangeEntry{CX: 3, CY: 4, Bits: "0000000000000000", Presence: "0001"}) {
		t.Fatalf("got %+v", entries[1])
	}
}

func TestClientRejectsMalformedResponses(t *testing.T) {
	cases := []struct {
		name     string
		response string
		call     func(*Client) error
	}{
		{
			name:     "chunk state without separator",
			response: "$4\r\n1111\r\n",
			call:     func(c *Client) error { _, err := c.ReadChunk(t.Context(), 0, 0); return err },
		},
		{
			name:     "chunk state with non-bit payload",
			response: "$6\r\n11x1|1\r\n",
			call:     func(c *Client) error { _, err := c.ReadChunk(t.Context(), 0, 0); return err },
		},
		{
			name:     "exists out of range",
			response: "+2\r\n",
			call:     func(c *Client) error { _, err := c.Exists(t.Context(), 0, 0); return err },
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
			name:     "range entry is truncated",
			response: "*1\r\n$3\r\n0 0\r\n",
			call:     func(c *Client) error { _, err := c.ChunkRange(t.Context(), 0, 0, 0, 0); return err },
		},
		{
			name:     "version is not a number",
			response: "$3\r\nabc\r\n",
			call:     func(c *Client) error { _, err := c.ChunkVersion(t.Context(), 0, 0); return err },
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
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		name string
		call func() error
	}{
		{"set with non-bit payload", func() error { return client.Set(ctx, 0, 0, "10x0") }},
		{"set with empty payload", func() error { return client.Set(ctx, 0, 0, "") }},
		{"mset with non-bit payload", func() error { return client.MSet(ctx, []Block{{Bits: "2"}}) }},
		{"chunk set with non-bit payload", func() error { return client.SetChunk(ctx, 0, 0, "abc") }},
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

func TestClientSetChunkStateValidatesAgainstGeometry(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	fullBits := strings.Repeat("1", testChunkPayloadBits)
	fullPresence := strings.Repeat("1", testChunkBlockCount)

	if err := client.SetChunkState(ctx, 0, 0, ChunkStateInput{Bits: "1111", Presence: fullPresence}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("short payload: got %v, want ErrProtocol", err)
	}
	if err := client.SetChunkState(ctx, 0, 0, ChunkStateInput{Bits: fullBits, Presence: "1"}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("short presence: got %v, want ErrProtocol", err)
	}
	if err := client.SetChunkState(ctx, 0, 0, ChunkStateInput{Bits: fullBits, Presence: fullPresence}); err != nil {
		t.Fatalf("SetChunkState: %v", err)
	}

	// Geometry is static, so INFO is fetched once and cached.
	var infoCalls int
	for _, command := range server.commands() {
		if command == "INFO" {
			infoCalls++
		}
	}
	if infoCalls != 1 {
		t.Fatalf("got %d INFO calls, want 1", infoCalls)
	}
}

func TestClientChunkBinStateChecksLength(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "INFO" {
			writeBulkString(conn, infoPayload)
			return
		}
		writeBulk(conn, make([]byte, testChunkPayloadBytes)) // presence bytes missing
	}))
	client := newTestClient(t, server, nil)

	if _, err := client.ChunkBinState(t.Context(), 0, 0); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestClientChunkBinCompressed(t *testing.T) {
	payload := []byte{0xab, 0xcd}
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "INFO" {
			writeBulkString(conn, infoPayload)
			return
		}
		writeBulk(conn, ZRLECompress(payload))
	}))
	client := newTestClient(t, server, nil)

	got, err := client.ChunkBinCompressed(t.Context(), 0, 0)
	if err != nil {
		t.Fatalf("ChunkBinCompressed: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %v, want %v", got, payload)
	}
}

func TestClientChunkBinCompressedRejectsWrongSize(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "INFO" {
			writeBulkString(conn, infoPayload)
			return
		}
		// One byte short of the geometry-derived payload size.
		writeBulk(conn, ZRLECompress([]byte{0x01}))
	}))
	client := newTestClient(t, server, nil)

	if _, err := client.ChunkBinCompressed(t.Context(), 0, 0); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestClientVersionMismatchIsNotAnError(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR VERSION_MISMATCH current=42")
	}))
	client := newTestClient(t, server, nil)

	result, err := client.ChunkCompareAndSet(t.Context(), 0, 0, 41, ChunkStateInput{Bits: "1111", Presence: "1"})
	if err != nil {
		t.Fatalf("ChunkCompareAndSet: %v", err)
	}
	if result.OK || result.Version != 42 {
		t.Fatalf("got %+v, want {OK:false Version:42}", result)
	}

	result, err = client.ChunkBatchIfVersion(t.Context(), 0, 0, 41, []BatchOperation{UnsetOp(0, 0)})
	if err != nil {
		t.Fatalf("ChunkBatchIfVersion: %v", err)
	}
	if result.OK || result.Version != 42 {
		t.Fatalf("got %+v, want {OK:false Version:42}", result)
	}
}

func TestClientVersionMismatchWithoutCurrentIsProtocolError(t *testing.T) {
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR VERSION_MISMATCH stale")
	}))
	client := newTestClient(t, server, nil)

	if _, err := client.ChunkCompareAndSet(t.Context(), 0, 0, 1, ChunkStateInput{Bits: "1", Presence: "1"}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestClientMutationSuccessReturnsVersion(t *testing.T) {
	server := newFakeServer(t, withAuth(genericHandler))
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
	server := newFakeServer(t, withAuth(genericHandler))
	client := newTestClient(t, server, nil)

	info, err := client.Info(t.Context())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Raw != infoPayload {
		t.Fatalf("got raw %q", info.Raw)
	}
	if info.Values["block_bits"] != "4" {
		t.Fatalf("got block_bits %q, want 4", info.Values["block_bits"])
	}
}

func TestClientCommandTimeoutDropsConnection(t *testing.T) {
	var answer bool
	var mu sync.Mutex
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
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
	server := newFakeServer(t, withAuth(func(*fakeServer, net.Conn, string) {}))
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
	server := newFakeServer(t, withAuth(genericHandler))
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
	server := newFakeServer(t, withAuth(func(s *fakeServer, _ net.Conn, _ string) {
		go s.dropConnections()
	}))
	client := newTestClient(t, server, nil)

	if err := client.Ping(t.Context()); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want ErrConnection", err)
	}
}

func TestClientReconnectsWhenTheConnectionDiesBeforeUse(t *testing.T) {
	// The server answers AUTH and hangs up immediately, so the connection is
	// already dead by the time the client finishes connecting and would publish
	// it. Each attempt must still dial a fresh socket instead of getting stuck
	// on the dead one.
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		if strings.HasPrefix(command, "AUTH ") {
			writeSimple(conn, "OK")
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
	server := newFakeServer(t, withAuth(genericHandler))
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
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
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
	server := newFakeServer(t, withAuth(func(_ *fakeServer, conn net.Conn, _ string) {
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
	server := newFakeServer(t, withAuth(genericHandler))
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
