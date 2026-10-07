package chunkdb

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// extraEntry encodes one EXTRA section entry.
func extraEntry(index, bitLength uint32, value ...byte) []byte {
	entry := []byte{
		byte(index), byte(index >> 8), byte(index >> 16), byte(index >> 24),
		byte(bitLength), byte(bitLength >> 8), byte(bitLength >> 16), byte(bitLength >> 24),
	}
	return append(entry, value...)
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

// requireRequestError fails unless err is a client-side validation error.
func requireRequestError(t *testing.T, err error) {
	t.Helper()
	var typed *Error
	if !errors.Is(err, ErrProtocol) || !errors.As(err, &typed) || typed.Phase != PhaseRequest {
		t.Fatalf("got %v, want a request error", err)
	}
}

// smallExtraHello answers HELLO like a server whose max_extra_chunk_bytes is
// 16, and everything else with genericHandler.
func smallExtraHello(_ *fakeServer, conn net.Conn, command string) {
	if verbOf(command) == "HELLO" {
		writeBulkString(conn, strings.Replace(helloLimits, "max_extra_chunk_bytes=16777216",
			"max_extra_chunk_bytes=16", 1)+defaultInfo)
		return
	}
	genericHandler(nil, conn, command)
}

func TestExtraSectionRoundTrip(t *testing.T) {
	values := map[int]ExtraValue{
		3: {BitLength: 12, Bytes: []byte{0xab, 0x0c}},
		0: {BitLength: 1, Bytes: []byte{0x01}},
		1: {BitLength: 16, Bytes: []byte{0x0d, 0x0a}},
	}
	want := concat(
		extraEntry(0, 1, 0x01),
		extraEntry(1, 16, 0x0d, 0x0a),
		extraEntry(3, 12, 0xab, 0x0c),
	)

	section, err := EncodeExtraSection(values, testBlockCount)
	if err != nil {
		t.Fatalf("EncodeExtraSection: %v", err)
	}
	if !bytes.Equal(section, want) {
		t.Fatalf("got %x, want %x", section, want)
	}
	decoded, err := DecodeExtraSection(section, testBlockCount)
	if err != nil {
		t.Fatalf("DecodeExtraSection: %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("got %+v, want %+v", decoded, values)
	}

	// No values encode to an empty section, which decodes to an empty map.
	for _, empty := range []map[int]ExtraValue{nil, {}} {
		if section, err := EncodeExtraSection(empty, testBlockCount); err != nil || len(section) != 0 {
			t.Fatalf("EncodeExtraSection(%v): %x, %v", empty, section, err)
		}
	}
	if decoded, err := DecodeExtraSection(nil, testBlockCount); err != nil || decoded == nil || len(decoded) != 0 {
		t.Fatalf("DecodeExtraSection(empty): %v, %v", decoded, err)
	}
}

func TestEncodeExtraSectionClearsPadding(t *testing.T) {
	input := []byte{0xff, 0xff}
	section, err := EncodeExtraSection(map[int]ExtraValue{2: {BitLength: 12, Bytes: input}}, testBlockCount)
	if err != nil {
		t.Fatalf("EncodeExtraSection: %v", err)
	}
	if want := extraEntry(2, 12, 0xff, 0x0f); !bytes.Equal(section, want) {
		t.Fatalf("got %x, want %x", section, want)
	}
	if !bytes.Equal(input, []byte{0xff, 0xff}) {
		t.Fatalf("the encoder changed its input to %x", input)
	}
}

func TestEncodeExtraSectionRejectsBadValues(t *testing.T) {
	cases := map[string]map[int]ExtraValue{
		"negative index":       {-1: {BitLength: 1, Bytes: []byte{1}}},
		"index past the chunk": {testBlockCount: {BitLength: 1, Bytes: []byte{1}}},
		"zero bits":            {0: {BitLength: 0, Bytes: []byte{}}},
		"negative bits":        {0: {BitLength: -8, Bytes: []byte{1}}},
		"too many bits":        {0: {BitLength: maxExtraBlockBits + 1, Bytes: []byte{1}}},
		"too few bytes":        {0: {BitLength: 9, Bytes: []byte{1}}},
		"too many bytes":       {0: {BitLength: 8, Bytes: []byte{1, 0}}},
		"nil bytes":            {0: {BitLength: 1}},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := EncodeExtraSection(values, testBlockCount)
			requireRequestError(t, err)
		})
	}
}

func TestDecodeExtraSectionRejectsBadSections(t *testing.T) {
	cases := map[string][]byte{
		"truncated header":    extraEntry(0, 8)[:7],
		"truncated value":     extraEntry(0, 9, 0x01),
		"index past chunk":    extraEntry(testBlockCount, 1, 0x01),
		"largest index":       extraEntry(0xffffffff, 1, 0x01),
		"descending indexes":  concat(extraEntry(2, 1, 0x01), extraEntry(1, 1, 0x01)),
		"repeated index":      concat(extraEntry(1, 1, 0x01), extraEntry(1, 1, 0x01)),
		"zero bits":           extraEntry(0, 0),
		"more bits than any":  extraEntry(0, maxExtraBlockBits+1),
		"largest bit length":  extraEntry(0, 0xffffffff, 0x01),
		"set padding bits":    extraEntry(0, 3, 0x08),
		"trailing bytes":      concat(extraEntry(0, 8, 0xff), []byte{0, 0}),
		"padding of a second": concat(extraEntry(0, 8, 0xff), extraEntry(3, 12, 0x00, 0x10)),
	}
	for name, section := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeExtraSection(section, testBlockCount)
			var typed *Error
			if !errors.Is(err, ErrProtocol) || !errors.As(err, &typed) || typed.Phase != PhaseProtocol {
				t.Fatalf("got %v, want a protocol error", err)
			}
		})
	}
}

func TestClientXGet(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch command {
		case "XGET 0 0":
			writeNull(conn)
		case "XGET 1 0":
			writeBulk(conn, []byte{12, 0, 0, 0, 0xab, 0x0c})
		default:
			genericHandler(nil, conn, command)
		}
	}))
	client := newTestClient(t, server, nil)

	none, err := client.XGet(t.Context(), 0, 0)
	if err != nil || none != nil {
		t.Fatalf("XGet of a block without a value: %+v, %v", none, err)
	}
	value, err := client.XGet(t.Context(), 1, 0)
	if err != nil {
		t.Fatalf("XGet: %v", err)
	}
	if value == nil || value.BitLength != 12 || !bytes.Equal(value.Bytes, []byte{0xab, 0x0c}) {
		t.Fatalf("got %+v, want 12 bits ab 0c", value)
	}
}

func TestClientXGetRejectsMalformedReplies(t *testing.T) {
	cases := map[string]string{
		"shorter than the bit length": bulkWire([]byte{1, 0, 0}),
		"zero bits":                   bulkWire([]byte{0, 0, 0, 0}),
		"too few bytes":               bulkWire([]byte{9, 0, 0, 0, 0xff}),
		"too many bytes":              bulkWire([]byte{8, 0, 0, 0, 0xff, 0x00}),
		"more bits than any":          bulkWire([]byte{0xff, 0xff, 0xff, 0xff, 0x01}),
		"set padding bits":            bulkWire([]byte{4, 0, 0, 0, 0x1f}),
		"simple reply":                "+OK\r\n",
		"array reply":                 "*0\r\n",
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, respondWith(response))
			client := newTestClient(t, server, nil)
			if _, err := client.XGet(t.Context(), 0, 0); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestClientXPutWireFormat(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		x, y  int64
		value ExtraValue
		want  string
	}{
		{1, -2, ExtraValue{BitLength: 12, Bytes: []byte{0xab, 0x0c}}, "XPUT 1 -2 12 2\r\n\xab\x0c\r\n"},
		{0, 0, ExtraValue{BitLength: 1, Bytes: []byte{0x01}}, "XPUT 0 0 1 1\r\n\x01\r\n"},
		// Payload bytes that look like line ends stay inside the payload.
		{3, 4, ExtraValue{BitLength: 16, Bytes: []byte{'\r', '\n'}}, "XPUT 3 4 16 2\r\n\r\n\r\n"},
	}
	for _, testCase := range cases {
		if err := client.XPut(ctx, testCase.x, testCase.y, testCase.value); err != nil {
			t.Fatalf("XPut: %v", err)
		}
		if got := server.lastPut(t); string(got) != testCase.want {
			t.Fatalf("got %q, want %q", got, testCase.want)
		}
	}

	// The requests stayed framed: the connection still works.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestClientXPutValidatesBeforeSending(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	cases := map[string]ExtraValue{
		"zero bits":      {BitLength: 0, Bytes: []byte{}},
		"negative bits":  {BitLength: -1, Bytes: []byte{1}},
		"too many bits":  {BitLength: maxExtraBlockBits + 1, Bytes: []byte{1}},
		"too few bytes":  {BitLength: 12, Bytes: []byte{1}},
		"too many bytes": {BitLength: 12, Bytes: []byte{1, 2, 3}},
		"nil bytes":      {BitLength: 8},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(server.commands())
			requireRequestError(t, client.XPut(t.Context(), 0, 0, value))
			if len(server.commands()) != before {
				t.Fatalf("client sent %q, want nothing", server.commands()[before:])
			}
		})
	}
}

// A table without extra data, a limit or an unset block: the server answers
// INVALID_ARGUMENT, and the connection stays usable.
func TestClientExtraServerErrorsAreTyped(t *testing.T) {
	const message = "extra data is not enabled on table 'default' (TABLESET default extra_max_block_bits <bits>)"
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch verbOf(command) {
		case "XGET", "XPUT", "XDEL", "CHUNKGET", "CHUNKPUT", "CHUNKBATCH":
			writeServerError(conn, "ERR INVALID_ARGUMENT "+message)
		default:
			genericHandler(nil, conn, command)
		}
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	calls := map[string]func() error{
		"XGET": func() error { _, err := client.XGet(ctx, 0, 0); return err },
		"XPUT": func() error { return client.XPut(ctx, 0, 0, ExtraValue{BitLength: 3, Bytes: []byte{5}}) },
		"XDEL": func() error { return client.XDel(ctx, 0, 0) },
		"CHUNKGET": func() error {
			_, err := client.GetChunkStateExtra(ctx, 0, 0, GetOptions{ZRLE: true})
			return err
		},
		"CHUNKPUT": func() error {
			_, err := client.PutChunkStateExtra(ctx, 0, 0, ChunkStateExtraInput{
				Payload: []byte{0, 0}, Presence: []byte{1},
				Extra: map[int]ExtraValue{0: {BitLength: 3, Bytes: []byte{5}}},
			}, PutOptions{})
			return err
		},
		"CHUNKBATCH": func() error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{XPutOp(0, 0, "101")})
			return err
		},
	}
	for command, call := range calls {
		err := call()
		var typed *Error
		if !errors.Is(err, ErrServer) || !errors.As(err, &typed) {
			t.Fatalf("%s: got %v, want ErrServer", command, err)
		}
		if typed.ServerCode != "INVALID_ARGUMENT" || typed.ServerMessage != message || typed.Command != command {
			t.Fatalf("%s: got command %q code %q message %q", command, typed.Command, typed.ServerCode, typed.ServerMessage)
		}
	}

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want the first one to stay usable", got)
	}
}

func TestClientGetChunkStateExtra(t *testing.T) {
	payload := []byte{0xab, 0xcd}
	presence := []byte{0x0b}
	section := concat(extraEntry(0, 1, 0x01), extraEntry(3, 12, 0xab, 0x0c))
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		body := concat(payload, presence, section)
		if strings.HasSuffix(command, " ZRLE") {
			body = ZRLECompress(body)
		}
		writeBulk(conn, body)
	}))
	client := newTestClient(t, server, nil)
	want := map[int]ExtraValue{
		0: {BitLength: 1, Bytes: []byte{0x01}},
		3: {BitLength: 12, Bytes: []byte{0xab, 0x0c}},
	}

	for _, zrle := range []bool{false, true} {
		chunk, err := client.GetChunkStateExtra(t.Context(), 0, 0, GetOptions{ZRLE: zrle})
		if err != nil {
			t.Fatalf("GetChunkStateExtra zrle=%v: %v", zrle, err)
		}
		if !chunk.Exists || !bytes.Equal(chunk.Payload, payload) || !bytes.Equal(chunk.Presence, presence) ||
			!maps.EqualFunc(chunk.Extra, want, func(a, b ExtraValue) bool {
				return a.BitLength == b.BitLength && bytes.Equal(a.Bytes, b.Bytes)
			}) {
			t.Fatalf("zrle=%v: got %+v", zrle, chunk)
		}
		// Payload and presence do not reach into the values.
		if cap(chunk.Presence) != len(presence) {
			t.Fatalf("zrle=%v: presence has capacity %d", zrle, cap(chunk.Presence))
		}
	}
}

func TestClientGetChunkStateExtraAbsent(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)

	for _, zrle := range []bool{false, true} {
		chunk, err := client.GetChunkStateExtra(t.Context(), 0, 0, GetOptions{ZRLE: zrle})
		if err != nil {
			t.Fatalf("GetChunkStateExtra zrle=%v: %v", zrle, err)
		}
		if chunk.Exists || len(chunk.Payload) != testChunkPayloadBytes || len(chunk.Presence) != testPresenceBytes ||
			chunk.Extra == nil || len(chunk.Extra) != 0 {
			t.Fatalf("zrle=%v: got %+v, want an absent chunk without values", zrle, chunk)
		}
	}
}

func TestClientGetChunkStateExtraRejectsMalformedReplies(t *testing.T) {
	state := []byte{0, 0, 1}
	// smallExtraHello allows 16 bytes of extra data: one value of 8 bytes is
	// the most a section can hold.
	fits := concat(state, extraEntry(0, 64, make([]byte, 8)...))
	cases := []struct {
		name string
		body []byte
		zrle bool
	}{
		{name: "shorter than the state", body: state[:2]},
		{name: "section over the server's limit", body: concat(state, extraEntry(0, 72, make([]byte, 9)...))},
		{name: "invalid section", body: concat(state, extraEntry(1, 1, 1), extraEntry(0, 1, 1))},
		{name: "section with set padding", body: concat(state, extraEntry(0, 1, 0x03))},
		{name: "zrle declaring less than the state", body: ZRLECompress(state[:2]), zrle: true},
		{name: "zrle declaring more than the limit", body: ZRLECompress(concat(fits, []byte{0})), zrle: true},
		{name: "zrle that is not zrle", body: []byte("abcdef"), zrle: true},
		{name: "zrle too short", body: []byte{1, 2}, zrle: true},
		{name: "zrle producing less than declared", body: ZRLECompress(fits)[:8], zrle: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newFakeServer(t, func(s *fakeServer, conn net.Conn, command string) {
				if verbOf(command) == "CHUNKGET" {
					writeBulk(conn, testCase.body)
					return
				}
				smallExtraHello(s, conn, command)
			})
			client := newTestClient(t, server, nil)
			if _, err := client.GetChunkStateExtra(t.Context(), 0, 0, GetOptions{ZRLE: testCase.zrle}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}

	// The largest section the server allows is accepted.
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "CHUNKGET" {
			body := fits
			if strings.HasSuffix(command, " ZRLE") {
				body = ZRLECompress(fits)
			}
			writeBulk(conn, body)
			return
		}
		smallExtraHello(s, conn, command)
	})
	client := newTestClient(t, server, nil)
	for _, zrle := range []bool{false, true} {
		chunk, err := client.GetChunkStateExtra(t.Context(), 0, 0, GetOptions{ZRLE: zrle})
		if err != nil || len(chunk.Extra) != 1 || chunk.Extra[0].BitLength != 64 {
			t.Fatalf("zrle=%v: got %+v, %v", zrle, chunk, err)
		}
	}
}

func TestClientPutChunkStateExtraWireFormat(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	// terrain chunks hold 10 state bytes, enough for zrle to pay off.
	terrain := newTestClient(t, server, func(o *Options) { o.Table = "terrain" })
	ctx := t.Context()

	version := uint64(9)
	values := map[int]ExtraValue{
		3: {BitLength: 12, Bytes: []byte{0xab, 0x0c}},
		0: {BitLength: 1, Bytes: []byte{0x01}},
	}
	section := concat(extraEntry(0, 1, 0x01), extraEntry(3, 12, 0xab, 0x0c))
	zeroState := make([]byte, 10)
	sparseValue := ExtraValue{BitLength: 64, Bytes: make([]byte, 8)}
	sparse := concat(zeroState, extraEntry(15, 64, sparseValue.Bytes...))
	compressed := ZRLECompress(sparse)
	if len(compressed) >= len(sparse) {
		t.Fatalf("the sparse chunk does not shrink: %d >= %d bytes", len(compressed), len(sparse))
	}
	dense := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	cases := []struct {
		name string
		call func() (MutationResult, error)
		want []byte
	}{
		{
			name: "state and values",
			call: func() (MutationResult, error) {
				return client.PutChunkStateExtra(ctx, 1, -2, ChunkStateExtraInput{
					Payload: []byte{0xab, 0xcd}, Presence: []byte{0x09}, Extra: values,
				}, PutOptions{})
			},
			want: concat([]byte("CHUNKPUT 1 -2 STATE EXTRA 22\r\n\xab\xcd\x09"), section, []byte("\r\n")),
		},
		{
			name: "no values",
			call: func() (MutationResult, error) {
				return client.PutChunkStateExtra(ctx, 1, 2, ChunkStateExtraInput{
					Payload: []byte{0xab, 0xcd}, Presence: []byte{0x0f},
				}, PutOptions{IfVersion: &version})
			},
			want: []byte("CHUNKPUT 1 2 STATE EXTRA IF 9 3\r\n\xab\xcd\x0f\r\n"),
		},
		{
			name: "zrle that shrinks",
			call: func() (MutationResult, error) {
				return terrain.PutChunkStateExtra(ctx, 1, 2, ChunkStateExtraInput{
					Payload: zeroState[:8], Presence: zeroState[8:],
					Extra: map[int]ExtraValue{15: sparseValue},
				}, PutOptions{ZRLE: true, IfVersion: &version})
			},
			want: concat([]byte(fmt.Sprintf("CHUNKPUT 1 2 STATE EXTRA ZRLE IF 9 %d\r\n", len(compressed))),
				compressed, []byte("\r\n")),
		},
		{
			name: "zrle of a dense chunk is sent raw",
			call: func() (MutationResult, error) {
				return terrain.PutChunkStateExtra(ctx, 1, 2, ChunkStateExtraInput{
					Payload: dense[:8], Presence: dense[8:],
				}, PutOptions{ZRLE: true})
			},
			want: concat([]byte("CHUNKPUT 1 2 STATE EXTRA 10\r\n"), dense, []byte("\r\n")),
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

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestClientPutChunkStateExtraValidatesBeforeSending(t *testing.T) {
	server := newFakeServer(t, smallExtraHello)
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	valid := func(extra map[int]ExtraValue) ChunkStateExtraInput {
		return ChunkStateExtraInput{Payload: []byte{0, 0}, Presence: []byte{0x0f}, Extra: extra}
	}
	cases := map[string]ChunkStateExtraInput{
		"short payload":    {Payload: []byte{0}, Presence: []byte{0x0f}},
		"long presence":    {Payload: []byte{0, 0}, Presence: []byte{0x0f, 0}},
		"index past chunk": valid(map[int]ExtraValue{testBlockCount: {BitLength: 1, Bytes: []byte{1}}}),
		"negative index":   valid(map[int]ExtraValue{-1: {BitLength: 1, Bytes: []byte{1}}}),
		"zero bits":        valid(map[int]ExtraValue{0: {BitLength: 0, Bytes: []byte{}}}),
		"wrong byte count": valid(map[int]ExtraValue{0: {BitLength: 9, Bytes: []byte{1}}}),
		// 8 + 9 bytes is more than the server's 16: it would close the
		// connection.
		"section over the server's limit": valid(map[int]ExtraValue{0: {BitLength: 72, Bytes: make([]byte, 9)}}),
		"sections over the server's limit": valid(map[int]ExtraValue{
			0: {BitLength: 1, Bytes: []byte{1}}, 1: {BitLength: 1, Bytes: []byte{1}},
		}),
	}
	for name, state := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(server.commands())
			_, err := client.PutChunkStateExtra(ctx, 0, 0, state, PutOptions{ZRLE: true})
			requireRequestError(t, err)
			if len(server.commands()) != before {
				t.Fatalf("client sent %q, want nothing", server.commands()[before:])
			}
		})
	}

	// The largest section the server allows is sent.
	fits := valid(map[int]ExtraValue{3: {BitLength: 64, Bytes: make([]byte, 8)}})
	if _, err := client.PutChunkStateExtra(ctx, 0, 0, fits, PutOptions{}); err != nil {
		t.Fatalf("PutChunkStateExtra: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want the first one to stay usable", got)
	}
}

func TestClientHelloWithoutExtraData(t *testing.T) {
	// A server without the extra-data capability reports neither the server
	// limit nor the table options.
	var kept []string
	for _, line := range strings.Split(helloLimits+defaultInfo, "\n") {
		if !strings.HasPrefix(line, "extra_") && !strings.HasPrefix(line, "max_extra_") {
			kept = append(kept, line)
		}
	}
	reply := strings.Replace(strings.Join(kept, "\n"), "capabilities=zrle,extra-data", "capabilities=zrle", 1)
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) { writeBulkString(conn, reply) })
	client := newTestClient(t, server, nil)

	info := client.ServerInfo()
	if info.MaxExtraChunkBytes != 0 || info.Table == nil || info.Table.Options.ExtraMaxBlockBits != 0 ||
		info.Table.Options.ExtraMaxChunkBytes != 0 || len(info.Capabilities) != 1 {
		t.Fatalf("got %+v, table %+v", info, info.Table)
	}
	// No extra data can be sent to it: an XPUT value would be read as
	// commands.
	_, err := client.PutChunkStateExtra(t.Context(), 0, 0, ChunkStateExtraInput{
		Payload: []byte{0, 0}, Presence: []byte{1}, Extra: map[int]ExtraValue{0: {BitLength: 1, Bytes: []byte{1}}},
	}, PutOptions{})
	requireRequestError(t, err)
	err = client.XPut(t.Context(), 0, 0, ExtraValue{BitLength: 8, Bytes: []byte("x")})
	requireRequestError(t, err)
	if !strings.Contains(err.Error(), "needs a server with extra data") {
		t.Fatalf("got %v, want the missing extra data named", err)
	}
}

// XPut is bounded by the server's max_extra_chunk_bytes minus the 8-byte
// entry header: the server refuses a longer value unread and closes.
func TestClientXPutBoundedByServerCap(t *testing.T) {
	var xputs atomic.Int32
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "XPUT" {
			xputs.Add(1)
		}
		smallExtraHello(s, conn, command)
	})
	client := newTestClient(t, server, nil)
	requireRequestError(t, client.XPut(t.Context(), 0, 0, ExtraValue{BitLength: 72, Bytes: make([]byte, 9)}))
	if xputs.Load() != 0 {
		t.Fatal("the value over the cap was sent")
	}
}

// A request line longer than the server's max_line_bytes is refused before
// it is sent: the server would answer BAD_REQUEST and close the connection.
func TestClientRefusesLineOverServerLimit(t *testing.T) {
	var batches atomic.Int32
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "CHUNKBATCH" {
			batches.Add(1)
		}
		genericHandler(s, conn, command)
	}))
	client := newTestClient(t, server, nil)
	_, err := client.ChunkBatch(t.Context(), 0, 0, []BatchOperation{XPutOp(0, 0, strings.Repeat("1", 70000))})
	requireRequestError(t, err)
	if batches.Load() != 0 {
		t.Fatal("the long line was sent")
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("connection should stay usable: %v", err)
	}
}

// A chunk read is bounded by the size its geometry implies, which may exceed
// MaxBulkBytes: here 64 MiB of payload, 2 MiB of presence and extra data.
func TestClientReadsChunkStateLargerThanMaxBulkBytes(t *testing.T) {
	big := strings.NewReplacer("block_bits=4\n", "block_bits=32\n",
		"chunk_width_blocks=2\n", "chunk_width_blocks=4096\n",
		"chunk_height_blocks=2\n", "chunk_height_blocks=4096\n").Replace(defaultInfo)
	const stateBytes = 4096*4096*4 + 4096*4096/8
	reply := concat(make([]byte, stateBytes), extraEntry(0, 8, 0x5a))
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		switch {
		case verbOf(command) == "HELLO":
			writeBulkString(conn, helloLimits+big)
		case strings.HasSuffix(command, " EXTRA"):
			writeBulk(conn, reply)
		default:
			writeBulk(conn, reply[:stateBytes])
		}
	})
	// 66 MiB over loopback: allow for a loaded machine.
	client := newTestClient(t, server, func(o *Options) { o.CommandTimeout = 60 * time.Second })
	state, err := client.GetChunkState(t.Context(), 0, 0, GetOptions{})
	if err != nil || len(state.Payload)+len(state.Presence) != stateBytes {
		t.Fatalf("GetChunkState: %v", err)
	}
	extra, err := client.GetChunkStateExtra(t.Context(), 0, 0, GetOptions{})
	if err != nil || len(extra.Extra) != 1 {
		t.Fatalf("GetChunkStateExtra: %v", err)
	}
}
