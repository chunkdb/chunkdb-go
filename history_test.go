package chunkdb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// historyHello answers HELLO like a server whose limits are changed by
// replace (old, new pairs), and everything else with genericHandler.
func historyHello(replace ...string) func(*fakeServer, net.Conn, string) {
	reply := strings.NewReplacer(replace...).Replace(helloLimits + defaultInfo)
	return func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "HELLO" {
			writeBulkString(conn, reply)
			return
		}
		genericHandler(nil, conn, command)
	}
}

// withoutHistoryHello is the HELLO of a server without the history
// capability: no max_tag_bytes, max_history_limit or table history lines.
func withoutHistoryHello() string {
	var kept []string
	for _, line := range strings.Split(helloLimits+defaultInfo, "\n") {
		if !strings.HasPrefix(line, "history") && !strings.HasPrefix(line, "max_tag_bytes") &&
			!strings.HasPrefix(line, "max_history_limit") {
			kept = append(kept, line)
		}
	}
	return strings.Replace(strings.Join(kept, "\n"), "capabilities=zrle,extra-data,history", "capabilities=zrle,extra-data", 1)
}

// arrayWire frames items as an array response.
func arrayWire(items ...string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "*%d\r\n", len(items))
	for _, item := range items {
		out.WriteString(bulkWire([]byte(item)))
	}
	return out.String()
}

func TestClientHistoryCommandEncoding(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	version := uint64(9)
	longest := bytes.Repeat([]byte{0xab}, 255)

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "history",
			call: func() error { _, err := client.History(ctx, 1, -2, HistoryOptions{}); return err },
			want: "HISTORY 1 -2",
		},
		{
			name: "history with every option",
			call: func() error {
				_, err := client.History(ctx, 1, 2, HistoryOptions{
					Limit: 20, Ascending: true, After: "7:3", Before: "90", SinceMs: 1000, UntilMs: 2000, Tag: []byte("job"),
				})
				return err
			},
			want: "HISTORY 1 2 LIMIT 20 ASC AFTER 7:3 BEFORE 90 SINCE 1000 UNTIL 2000 TAG 6a6f62",
		},
		{
			name: "chunk history at the largest limit",
			call: func() error { _, err := client.ChunkHistory(ctx, -1, 2, HistoryOptions{Limit: 1024}); return err },
			want: "CHUNKHISTORY -1 2 LIMIT 1024",
		},
		{
			name: "chunk history after a chunk version",
			call: func() error {
				_, err := client.ChunkHistory(ctx, 0, 0, HistoryOptions{Ascending: true, After: RevisionCursor(1041)})
				return err
			},
			want: "CHUNKHISTORY 0 0 ASC AFTER 1041",
		},
		{
			name: "range history with the largest cursor",
			call: func() error {
				_, err := client.RangeHistory(ctx, -1, -2, 3, 4, HistoryOptions{Before: "18446744073709551615:4294967295"})
				return err
			},
			want: "RANGEHISTORY -1 -2 3 4 BEFORE 18446744073709551615:4294967295",
		},
		{
			name: "get at the present",
			call: func() error { _, err := client.GetAt(ctx, 1, 2, HistoryPoint{}); return err },
			want: "GET 1 2",
		},
		{
			name: "get at a revision",
			call: func() error { _, err := client.GetAt(ctx, 1, 2, AtRevision(5)); return err },
			want: "GET 1 2 AT 5",
		},
		{
			name: "get at revision 0",
			call: func() error { _, err := client.GetAt(ctx, 1, 2, AtRevision(0)); return err },
			want: "GET 1 2 AT 0",
		},
		{
			name: "get at the largest revision",
			call: func() error { _, err := client.GetAt(ctx, 1, 2, AtRevision(math.MaxUint64)); return err },
			want: "GET 1 2 AT 18446744073709551615",
		},
		{
			name: "get at a time",
			call: func() error { _, err := client.GetAt(ctx, 1, 2, AtTimeMs(1700000000000)); return err },
			want: "GET 1 2 AT TIME 1700000000000",
		},
		{
			name: "chunk get at a revision",
			call: func() error { _, err := client.GetChunk(ctx, 1, 2, GetOptions{At: AtRevision(5)}); return err },
			want: "CHUNKGET 1 2 AT 5",
		},
		{
			name: "chunk get zrle at a time",
			call: func() error {
				_, err := client.GetChunk(ctx, 1, 2, GetOptions{ZRLE: true, At: AtTimeMs(7)})
				return err
			},
			want: "CHUNKGET 1 2 ZRLE AT TIME 7",
		},
		{
			name: "chunk get state at a revision",
			call: func() error { _, err := client.GetChunkState(ctx, 1, 2, GetOptions{At: AtRevision(5)}); return err },
			want: "CHUNKGET 1 2 STATE AT 5",
		},
		{
			name: "chunk get state zrle at time 0",
			call: func() error {
				_, err := client.GetChunkState(ctx, 1, 2, GetOptions{ZRLE: true, At: AtTimeMs(0)})
				return err
			},
			want: "CHUNKGET 1 2 STATE ZRLE AT TIME 0",
		},
		{
			name: "chunk get state extra zrle at a revision",
			call: func() error {
				_, err := client.GetChunkStateExtra(ctx, 1, 2, GetOptions{ZRLE: true, At: AtRevision(9)})
				return err
			},
			want: "CHUNKGET 1 2 STATE EXTRA ZRLE AT 9",
		},
		{
			name: "range at a revision",
			call: func() error {
				_, err := client.ChunkRange(ctx, -1, -2, 3, 4, GetOptions{At: AtRevision(5)})
				return err
			},
			want: "CHUNKRANGE -1 -2 3 4 STATE AT 5",
		},
		{
			name: "radius zrle at a time",
			call: func() error {
				_, err := client.ChunkRadius(ctx, 1, 2, 3, GetOptions{ZRLE: true, At: AtTimeMs(9)})
				return err
			},
			want: "CHUNKRADIUS 1 2 3 STATE ZRLE AT TIME 9",
		},
		{
			name: "tagged set",
			call: func() error { return client.Set(ctx, 1, 2, "1010", WithTag([]byte("job"))) },
			want: "SET 1 2 1010 TAG 6a6f62",
		},
		{
			name: "set with the longest tag",
			call: func() error { return client.Set(ctx, 1, 2, "1010", WithTag(longest)) },
			want: "SET 1 2 1010 TAG " + strings.Repeat("ab", 255),
		},
		{
			name: "set with an empty tag and a nil option",
			call: func() error { return client.Set(ctx, 1, 2, "1010", WithTag([]byte{}), nil, WithTag(nil)) },
			want: "SET 1 2 1010",
		},
		{
			name: "the last tag wins",
			call: func() error { return client.Set(ctx, 1, 2, "1010", WithTag([]byte{1}), WithTag([]byte{2})) },
			want: "SET 1 2 1010 TAG 02",
		},
		{
			name: "tagged unset",
			call: func() error { return client.Unset(ctx, 1, 2, WithTag([]byte{1})) },
			want: "UNSET 1 2 TAG 01",
		},
		{
			name: "tagged mset",
			call: func() error {
				return client.MSet(ctx, []Block{{X: 1, Y: 2, Bits: "1010"}, {X: 3, Y: 4, Bits: "0101"}}, WithTag([]byte{0xff, 0}))
			},
			want: "MSET 1 2 1010 3 4 0101 TAG ff00",
		},
		{
			name: "tagged xdel",
			call: func() error { return client.XDel(ctx, 1, 2, WithTag([]byte{0x0a})) },
			want: "XDEL 1 2 TAG 0a",
		},
		{
			name: "tagged batch",
			call: func() error {
				_, err := client.ChunkBatch(ctx, 1, 2, []BatchOperation{SetOp(1, 2, "1010")}, WithTag([]byte("j")))
				return err
			},
			want: "CHUNKBATCH 1 2 TAG 6a SET 1 2 1010",
		},
		{
			name: "tagged batch with a version",
			call: func() error {
				_, err := client.ChunkBatchIfVersion(ctx, 1, 2, 9, []BatchOperation{UnsetOp(1, 2)}, WithTag([]byte("j")))
				return err
			},
			want: "CHUNKBATCH 1 2 IF 9 TAG 6a UNSET 1 2",
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

	// The tag of a write with a payload comes before its length.
	puts := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "tagged xput",
			call: func() error {
				return client.XPut(ctx, 1, 2, ExtraValue{BitLength: 12, Bytes: []byte{0xab, 0x0c}}, WithTag([]byte("j")))
			},
			want: "XPUT 1 2 12 TAG 6a 2\r\n\xab\x0c\r\n",
		},
		{
			name: "tagged chunk put",
			call: func() error {
				_, err := client.PutChunk(ctx, 1, 2, []byte{1, 2}, PutOptions{Tag: []byte{1}})
				return err
			},
			want: "CHUNKPUT 1 2 TAG 01 2\r\n\x01\x02\r\n",
		},
		{
			name: "tagged chunk put state with a version",
			call: func() error {
				_, err := client.PutChunkState(ctx, 1, 2, ChunkStateInput{Payload: []byte{1, 2}, Presence: []byte{3}},
					PutOptions{IfVersion: &version, Tag: []byte{1}})
				return err
			},
			want: "CHUNKPUT 1 2 STATE IF 9 TAG 01 3\r\n\x01\x02\x03\r\n",
		},
		{
			name: "tagged chunk put state extra",
			call: func() error {
				_, err := client.PutChunkStateExtra(ctx, 1, 2, ChunkStateExtraInput{Payload: []byte{0xab, 0xcd}, Presence: []byte{0x0f}},
					PutOptions{Tag: []byte{1, 2}})
				return err
			},
			want: "CHUNKPUT 1 2 STATE EXTRA TAG 0102 3\r\n\xab\xcd\x0f\r\n",
		},
	}
	for _, testCase := range puts {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			if got := server.lastPut(t); string(got) != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}

	// Every request stayed framed: the connection still works.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

func TestClientHistoryPage(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch command {
		case "CHUNKHISTORY 0 0":
			writeArray(conn, "CURSOR 8:3",
				"5 1700000000001 1 -1 - 1010 - - 6a6f62",
				"6 1700000000002 -9223372036854775808 9223372036854775807 1010 0101 12:ab0c 3:05 -",
				"8 0 0 0 0101 - 1:01 - ff")
		case "HISTORY 0 0":
			writeArray(conn, "END")
		case "RANGEHISTORY 0 0 1 1":
			writeArray(conn, "CURSOR 12")
		default:
			genericHandler(nil, conn, command)
		}
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	page, err := client.ChunkHistory(ctx, 0, 0, HistoryOptions{})
	if err != nil {
		t.Fatalf("ChunkHistory: %v", err)
	}
	want := HistoryPage{
		Cursor: "8:3",
		Events: []HistoryEvent{
			{
				Revision: 5, TimeMs: 1700000000001, X: 1, Y: -1,
				After: BlockState{Exists: true, Bits: "1010"}, Tag: []byte("job"),
			},
			{
				Revision: 6, TimeMs: 1700000000002, X: math.MinInt64, Y: math.MaxInt64,
				Before:      BlockState{Exists: true, Bits: "1010"},
				After:       BlockState{Exists: true, Bits: "0101"},
				BeforeExtra: &ExtraValue{BitLength: 12, Bytes: []byte{0xab, 0x0c}},
				AfterExtra:  &ExtraValue{BitLength: 3, Bytes: []byte{0x05}},
			},
			{
				Revision: 8, TimeMs: 0, X: 0, Y: 0,
				Before:      BlockState{Exists: true, Bits: "0101"},
				BeforeExtra: &ExtraValue{BitLength: 1, Bytes: []byte{0x01}},
				Tag:         []byte{0xff},
			},
		},
	}
	if !reflect.DeepEqual(page, want) {
		t.Fatalf("got %+v, want %+v", page, want)
	}

	// The end of a window, and a page that is empty but not the end.
	end, err := client.History(ctx, 0, 0, HistoryOptions{})
	if err != nil || end.Cursor != "" || end.Events == nil || len(end.Events) != 0 {
		t.Fatalf("History: %+v, %v; want an empty last page", end, err)
	}
	empty, err := client.RangeHistory(ctx, 0, 0, 1, 1, HistoryOptions{})
	if err != nil || empty.Cursor != "12" || empty.Events == nil || len(empty.Events) != 0 {
		t.Fatalf("RangeHistory: %+v, %v; want an empty page with a cursor", empty, err)
	}
}

func TestClientHistoryRejectsMalformedReplies(t *testing.T) {
	const valid = "1 2 0 0 - 1010 - - -"
	cases := map[string]string{
		"not an array":               "+OK\r\n",
		"empty array":                "*0\r\n",
		"null header":                "*1\r\n$-1\r\n",
		"unknown header":             arrayWire("NOPE"),
		"cursor without it":          arrayWire("CURSOR "),
		"cursor not a revision":      arrayWire("CURSOR x"),
		"negative cursor":            arrayWire("CURSOR -1"),
		"cursor block too big":       arrayWire("CURSOR 1:4294967296"),
		"cursor of three parts":      arrayWire("CURSOR 1:2:3"),
		"null event":                 "*2\r\n$3\r\nEND\r\n$-1\r\n",
		"too few fields":             arrayWire("END", "1 2 0 0 - 1010 - -"),
		"too many fields":            arrayWire("END", valid+" -"),
		"an empty field":             arrayWire("END", "1 2 0 0  1010 - - -"),
		"revision not a number":      arrayWire("END", "x 2 0 0 - 1010 - - -"),
		"revision not canonical":     arrayWire("END", "01 2 0 0 - 1010 - - -"),
		"revision over 64 bits":      arrayWire("END", "18446744073709551616 2 0 0 - 1010 - - -"),
		"negative time":              arrayWire("END", "1 -2 0 0 - 1010 - - -"),
		"time not canonical":         arrayWire("END", "1 +2 0 0 - 1010 - - -"),
		"x beyond int64":             arrayWire("END", "1 2 9223372036854775808 0 - 1010 - - -"),
		"y beyond int64":             arrayWire("END", "1 2 0 -9223372036854775809 - 1010 - - -"),
		"x beyond int64 far":         arrayWire("END", "1 2 36893488147419103231 3 - 0000 - - 05"),
		"coordinate negative 0":      arrayWire("END", "1 2 -0 0 - 1010 - - -"),
		"bits of the wrong length":   arrayWire("END", "1 2 0 0 - 10101 - - -"),
		"bits that are not bits":     arrayWire("END", "1 2 0 0 10a0 1010 - - -"),
		"extra without a colon":      arrayWire("END", "1 2 0 0 1010 1010 12ab0c - -"),
		"extra of zero bits":         arrayWire("END", "1 2 0 0 1010 1010 0:00 - -"),
		"extra length not canonical": arrayWire("END", "1 2 0 0 1010 1010 012:ab0c - -"),
		"extra too long":             arrayWire("END", "1 2 0 0 1010 1010 134217665:00 - -"),
		"extra too few bytes":        arrayWire("END", "1 2 0 0 1010 1010 12:ab - -"),
		"extra too many bytes":       arrayWire("END", "1 2 0 0 1010 1010 - 8:abcd -"),
		"extra with set padding":     arrayWire("END", "1 2 0 0 1010 1010 12:abfc - -"),
		"extra not hex":              arrayWire("END", "1 2 0 0 1010 1010 8:zz - -"),
		"extra of an absent block":   arrayWire("END", "1 2 0 0 - 1010 8:ff - -"),
		"extra after an unset":       arrayWire("END", "1 2 0 0 1010 - - 8:ff -"),
		"tag of odd length":          arrayWire("END", "1 2 0 0 - 1010 - - abc"),
		"tag not hex":                arrayWire("END", "1 2 0 0 - 1010 - - zz"),
		"bad second event":           arrayWire("CURSOR 3", valid, "3 2 0 0 - 1010 - - -x"),
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			server := newFakeServer(t, respondWith(response))
			client := newTestClient(t, server, nil)
			if _, err := client.ChunkHistory(t.Context(), 0, 0, HistoryOptions{}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestClientHistoryValidatesBeforeSending(t *testing.T) {
	server := newFakeServer(t, withHello(genericHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	history := func(opts HistoryOptions) func() error {
		return func() error { _, err := client.History(ctx, 0, 0, opts); return err }
	}

	cases := map[string]func() error{
		"negative limit":            history(HistoryOptions{Limit: -1}),
		"limit over the server's":   history(HistoryOptions{Limit: 1025}),
		"after not a revision":      history(HistoryOptions{After: "x"}),
		"after with a space":        history(HistoryOptions{After: "1 LIMIT 5"}),
		"after with a sign":         history(HistoryOptions{After: "+1"}),
		"after without a block":     history(HistoryOptions{After: "1:"}),
		"after without a revision":  history(HistoryOptions{After: ":1"}),
		"before of three parts":     history(HistoryOptions{Before: "1:2:3"}),
		"before over 64 bits":       history(HistoryOptions{Before: "18446744073709551616"}),
		"before block over 32 bits": history(HistoryOptions{Before: "1:4294967296"}),
		"negative since":            history(HistoryOptions{SinceMs: -1}),
		"negative until":            history(HistoryOptions{UntilMs: -1}),
		"filter tag too long":       history(HistoryOptions{Tag: make([]byte, 256)}),
		"range history":             func() error { _, err := client.RangeHistory(ctx, 0, 0, 1, 1, HistoryOptions{Limit: 2000}); return err },
		"chunk history":             func() error { _, err := client.ChunkHistory(ctx, 0, 0, HistoryOptions{After: "-"}); return err },
		"get at a negative time":    func() error { _, err := client.GetAt(ctx, 0, 0, AtTimeMs(-1)); return err },
		"chunk get at a negative time": func() error {
			_, err := client.GetChunk(ctx, 0, 0, GetOptions{At: AtTimeMs(-1)})
			return err
		},
		"chunk get state at a negative time": func() error {
			_, err := client.GetChunkState(ctx, 0, 0, GetOptions{At: AtTimeMs(math.MinInt64)})
			return err
		},
		"chunk get state extra at a negative time": func() error {
			_, err := client.GetChunkStateExtra(ctx, 0, 0, GetOptions{At: AtTimeMs(-1)})
			return err
		},
		"range at a negative time": func() error {
			_, err := client.ChunkRange(ctx, 0, 0, 1, 1, GetOptions{At: AtTimeMs(-1)})
			return err
		},
		"radius at a negative time": func() error {
			_, err := client.ChunkRadius(ctx, 0, 0, 1, GetOptions{At: AtTimeMs(-1)})
			return err
		},
		"set with a tag over the server's": func() error {
			return client.Set(ctx, 0, 0, "1010", WithTag(make([]byte, 256)))
		},
		"chunk put with a tag over the server's": func() error {
			_, err := client.PutChunk(ctx, 0, 0, []byte{0, 0}, PutOptions{Tag: make([]byte, 256)})
			return err
		},
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(server.commands())
			requireRequestError(t, call())
			if len(server.commands()) != before {
				t.Fatalf("client sent %q, want nothing", server.commands()[before:])
			}
		})
	}
}

// A tag needs a server with history: one without it refuses a tagged
// CHUNKPUT or XPUT unread and closes the connection. Writes without a tag
// still go.
func TestClientTagsNeedHistory(t *testing.T) {
	reply := withoutHistoryHello()
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "HELLO" {
			writeBulkString(conn, reply)
			return
		}
		genericHandler(nil, conn, command)
	})
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	info := client.ServerInfo()
	if info.MaxTagBytes != 0 || info.MaxHistoryLimit != 0 || info.Table.Options.History || slices.Contains(info.Capabilities, "history") {
		t.Fatalf("got %+v, want no history", info)
	}

	tag := []byte{1}
	calls := map[string]func(tag []byte) error{
		"SET":   func(tag []byte) error { return client.Set(ctx, 0, 0, "1010", WithTag(tag)) },
		"UNSET": func(tag []byte) error { return client.Unset(ctx, 0, 0, WithTag(tag)) },
		"MSET":  func(tag []byte) error { return client.MSet(ctx, []Block{{Bits: "1010"}}, WithTag(tag)) },
		"XPUT": func(tag []byte) error {
			return client.XPut(ctx, 0, 0, ExtraValue{BitLength: 1, Bytes: []byte{1}}, WithTag(tag))
		},
		"XDEL": func(tag []byte) error { return client.XDel(ctx, 0, 0, WithTag(tag)) },
		"CHUNKPUT": func(tag []byte) error {
			_, err := client.PutChunk(ctx, 0, 0, []byte{0, 0}, PutOptions{Tag: tag})
			return err
		},
		"CHUNKPUT STATE": func(tag []byte) error {
			_, err := client.PutChunkState(ctx, 0, 0, ChunkStateInput{Payload: []byte{0, 0}, Presence: []byte{1}}, PutOptions{Tag: tag})
			return err
		},
		"CHUNKPUT STATE EXTRA": func(tag []byte) error {
			_, err := client.PutChunkStateExtra(ctx, 0, 0, ChunkStateExtraInput{Payload: []byte{0, 0}, Presence: []byte{1}},
				PutOptions{Tag: tag})
			return err
		},
		"CHUNKBATCH": func(tag []byte) error {
			_, err := client.ChunkBatch(ctx, 0, 0, []BatchOperation{UnsetOp(0, 0)}, WithTag(tag))
			return err
		},
		"CHUNKBATCH IF": func(tag []byte) error {
			_, err := client.ChunkBatchIfVersion(ctx, 0, 0, 1, []BatchOperation{UnsetOp(0, 0)}, WithTag(tag))
			return err
		},
	}
	for name, call := range calls {
		before := len(server.commands())
		err := call(tag)
		requireRequestError(t, err)
		if !strings.Contains(err.Error(), "needs a server with history") {
			t.Fatalf("%s: got %v, want the missing history named", name, err)
		}
		if len(server.commands()) != before {
			t.Fatalf("%s: client sent %q, want nothing", name, server.commands()[before:])
		}
		if err := call(nil); err != nil {
			t.Fatalf("%s without a tag: %v", name, err)
		}
	}
	if got := server.acceptedConns(); got != 1 {
		t.Fatalf("got %d connections, want 1", got)
	}
}

// A tag is bounded by the server's max_tag_bytes.
func TestClientTagBoundedByServerLimit(t *testing.T) {
	var sets atomic.Int32
	hello := historyHello("max_tag_bytes=255", "max_tag_bytes=4", "max_history_limit=1024", "max_history_limit=10")
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "SET" {
			sets.Add(1)
		}
		hello(s, conn, command)
	})
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	if info := client.ServerInfo(); info.MaxTagBytes != 4 || info.MaxHistoryLimit != 10 {
		t.Fatalf("got %+v", info)
	}

	requireRequestError(t, client.Set(ctx, 0, 0, "1010", WithTag([]byte("12345"))))
	if sets.Load() != 0 {
		t.Fatal("the tag over the limit was sent")
	}
	if err := client.Set(ctx, 0, 0, "1010", WithTag([]byte("1234"))); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// So are a filter tag and the limit.
	_, err := client.History(ctx, 0, 0, HistoryOptions{Tag: []byte("12345")})
	requireRequestError(t, err)
	_, err = client.History(ctx, 0, 0, HistoryOptions{Limit: 11})
	requireRequestError(t, err)
	if _, err := client.History(ctx, 0, 0, HistoryOptions{Limit: 10, Tag: []byte("1234")}); err != nil {
		t.Fatalf("History: %v", err)
	}
}

// historyScript answers the history commands from a table of replies, and
// everything else with genericHandler.
func historyScript(replies map[string]string) func(*fakeServer, net.Conn, string) {
	return withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if reply, ok := replies[command]; ok {
			_, _ = conn.Write([]byte(reply))
			return
		}
		if strings.HasSuffix(verbOf(command), "HISTORY") {
			writeServerError(conn, "ERR INVALID_ARGUMENT unexpected "+command)
			return
		}
		genericHandler(nil, conn, command)
	})
}

// historyCommands returns the history commands the server received.
func historyCommands(server *fakeServer) []string {
	var out []string
	for _, command := range server.commands() {
		if strings.HasSuffix(verbOf(command), "HISTORY") {
			out = append(out, command)
		}
	}
	return out
}

func eventLine(revision int) string {
	return fmt.Sprintf("%d 1700000000000 0 0 - 1010 - - -", revision)
}

func TestClientHistoryEvents(t *testing.T) {
	server := newFakeServer(t, historyScript(map[string]string{
		// Oldest first: a full page, an empty one with a cursor, the last.
		"CHUNKHISTORY 0 0 LIMIT 2 ASC BEFORE 100":           arrayWire("CURSOR 3:1", eventLine(1), eventLine(3)),
		"CHUNKHISTORY 0 0 LIMIT 2 ASC AFTER 3:1 BEFORE 100": arrayWire("CURSOR 7"),
		"CHUNKHISTORY 0 0 LIMIT 2 ASC AFTER 7 BEFORE 100":   arrayWire("END", eventLine(8)),
		// Newest first: the cursor goes to BEFORE, AFTER stays.
		"HISTORY 1 1 AFTER 2":            arrayWire("CURSOR 9:5", eventLine(9)),
		"HISTORY 1 1 AFTER 2 BEFORE 9:5": arrayWire("END", eventLine(4)),
		// An error ends the iteration.
		"RANGEHISTORY 0 0 1 1":          arrayWire("CURSOR 4", eventLine(5)),
		"RANGEHISTORY 0 0 1 1 BEFORE 4": "-ERR NOT_RETAINED start=3\r\n",
		// A page that moves nothing.
		"HISTORY 2 2 ASC AFTER 5": arrayWire("CURSOR 5"),
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	collect := func(events func(func(HistoryEvent, error) bool)) ([]uint64, error) {
		var revisions []uint64
		for event, err := range events {
			if err != nil {
				return revisions, err
			}
			revisions = append(revisions, event.Revision)
		}
		return revisions, nil
	}

	revisions, err := collect(client.ChunkHistoryEvents(ctx, 0, 0, HistoryOptions{Limit: 2, Ascending: true, Before: "100"}))
	if err != nil || !slices.Equal(revisions, []uint64{1, 3, 8}) {
		t.Fatalf("ChunkHistoryEvents: %v, %v", revisions, err)
	}
	revisions, err = collect(client.HistoryEvents(ctx, 1, 1, HistoryOptions{After: "2"}))
	if err != nil || !slices.Equal(revisions, []uint64{9, 4}) {
		t.Fatalf("HistoryEvents: %v, %v", revisions, err)
	}
	revisions, err = collect(client.RangeHistoryEvents(ctx, 0, 0, 1, 1, HistoryOptions{}))
	if start, ok := NotRetainedStart(err); !ok || start != 3 || !slices.Equal(revisions, []uint64{5}) {
		t.Fatalf("RangeHistoryEvents: %v, %v; want event 5, then NOT_RETAINED start=3", revisions, err)
	}
	revisions, err = collect(client.HistoryEvents(ctx, 2, 2, HistoryOptions{Ascending: true, After: "5"}))
	if !errors.Is(err, ErrProtocol) || len(revisions) != 0 {
		t.Fatalf("HistoryEvents of a cursor that does not move: %v, %v", revisions, err)
	}

	want := []string{
		"CHUNKHISTORY 0 0 LIMIT 2 ASC BEFORE 100",
		"CHUNKHISTORY 0 0 LIMIT 2 ASC AFTER 3:1 BEFORE 100",
		"CHUNKHISTORY 0 0 LIMIT 2 ASC AFTER 7 BEFORE 100",
		"HISTORY 1 1 AFTER 2",
		"HISTORY 1 1 AFTER 2 BEFORE 9:5",
		"RANGEHISTORY 0 0 1 1",
		"RANGEHISTORY 0 0 1 1 BEFORE 4",
		"HISTORY 2 2 ASC AFTER 5",
	}
	if got := historyCommands(server); !slices.Equal(got, want) {
		t.Fatalf("got commands %q, want %q", got, want)
	}

	// Breaking out of the loop reads no further page.
	for event, err := range client.ChunkHistoryEvents(ctx, 0, 0, HistoryOptions{Limit: 2, Ascending: true, Before: "100"}) {
		if err != nil || event.Revision != 1 {
			t.Fatalf("got %+v, %v", event, err)
		}
		break
	}
	if got := historyCommands(server); len(got) != len(want)+1 {
		t.Fatalf("got commands %q after a break, want one more page", got[len(want):])
	}
}

func TestClientNotRetained(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch command {
		case "GET 0 0 AT 0":
			writeServerError(conn, "ERR NOT_RETAINED start=1")
		case "CHUNKHISTORY 0 0 ASC":
			writeServerError(conn, "ERR NOT_RETAINED start=18446744073709551615")
		case "GET 0 0 AT 99":
			writeServerError(conn, "ERR OUT_OF_RANGE AT 99 is not below the next revision (9)")
		default:
			genericHandler(nil, conn, command)
		}
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	_, err := client.GetAt(ctx, 0, 0, AtRevision(0))
	var typed *Error
	if !errors.Is(err, ErrServer) || !errors.As(err, &typed) || typed.ServerCode != CodeNotRetained ||
		typed.ServerMessage != "start=1" || typed.Command != "GET" {
		t.Fatalf("got %v, want a NOT_RETAINED server error", err)
	}
	if start, ok := NotRetainedStart(fmt.Errorf("reading the past: %w", err)); !ok || start != 1 {
		t.Fatalf("NotRetainedStart: %d, %v", start, ok)
	}
	_, err = client.ChunkHistory(ctx, 0, 0, HistoryOptions{Ascending: true})
	if start, ok := NotRetainedStart(err); !ok || start != math.MaxUint64 {
		t.Fatalf("NotRetainedStart: %d, %v from %v", start, ok, err)
	}
	_, err = client.GetAt(ctx, 0, 0, AtRevision(99))
	if !errors.As(err, &typed) || typed.ServerCode != "OUT_OF_RANGE" {
		t.Fatalf("got %v, want OUT_OF_RANGE", err)
	}
	for _, other := range []error{nil, err, errors.New("start=1")} {
		if start, ok := NotRetainedStart(other); ok || start != 0 {
			t.Fatalf("NotRetainedStart(%v): %d, %v", other, start, ok)
		}
	}
	// Server errors keep the connection.
	if err := client.Ping(ctx); err != nil || server.acceptedConns() != 1 {
		t.Fatalf("Ping: %v; %d connections", err, server.acceptedConns())
	}

	// NOT_RETAINED carries its start revision; without one it is malformed.
	for _, reply := range []string{"ERR NOT_RETAINED", "ERR NOT_RETAINED start=", "ERR NOT_RETAINED start=x",
		"ERR NOT_RETAINED start=01", "ERR NOT_RETAINED start=1 more", "ERR NOT_RETAINED begin=1"} {
		server := newFakeServer(t, respondWith("-"+reply+"\r\n"))
		client := newTestClient(t, server, nil)
		if _, err := client.GetAt(ctx, 0, 0, AtRevision(0)); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%q: got %v, want ErrProtocol", reply, err)
		}
	}
}

// An event whose block had the largest extra data before and after the
// change is longer than MaxBulkBytes in hex; it is read all the same.
func TestClientHistoryEventLargerThanMaxBulkBytes(t *testing.T) {
	const valueBytes = 16<<20 - extraEntryHeaderBytes
	extra := fmt.Sprintf("%d:%s", valueBytes*8, strings.Repeat("5a", valueBytes))
	event := "1 2 0 0 1010 0101 " + extra + " " + extra + " -"
	if len(event) <= MaxBulkBytes {
		t.Fatalf("the event is only %d bytes", len(event))
	}
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if verbOf(command) == "HISTORY" {
			writeArray(conn, "END", event)
			return
		}
		genericHandler(nil, conn, command)
	}))
	// 64 MiB over loopback: allow for a loaded machine.
	client := newTestClient(t, server, func(o *Options) { o.CommandTimeout = 60 * time.Second })
	page, err := client.History(t.Context(), 0, 0, HistoryOptions{})
	if err != nil || len(page.Events) != 1 || page.Events[0].AfterExtra == nil ||
		page.Events[0].AfterExtra.BitLength != valueBytes*8 || len(page.Events[0].BeforeExtra.Bytes) != valueBytes {
		t.Fatalf("History: %v", err)
	}
}
