package chunkdb

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func readOne(t *testing.T, input string) Reply {
	t.Helper()
	reply, err := readReply(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatalf("readReply(%q): %v", input, err)
	}
	return reply
}

func TestReadReplyScalars(t *testing.T) {
	if r := readOne(t, "+PONG\r\n"); r.Kind != ReplySimple || r.Text != "PONG" {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "+OK\n"); r.Kind != ReplySimple || r.Text != "OK" {
		t.Fatalf("bare LF: got %+v", r)
	}
	if r := readOne(t, "-ERR SYNTAX column 1: unknown statement 'FOO'\r\n"); r.Kind != ReplyError ||
		r.Code != CodeSyntax || r.Message != "column 1: unknown statement 'FOO'" {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "-ERR BUSY\r\n"); r.Code != CodeBusy || r.Message != "" {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "-WRONG thing\r\n"); r.Code != "ERR" || r.Message != "WRONG thing" {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "#t\r\n"); r.Kind != ReplyBoolean || !r.Bool {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "#f\r\n"); r.Kind != ReplyBoolean || r.Bool {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "_\r\n"); r.Kind != ReplyNull {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "$-1\r\n"); r.Kind != ReplyNull {
		t.Fatalf("got %+v", r)
	}
}

func TestReadReplyIntegers(t *testing.T) {
	cases := []struct {
		input   string
		i64     int64
		i64OK   bool
		u64     uint64
		u64OK   bool
		comment string
	}{
		{":0\r\n", 0, true, 0, true, "zero"},
		{":-0\r\n", 0, true, 0, true, "negative zero"},
		{":1043\r\n", 1043, true, 1043, true, "small"},
		{":-3\r\n", -3, true, 0, false, "negative"},
		{":9223372036854775807\r\n", math.MaxInt64, true, math.MaxInt64, true, "i64 max"},
		{":-9223372036854775808\r\n", math.MinInt64, true, 0, false, "i64 min"},
		{":9223372036854775808\r\n", 0, false, 1 << 63, true, "above i64"},
		{":18446744073709551615\r\n", 0, false, math.MaxUint64, true, "u64 max"},
	}
	for _, testCase := range cases {
		r := readOne(t, testCase.input)
		i64, i64OK := r.Int64()
		u64, u64OK := r.Uint64()
		if r.Kind != ReplyInteger || i64 != testCase.i64 || i64OK != testCase.i64OK || u64 != testCase.u64 || u64OK != testCase.u64OK {
			t.Fatalf("%s: got %d %v, %d %v", testCase.comment, i64, i64OK, u64, u64OK)
		}
	}
	for _, input := range []string{":18446744073709551616\r\n", ":-9223372036854775809\r\n", ":\r\n", ":+1\r\n", ":1x\r\n"} {
		if _, err := readReply(bufio.NewReader(strings.NewReader(input))); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%q: got %v, want ErrProtocol", input, err)
		}
	}
}

func TestReadReplyDoubles(t *testing.T) {
	cases := map[string]float64{
		",1.5\r\n":                    1.5,
		",-2e-3\r\n":                  -2e-3,
		",inf\r\n":                    math.Inf(1),
		",-inf\r\n":                   math.Inf(-1),
		",3.4028234663852886e+38\r\n": math.MaxFloat32,
	}
	for input, want := range cases {
		if r := readOne(t, input); r.Kind != ReplyDouble || r.Double != want {
			t.Fatalf("%q: got %+v", input, r)
		}
	}
	if r := readOne(t, ",nan\r\n"); r.Kind != ReplyDouble || !math.IsNaN(r.Double) {
		t.Fatalf("got %+v", r)
	}
	if _, err := readReply(bufio.NewReader(strings.NewReader(",x\r\n"))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestReadReplyBulkIsBinary(t *testing.T) {
	payload := "a\r\nb\x00\n\r"
	r := readOne(t, "$7\r\n"+payload+"\r\n")
	if r.Kind != ReplyBulk || string(r.Bulk) != payload {
		t.Fatalf("got %+v", r)
	}
	if r := readOne(t, "$0\r\n\r\n"); r.Kind != ReplyBulk || r.Bulk == nil || len(r.Bulk) != 0 {
		t.Fatalf("got %+v", r)
	}
}

func TestReadReplyAggregates(t *testing.T) {
	r := readOne(t, "*3\r\n*3\r\n:0\r\n:-1\r\n$2\r\nab\r\n_\r\n*0\r\n")
	if r.Kind != ReplyArray || len(r.Array) != 3 || len(r.Array[0].Array) != 3 ||
		string(r.Array[0].Array[2].Bulk) != "ab" || r.Array[1].Kind != ReplyNull || len(r.Array[2].Array) != 0 {
		t.Fatalf("got %+v", r)
	}

	m := readOne(t, "%2\r\n$6\r\nchunks\r\n*1\r\n*2\r\n:1\r\n:2\r\n+more\r\n#t\r\n")
	if m.Kind != ReplyMap || len(m.Map) != 2 {
		t.Fatalf("got %+v", m)
	}
	chunks, ok := m.Lookup("chunks")
	if !ok || len(chunks.Array) != 1 {
		t.Fatalf("got %+v", chunks)
	}
	if more, ok := m.Lookup("more"); !ok || !more.Bool {
		t.Fatalf("got %+v", more)
	}
	if _, ok := m.Lookup("missing"); ok {
		t.Fatal("found a missing key")
	}
}

func TestReadReplySequence(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("+OK\r\n:7\r\n$1\r\nx\r\n"))
	for _, want := range []ReplyKind{ReplySimple, ReplyInteger, ReplyBulk} {
		r, err := readReply(reader)
		if err != nil || r.Kind != want {
			t.Fatalf("got %+v, %v; want kind %d", r, err, want)
		}
	}
	if _, err := readReply(reader); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want EOF", err)
	}
}

func TestReadReplyRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"unknown prefix":   "!x\r\n",
		"bad bulk length":  "$x\r\n",
		"negative bulk":    "$-2\r\n",
		"huge bulk":        "$999999999999\r\n",
		"bad terminator":   "$1\r\nxyz",
		"bad array length": "*-1\r\n",
		"bad map length":   "%x\r\n",
		"bad boolean":      "#x\r\n",
		"bad null":         "_x\r\n",
		"deep nesting":     strings.Repeat("*1\r\n", maxReplyDepth+2) + ":1\r\n",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readReply(bufio.NewReader(strings.NewReader(input))); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
	if _, err := readReply(bufio.NewReader(strings.NewReader("$5\r\nab"))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want an I/O error for a cut-off bulk", err)
	}
}

func TestEncodeRequest(t *testing.T) {
	wire, err := encodeRequest("SET BLOCK", "SET BLOCK 0 0 IN t a = $1, b = $2, c = $3", [][]byte{[]byte("a\r\nb"), nil, {}})
	if err != nil {
		t.Fatalf("encodeRequest: %v", err)
	}
	want := "SET BLOCK 0 0 IN t a = $1, b = $2, c = $3\r\n$4\r\na\r\nb\r\n$-1\r\n$0\r\n\r\n"
	if !bytes.Equal(wire, []byte(want)) {
		t.Fatalf("got %q, want %q", wire, want)
	}
	for _, statement := range []string{"PING\r\n", "PING\nPING", "a\rb"} {
		if _, err := encodeRequest("PING", statement, nil); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%q: got %v, want ErrProtocol", statement, err)
		}
	}
}
