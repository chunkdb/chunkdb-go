package chunkdb

import (
	"bufio"
	"errors"
	"math"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestFoundationBigNumbers(t *testing.T) {
	for _, text := range []string{"-170141183460469231731687303715884105728", "-9223372036854775809", "-9223372036854775808", "0", "9223372036854775807", "9223372036854775808", "18446744073709551615", "18446744073709551616", "170141183460469231731687303715884105727"} {
		r := readOne(t, "("+text+"\r\n")
		exact, _ := new(big.Int).SetString(text, 10)
		value, ok := r.BigInt()
		if !ok || value.Cmp(exact) != 0 {
			t.Fatal(text, r)
		}
		i, iok := r.Int64()
		u, uok := r.Uint64()
		if iok != exact.IsInt64() || (iok && i != exact.Int64()) || uok != exact.IsUint64() || (uok && u != exact.Uint64()) {
			t.Fatal(text, i, iok, u, uok)
		}
		value.SetInt64(9)
		again, _ := r.BigInt()
		if again.Cmp(exact) != 0 {
			t.Fatal("BigInt accessor leaked storage")
		}
	}
	for _, text := range []string{"", "+1", " 1", "1x", "--1", "0x10", "1.0"} {
		_, err := readReply(bufio.NewReader(strings.NewReader("(" + text + "\r\n")))
		if !errors.Is(err, ErrProtocol) {
			t.Fatal(text, err)
		}
	}
	if v, ok := readOne(t, ":-9223372036854775808\r\n").BigInt(); !ok || v.Int64() != math.MinInt64 {
		t.Fatal(v, ok)
	}
}

func TestFoundationParameterOccurrences(t *testing.T) {
	for statement, want := range map[string]int{
		"bad $1 $1": 2, "bad $3": 1, "bad $0 $999999999999999999999999999999": 2,
		`bad '$9' "$8" $2 $1`: 2, `bad 'a''$8' x'$7' b'$6' $1`: 1,
		`bad "a""$8" $1`: 1, `bad '$9 $1`: 0, `bad "unterminated $1`: 0,
		"bad! $1 nonsense $2": 2, "bad $ $x": 0,
	} {
		if got := parameterOccurrences(statement); got != want {
			t.Fatal(statement, got, want)
		}
		frames := make([][]byte, want)
		if _, err := encodeRequest("BAD", statement, frames); err != nil {
			t.Fatal(statement, err)
		}
		if _, err := encodeRequest("BAD", statement, append(frames, []byte("extra"))); !errors.Is(err, ErrProtocol) {
			t.Fatal(statement, err)
		}
	}
	wire, err := encodeRequest("BAD", "bad $2 $1", [][]byte{[]byte("one"), []byte("two")})
	if err != nil || string(wire) != "bad $2 $1\r\n$3\r\none\r\n$3\r\ntwo\r\n" {
		t.Fatal(string(wire), err)
	}
}

func TestFoundationErrorKeysAndClasses(t *testing.T) {
	r := readOne(t, "-ERR VERSION_MISMATCH current=18446744073709551615 extra=a%20b%3Dc percent=%Q0 reread first\r\n")
	e := replyError(PhaseResponse, "SET BLOCK", r)
	var base *Error
	var mismatch *VersionMismatchError
	if !errors.As(e, &base) || !errors.As(e, &mismatch) || mismatch.Current != math.MaxUint64 || base.RetryClass != RetryReread || base.ServerKeys["extra"] != "a b=c" || base.ServerKeys["percent"] != "%Q0" || base.ServerMessage != "reread first" {
		t.Fatal(e, base)
	}
	for code, want := range map[string]RetryClass{CodeConditionFailed: RetryReread, CodeSchemaMismatch: RetryReread, CodeConflict: RetryRerun, CodeBusy: RetryBackoff, CodeDiskFull: RetryBackoff, CodeRateLimited: RetryBackoff, CodeOutcomeUnknown: RetryUnknownOutcome, CodeMigrationMismatch: RetryFatal, CodeSlotBusy: RetryFatal, CodeCancelled: RetryFatal, CodeTimeout: RetryFatal, CodeKilled: RetryFatal, "FUTURE_CODE": RetryFatal} {
		base := serverError(PhaseResponse, "DO", code, "CONFLICT backoff unknown_outcome")
		if base.RetryClass != want {
			t.Fatal(code, base.RetryClass, want)
		}
	}
	r = readOne(t, "-ERR FUTURE_CODE revision=18446744073709551616 connection_id=18446744073709551615 reason=future hello\r\n")
	e = replyError(PhaseResponse, "DO", r)
	if !errors.As(e, &base) || base.Metadata.Revision != nil || base.Metadata.ConnectionID == nil || *base.Metadata.ConnectionID != math.MaxUint64 || base.RetryClass != RetryFatal {
		t.Fatal(e, base)
	}
}

func TestFoundationUnknownPushDoesNotConsumeReplies(t *testing.T) {
	firstSeen := make(chan struct{})
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, statement string) {
		switch statement {
		case "FIRST":
			close(firstSeen)
		case "SECOND":
			writeRaw(conn, "+FIRST\r\n"+push(respBulk("future"), respMap("nested", respArray(respBulk("bytes"), "(18446744073709551616\r\n")))+"+SECOND\r\n")
		}
	}))
	c := newTestClient(t, server, func(o *Options) { o.PipelineDepth = 2 })
	first := make(chan result, 1)
	go func() { r, e := c.Do(t.Context(), "FIRST"); first <- result{r, e} }()
	select {
	case <-firstSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("FIRST request never arrived")
	}
	second, e := c.Do(t.Context(), "SECOND")
	if e != nil || second.Text != "SECOND" {
		t.Fatal(second, e)
	}
	select {
	case got := <-first:
		if got.err != nil || got.reply.Text != "FIRST" {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FIRST response never returned")
	}
}

func TestFoundationUnknownColumnAndExtendedRows(t *testing.T) {
	schema, err := parseSchema(readOne(t, describeReply("world", 1, []string{
		fakeColumn(1, "id", "u8", false, false, respInt(7)), fakeColumn(2, "future", "decimal(20)", false, false, respBulk("123.40")), fakeColumn(3, "flag", "bool", false, false, respBool(true)),
	})))
	if err != nil {
		t.Fatal(err)
	}
	if schema.Columns[1].Type.String() != "decimal(20)" || string(schema.Columns[1].Default.([]byte)) != "123.40" {
		t.Fatal(schema)
	}
	row, err := decodeWatchRow(schema.Columns, readOne(t, respArray(respInt(8), respBulk("999.99"), respBool(false), respMap("extra", respNull))))
	if err != nil || row["id"] != uint64(8) || string(row["future"].([]byte)) != "999.99" || row["flag"] != false {
		t.Fatal(row, err)
	}
	if _, err := decodeWatchRow(schema.Columns, readOne(t, respArray(respInt(8), respInt(9), respBool(false)))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if _, err := DecodeChunk(schema, nil); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "unknown column type") {
		t.Fatal(err)
	}
	if _, err := EncodeChunk(schema, NewChunk(schema)); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
