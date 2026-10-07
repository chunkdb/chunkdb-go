package chunkdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func readFrameFrom(t *testing.T, wire string) (Frame, error) {
	t.Helper()
	return ReadFrame(bufio.NewReader(strings.NewReader(wire)))
}

func TestSerializeCommand(t *testing.T) {
	line, err := SerializeCommand("PING")
	if err != nil {
		t.Fatalf("SerializeCommand: %v", err)
	}
	if string(line) != "PING\r\n" {
		t.Fatalf("got %q, want %q", line, "PING\r\n")
	}

	line, err = SerializeCommand("GET", "1", "-2")
	if err != nil {
		t.Fatalf("SerializeCommand: %v", err)
	}
	if string(line) != "GET 1 -2\r\n" {
		t.Fatalf("got %q, want %q", line, "GET 1 -2\r\n")
	}
}

func TestSerializeCommandRejectsControlCharacters(t *testing.T) {
	for _, part := range []string{"a\rb", "a\nb"} {
		if _, err := SerializeCommand("SET", part); !errors.Is(err, ErrProtocol) {
			t.Fatalf("part %q: got %v, want ErrProtocol", part, err)
		}
	}
}

func TestReadFrameSimple(t *testing.T) {
	frame, err := readFrameFrom(t, "+PONG\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameSimple || frame.Simple != "PONG" {
		t.Fatalf("got %+v, want simple PONG", frame)
	}
}

func TestReadFrameSimpleAcceptsBareLF(t *testing.T) {
	frame, err := readFrameFrom(t, "+OK\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameSimple || frame.Simple != "OK" {
		t.Fatalf("got %+v, want simple OK", frame)
	}
}

func TestReadFrameError(t *testing.T) {
	frame, err := readFrameFrom(t, "-ERR AUTH_REQUIRED use HELLO 2 AUTH <token>\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameError {
		t.Fatalf("got kind %v, want FrameError", frame.Kind)
	}
	if frame.Code != "AUTH_REQUIRED" {
		t.Fatalf("got code %q, want AUTH_REQUIRED", frame.Code)
	}
	if frame.Message != "use HELLO 2 AUTH <token>" {
		t.Fatalf("got message %q", frame.Message)
	}
	if frame.Raw != "ERR AUTH_REQUIRED use HELLO 2 AUTH <token>" {
		t.Fatalf("got raw %q", frame.Raw)
	}
}

func TestReadFrameErrorWithoutCode(t *testing.T) {
	frame, err := readFrameFrom(t, "-ERR INTERNAL\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Code != "INTERNAL" || frame.Message != "" {
		t.Fatalf("got code %q message %q", frame.Code, frame.Message)
	}
}

func TestReadFrameErrorWithoutErrPrefix(t *testing.T) {
	frame, err := readFrameFrom(t, "-BUSY\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Code != "ERR" || frame.Message != "BUSY" {
		t.Fatalf("got code %q message %q", frame.Code, frame.Message)
	}
}

func TestReadFrameBulkBinaryPayload(t *testing.T) {
	payload := []byte{0x00, 0x01, 0x02, 0x0a, 0x0d}
	var wire bytes.Buffer
	fmt.Fprintf(&wire, "$%d\r\n", len(payload))
	wire.Write(payload)
	wire.WriteString("\r\n")

	frame, err := ReadFrame(bufio.NewReader(&wire))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameBulk {
		t.Fatalf("got kind %v, want FrameBulk", frame.Kind)
	}
	if !bytes.Equal(frame.Bulk, payload) {
		t.Fatalf("got %v, want %v", frame.Bulk, payload)
	}
}

func TestReadFrameBulkEmptyPayload(t *testing.T) {
	frame, err := readFrameFrom(t, "$0\r\n\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameBulk || len(frame.Bulk) != 0 {
		t.Fatalf("got %+v, want empty bulk", frame)
	}
}

func TestReadFrameArray(t *testing.T) {
	frame, err := readFrameFrom(t, "*2\r\n$3\r\nEND\r\n$3\r\n1 2\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameArray {
		t.Fatalf("got kind %v, want FrameArray", frame.Kind)
	}
	if len(frame.Array) != 2 {
		t.Fatalf("got %d items, want 2", len(frame.Array))
	}
	if frame.Array[0].Kind != FrameBulk || string(frame.Array[0].Bulk) != "END" ||
		frame.Array[1].Kind != FrameBulk || string(frame.Array[1].Bulk) != "1 2" {
		t.Fatalf("got %+v", frame.Array)
	}
}

func TestReadFrameNull(t *testing.T) {
	frame, err := readFrameFrom(t, "$-1\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameNull || frame.Bulk != nil {
		t.Fatalf("got %+v, want a null frame", frame)
	}
}

func TestReadFrameArrayWithNulls(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*3\r\n$2\r\n10\r\n$-1\r\n$0\r\n\r\n+OK\r\n"))
	frame, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameArray || len(frame.Array) != 3 {
		t.Fatalf("got %+v, want an array of 3 items", frame)
	}
	if frame.Array[0].Kind != FrameBulk || string(frame.Array[0].Bulk) != "10" {
		t.Fatalf("item 0: got %+v", frame.Array[0])
	}
	if frame.Array[1].Kind != FrameNull {
		t.Fatalf("item 1: got %+v, want null", frame.Array[1])
	}
	// An empty bulk is not a null.
	if frame.Array[2].Kind != FrameBulk || len(frame.Array[2].Bulk) != 0 {
		t.Fatalf("item 2: got %+v, want an empty bulk", frame.Array[2])
	}

	// The reader is positioned at the next frame.
	next, err := ReadFrame(reader)
	if err != nil || next.Kind != FrameSimple || next.Simple != "OK" {
		t.Fatalf("got %+v, %v, want +OK", next, err)
	}
}

func TestReadFrameEmptyArray(t *testing.T) {
	frame, err := readFrameFrom(t, "*0\r\n")
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Kind != FrameArray || len(frame.Array) != 0 {
		t.Fatalf("got %+v, want empty array", frame)
	}
}

func TestReadFrameSequentialFrames(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("+OK\r\n$4\r\n1010\r\n+PONG\r\n"))
	for _, want := range []string{"OK", "1010", "PONG"} {
		frame, err := ReadFrame(reader)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		got := frame.Simple
		if frame.Kind == FrameBulk {
			got = string(frame.Bulk)
		}
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestReadFrameRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"unknown prefix":         "?OK\r\n",
		"invalid bulk length":    "$abc\r\n",
		"negative bulk length":   "$-2\r\n",
		"non-canonical null":     "$-01\r\n",
		"invalid array length":   "*abc\r\n",
		"non-bulk array item":    "*1\r\n+OK\r\n",
		"bad bulk terminator":    "$2\r\nabxx",
		"oversized bulk payload": fmt.Sprintf("$%d\r\n", MaxBulkBytes+1),
	}

	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readFrameFrom(t, wire); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestReadFrameTruncatedInputReportsIOError(t *testing.T) {
	if _, err := readFrameFrom(t, "$10\r\nshort"); err == nil {
		t.Fatal("expected an error for a truncated payload")
	}
}

func TestParseInfo(t *testing.T) {
	values := ParseInfo([]byte("chunkdb_version=1\r\nblock_bits=16\nempty=\nflag\n\n"))
	want := map[string]string{
		"chunkdb_version": "1",
		"block_bits":      "16",
		"empty":           "",
		"flag":            "",
	}
	if len(values) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(values), len(want), values)
	}
	for key, value := range want {
		if values[key] != value {
			t.Fatalf("key %q: got %q, want %q", key, values[key], value)
		}
	}
}
