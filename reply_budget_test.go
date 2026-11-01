package chunkdb

import (
	"bufio"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

type areaHeaderOnly struct {
	text     string
	bodyRead bool
}

func (r *areaHeaderOnly) Read(target []byte) (int, error) {
	if r.text == "" {
		r.bodyRead = true
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(target, r.text)
	r.text = r.text[n:]
	return n, nil
}

func TestAreaReadBudgetBeforeBodyAndAggregateAllocation(t *testing.T) {
	for _, test := range []struct {
		wire    string
		options areaReplyOptions
		cap     uint64
	}{
		{"*1\r\n*3\r\n:0\r\n:0\r\n$33554432\r\n", areaReplyOptions{knownShape: true}, 80},
		{"*123", areaReplyOptions{knownShape: true}, 4},
		{"*3\r\n", areaReplyOptions{knownShape: true}, 100},
		{"%3\r\n+chunks\r\n*3\r\n", areaReplyOptions{knownShape: true, page: true}, 100},
		{"*2\r\n:1\r\n%3\r\n+chunks\r\n*3\r\n", areaReplyOptions{knownShape: true, page: true, withSchema: true}, 100},
		{"%4\r\n+extra\r\n*10000000\r\n", areaReplyOptions{knownShape: true, page: true}, 64 << 20},
	} {
		t.Run(test.wire, func(t *testing.T) {
			input := &areaHeaderOnly{text: test.wire}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := readReplyLimited(bufio.NewReader(input), &areaReplyLimits{maxBytes: test.cap, maxChunks: 2, areaReplyOptions: test.options})
			runtime.ReadMemStats(&after)
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "negotiated") || input.bodyRead || after.TotalAlloc-before.TotalAlloc > 4*1024*1024 {
				t.Fatalf("budget did not refuse before allocation/body: %v body_read=%v allocated=%d", err, input.bodyRead, after.TotalAlloc-before.TotalAlloc)
			}
		})
	}
}

func TestAreaReadBudgetPreservesPipelineFraming(t *testing.T) {
	page := respMap("chunks", respArray(), "more", respBool(false), "after", respNull)
	for _, test := range []struct {
		wire    string
		options areaReplyOptions
	}{
		{page, areaReplyOptions{knownShape: true, page: true}},
		{respArray(respInt(1), page), areaReplyOptions{knownShape: true, page: true, withSchema: true}},
		{respArray(), areaReplyOptions{knownShape: true}},
	} {
		limits := &areaReplyLimits{maxBytes: uint64(len(test.wire)), maxChunks: 2, areaReplyOptions: test.options}
		reader := bufio.NewReader(strings.NewReader(">1\r\n+future\r\n" + test.wire + "+PONG\r\n"))
		if push, err := readReplyLimited(reader, limits); err != nil || push.Kind != ReplyPush {
			t.Fatal(push, err)
		}
		if _, err := readReplyLimited(reader, limits); err != nil {
			t.Fatal("exact-fitting area", err)
		}
		if pong, err := readReply(reader); err != nil || pong.Kind != ReplySimple || pong.Text != "PONG" {
			t.Fatal("pipeline tail lost", pong, err)
		}
		limits.maxBytes--
		if _, err := readReplyLimited(bufio.NewReader(strings.NewReader(test.wire)), limits); !errors.Is(err, ErrProtocol) {
			t.Fatal("short budget accepted", err)
		}
	}
	reader := bufio.NewReader(strings.NewReader("-ERR OUT_OF_RANGE detail\r\n+PONG\r\n"))
	if reply, err := readReplyLimited(reader, &areaReplyLimits{maxBytes: 1, maxChunks: 1}); err != nil || reply.Kind != ReplyError {
		t.Fatal(reply, err)
	}
	if pong, err := readReply(reader); err != nil || pong.Text != "PONG" {
		t.Fatal(pong, err)
	}
}
