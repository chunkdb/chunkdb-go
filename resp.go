package chunkdb

import (
	"bufio"
	"io"
	"math"
	"strconv"
	"strings"
)

// MaxBulkBytes bounds a single bulk string the client accepts. A chunk form
// holds at most 64 MiB of payload and 64 MiB of text and bytes values; the
// bound keeps a corrupt or hostile length header from triggering an
// unbounded allocation.
const MaxBulkBytes = 160 << 20

// maxReplyLineBytes bounds simple-string, error, and header lines.
const maxReplyLineBytes = 1 << 20

// maxReplyDepth bounds the nesting of arrays and maps in one reply.
const maxReplyDepth = 32

// ReplyKind is the RESP3 type of a [Reply].
type ReplyKind int

const (
	// ReplySimple is "+<text>", for example +OK and +PONG.
	ReplySimple ReplyKind = iota + 1
	// ReplyError is "-ERR <code> <message>". [Client.Do] returns it as an
	// error, never as a Reply.
	ReplyError
	// ReplyInteger is ":<n>"; read it with [Reply.Int64] or [Reply.Uint64].
	ReplyInteger
	// ReplyDouble is ",<n>", including inf, -inf and nan.
	ReplyDouble
	// ReplyBoolean is "#t" or "#f".
	ReplyBoolean
	// ReplyNull is "_": NULL, or an absent block.
	ReplyNull
	// ReplyBulk is "$<length>" followed by that many bytes.
	ReplyBulk
	// ReplyArray is "*<n>" followed by n replies.
	ReplyArray
	// ReplyMap is "%<n>" followed by n key/value pairs.
	ReplyMap
)

// Reply is one decoded server reply. Only the fields belonging to Kind are
// set.
type Reply struct {
	Kind ReplyKind
	// Text is the text of a [ReplySimple].
	Text string
	// Code and Message are the parts of a [ReplyError].
	Code    string
	Message string
	// Double is the value of a [ReplyDouble].
	Double float64
	// Bool is the value of a [ReplyBoolean].
	Bool bool
	// Bulk is the payload of a [ReplyBulk].
	Bulk []byte
	// Array holds the elements of a [ReplyArray].
	Array []Reply
	// Map holds the pairs of a [ReplyMap] in the order the server sent them.
	Map []MapEntry

	// A ReplyInteger is its sign and magnitude, so that both the int64 and
	// the uint64 range fit.
	negative  bool
	magnitude uint64
}

// MapEntry is one key/value pair of a [ReplyMap].
type MapEntry struct {
	Key   Reply
	Value Reply
}

// Int64 returns the value of a [ReplyInteger] and whether it fits an int64.
func (r Reply) Int64() (int64, bool) {
	if r.Kind != ReplyInteger {
		return 0, false
	}
	if r.negative {
		if r.magnitude > 1<<63 {
			return 0, false
		}
		// Two's complement: a magnitude of 2^63 is math.MinInt64.
		return int64(-r.magnitude), true
	}
	if r.magnitude > math.MaxInt64 {
		return 0, false
	}
	return int64(r.magnitude), true
}

// Uint64 returns the value of a [ReplyInteger] and whether it fits a uint64.
// Values of uN columns above the int64 range are read this way.
func (r Reply) Uint64() (uint64, bool) {
	if r.Kind != ReplyInteger || (r.negative && r.magnitude != 0) {
		return 0, false
	}
	return r.magnitude, true
}

// Lookup returns the value of a [ReplyMap] whose key is the bulk or simple
// string key.
func (r Reply) Lookup(key string) (Reply, bool) {
	for _, entry := range r.Map {
		if (entry.Key.Kind == ReplyBulk && string(entry.Key.Bulk) == key) ||
			(entry.Key.Kind == ReplySimple && entry.Key.Text == key) {
			return entry.Value, true
		}
	}
	return Reply{}, false
}

// integerReply builds a [ReplyInteger].
func integerReply(negative bool, magnitude uint64) Reply {
	return Reply{Kind: ReplyInteger, negative: negative && magnitude != 0, magnitude: magnitude}
}

// encodeRequest frames one statement and its parameters: the statement line,
// then per parameter "$<length>\r\n<bytes>\r\n", or "$-1\r\n" for a nil
// (NULL) parameter. A statement containing CR or LF is rejected.
func encodeRequest(command, statement string, params [][]byte) ([]byte, error) {
	if strings.ContainsAny(statement, "\r\n") {
		return nil, requestErrorf(command, "a statement is one line: it must not contain CR or LF")
	}
	size := len(statement) + 2
	for _, param := range params {
		size += len(param) + 16
	}
	out := make([]byte, 0, size)
	out = append(out, statement...)
	out = append(out, '\r', '\n')
	for _, param := range params {
		if param == nil {
			out = append(out, "$-1\r\n"...)
			continue
		}
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(param)), 10)
		out = append(out, '\r', '\n')
		out = append(out, param...)
		out = append(out, '\r', '\n')
	}
	return out, nil
}

// readReply decodes the next reply from r.
func readReply(r *bufio.Reader) (Reply, error) {
	return readReplyAt(r, 0)
}

func readReplyAt(r *bufio.Reader, depth int) (Reply, error) {
	if depth > maxReplyDepth {
		return Reply{}, protocolErrorf("", "reply nests deeper than %d levels", maxReplyDepth)
	}
	prefix, err := r.ReadByte()
	if err != nil {
		return Reply{}, err
	}
	line, err := readReplyLine(r)
	if err != nil {
		return Reply{}, err
	}

	switch prefix {
	case '+':
		return Reply{Kind: ReplySimple, Text: line}, nil

	case '-':
		code, message := splitErrorLine(line)
		return Reply{Kind: ReplyError, Code: code, Message: message}, nil

	case ':':
		return parseInteger(line)

	case ',':
		value, err := parseDouble(line)
		if err != nil {
			return Reply{}, err
		}
		return Reply{Kind: ReplyDouble, Double: value}, nil

	case '#':
		switch line {
		case "t":
			return Reply{Kind: ReplyBoolean, Bool: true}, nil
		case "f":
			return Reply{Kind: ReplyBoolean, Bool: false}, nil
		}
		return Reply{}, protocolErrorf("", "invalid boolean reply: %q", line)

	case '_':
		if line != "" {
			return Reply{}, protocolErrorf("", "invalid null reply: %q", line)
		}
		return Reply{Kind: ReplyNull}, nil

	case '$':
		if line == "-1" {
			return Reply{Kind: ReplyNull}, nil
		}
		length, err := strconv.Atoi(line)
		if err != nil || length < 0 {
			return Reply{}, protocolErrorf("", "invalid bulk length: %q", line)
		}
		if length > MaxBulkBytes {
			return Reply{}, protocolErrorf("", "bulk reply of %d bytes exceeds the %d-byte limit", length, MaxBulkBytes)
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Reply{}, err
		}
		if payload[length] != '\r' || payload[length+1] != '\n' {
			return Reply{}, protocolErrorf("", "bulk reply is not terminated by CRLF")
		}
		return Reply{Kind: ReplyBulk, Bulk: payload[:length:length]}, nil

	case '*':
		count, err := parseCount(line, "array")
		if err != nil {
			return Reply{}, err
		}
		// The capacity hint is clamped so a bogus count cannot preallocate an
		// arbitrarily large slice before any element has arrived.
		items := make([]Reply, 0, min(count, 1024))
		for range count {
			item, err := readReplyAt(r, depth+1)
			if err != nil {
				return Reply{}, err
			}
			items = append(items, item)
		}
		return Reply{Kind: ReplyArray, Array: items}, nil

	case '%':
		count, err := parseCount(line, "map")
		if err != nil {
			return Reply{}, err
		}
		entries := make([]MapEntry, 0, min(count, 1024))
		for range count {
			key, err := readReplyAt(r, depth+1)
			if err != nil {
				return Reply{}, err
			}
			value, err := readReplyAt(r, depth+1)
			if err != nil {
				return Reply{}, err
			}
			entries = append(entries, MapEntry{Key: key, Value: value})
		}
		return Reply{Kind: ReplyMap, Map: entries}, nil
	}
	return Reply{}, protocolErrorf("", "unexpected reply prefix %q", string(prefix))
}

func parseInteger(line string) (Reply, error) {
	digits, negative := strings.CutPrefix(line, "-")
	if digits == "" || digits[0] == '+' {
		return Reply{}, protocolErrorf("", "invalid integer reply: %q", line)
	}
	magnitude, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || (negative && magnitude > 1<<63) {
		return Reply{}, protocolErrorf("", "integer reply out of range: %q", line)
	}
	return integerReply(negative, magnitude), nil
}

func parseDouble(line string) (float64, error) {
	switch strings.ToLower(line) {
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	case "nan":
		return math.NaN(), nil
	}
	value, err := strconv.ParseFloat(line, 64)
	if err != nil {
		return 0, protocolErrorf("", "invalid double reply: %q", line)
	}
	return value, nil
}

func parseCount(line, what string) (int, error) {
	count, err := strconv.Atoi(line)
	if err != nil || count < 0 {
		return 0, protocolErrorf("", "invalid %s length: %q", what, line)
	}
	return count, nil
}

// splitErrorLine splits "ERR <code> <message>" into its parts. A line that
// does not carry the ERR prefix is reported verbatim under the generic code.
func splitErrorLine(line string) (code, message string) {
	rest, found := strings.CutPrefix(line, "ERR ")
	if !found {
		return "ERR", line
	}
	code, message, _ = strings.Cut(rest, " ")
	return code, message
}

// readReplyLine reads one CRLF- or LF-terminated line without its terminator.
func readReplyLine(r *bufio.Reader) (string, error) {
	var buf []byte
	for {
		fragment, err := r.ReadSlice('\n')
		if len(buf)+len(fragment) > maxReplyLineBytes {
			return "", protocolErrorf("", "reply line exceeds %d bytes", maxReplyLineBytes)
		}
		buf = append(buf, fragment...)
		if err == nil {
			break
		}
		if err != bufio.ErrBufferFull {
			return "", err
		}
	}
	line := strings.TrimSuffix(string(buf), "\n")
	return strings.TrimSuffix(line, "\r"), nil
}
