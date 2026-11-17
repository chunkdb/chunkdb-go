package chunkdb

import (
	"bufio"
	"io"
	"math"
	"math/big"
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
	// ReplySimple holds a simple string.
	ReplySimple ReplyKind = iota + 1
	// ReplyError holds a server error. [Client.Do] returns it as an
	// error, never as a Reply.
	ReplyError
	// ReplyInteger holds an integer; read it with [Reply.Int64] or [Reply.Uint64].
	ReplyInteger
	// ReplyDouble holds a floating-point value, including infinities and NaN.
	ReplyDouble
	// ReplyBoolean holds a boolean.
	ReplyBoolean
	// ReplyNull means NULL or an absent block.
	ReplyNull
	// ReplyBulk holds a byte string.
	ReplyBulk
	// ReplyArray holds a list of replies.
	ReplyArray
	// ReplyMap holds key/value pairs.
	ReplyMap
	// ReplyPush carries an unsolicited WATCH event.
	ReplyPush
	// ReplyBigNumber holds an exact integer of arbitrary size.
	ReplyBigNumber
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
	// Keys contains percent-decoded structured error metadata.
	Keys map[string]string
	// BigNumber holds the value of a ReplyBigNumber.
	BigNumber *big.Int
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
	if r.Kind == ReplyBigNumber {
		if r.BigNumber == nil || !r.BigNumber.IsInt64() {
			return 0, false
		}
		return r.BigNumber.Int64(), true
	}
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
	if r.Kind == ReplyBigNumber {
		if r.BigNumber == nil || !r.BigNumber.IsUint64() {
			return 0, false
		}
		return r.BigNumber.Uint64(), true
	}
	if r.Kind != ReplyInteger || (r.negative && r.magnitude != 0) {
		return 0, false
	}
	return r.magnitude, true
}

// BigInt returns a copy of either integer representation, without narrowing.
func (r Reply) BigInt() (*big.Int, bool) {
	if r.Kind == ReplyBigNumber && r.BigNumber != nil {
		return new(big.Int).Set(r.BigNumber), true
	}
	if r.Kind != ReplyInteger {
		return nil, false
	}
	n := new(big.Int).SetUint64(r.magnitude)
	if r.negative {
		n.Neg(n)
	}
	return n, true
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
	if count := parameterOccurrences(statement); count != len(params) {
		return nil, requestErrorf(command, "statement has %d parameter occurrences, got %d frames", count, len(params))
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
	return readReplyAtLimited(r, depth, nil, replyShapeNone)
}

func readReplyAtLimited(r *bufio.Reader, depth int, budget *replyBudget, shape replyShape) (Reply, error) {
	if depth > maxReplyDepth {
		return Reply{}, protocolErrorf("", "reply nests deeper than %d levels", maxReplyDepth)
	}
	if budget != nil {
		if err := budget.consume(1); err != nil {
			return Reply{}, err
		}
	}
	prefix, err := r.ReadByte()
	if err != nil {
		return Reply{}, err
	}
	var line string
	if budget == nil {
		line, err = readReplyLine(r)
	} else {
		line, err = budget.line(r)
	}
	if err != nil {
		return Reply{}, err
	}

	switch prefix {
	case '+':
		return Reply{Kind: ReplySimple, Text: line}, nil

	case '-':
		code, message := splitErrorLine(line)
		keys, message := parseErrorKeys(message)
		return Reply{Kind: ReplyError, Code: code, Message: message, Keys: keys}, nil

	case ':':
		return parseInteger(line)
	case '(':
		digits := strings.TrimPrefix(line, "-")
		if digits == "" {
			return Reply{}, protocolErrorf("", "invalid big number: %q", line)
		}
		for _, c := range digits {
			if c < '0' || c > '9' {
				return Reply{}, protocolErrorf("", "invalid big number: %q", line)
			}
		}
		number, ok := new(big.Int).SetString(line, 10)
		if !ok {
			return Reply{}, protocolErrorf("", "invalid big number: %q", line)
		}
		return Reply{Kind: ReplyBigNumber, BigNumber: number}, nil

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
		if budget != nil {
			if err := budget.consume(uint64(length) + 2); err != nil {
				return Reply{}, err
			}
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Reply{}, err
		}
		if payload[length] != '\r' || payload[length+1] != '\n' {
			return Reply{}, protocolErrorf("", "bulk reply is not terminated by CRLF")
		}
		return Reply{Kind: ReplyBulk, Bulk: payload[:length:length]}, nil

	case '*', '>':
		if prefix == '>' && depth != 0 {
			return Reply{}, protocolErrorf("", "push must be a top-level reply")
		}
		count, err := parseCount(line, "array")
		if err != nil {
			return Reply{}, err
		}
		if budget != nil {
			if err := budget.aggregate(uint64(count), false, shape); err != nil {
				return Reply{}, err
			}
		}
		// The capacity hint is clamped so a bogus count cannot preallocate an
		// arbitrarily large slice before any element has arrived.
		items := make([]Reply, 0, min(count, 1024))
		for i := range count {
			child := replyShapeNone
			if shape == replyShapeSchema && i == 1 && budget != nil {
				child = replyShapeRows
				if budget.limits.page {
					child = replyShapePage
				}
			}
			item, err := readReplyAtLimited(r, depth+1, budget, child)
			if err != nil {
				return Reply{}, err
			}
			items = append(items, item)
		}
		kind := ReplyArray
		if prefix == '>' {
			kind = ReplyPush
		}
		return Reply{Kind: kind, Array: items}, nil

	case '%':
		count, err := parseCount(line, "map")
		if err != nil {
			return Reply{}, err
		}
		if budget != nil {
			if err := budget.aggregate(uint64(count), true, shape); err != nil {
				return Reply{}, err
			}
		}
		entries := make([]MapEntry, 0, min(count, 1024))
		for range count {
			key, err := readReplyAtLimited(r, depth+1, budget, replyShapeNone)
			if err != nil {
				return Reply{}, err
			}
			child := replyShapeNone
			if shape == replyShapePage && ((key.Kind == ReplyBulk && string(key.Bulk) == "chunks") || (key.Kind == ReplySimple && key.Text == "chunks")) {
				child = replyShapeRows
			}
			value, err := readReplyAtLimited(r, depth+1, budget, child)
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

// parameterOccurrences counts frames without depending on statement grammar.
func parameterOccurrences(statement string) int {
	count := 0
	for i := 0; i < len(statement); i++ {
		if quote := statement[i]; quote == '\'' || quote == '"' {
			for i++; i < len(statement); i++ {
				if statement[i] != quote {
					continue
				}
				if i+1 < len(statement) && statement[i+1] == quote {
					i++
					continue
				}
				break
			}
			continue
		}
		if statement[i] == '$' && i+1 < len(statement) && statement[i+1] >= '0' && statement[i+1] <= '9' {
			count++
			for i+1 < len(statement) && statement[i+1] >= '0' && statement[i+1] <= '9' {
				i++
			}
		}
	}
	return count
}
