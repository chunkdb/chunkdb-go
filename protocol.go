package chunkdb

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// MaxBulkBytes bounds a single bulk payload the client will accept. It matches
// the server's hard response-size limit for chunk state transfers, and keeps a
// corrupt or hostile length header from triggering an unbounded allocation.
const MaxBulkBytes = 64 << 20

// maxLineBytes bounds simple-string, error, and header lines. Every framing
// line defined by the protocol is far shorter than this.
const maxLineBytes = 1 << 20

// FrameKind identifies the response framing of a [Frame].
type FrameKind int

const (
	// FrameSimple is "+<TEXT>".
	FrameSimple FrameKind = iota + 1
	// FrameError is "-ERR <CODE> <MESSAGE>".
	FrameError
	// FrameBulk is "$<LEN>\r\n<PAYLOAD>".
	FrameBulk
	// FrameArray is "*<N>" followed by N bulk or null frames.
	FrameArray
	// FrameNull is "$-1": no value, such as an unset block.
	FrameNull
)

// Frame is one decoded server response. Only the fields belonging to Kind are
// populated.
type Frame struct {
	Kind FrameKind

	// Simple holds the text of a [FrameSimple] response.
	Simple string

	// Code, Message, and Raw describe a [FrameError] response.
	Code    string
	Message string
	Raw     string

	// Bulk holds the payload of a [FrameBulk] response.
	Bulk []byte

	// Array holds the items of a [FrameArray] response, each a [FrameBulk] or
	// a [FrameNull] frame.
	Array []Frame
}

// SerializeCommand encodes one command line. Parts are joined with spaces and
// terminated with CRLF; a part containing CR or LF is rejected.
func SerializeCommand(parts ...string) ([]byte, error) {
	command := ""
	if len(parts) > 0 {
		command = parts[0]
	}
	size := 2
	for _, part := range parts {
		if strings.ContainsAny(part, "\r\n") {
			return nil, requestErrorf(command, "command part contains invalid control characters")
		}
		size += len(part) + 1
	}

	out := make([]byte, 0, size)
	for i, part := range parts {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, part...)
	}
	return append(out, '\r', '\n'), nil
}

// ReadFrame decodes the next response frame from r.
func ReadFrame(r *bufio.Reader) (Frame, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Frame{}, err
	}

	switch prefix {
	case '+':
		line, err := readLine(r)
		if err != nil {
			return Frame{}, err
		}
		return Frame{Kind: FrameSimple, Simple: line}, nil

	case '-':
		line, err := readLine(r)
		if err != nil {
			return Frame{}, err
		}
		code, message := splitErrorLine(line)
		return Frame{Kind: FrameError, Code: code, Message: message, Raw: line}, nil

	case '$':
		return readBulk(r)

	case '*':
		header, err := readLine(r)
		if err != nil {
			return Frame{}, err
		}
		count, err := strconv.Atoi(header)
		if err != nil || count < 0 {
			return Frame{}, protocolErrorf("", "invalid array length: %s", header)
		}

		// The capacity hint is clamped so a bogus count cannot preallocate an
		// arbitrarily large slice before any payload has arrived.
		items := make([]Frame, 0, min(count, 1024))
		for range count {
			itemPrefix, err := r.ReadByte()
			if err != nil {
				return Frame{}, err
			}
			if itemPrefix != '$' {
				return Frame{}, protocolErrorf("", "expected bulk item in array response")
			}
			item, err := readBulk(r)
			if err != nil {
				return Frame{}, err
			}
			items = append(items, item)
		}
		return Frame{Kind: FrameArray, Array: items}, nil

	default:
		return Frame{}, protocolErrorf("", "unexpected response frame prefix: %q", string(prefix))
	}
}

// ParseInfo parses a key=value payload (INFO, HELLO, TABLEINFO) into its
// key/value pairs. A line without
// "=" maps to an empty value.
func ParseInfo(payload []byte) map[string]string {
	values := make(map[string]string)
	for _, line := range strings.Split(string(payload), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			values[line] = ""
			continue
		}
		values[key] = value
	}
	return values
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

// readLine reads one CRLF- or LF-terminated line without its terminator.
func readLine(r *bufio.Reader) (string, error) {
	var buf []byte
	for {
		fragment, err := r.ReadSlice('\n')
		if len(buf)+len(fragment) > maxLineBytes {
			return "", protocolErrorf("", "response line exceeds %d bytes", maxLineBytes)
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

// readBulk reads a "$<LEN>" header, its payload, and the payload terminator,
// or a "$-1" null.
func readBulk(r *bufio.Reader) (Frame, error) {
	header, err := readLine(r)
	if err != nil {
		return Frame{}, err
	}
	if header == "-1" {
		return Frame{Kind: FrameNull}, nil
	}

	length, err := strconv.Atoi(header)
	if err != nil || length < 0 {
		return Frame{}, protocolErrorf("", "invalid bulk length: %s", header)
	}
	if length > MaxBulkBytes {
		return Frame{}, protocolErrorf("", "bulk payload of %d bytes exceeds the %d-byte limit", length, MaxBulkBytes)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}

	terminator, err := r.ReadByte()
	if err != nil {
		return Frame{}, err
	}
	switch terminator {
	case '\n':
		return Frame{Kind: FrameBulk, Bulk: payload}, nil
	case '\r':
		next, err := r.ReadByte()
		if err != nil {
			return Frame{}, err
		}
		if next != '\n' {
			return Frame{}, protocolErrorf("", "invalid bulk terminator")
		}
		return Frame{Kind: FrameBulk, Bulk: payload}, nil
	default:
		return Frame{}, protocolErrorf("", "invalid bulk terminator")
	}
}
