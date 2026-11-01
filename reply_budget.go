package chunkdb

import (
	"bufio"
	"regexp"
	"strconv"
	"strings"
	"unsafe"
)

var areaReadPrefix = regexp.MustCompile(`(?i)^[ \t]*GET[ \t]+AREA\b`)

func isAreaReadStatement(statement string) bool { return areaReadPrefix.MatchString(statement) }

type areaReplyOptions struct {
	response   **areaResponseControl
	knownShape bool
	page       bool
	withSchema bool
	limit      uint64
}

type areaReplyLimits struct {
	maxBytes  uint64
	maxChunks uint64
	areaReplyOptions
}

type replyShape uint8

const (
	replyShapeNone replyShape = iota
	replyShapeRows
	replyShapePage
	replyShapeSchema
)

type replyBudget struct {
	limits    areaReplyLimits
	remaining uint64
	slots     uint64
}

func (b *replyBudget) exceed(reason string) error {
	return protocolErrorf("GET AREA", "reply exceeds its negotiated %d-byte or %d-cell bound: %s", b.limits.maxBytes, b.limits.maxChunks, reason)
}

func (b *replyBudget) consume(count uint64) error {
	if count > b.remaining {
		return b.exceed("encoded response bytes")
	}
	b.remaining -= count
	return nil
}

func (b *replyBudget) aggregate(count uint64, pairs bool, shape replyShape) error {
	if shape == replyShapeRows && count > b.limits.maxChunks {
		return b.exceed("chunk array cardinality")
	}
	multiplier := uint64(1)
	if pairs {
		multiplier = 2
	}
	if count > b.remaining/(3*multiplier) {
		return b.exceed("aggregate cannot fit its remaining encoded bytes")
	}
	if count > b.slots/multiplier {
		return b.exceed("decoded reply aggregate storage")
	}
	b.slots -= count * multiplier
	return nil
}

func (b *replyBudget) line(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		if b.remaining == 0 {
			return "", b.exceed("unterminated reply line")
		}
		value, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if err := b.consume(1); err != nil {
			return "", err
		}
		if len(line) == maxReplyLineBytes {
			return "", protocolErrorf("GET AREA", "reply line exceeds %d bytes", maxReplyLineBytes)
		}
		line = append(line, value)
		if value == '\n' {
			text, valid := strings.CutSuffix(string(line), "\r\n")
			if !valid {
				return "", protocolErrorf("GET AREA", "reply line is not terminated by CRLF")
			}
			return text, nil
		}
	}
}

func readReplyLimited(reader *bufio.Reader, limits *areaReplyLimits) (Reply, error) {
	if limits == nil {
		return readReply(reader)
	}
	prefix, err := reader.Peek(1)
	if err != nil {
		return Reply{}, err
	}
	if prefix[0] == '-' || prefix[0] == '>' {
		return readReply(reader)
	}
	budget := &replyBudget{limits: *limits, remaining: limits.maxBytes, slots: (64 * 1024 * 1024) / uint64(unsafe.Sizeof(Reply{}))}
	shape := replyShapeNone
	if limits.knownShape {
		shape = replyShapeRows
		if limits.page {
			shape = replyShapePage
		}
		if limits.withSchema {
			shape = replyShapeSchema
		}
	}
	return readReplyAtLimited(reader, 0, budget, shape)
}

func limitsForArea(info *ServerInfo, options []areaReplyOptions) (*areaReplyLimits, error) {
	if info == nil || info.MaxAreaChunks <= 0 || info.MaxResponseBytes <= 0 {
		return nil, requestErrorf("GET AREA", "negotiated reply limits must be positive")
	}
	if (info.Limits.MaxAreaChunks != 0 && info.Limits.MaxAreaChunks != uint64(info.MaxAreaChunks)) || (info.Limits.MaxAreaResponseBytes != 0 && info.Limits.MaxAreaResponseBytes != uint64(info.MaxResponseBytes)) {
		return nil, requestErrorf("GET AREA", "inconsistent negotiated reply limits")
	}
	limits := &areaReplyLimits{maxBytes: uint64(info.MaxResponseBytes), maxChunks: uint64(info.MaxAreaChunks)}
	if len(options) != 0 {
		limits.areaReplyOptions = options[0]
	}
	if limits.limit != 0 && limits.limit < limits.maxChunks {
		limits.maxChunks = limits.limit
	}
	return limits, nil
}

// Identify only the reply envelope of a framed raw statement, preserving quoted
// identifiers and comma-delimited column names that happen to name clauses.
func rawAreaReplyOptions(statement string) areaReplyOptions {
	var words []string
	wordStart := func(value byte) bool {
		return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_'
	}
	digit := func(value byte) bool { return value >= '0' && value <= '9' }
	for i := 0; i < len(statement); {
		if strings.ContainsRune(" \t\r\n", rune(statement[i])) {
			i++
			continue
		}
		if statement[i] == ',' {
			words = append(words, ",")
			i++
			continue
		}
		if quote := statement[i]; quote == '\'' || quote == '"' {
			i++
			closed := false
			for i < len(statement) {
				if statement[i] != quote {
					i++
					continue
				}
				i++
				if i < len(statement) && statement[i] == quote {
					i++
					continue
				}
				closed = true
				break
			}
			if !closed {
				return areaReplyOptions{}
			}
			words = append(words, "?")
			continue
		}
		start := i
		if wordStart(statement[i]) {
			i++
			for i < len(statement) && (wordStart(statement[i]) || digit(statement[i])) {
				i++
			}
		} else if digit(statement[i]) || statement[i] == '-' && i+1 < len(statement) && digit(statement[i+1]) {
			i++
			for i < len(statement) && digit(statement[i]) {
				i++
			}
		} else {
			i++
		}
		words = append(words, strings.ToUpper(statement[start:i]))
	}
	from := -1
	for i := 2; i < len(words); i++ {
		if words[i] == "FROM" {
			from = i
			break
		}
	}
	if from < 0 || from+1 >= len(words) {
		return areaReplyOptions{}
	}
	at := from + 2
	if at < len(words) && words[at] == "COLUMNS" {
		at += 2
		if at > len(words) {
			return areaReplyOptions{}
		}
		for at < len(words) && words[at] == "," {
			at += 2
			if at > len(words) {
				return areaReplyOptions{}
			}
		}
	}
	options := areaReplyOptions{knownShape: true}
	for at < len(words) {
		switch words[at] {
		case "LIMIT":
			if at+1 >= len(words) {
				return areaReplyOptions{}
			}
			limit, err := strconv.ParseUint(words[at+1], 10, 64)
			if err != nil {
				return areaReplyOptions{}
			}
			options.page = true
			options.limit = limit
			at += 2
		case "AFTER":
			options.page = true
			at++
			axes := 0
			for at < len(words) {
				if _, err := strconv.ParseInt(words[at], 10, 64); err != nil {
					break
				}
				at++
				axes++
			}
			if axes != 2 && axes != 3 {
				return areaReplyOptions{}
			}
		case "WITH":
			if at+1 >= len(words) || words[at+1] != "SCHEMA" {
				return areaReplyOptions{}
			}
			options.withSchema = true
			at += 2
		default:
			return areaReplyOptions{}
		}
	}
	return options
}
