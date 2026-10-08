package chunkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ColumnKind is the family of a [ColumnType].
type ColumnKind int

const (
	// ColumnUnsigned is uN: an unsigned integer of N bits, N in 1..64.
	ColumnUnsigned ColumnKind = iota + 1
	// ColumnSigned is iN: a two's complement integer of N bits, N in 2..64.
	ColumnSigned
	// ColumnBool is bool.
	ColumnBool
	// ColumnF32 is f32, an IEEE 754 single.
	ColumnF32
	// ColumnF64 is f64, an IEEE 754 double.
	ColumnF64
	// ColumnBits is bits(N): a string of N bits, N in 1..65535.
	ColumnBits
	// ColumnText is text(max): UTF-8 of at most max bytes.
	ColumnText
	// ColumnBytes is bytes(max): at most max bytes.
	ColumnBytes
)

// ColumnType is a column's type. Size is N for uN, iN and bits(N), the
// maximum length in bytes for text(max) and bytes(max), and unused otherwise.
type ColumnType struct {
	Kind ColumnKind
	Size int
}

// TypeUint is uN.
func TypeUint(bits int) ColumnType { return ColumnType{Kind: ColumnUnsigned, Size: bits} }

// TypeInt is iN.
func TypeInt(bits int) ColumnType { return ColumnType{Kind: ColumnSigned, Size: bits} }

// TypeBool is bool.
func TypeBool() ColumnType { return ColumnType{Kind: ColumnBool} }

// TypeF32 is f32.
func TypeF32() ColumnType { return ColumnType{Kind: ColumnF32} }

// TypeF64 is f64.
func TypeF64() ColumnType { return ColumnType{Kind: ColumnF64} }

// TypeBits is bits(N).
func TypeBits(bits int) ColumnType { return ColumnType{Kind: ColumnBits, Size: bits} }

// TypeText is text(max).
func TypeText(maxBytes int) ColumnType { return ColumnType{Kind: ColumnText, Size: maxBytes} }

// TypeBytes is bytes(max).
func TypeBytes(maxBytes int) ColumnType { return ColumnType{Kind: ColumnBytes, Size: maxBytes} }

// ParseColumnType parses a type as CQL and DESCRIBE write it: uN, iN, bool,
// f32, f64, bits(N), text(max) or bytes(max).
func ParseColumnType(text string) (ColumnType, error) {
	word := strings.ToLower(strings.TrimSpace(text))
	invalid := func() (ColumnType, error) {
		return ColumnType{}, requestErrorf("", "invalid column type %q", text)
	}
	switch word {
	case "bool":
		return TypeBool(), nil
	case "f32":
		return TypeF32(), nil
	case "f64":
		return TypeF64(), nil
	}
	for _, sized := range []struct {
		name string
		kind ColumnKind
	}{{"bits", ColumnBits}, {"text", ColumnText}, {"bytes", ColumnBytes}} {
		rest, ok := strings.CutPrefix(word, sized.name+"(")
		if !ok {
			continue
		}
		digits, ok := strings.CutSuffix(rest, ")")
		size, err := strconv.ParseUint(strings.TrimSpace(digits), 10, 32)
		if !ok || err != nil {
			return invalid()
		}
		t := ColumnType{Kind: sized.kind, Size: int(size)}
		if t.validate() != nil {
			return invalid()
		}
		return t, nil
	}
	if len(word) > 1 && (word[0] == 'u' || word[0] == 'i') {
		size, err := strconv.ParseUint(word[1:], 10, 8)
		if err != nil || word[1] == '+' {
			return invalid()
		}
		t := ColumnType{Kind: ColumnUnsigned, Size: int(size)}
		if word[0] == 'i' {
			t.Kind = ColumnSigned
		}
		if t.validate() != nil {
			return invalid()
		}
		return t, nil
	}
	return invalid()
}

// String formats the type as CQL writes it.
func (t ColumnType) String() string {
	switch t.Kind {
	case ColumnUnsigned:
		return "u" + strconv.Itoa(t.Size)
	case ColumnSigned:
		return "i" + strconv.Itoa(t.Size)
	case ColumnBool:
		return "bool"
	case ColumnF32:
		return "f32"
	case ColumnF64:
		return "f64"
	case ColumnBits:
		return "bits(" + strconv.Itoa(t.Size) + ")"
	case ColumnText:
		return "text(" + strconv.Itoa(t.Size) + ")"
	case ColumnBytes:
		return "bytes(" + strconv.Itoa(t.Size) + ")"
	}
	return fmt.Sprintf("ColumnType(%d, %d)", t.Kind, t.Size)
}

func (t ColumnType) validate() error {
	ok := false
	switch t.Kind {
	case ColumnUnsigned:
		ok = t.Size >= 1 && t.Size <= 64
	case ColumnSigned:
		ok = t.Size >= 2 && t.Size <= 64
	case ColumnBool, ColumnF32, ColumnF64:
		ok = true
	case ColumnBits:
		ok = t.Size >= 1 && t.Size <= 65535
	case ColumnText, ColumnBytes:
		ok = t.Size >= 1 && t.Size <= math.MaxUint32
	}
	if !ok {
		return requestErrorf("", "invalid column type %s", t)
	}
	return nil
}

// fixedWidth reports whether values of t live in the chunk payload, and with
// how many bits.
func (t ColumnType) fixedWidth() (int, bool) {
	switch t.Kind {
	case ColumnUnsigned, ColumnSigned, ColumnBits:
		return t.Size, true
	case ColumnBool:
		return 1, true
	case ColumnF32:
		return 32, true
	case ColumnF64:
		return 64, true
	}
	return 0, false
}

// Bits is the value of a bits(N) column: N bits, bit i being
// Bytes[i/8]>>(i%8)&1. Bits past N in the last byte are zero.
type Bits struct {
	N     int
	Bytes []byte
}

// NewBits returns n zero bits.
func NewBits(n int) Bits {
	return Bits{N: n, Bytes: make([]byte, (n+7)/8)}
}

// ParseBits parses a string of 0 and 1 digits, the first digit being the
// lowest bit, as in the CQL literal b'1010'.
func ParseBits(digits string) (Bits, error) {
	b := NewBits(len(digits))
	for i, digit := range []byte(digits) {
		switch digit {
		case '1':
			b.Bytes[i/8] |= 1 << (i % 8)
		case '0':
		default:
			return Bits{}, requestErrorf("", "bits take the digits 0 and 1, got %q", digits)
		}
	}
	return b, nil
}

// Bit reports bit i.
func (b Bits) Bit(i int) bool { return b.Bytes[i/8]>>(i%8)&1 == 1 }

// SetBit sets bit i to v. It changes the shared Bytes.
func (b Bits) SetBit(i int, v bool) {
	if v {
		b.Bytes[i/8] |= 1 << (i % 8)
	} else {
		b.Bytes[i/8] &^= 1 << (i % 8)
	}
}

// String formats the bits as ParseBits reads them, lowest bit first.
func (b Bits) String() string {
	var out strings.Builder
	out.Grow(b.N)
	for i := range b.N {
		if b.Bit(i) {
			out.WriteByte('1')
		} else {
			out.WriteByte('0')
		}
	}
	return out.String()
}

// Equal reports whether b and other hold the same bits.
func (b Bits) Equal(other Bits) bool {
	return b.N == other.N && bytes.Equal(b.Bytes, other.Bytes)
}

// valueError reports a value that column (or type) cannot take.
func valueError(column string, t ColumnType, format string, args ...any) error {
	what := t.String()
	if column != "" {
		what = "column " + column + " (" + what + ")"
	}
	return requestErrorf("", "%s: %s", what, fmt.Sprintf(format, args...))
}

// normalize converts v to the Go type of t's values (uint64, int64, bool,
// float32, float64, [Bits], string, []byte) and checks that t holds it. nil
// stays nil.
func normalize(column string, t ColumnType, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	fail := func(format string, args ...any) (any, error) {
		return nil, valueError(column, t, format, args...)
	}
	switch t.Kind {
	case ColumnUnsigned, ColumnSigned:
		negative, magnitude, ok := integerOf(v)
		if !ok {
			return fail("takes an integer, got %T", v)
		}
		if t.Kind == ColumnUnsigned {
			if negative || (t.Size < 64 && magnitude >= 1<<t.Size) {
				return fail("%s is out of range", formatInteger(negative, magnitude))
			}
			return magnitude, nil
		}
		limit := uint64(1) << (t.Size - 1)
		if (negative && magnitude > limit) || (!negative && magnitude >= limit) {
			return fail("%s is out of range", formatInteger(negative, magnitude))
		}
		if negative {
			return int64(-magnitude), nil
		}
		return int64(magnitude), nil
	case ColumnBool:
		value := reflect.ValueOf(v)
		if value.Kind() != reflect.Bool {
			return fail("takes a bool, got %T", v)
		}
		return value.Bool(), nil
	case ColumnF32, ColumnF64:
		value := reflect.ValueOf(v)
		if value.Kind() != reflect.Float32 && value.Kind() != reflect.Float64 {
			return fail("takes a float32 or float64, got %T", v)
		}
		number := value.Float()
		if t.Kind == ColumnF64 {
			return number, nil
		}
		if !math.IsInf(number, 0) && !math.IsNaN(number) && math.Abs(number) > math.MaxFloat32 {
			return fail("%g is out of range", number)
		}
		return float32(number), nil
	case ColumnBits:
		b, ok := v.(Bits)
		if !ok {
			return fail("takes chunkdb.Bits, got %T", v)
		}
		if b.N != t.Size || len(b.Bytes) != (t.Size+7)/8 {
			return fail("takes %d bits in %d bytes, got %d bits in %d bytes", t.Size, (t.Size+7)/8, b.N, len(b.Bytes))
		}
		if tail := t.Size % 8; tail != 0 && b.Bytes[len(b.Bytes)-1]>>tail != 0 {
			return fail("has bits set past its width")
		}
		return Bits{N: b.N, Bytes: bytes.Clone(b.Bytes)}, nil
	case ColumnText:
		value := reflect.ValueOf(v)
		if value.Kind() != reflect.String {
			return fail("takes a string, got %T", v)
		}
		text := value.String()
		if !utf8.ValidString(text) {
			return fail("the text is not valid UTF-8")
		}
		if len(text) > t.Size {
			return fail("%d bytes is longer than %d", len(text), t.Size)
		}
		return text, nil
	case ColumnBytes:
		value := reflect.ValueOf(v)
		if value.Kind() != reflect.Slice || value.Type().Elem().Kind() != reflect.Uint8 {
			return fail("takes a []byte, got %T", v)
		}
		data := value.Bytes()
		if len(data) > t.Size {
			return fail("%d bytes is longer than %d", len(data), t.Size)
		}
		// Never nil: a nil []byte is the empty value, not NULL.
		return append([]byte{}, data...), nil
	}
	return fail("unknown column type")
}

// integerOf reads any Go integer as its sign and magnitude.
func integerOf(v any) (negative bool, magnitude uint64, ok bool) {
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := value.Int()
		if n < 0 {
			return true, uint64(-(n + 1)) + 1, true
		}
		return false, uint64(n), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return false, value.Uint(), true
	}
	return false, 0, false
}

func formatInteger(negative bool, magnitude uint64) string {
	if negative {
		return "-" + strconv.FormatUint(magnitude, 10)
	}
	return strconv.FormatUint(magnitude, 10)
}

// EncodeValue returns the parameter frame of v for a column of type t, as
// [Client.Do] sends it: uN and iN 8 bytes little-endian, bool 1 byte, f32 4
// and f64 8 bytes IEEE 754 little-endian, bits(N) (N+7)/8 bytes lowest bit
// first, text UTF-8, bytes as they are. A nil v is NULL and returns nil.
//
// The Go types a column type takes, and the type values are read back as:
//
//   - uN: any integer type, read as uint64
//   - iN: any integer type, read as int64
//   - bool: bool
//   - f32: float32 or float64, read as float32
//   - f64: float32 or float64, read as float64
//   - bits(N): [Bits] of exactly N bits
//   - text(max): string, valid UTF-8 of at most max bytes
//   - bytes(max): []byte of at most max bytes
//
// A value that does not fit the type is an error.
func EncodeValue(t ColumnType, v any) ([]byte, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
	value, err := normalize("", t, v)
	if err != nil || value == nil {
		return nil, err
	}
	return encodeNormalized(value), nil
}

// encodeNormalized is the parameter frame of a normalized value.
func encodeNormalized(value any) []byte {
	switch v := value.(type) {
	case uint64:
		return binary.LittleEndian.AppendUint64(nil, v)
	case int64:
		return binary.LittleEndian.AppendUint64(nil, uint64(v))
	case bool:
		if v {
			return []byte{1}
		}
		return []byte{0}
	case float32:
		return binary.LittleEndian.AppendUint32(nil, math.Float32bits(v))
	case float64:
		return binary.LittleEndian.AppendUint64(nil, math.Float64bits(v))
	case Bits:
		return append([]byte{}, v.Bytes...)
	case string:
		return append([]byte{}, v...)
	case []byte:
		return append([]byte{}, v...)
	}
	panic(fmt.Sprintf("chunkdb: unexpected normalized value %T", value))
}

// decodeValue reads a reply as a value of a column of type t.
func decodeValue(t ColumnType, reply Reply) (any, error) {
	if reply.Kind == ReplyNull {
		return nil, nil
	}
	mismatch := func() (any, error) {
		return nil, protocolErrorf("", "a %s value cannot be a reply of kind %d", t, reply.Kind)
	}
	switch t.Kind {
	case ColumnUnsigned:
		value, ok := reply.Uint64()
		if !ok || (t.Size < 64 && value >= 1<<t.Size) {
			return mismatch()
		}
		return value, nil
	case ColumnSigned:
		value, ok := reply.Int64()
		if !ok || (t.Size < 64 && (value >= 1<<(t.Size-1) || value < -(1<<(t.Size-1)))) {
			return mismatch()
		}
		return value, nil
	case ColumnBool:
		if reply.Kind != ReplyBoolean {
			return mismatch()
		}
		return reply.Bool, nil
	case ColumnF32:
		if reply.Kind != ReplyDouble {
			return mismatch()
		}
		return float32(reply.Double), nil
	case ColumnF64:
		if reply.Kind != ReplyDouble {
			return mismatch()
		}
		return reply.Double, nil
	case ColumnBits:
		if reply.Kind != ReplyBulk || len(reply.Bulk) != (t.Size+7)/8 {
			return mismatch()
		}
		return Bits{N: t.Size, Bytes: bytes.Clone(reply.Bulk)}, nil
	case ColumnText:
		if reply.Kind != ReplyBulk {
			return mismatch()
		}
		return string(reply.Bulk), nil
	case ColumnBytes:
		if reply.Kind != ReplyBulk {
			return mismatch()
		}
		return append([]byte{}, reply.Bulk...), nil
	}
	return mismatch()
}

// literal formats a normalized value as a CQL literal, for DEFAULT.
func literal(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case uint64:
		return strconv.FormatUint(v, 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case float32:
		return formatFloat(float64(v), 32)
	case float64:
		return formatFloat(v, 64)
	case Bits:
		return "b'" + v.String() + "'"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "x'" + hex.EncodeToString(v) + "'"
	}
	panic(fmt.Sprintf("chunkdb: unexpected normalized value %T", value))
}

func formatFloat(v float64, bitSize int) string {
	switch {
	case math.IsNaN(v):
		return "nan"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	return strconv.FormatFloat(v, 'g', -1, bitSize)
}
