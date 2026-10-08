package chunkdb

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestParseColumnType(t *testing.T) {
	cases := map[string]ColumnType{
		"u1":            TypeUint(1),
		"u64":           TypeUint(64),
		"i2":            TypeInt(2),
		"I64":           TypeInt(64),
		"bool":          TypeBool(),
		"f32":           TypeF32(),
		"f64":           TypeF64(),
		"bits(1)":       TypeBits(1),
		"bits(65535)":   TypeBits(65535),
		"text(256)":     TypeText(256),
		"bytes(4)":      TypeBytes(4),
		" bytes( 4 ) ":  TypeBytes(4),
		"text(1048576)": TypeText(1 << 20),
	}
	for text, want := range cases {
		got, err := ParseColumnType(text)
		if err != nil || got != want {
			t.Fatalf("ParseColumnType(%q) = %v, %v; want %v", text, got, err, want)
		}
	}
	for _, text := range []string{"", "u0", "u65", "i1", "i65", "u", "u+8", "f16", "bits(0)", "bits(65536)",
		"text(0)", "text", "text(x)", "bytes(4", "int", "u8 NULL"} {
		if _, err := ParseColumnType(text); !errors.Is(err, ErrProtocol) {
			t.Fatalf("ParseColumnType(%q): got %v, want an error", text, err)
		}
	}
	for _, text := range []string{"u10", "i8", "bool", "f32", "f64", "bits(3)", "text(16)", "bytes(4)"} {
		parsed, _ := ParseColumnType(text)
		if parsed.String() != text {
			t.Fatalf("got %q, want %q", parsed.String(), text)
		}
	}
}

func TestEncodeValue(t *testing.T) {
	type myInt int
	mask, _ := ParseBits("1000000001")
	cases := []struct {
		name  string
		typ   ColumnType
		value any
		want  []byte
	}{
		{"u10", TypeUint(10), 1023, []byte{0xff, 3, 0, 0, 0, 0, 0, 0}},
		{"u64 max", TypeUint(64), uint64(math.MaxUint64), bytes.Repeat([]byte{0xff}, 8)},
		{"named integer", TypeUint(8), myInt(5), []byte{5, 0, 0, 0, 0, 0, 0, 0}},
		{"i8 min", TypeInt(8), int8(-128), []byte{0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"i64 min", TypeInt(64), int64(math.MinInt64), []byte{0, 0, 0, 0, 0, 0, 0, 0x80}},
		{"i64 from uint", TypeInt(64), uint(7), []byte{7, 0, 0, 0, 0, 0, 0, 0}},
		{"bool", TypeBool(), true, []byte{1}},
		{"false", TypeBool(), false, []byte{0}},
		{"f32", TypeF32(), float32(-1), []byte{0, 0, 0x80, 0xbf}},
		{"f32 from f64", TypeF32(), 1.5, []byte{0, 0, 0xc0, 0x3f}},
		{"f32 inf", TypeF32(), math.Inf(1), []byte{0, 0, 0x80, 0x7f}},
		{"f64", TypeF64(), 1.0, []byte{0, 0, 0, 0, 0, 0, 0xf0, 0x3f}},
		{"bits", TypeBits(10), mask, []byte{0x01, 0x02}},
		{"text", TypeText(8), "héllo", []byte("héllo")},
		{"empty text", TypeText(8), "", []byte{}},
		{"bytes", TypeBytes(4), []byte{0, '\r', '\n', 0xff}, []byte{0, '\r', '\n', 0xff}},
		{"nil bytes is empty", TypeBytes(4), []byte(nil), []byte{}},
		{"NULL", TypeUint(8), nil, nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := EncodeValue(testCase.typ, testCase.value)
			if err != nil {
				t.Fatalf("EncodeValue: %v", err)
			}
			if !bytes.Equal(got, testCase.want) || (got == nil) != (testCase.want == nil) {
				t.Fatalf("got %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestEncodeValueRejects(t *testing.T) {
	cases := []struct {
		name  string
		typ   ColumnType
		value any
	}{
		{"u10 overflow", TypeUint(10), 1024},
		{"u8 negative", TypeUint(8), -1},
		{"u from float", TypeUint(8), 1.0},
		{"u from string", TypeUint(8), "1"},
		{"i8 overflow", TypeInt(8), 128},
		{"i8 underflow", TypeInt(8), -129},
		{"i64 from huge uint", TypeInt(64), uint64(1 << 63)},
		{"bool from int", TypeBool(), 1},
		{"f32 overflow", TypeF32(), 1e39},
		{"f64 from int", TypeF64(), 1},
		{"bits wrong width", TypeBits(10), NewBits(9)},
		{"bits short bytes", TypeBits(10), Bits{N: 10, Bytes: []byte{0}}},
		{"bits past width", TypeBits(10), Bits{N: 10, Bytes: []byte{0, 0x04}}},
		{"bits from string", TypeBits(4), "1010"},
		{"text too long", TypeText(2), "abc"},
		{"text not UTF-8", TypeText(8), "\xc3"},
		{"text from bytes", TypeText(8), []byte("a")},
		{"bytes too long", TypeBytes(2), []byte{1, 2, 3}},
		{"bytes from string", TypeBytes(2), "a"},
		{"invalid type", ColumnType{}, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := EncodeValue(testCase.typ, testCase.value); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want an error", err)
			}
		})
	}
}

func TestBits(t *testing.T) {
	b, err := ParseBits("1101")
	if err != nil || b.N != 4 || !bytes.Equal(b.Bytes, []byte{0x0b}) || b.String() != "1101" {
		t.Fatalf("got %+v, %v", b, err)
	}
	if !b.Bit(0) || b.Bit(2) {
		t.Fatal("wrong bits")
	}
	b.SetBit(2, true)
	b.SetBit(0, false)
	if b.String() != "0111" || !b.Equal(Bits{N: 4, Bytes: []byte{0x0e}}) {
		t.Fatalf("got %s", b)
	}
	if _, err := ParseBits("102"); err == nil {
		t.Fatal("ParseBits accepted a 2")
	}
}

func TestDecodeValue(t *testing.T) {
	cases := []struct {
		typ   ColumnType
		reply Reply
		want  any
	}{
		{TypeUint(64), integerReply(false, math.MaxUint64), uint64(math.MaxUint64)},
		{TypeInt(8), integerReply(true, 128), int64(-128)},
		{TypeBool(), Reply{Kind: ReplyBoolean, Bool: true}, true},
		{TypeF32(), Reply{Kind: ReplyDouble, Double: 0.1}, float32(0.1)},
		{TypeText(4), Reply{Kind: ReplyBulk, Bulk: []byte("ab")}, "ab"},
		{TypeUint(8), Reply{Kind: ReplyNull}, nil},
	}
	for _, testCase := range cases {
		got, err := decodeValue(testCase.typ, testCase.reply)
		if err != nil || got != testCase.want {
			t.Fatalf("%s: got %v, %v; want %v", testCase.typ, got, err, testCase.want)
		}
	}
	for _, bad := range []struct {
		typ   ColumnType
		reply Reply
	}{
		{TypeUint(8), integerReply(false, 256)},
		{TypeUint(8), integerReply(true, 1)},
		{TypeInt(8), integerReply(false, 128)},
		{TypeBits(9), Reply{Kind: ReplyBulk, Bulk: []byte{1}}},
		{TypeBool(), integerReply(false, 1)},
		{TypeBytes(4), Reply{Kind: ReplySimple, Text: "OK"}},
	} {
		if _, err := decodeValue(bad.typ, bad.reply); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s %+v: got %v, want ErrProtocol", bad.typ, bad.reply, err)
		}
	}
}

func TestLiteral(t *testing.T) {
	mask, _ := ParseBits("011")
	cases := map[string]any{
		"NULL":                 nil,
		"18446744073709551615": uint64(math.MaxUint64),
		"-5":                   int64(-5),
		"TRUE":                 true,
		"1.5":                  float32(1.5),
		"0.1":                  float32(0.1),
		"1e+300":               1e300,
		"nan":                  math.NaN(),
		"-inf":                 math.Inf(-1),
		"b'011'":               mask,
		"'it''s'":              "it's",
		"x'00ff'":              []byte{0, 0xff},
	}
	for want, value := range cases {
		if got := literal(value); got != want {
			t.Fatalf("literal(%v) = %q, want %q", value, got, want)
		}
	}
}
