package chunkdb

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// TestZRLECompressMatchesReferenceVectors pins the encoder to bytes produced by
// the @chunkdb/client reference implementation of the same codec.
func TestZRLECompressMatchesReferenceVectors(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
		want  string
	}{
		{"empty", nil, "0100000000"},
		{"single zero", []byte{0}, "01010000000001"},
		{"zero run then literals", []byte{0, 0, 0, 7, 7}, "0105000000000301020707"},
		{"long zero run", make([]byte, 200), "01c800000000c801"},
		{"short interior gap", []byte{1, 0, 2, 3}, "0104000000010401000203"},
		{"mixed gaps", []byte{1, 2, 0, 3, 0, 0, 4, 5}, "010800000001080102000300000405"},
		{"long interior gap", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 2}, "010a0000000101010008010102"},
		{"trailing zeros", append(bytes.Repeat([]byte{1}, 3), make([]byte, 100)...), "016700000001030101010064"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := hex.EncodeToString(ZRLECompress(testCase.input)); got != testCase.want {
				t.Fatalf("got %s, want %s", got, testCase.want)
			}
		})
	}
}

func TestZRLERoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"empty":            {},
		"all zeros":        make([]byte, 4096),
		"no zeros":         bytes.Repeat([]byte{0xff, 0x0f}, 512),
		"leading zeros":    append(make([]byte, 100), bytes.Repeat([]byte{0x01}, 10)...),
		"trailing zeros":   append(bytes.Repeat([]byte{0x01}, 10), make([]byte, 100)...),
		"short zero gaps":  {1, 2, 0, 3, 0, 0, 4, 5},
		"long zero gaps":   {1, 0, 0, 0, 0, 0, 0, 0, 0, 2},
		"single zero byte": {0},
		"single literal":   {0x7f},
	}

	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			compressed := ZRLECompress(input)
			decompressed, err := ZRLEDecompress(compressed, len(input))
			if err != nil {
				t.Fatalf("ZRLEDecompress: %v", err)
			}
			if !bytes.Equal(decompressed, input) {
				t.Fatalf("round trip mismatch:\n got %v\nwant %v", decompressed, input)
			}
		})
	}
}

func TestZRLECompressWireFormat(t *testing.T) {
	compressed := ZRLECompress([]byte{0, 0, 0, 7, 7})
	want := []byte{
		0x01,                   // codec id
		0x05, 0x00, 0x00, 0x00, // uncompressed size, little endian
		0x00, 0x03, // three zero bytes
		0x01, 0x02, 0x07, 0x07, // two literal bytes
	}
	if !bytes.Equal(compressed, want) {
		t.Fatalf("got %v, want %v", compressed, want)
	}
}

func TestZRLECompressLongRunUsesVarint(t *testing.T) {
	// 200 zero bytes need a two-byte uleb128 run length.
	compressed := ZRLECompress(make([]byte, 200))
	want := []byte{0x01, 0xc8, 0x00, 0x00, 0x00, 0x00, 0xc8, 0x01}
	if !bytes.Equal(compressed, want) {
		t.Fatalf("got %v, want %v", compressed, want)
	}
}

func TestZRLECompressKeepsShortZeroGapsInline(t *testing.T) {
	// A one- or two-byte zero gap between literals stays inside the literal
	// run instead of paying for two extra tokens.
	compressed := ZRLECompress([]byte{1, 0, 2, 3})
	want := []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04, 1, 0, 2, 3}
	if !bytes.Equal(compressed, want) {
		t.Fatalf("got %v, want %v", compressed, want)
	}
}

func TestZRLEDecompressRejectsBadInput(t *testing.T) {
	valid := ZRLECompress([]byte{1, 2, 3, 0, 0})

	cases := []struct {
		name         string
		input        []byte
		expectedSize int
	}{
		{name: "too small", input: []byte{0x01, 0x00}, expectedSize: 0},
		{name: "unknown codec", input: []byte{0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01}, expectedSize: 1},
		{name: "declared size mismatch", input: valid, expectedSize: 4},
		{name: "negative expected size", input: valid, expectedSize: -1},
		{name: "truncated varint", input: []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x80}, expectedSize: 2},
		{name: "zero-length run", input: []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00}, expectedSize: 2},
		{name: "unknown token", input: []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x02}, expectedSize: 2},
		{name: "run overflows output", input: []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x03}, expectedSize: 2},
		{name: "truncated literal run", input: []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x07}, expectedSize: 2},
		{name: "output shorter than expected", input: []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x02}, expectedSize: 4},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := ZRLEDecompress(testCase.input, testCase.expectedSize); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestZRLEDecompressZeroFillsGaps(t *testing.T) {
	// A payload that is a single zero run must decode to an all-zero buffer.
	decompressed, err := ZRLEDecompress([]byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x04}, 4)
	if err != nil {
		t.Fatalf("ZRLEDecompress: %v", err)
	}
	if !bytes.Equal(decompressed, make([]byte, 4)) {
		t.Fatalf("got %v, want four zero bytes", decompressed)
	}
}
