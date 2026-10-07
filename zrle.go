package chunkdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Decoder and encoder for chunkdb's "zrle" zero-run-length codec used by the
// ZRLE option of the chunk commands (CHUNKGET, CHUNKPUT, CHUNKRANGE,
// CHUNKRADIUS):
//
//	[0x01][u32le uncompressedSize][token...]
//	token := 0x00 <uleb128 n>            n zero bytes
//	       | 0x01 <uleb128 n> <n bytes>  n literal bytes
//
// Decompression is bounded: the caller supplies the exact expected output size
// and malformed, truncated, or oversized inputs are rejected.

const zrleCodecID = 0x01

// ZRLECompress encodes input with the zrle codec.
func ZRLECompress(input []byte) []byte {
	out := make([]byte, 5, len(input)+5)
	out[0] = zrleCodecID
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(input)))

	for i := 0; i < len(input); {
		if input[i] == 0 {
			run := 1
			for i+run < len(input) && input[i+run] == 0 {
				run++
			}
			out = append(out, 0x00)
			out = appendUleb128(out, uint64(run))
			i += run
			continue
		}

		run := 1
		for i+run < len(input) {
			if input[i+run] != 0 {
				run++
				continue
			}
			// A short interior zero gap is cheaper to keep inside the literal
			// run than to split into three tokens.
			zeros := 0
			for i+run+zeros < len(input) && input[i+run+zeros] == 0 {
				zeros++
			}
			if zeros <= 2 && i+run+zeros < len(input) {
				run += zeros + 1
				continue
			}
			break
		}
		out = append(out, 0x01)
		out = appendUleb128(out, uint64(run))
		out = append(out, input[i:i+run]...)
		i += run
	}
	return out
}

// zrleDeclaredSize reads the uncompressed size a zrle payload declares, so a
// caller that accepts a range of sizes can check it before decompressing.
func zrleDeclaredSize(input []byte) (int, error) {
	if len(input) < 5 {
		return 0, errors.New("zrle: input too small")
	}
	if input[0] != zrleCodecID {
		return 0, fmt.Errorf("zrle: unsupported codec id 0x%02x", input[0])
	}
	return int(binary.LittleEndian.Uint32(input[1:5])), nil
}

// ZRLEDecompress decodes a zrle payload. The declared and produced sizes must
// both equal expectedSize.
func ZRLEDecompress(input []byte, expectedSize int) ([]byte, error) {
	if expectedSize < 0 {
		return nil, errors.New("zrle: negative expected size")
	}
	if len(input) < 5 {
		return nil, errors.New("zrle: input too small")
	}
	if input[0] != zrleCodecID {
		return nil, fmt.Errorf("zrle: unsupported codec id 0x%02x", input[0])
	}
	if declared := binary.LittleEndian.Uint32(input[1:5]); declared != uint32(expectedSize) {
		return nil, fmt.Errorf("zrle: declared size %d does not match expected size %d", declared, expectedSize)
	}

	out := make([]byte, expectedSize)
	written := 0
	cursor := 5
	for cursor < len(input) {
		token := input[cursor]
		cursor++

		run, next, err := readUleb128(input, cursor)
		if err != nil {
			return nil, err
		}
		cursor = next

		if run == 0 {
			return nil, errors.New("zrle: zero-length run")
		}
		if run > uint64(expectedSize-written) {
			return nil, errors.New("zrle: output overflows expected size")
		}

		switch token {
		case 0x00:
			// out is already zero-filled.
			written += int(run)
		case 0x01:
			if run > uint64(len(input)-cursor) {
				return nil, errors.New("zrle: truncated literal run")
			}
			written += copy(out[written:], input[cursor:cursor+int(run)])
			cursor += int(run)
		default:
			return nil, fmt.Errorf("zrle: unknown token 0x%02x", token)
		}
	}
	if written != expectedSize {
		return nil, fmt.Errorf("zrle: produced %d bytes, expected %d", written, expectedSize)
	}
	return out, nil
}

func appendUleb128(dst []byte, value uint64) []byte {
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func readUleb128(input []byte, cursor int) (uint64, int, error) {
	var value uint64
	var shift uint
	for {
		if cursor >= len(input) {
			return 0, 0, errors.New("zrle: truncated varint")
		}
		if shift >= 63 {
			return 0, 0, errors.New("zrle: varint too large")
		}
		b := input[cursor]
		cursor++
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, cursor, nil
		}
		shift += 7
	}
}

// chunkReplyBound is the largest CHUNKGET reply for rawBytes of chunk data:
// the bytes themselves, or their zrle encoding, which adds at most a few
// bytes of framing to incompressible data.
func chunkReplyBound(rawBytes int) int {
	return rawBytes + 64
}
