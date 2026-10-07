package chunkdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
)

const (
	// extraEntryHeaderBytes is the size of an EXTRA section entry before its
	// value bytes: block_index u32le, bit_length u32le.
	extraEntryHeaderBytes = 8
	// maxExtraBlockBits is the longest value the protocol allows: one value
	// filling the largest extra_max_chunk_bytes (16 MiB).
	maxExtraBlockBits = (16<<20 - extraEntryHeaderBytes) * 8
)

// extraValueBytes is the number of bytes that hold bitLength bits.
func extraValueBytes(bitLength int) int { return (bitLength + 7) / 8 }

// extraPaddingMask is the mask of the bits of a value's last byte that hold
// data.
func extraPaddingMask(bitLength int) byte {
	if used := bitLength % 8; used != 0 {
		return 0xff >> (8 - used)
	}
	return 0xff
}

// checkExtraValue reports why value cannot be written, or nil.
func checkExtraValue(value ExtraValue) error {
	if value.BitLength < 1 || value.BitLength > maxExtraBlockBits {
		return fmt.Errorf("bit length must be between 1 and %d, got %d", maxExtraBlockBits, value.BitLength)
	}
	if want := extraValueBytes(value.BitLength); len(value.Bytes) != want {
		return fmt.Errorf("a value of %d bits takes %d bytes, got %d", value.BitLength, want, len(value.Bytes))
	}
	return nil
}

// XGet reads the extra data of one block, or nil when the block has none.
// On a table without extra data the server fails it with INVALID_ARGUMENT.
func (c *Client) XGet(ctx context.Context, x, y int64) (*ExtraValue, error) {
	release, err := c.acquireSlot(ctx, "XGET")
	if err != nil {
		return nil, err
	}
	defer release()

	frame, err := c.exec(ctx, "XGET", coord(x), coord(y))
	if err != nil {
		return nil, err
	}
	switch frame.Kind {
	case FrameNull:
		return nil, nil
	case FrameBulk:
		value, err := decodeExtraReply(frame.Bulk)
		if err != nil {
			return nil, protocolErrorf("XGET", "invalid XGET reply: %s", err)
		}
		return &value, nil
	default:
		return nil, protocolErrorf("XGET", "expected bulk or null response for XGET")
	}
}

// decodeExtraReply parses an XGET reply: bit_length u32le, then the value
// bytes.
func decodeExtraReply(body []byte) (ExtraValue, error) {
	if len(body) < 4 {
		return ExtraValue{}, fmt.Errorf("%d bytes is shorter than the bit length", len(body))
	}
	bitLength := binary.LittleEndian.Uint32(body)
	if bitLength == 0 || bitLength > maxExtraBlockBits {
		return ExtraValue{}, fmt.Errorf("the value has %d bits, expected 1..%d", bitLength, maxExtraBlockBits)
	}
	if len(body)-4 != extraValueBytes(int(bitLength)) {
		return ExtraValue{}, fmt.Errorf("a value of %d bits in %d bytes", bitLength, len(body)-4)
	}
	value := ExtraValue{BitLength: int(bitLength), Bytes: body[4:]}
	if value.Bytes[len(value.Bytes)-1]&^extraPaddingMask(value.BitLength) != 0 {
		return ExtraValue{}, fmt.Errorf("the value has set bits past its bit length")
	}
	return value, nil
}

// XPut sets the extra data of a present block, replacing any value it had.
// The table must have extra data, and the value must fit its
// extra_max_block_bits and extra_max_chunk_bytes; the server checks those
// limits and refuses a value over them with INVALID_ARGUMENT. Padding bits
// past BitLength in the last byte are ignored.
func (c *Client) XPut(ctx context.Context, x, y int64, value ExtraValue) error {
	release, err := c.acquireSlot(ctx, "XPUT")
	if err != nil {
		return err
	}
	defer release()

	if err := checkExtraValue(value); err != nil {
		return requestErrorf("XPUT", "XPUT %s", err)
	}
	// Without a table, the server refuses XPUT without reading its bytes and
	// closes the connection.
	established, _, err := c.chunkConnection(ctx, "XPUT")
	if err != nil {
		return err
	}
	// A server without extra data does not read the bytes either: it would
	// take them for commands. One with it refuses a value longer than
	// max_extra_chunk_bytes minus its 8-byte entry header unread and closes.
	if err := requireExtraData(established, "XPUT"); err != nil {
		return err
	}
	if limit := established.maxExtraChunkBytes - extraEntryHeaderBytes; len(value.Bytes) > limit {
		return requestErrorf("XPUT", "XPUT value of %d bytes exceeds the server's max_extra_chunk_bytes minus %d (%d)",
			len(value.Bytes), extraEntryHeaderBytes, limit)
	}
	frame, err := c.execPayloadOn(ctx, established, value.Bytes, "XPUT", coord(x), coord(y),
		strconv.Itoa(value.BitLength), strconv.Itoa(len(value.Bytes)))
	if err != nil {
		return err
	}
	return expectOK(frame, "XPUT")
}

// XDel removes the extra data of one block. Removing a value that does not
// exist is not an error; on a table without extra data the server replies
// INVALID_ARGUMENT.
func (c *Client) XDel(ctx context.Context, x, y int64) error {
	release, err := c.acquireSlot(ctx, "XDEL")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "XDEL", coord(x), coord(y))
	if err != nil {
		return err
	}
	return expectOK(frame, "XDEL")
}

// GetChunkStateExtra reads the chunk's payload, presence bitmap and extra
// data in one request. The table must have extra data. An absent chunk
// reports Exists false, zero payload and presence, and no values.
//
// With [GetOptions.ZRLE], decompression is bounded by the state size plus
// [HelloInfo.MaxExtraChunkBytes].
func (c *Client) GetChunkStateExtra(ctx context.Context, cx, cy int64, opts GetOptions) (ChunkStateExtra, error) {
	release, err := c.acquireSlot(ctx, "CHUNKGET")
	if err != nil {
		return ChunkStateExtra{}, err
	}
	defer release()

	established, geo, err := c.chunkConnection(ctx, "CHUNKGET")
	if err != nil {
		return ChunkStateExtra{}, err
	}
	args := []string{coord(cx), coord(cy), "STATE", "EXTRA"}
	if opts.ZRLE {
		args = append(args, "ZRLE")
	}
	frame, err := c.execBoundedOn(ctx, established, chunkReplyBound(geo.stateBytes()+established.maxExtraChunkBytes),
		"CHUNKGET", args...)
	if err != nil {
		return ChunkStateExtra{}, err
	}
	body, err := expectBulk(frame, "CHUNKGET")
	if err != nil {
		return ChunkStateExtra{}, err
	}
	body, err = decodeChunkExtraBytes(body, geo.stateBytes(), established.maxExtraChunkBytes, opts.ZRLE)
	if err != nil {
		return ChunkStateExtra{}, err
	}
	extra, err := decodeExtraSection(body[geo.stateBytes():], geo.blockCount)
	if err != nil {
		return ChunkStateExtra{}, protocolErrorf("CHUNKGET", "invalid CHUNKGET EXTRA section: %s", err)
	}
	payload, presence := splitChunkState(body[:geo.stateBytes():geo.stateBytes()], geo)
	return ChunkStateExtra{Exists: anyBitSet(presence), Payload: payload, Presence: presence, Extra: extra}, nil
}

// decodeChunkExtraBytes checks a CHUNKGET STATE EXTRA reply: the state,
// then an EXTRA section of at most maxExtra bytes. A zrle reply must declare
// a size in that range before it is decompressed.
func decodeChunkExtraBytes(body []byte, stateBytes, maxExtra int, zrle bool) ([]byte, error) {
	maxBytes := stateBytes + maxExtra
	if zrle {
		declared, err := zrleDeclaredSize(body)
		if err != nil {
			return nil, protocolErrorf("CHUNKGET", "invalid CHUNKGET ZRLE payload: %s", err)
		}
		if declared < stateBytes || declared > maxBytes {
			return nil, protocolErrorf("CHUNKGET", "invalid CHUNKGET ZRLE payload: it declares %d bytes, expected %d..%d",
				declared, stateBytes, maxBytes)
		}
		decoded, err := ZRLEDecompress(body, declared)
		if err != nil {
			return nil, protocolErrorf("CHUNKGET", "invalid CHUNKGET ZRLE payload: %s", err)
		}
		return decoded, nil
	}
	if len(body) < stateBytes || len(body) > maxBytes {
		return nil, protocolErrorf("CHUNKGET", "CHUNKGET returned %d bytes, expected %d..%d", len(body), stateBytes, maxBytes)
	}
	return body, nil
}

// PutChunkStateExtra replaces the chunk's payload, presence bitmap and all of
// its extra data in one write. Payload and presence work as for
// [Client.PutChunkState]; every value in state.Extra must belong to a block
// the new presence bitmap marks present and fit the table's extra data
// limits, which the server checks. [PutOptions.IfVersion] and
// [PutOptions.ZRLE] work as for [Client.PutChunk].
func (c *Client) PutChunkStateExtra(ctx context.Context, cx, cy int64, state ChunkStateExtraInput, opts PutOptions) (MutationResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKPUT")
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	established, geo, err := c.chunkConnection(ctx, "CHUNKPUT")
	if err != nil {
		return MutationResult{}, err
	}
	if len(state.Payload) != geo.payloadBytes {
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT STATE EXTRA payload must be %d bytes, got %d", geo.payloadBytes, len(state.Payload))
	}
	if len(state.Presence) != geo.presenceBytes {
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT STATE EXTRA presence must be %d bytes, got %d", geo.presenceBytes, len(state.Presence))
	}
	if err := requireExtraData(established, "CHUNKPUT"); err != nil {
		return MutationResult{}, err
	}
	// A longer section is more than the server reads: it would close the
	// connection. Checked before anything is encoded.
	bytes := make([]byte, 0, geo.stateBytes())
	bytes = append(bytes, state.Payload...)
	bytes = append(bytes, state.Presence...)
	bytes, err = appendExtraSection(bytes, state.Extra, geo.blockCount, established.maxExtraChunkBytes)
	if err != nil {
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT EXTRA: %s", err)
	}
	return c.putChunkBytes(ctx, established, cx, cy, bytes, []string{"STATE", "EXTRA"}, opts)
}

// EncodeExtraSection encodes the extra data of a chunk of blockCount blocks
// as an EXTRA section: for each value in ascending block index, block_index
// u32le, bit_length u32le and the value bytes, with padding bits cleared. It
// fails with [ErrProtocol] for a block index outside 0..blockCount-1 or a
// malformed value. [Client.PutChunkStateExtra] encodes its values this way.
func EncodeExtraSection(values map[int]ExtraValue, blockCount int) ([]byte, error) {
	section, err := appendExtraSection([]byte{}, values, blockCount, -1)
	if err != nil {
		return nil, requestErrorf("", "cannot encode EXTRA section: %s", err)
	}
	return section, nil
}

// appendExtraSection appends the EXTRA section of values to dst. With
// maxSection >= 0, a section longer than that fails before it is encoded.
func appendExtraSection(dst []byte, values map[int]ExtraValue, blockCount int, maxSection int) ([]byte, error) {
	indexes := make([]int, 0, len(values))
	size := 0
	for index, value := range values {
		if index < 0 || index >= blockCount {
			return nil, fmt.Errorf("block index %d is outside the chunk's %d blocks", index, blockCount)
		}
		if err := checkExtraValue(value); err != nil {
			return nil, fmt.Errorf("block index %d: %w", index, err)
		}
		indexes = append(indexes, index)
		size += extraEntryHeaderBytes + len(value.Bytes)
	}
	if maxSection >= 0 && size > maxSection {
		return nil, fmt.Errorf("section of %d bytes exceeds the server's max_extra_chunk_bytes (%d)", size, maxSection)
	}
	dst = slices.Grow(dst, size)
	slices.Sort(indexes)
	for _, index := range indexes {
		value := values[index]
		dst = binary.LittleEndian.AppendUint32(dst, uint32(index))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(value.BitLength))
		dst = append(dst, value.Bytes...)
		dst[len(dst)-1] &= extraPaddingMask(value.BitLength)
	}
	return dst, nil
}

// DecodeExtraSection decodes the EXTRA section of a chunk of blockCount
// blocks into its values by block index. It fails with [ErrProtocol] unless
// the entries lie within the section in strictly ascending block index, each
// below blockCount, with at least 1 bit and zero padding bits. The values'
// Bytes share the memory of section.
func DecodeExtraSection(section []byte, blockCount int) (map[int]ExtraValue, error) {
	values, err := decodeExtraSection(section, blockCount)
	if err != nil {
		return nil, protocolErrorf("", "invalid EXTRA section: %s", err)
	}
	return values, nil
}

func decodeExtraSection(section []byte, blockCount int) (map[int]ExtraValue, error) {
	values := make(map[int]ExtraValue)
	previous := -1
	for at := 0; at < len(section); {
		if len(section)-at < extraEntryHeaderBytes {
			return nil, fmt.Errorf("entry header at byte %d extends past the section", at)
		}
		rawIndex := binary.LittleEndian.Uint32(section[at:])
		bitLength := binary.LittleEndian.Uint32(section[at+4:])
		if int64(rawIndex) >= int64(blockCount) {
			return nil, fmt.Errorf("block index %d is outside the chunk's %d blocks", rawIndex, blockCount)
		}
		index := int(rawIndex)
		if index <= previous {
			return nil, fmt.Errorf("block indexes are not strictly ascending (%d after %d)", index, previous)
		}
		if bitLength == 0 || bitLength > maxExtraBlockBits {
			return nil, fmt.Errorf("the value of block index %d has %d bits, expected 1..%d", index, bitLength, maxExtraBlockBits)
		}
		start := at + extraEntryHeaderBytes
		if extraValueBytes(int(bitLength)) > len(section)-start {
			return nil, fmt.Errorf("the value of block index %d extends past the section", index)
		}
		end := start + extraValueBytes(int(bitLength))
		value := ExtraValue{BitLength: int(bitLength), Bytes: section[start:end:end]}
		if value.Bytes[len(value.Bytes)-1]&^extraPaddingMask(value.BitLength) != 0 {
			return nil, fmt.Errorf("the value of block index %d has set bits past its bit length", index)
		}
		values[index] = value
		previous = index
		at = end
	}
	return values, nil
}

// requireExtraData refuses to send extra data to a server whose HELLO
// reported no max_extra_chunk_bytes: a server without extra data would read
// an XPUT value as request lines.
func requireExtraData(established *conn, command string) error {
	if established.maxExtraChunkBytes <= 0 {
		return requestErrorf(command, "%s needs a server with extra data (its HELLO reported no max_extra_chunk_bytes)", command)
	}
	return nil
}
