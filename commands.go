package chunkdb

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// Auth authenticates the connection. An empty token uses the token configured
// in [Options]; the client sends AUTH automatically after connecting unless
// [Options.DisableAutoAuth] is set.
func (c *Client) Auth(ctx context.Context, token string) error {
	release, err := c.acquireSlot(ctx, "AUTH")
	if err != nil {
		return err
	}
	defer release()

	if token == "" {
		token = c.opts.token()
	}
	if token == "" {
		return &Error{
			Kind:          KindAuth,
			Phase:         PhaseAuth,
			Command:       "AUTH",
			Message:       "server error " + codeAuthFailed + ": token is required",
			ServerCode:    codeAuthFailed,
			ServerMessage: "token is required",
		}
	}

	frame, err := c.exec(ctx, "AUTH", token)
	if err != nil {
		return err
	}
	return expectOK(frame, "AUTH")
}

// authOn authenticates a connection that has not been published yet, so it
// bypasses the pipeline slots held by the request that triggered the dial.
func (c *Client) authOn(ctx context.Context, established *conn, token string) error {
	frame, err := c.execOn(ctx, established, "AUTH", token)
	if err != nil {
		return err
	}
	return expectOK(frame, "AUTH")
}

// Ping checks liveness of the connection.
func (c *Client) Ping(ctx context.Context) error {
	release, err := c.acquireSlot(ctx, "PING")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "PING")
	if err != nil {
		return err
	}
	text, err := expectSimple(frame, "PING")
	if err != nil {
		return err
	}
	if text != "PONG" {
		return protocolErrorf("PING", "unexpected PING response: %s", text)
	}
	return nil
}

// Info returns the server's configuration and runtime counters.
func (c *Client) Info(ctx context.Context) (Info, error) {
	release, err := c.acquireSlot(ctx, "INFO")
	if err != nil {
		return Info{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "INFO")
	if err != nil {
		return Info{}, err
	}
	payload, err := expectBulk(frame, "INFO")
	if err != nil {
		return Info{}, err
	}
	return Info{Raw: string(payload), Values: ParseInfo(payload)}, nil
}

// Get reads one block as bit text. An unset block reads back as zero bits; use
// [Client.ReadBlock] to tell the two apart in one call.
func (c *Client) Get(ctx context.Context, x, y int64) (string, error) {
	release, err := c.acquireSlot(ctx, "GET")
	if err != nil {
		return "", err
	}
	defer release()

	frame, err := c.exec(ctx, "GET", coord(x), coord(y))
	if err != nil {
		return "", err
	}
	payload, err := expectBulk(frame, "GET")
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

// ReadBlock reads one block along with its presence. It is the preferred
// high-level read: an unset block reports Exists false, while an explicitly
// stored all-zero block reports Exists true.
func (c *Client) ReadBlock(ctx context.Context, x, y int64) (BlockState, error) {
	release, err := c.acquireSlot(ctx, "READBLOCK")
	if err != nil {
		return BlockState{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "EXISTS", coord(x), coord(y))
	if err != nil {
		return BlockState{}, err
	}
	present, err := expectBool(frame, "EXISTS")
	if err != nil {
		return BlockState{}, err
	}
	if !present {
		return BlockState{}, nil
	}

	frame, err = c.exec(ctx, "GET", coord(x), coord(y))
	if err != nil {
		return BlockState{}, err
	}
	payload, err := expectBulk(frame, "GET")
	if err != nil {
		return BlockState{}, err
	}
	return BlockState{Exists: true, Bits: string(payload)}, nil
}

// Exists reports whether a block is explicitly present.
func (c *Client) Exists(ctx context.Context, x, y int64) (bool, error) {
	release, err := c.acquireSlot(ctx, "EXISTS")
	if err != nil {
		return false, err
	}
	defer release()

	frame, err := c.exec(ctx, "EXISTS", coord(x), coord(y))
	if err != nil {
		return false, err
	}
	return expectBool(frame, "EXISTS")
}

// Set writes one block. bits must contain only 0 and 1 and match the server's
// configured block_bits.
func (c *Client) Set(ctx context.Context, x, y int64, bits string) error {
	release, err := c.acquireSlot(ctx, "SET")
	if err != nil {
		return err
	}
	defer release()

	if !isBitString(bits) {
		return requestErrorf("SET", "SET bits must contain only 0 and 1")
	}
	frame, err := c.exec(ctx, "SET", coord(x), coord(y), bits)
	if err != nil {
		return err
	}
	return expectOK(frame, "SET")
}

// Unset clears explicit presence for one block. Later reads still return zero
// bits.
func (c *Client) Unset(ctx context.Context, x, y int64) error {
	release, err := c.acquireSlot(ctx, "UNSET")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "UNSET", coord(x), coord(y))
	if err != nil {
		return err
	}
	return expectOK(frame, "UNSET")
}

// MSet writes many blocks in one round-trip.
//
// Items apply in order and are not atomic as a group: if one item fails,
// earlier items stay applied. Use [Client.ChunkBatch] for an atomic
// single-chunk update.
func (c *Client) MSet(ctx context.Context, blocks []Block) error {
	release, err := c.acquireSlot(ctx, "MSET")
	if err != nil {
		return err
	}
	defer release()

	if len(blocks) == 0 {
		return nil
	}
	args := make([]string, 0, len(blocks)*3)
	for _, block := range blocks {
		if !isBitString(block.Bits) {
			return requestErrorf("MSET", "MSET bits must contain only 0 and 1")
		}
		args = append(args, coord(block.X), coord(block.Y), block.Bits)
	}

	frame, err := c.exec(ctx, "MSET", args...)
	if err != nil {
		return err
	}
	return expectOK(frame, "MSET")
}

// MGet reads many blocks in one round-trip, returning one bit string per
// requested block in request order.
func (c *Client) MGet(ctx context.Context, blocks []BlockRef) ([]string, error) {
	release, err := c.acquireSlot(ctx, "MGET")
	if err != nil {
		return nil, err
	}
	defer release()

	if len(blocks) == 0 {
		return []string{}, nil
	}
	args := make([]string, 0, len(blocks)*2)
	for _, block := range blocks {
		args = append(args, coord(block.X), coord(block.Y))
	}

	frame, err := c.exec(ctx, "MGET", args...)
	if err != nil {
		return nil, err
	}
	items, err := expectArray(frame, "MGET")
	if err != nil {
		return nil, err
	}

	values := make([]string, 0, len(items))
	for _, item := range items {
		values = append(values, string(item))
	}
	return values, nil
}

// ChunkExists reports whether any block in the chunk is explicitly present.
func (c *Client) ChunkExists(ctx context.Context, cx, cy int64) (bool, error) {
	release, err := c.acquireSlot(ctx, "CHUNKEXISTS")
	if err != nil {
		return false, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKEXISTS", coord(cx), coord(cy))
	if err != nil {
		return false, err
	}
	return expectBool(frame, "CHUNKEXISTS")
}

// ReadChunk reads the exact chunk state. It is the preferred high-level chunk
// read.
func (c *Client) ReadChunk(ctx context.Context, cx, cy int64) (ChunkState, error) {
	release, err := c.acquireSlot(ctx, "CHUNK")
	if err != nil {
		return ChunkState{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNK", coord(cx), coord(cy), "STATE")
	if err != nil {
		return ChunkState{}, err
	}
	payload, err := expectBulk(frame, "CHUNK")
	if err != nil {
		return ChunkState{}, err
	}
	bits, presence, err := parseChunkStateText(string(payload), "CHUNK")
	if err != nil {
		return ChunkState{}, err
	}
	return ChunkState{
		Exists:   strings.ContainsRune(presence, '1'),
		Bits:     bits,
		Presence: presence,
	}, nil
}

// Chunk reads the full chunk payload as bit text. Absent chunks and unset
// blocks read back as zero bits.
func (c *Client) Chunk(ctx context.Context, cx, cy int64) (string, error) {
	release, err := c.acquireSlot(ctx, "CHUNK")
	if err != nil {
		return "", err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNK", coord(cx), coord(cy))
	if err != nil {
		return "", err
	}
	payload, err := expectBulk(frame, "CHUNK")
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

// SetChunk replaces the full chunk payload and marks the whole chunk present,
// including an all-zero payload.
func (c *Client) SetChunk(ctx context.Context, cx, cy int64, bits string) error {
	release, err := c.acquireSlot(ctx, "CHUNKSET")
	if err != nil {
		return err
	}
	defer release()

	if !isBitString(bits) {
		return requestErrorf("CHUNKSET", "CHUNKSET bits must contain only 0 and 1")
	}
	frame, err := c.exec(ctx, "CHUNKSET", coord(cx), coord(cy), bits)
	if err != nil {
		return err
	}
	return expectOK(frame, "CHUNKSET")
}

// SetChunkState replaces the full chunk payload and its per-block presence
// bitmap in one request.
func (c *Client) SetChunkState(ctx context.Context, cx, cy int64, state ChunkStateInput) error {
	release, err := c.acquireSlot(ctx, "CHUNKSET")
	if err != nil {
		return err
	}
	defer release()

	if !isBitString(state.Bits) {
		return requestErrorf("CHUNKSET", "CHUNKSET STATE payload bits must contain only 0 and 1")
	}
	if !isBitString(state.Presence) {
		return requestErrorf("CHUNKSET", "CHUNKSET STATE presence bits must contain only 0 and 1")
	}

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return err
	}
	if len(state.Bits) != geo.chunkPayloadBits {
		return requestErrorf("CHUNKSET", "CHUNKSET STATE payload bits must be %d bits", geo.chunkPayloadBits)
	}
	if len(state.Presence) != geo.chunkBlockCount {
		return requestErrorf("CHUNKSET", "CHUNKSET STATE presence bits must be %d bits", geo.chunkBlockCount)
	}

	frame, err := c.exec(ctx, "CHUNKSET", coord(cx), coord(cy), "STATE", state.Bits+"|"+state.Presence)
	if err != nil {
		return err
	}
	return expectOK(frame, "CHUNKSET")
}

// SetChunkBin replaces the full chunk payload from raw packed bytes, the
// layout [Client.ChunkBin] returns, and marks every block present. The server
// must support CHUNKSETBIN (chunkdb 1.3+).
func (c *Client) SetChunkBin(ctx context.Context, cx, cy int64, payload []byte) error {
	release, err := c.acquireSlot(ctx, "CHUNKSETBIN")
	if err != nil {
		return err
	}
	defer release()

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return err
	}
	if len(payload) != geo.chunkPayloadBytes {
		return requestErrorf("CHUNKSETBIN", "CHUNKSETBIN payload must be %d bytes", geo.chunkPayloadBytes)
	}
	frame, err := c.execPayload(ctx, payload, "CHUNKSETBIN", coord(cx), coord(cy), strconv.Itoa(len(payload)))
	if err != nil {
		return err
	}
	return expectOK(frame, "CHUNKSETBIN")
}

// SetChunkBinState replaces the full chunk payload and presence bitmap from
// raw bytes laid out as [payload bytes][presence bytes], the layout
// [Client.ChunkBinState] returns. The server must support CHUNKSETBIN
// (chunkdb 1.3+).
func (c *Client) SetChunkBinState(ctx context.Context, cx, cy int64, state []byte) error {
	release, err := c.acquireSlot(ctx, "CHUNKSETBIN")
	if err != nil {
		return err
	}
	defer release()

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return err
	}
	expected := geo.chunkPayloadBytes + geo.presenceBytes
	if len(state) != expected {
		return requestErrorf("CHUNKSETBIN", "CHUNKSETBIN STATE payload must be %d bytes", expected)
	}
	frame, err := c.execPayload(ctx, state, "CHUNKSETBIN", coord(cx), coord(cy), "STATE", strconv.Itoa(len(state)))
	if err != nil {
		return err
	}
	return expectOK(frame, "CHUNKSETBIN")
}

// ChunkBin reads the full chunk payload as raw packed bytes.
func (c *Client) ChunkBin(ctx context.Context, cx, cy int64) ([]byte, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBIN")
	if err != nil {
		return nil, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKBIN", coord(cx), coord(cy))
	if err != nil {
		return nil, err
	}
	return expectBulk(frame, "CHUNKBIN")
}

// ChunkBinState reads the exact chunk state as raw bytes, laid out as
// [payload bytes][presence bytes].
func (c *Client) ChunkBinState(ctx context.Context, cx, cy int64) ([]byte, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBIN")
	if err != nil {
		return nil, err
	}
	defer release()

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return nil, err
	}
	frame, err := c.exec(ctx, "CHUNKBIN", coord(cx), coord(cy), "STATE")
	if err != nil {
		return nil, err
	}
	payload, err := expectBulk(frame, "CHUNKBIN")
	if err != nil {
		return nil, err
	}
	if len(payload) != geo.chunkPayloadBytes+geo.presenceBytes {
		return nil, protocolErrorf("CHUNKBIN", "unexpected CHUNKBIN STATE payload length")
	}
	return payload, nil
}

// ChunkBinCompressed reads the same payload as [Client.ChunkBin], transferred
// compressed and decompressed client-side.
func (c *Client) ChunkBinCompressed(ctx context.Context, cx, cy int64) ([]byte, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBINC")
	if err != nil {
		return nil, err
	}
	defer release()

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return nil, err
	}
	return c.chunkBinCompressed(ctx, geo.chunkPayloadBytes, coord(cx), coord(cy))
}

// ChunkBinStateCompressed reads the same payload as [Client.ChunkBinState],
// transferred compressed and decompressed client-side.
func (c *Client) ChunkBinStateCompressed(ctx context.Context, cx, cy int64) ([]byte, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBINC")
	if err != nil {
		return nil, err
	}
	defer release()

	geo, err := c.chunkGeometry(ctx)
	if err != nil {
		return nil, err
	}
	return c.chunkBinCompressed(ctx, geo.chunkPayloadBytes+geo.presenceBytes, coord(cx), coord(cy), "STATE")
}

func (c *Client) chunkBinCompressed(ctx context.Context, expectedSize int, args ...string) ([]byte, error) {
	frame, err := c.exec(ctx, "CHUNKBINC", args...)
	if err != nil {
		return nil, err
	}
	payload, err := expectBulk(frame, "CHUNKBINC")
	if err != nil {
		return nil, err
	}
	decompressed, err := ZRLEDecompress(payload, expectedSize)
	if err != nil {
		return nil, protocolErrorf("CHUNKBINC", "invalid CHUNKBINC payload: %s", err)
	}
	return decompressed, nil
}

// ChunkScan enumerates populated chunks in ascending (cx, cy) order. limit must
// be between 1 and 1024. Pass nil to start, then feed
// [ScanResult.NextCursor] back until it is nil.
func (c *Client) ChunkScan(ctx context.Context, limit int, cursor *CoordPair) (ScanResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKSCAN")
	if err != nil {
		return ScanResult{}, err
	}
	defer release()

	args := []string{strconv.Itoa(limit)}
	if cursor != nil {
		args = append(args, coord(cursor.CX), coord(cursor.CY))
	}

	frame, err := c.exec(ctx, "CHUNKSCAN", args...)
	if err != nil {
		return ScanResult{}, err
	}
	items, err := expectArray(frame, "CHUNKSCAN")
	if err != nil {
		return ScanResult{}, err
	}
	if len(items) == 0 {
		return ScanResult{}, protocolErrorf("CHUNKSCAN", "empty CHUNKSCAN response")
	}

	result := ScanResult{Coords: make([]CoordPair, 0, len(items)-1)}

	header := string(items[0])
	if rest, found := strings.CutPrefix(header, "CURSOR "); found {
		cx, cy, err := parseCoordPair(rest, "CHUNKSCAN")
		if err != nil {
			return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN header: %s", header)
		}
		result.NextCursor = &CoordPair{CX: cx, CY: cy}
	} else if header != "END" {
		return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN header: %s", header)
	}

	for _, item := range items[1:] {
		cx, cy, err := parseCoordPair(string(item), "CHUNKSCAN")
		if err != nil {
			return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN entry: %s", item)
		}
		result.Coords = append(result.Coords, CoordPair{CX: cx, CY: cy})
	}
	return result, nil
}

// ChunkRange reads a bounded rectangle of chunks, at most 256 chunks and 64 MiB
// per request. Only populated chunks are returned.
func (c *Client) ChunkRange(ctx context.Context, cx0, cy0, cx1, cy1 int64) ([]RangeEntry, error) {
	release, err := c.acquireSlot(ctx, "CHUNKRANGE")
	if err != nil {
		return nil, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKRANGE", coord(cx0), coord(cy0), coord(cx1), coord(cy1))
	if err != nil {
		return nil, err
	}
	return parseRangeEntries(frame, "CHUNKRANGE")
}

// ChunkRadius reads the populated chunks within radiusChunks of (cx, cy), under
// the same limits as [Client.ChunkRange].
func (c *Client) ChunkRadius(ctx context.Context, cx, cy int64, radiusChunks int) ([]RangeEntry, error) {
	release, err := c.acquireSlot(ctx, "CHUNKRADIUS")
	if err != nil {
		return nil, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKRADIUS", coord(cx), coord(cy), strconv.Itoa(radiusChunks))
	if err != nil {
		return nil, err
	}
	return parseRangeEntries(frame, "CHUNKRADIUS")
}

// ChunkVersion returns the chunk's opaque version token.
func (c *Client) ChunkVersion(ctx context.Context, cx, cy int64) (uint64, error) {
	release, err := c.acquireSlot(ctx, "CHUNKVER")
	if err != nil {
		return 0, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKVER", coord(cx), coord(cy))
	if err != nil {
		return 0, err
	}
	payload, err := expectBulk(frame, "CHUNKVER")
	if err != nil {
		return 0, err
	}
	return parseVersion(string(payload), "CHUNKVER")
}

// ChunkCompareAndSet replaces the full chunk state only if the chunk still has
// expectedVersion.
//
// On a mismatch the returned result has OK false and carries the current
// version; the stored state is unchanged. Re-read, reconcile, and retry.
func (c *Client) ChunkCompareAndSet(ctx context.Context, cx, cy int64, expectedVersion uint64, state ChunkStateInput) (MutationResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKCAS")
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "CHUNKCAS",
		coord(cx), coord(cy),
		strconv.FormatUint(expectedVersion, 10),
		"STATE", state.Bits+"|"+state.Presence,
	)
	if err != nil {
		return mutationFromError(err, "CHUNKCAS")
	}
	return mutationFromFrame(frame, "CHUNKCAS")
}

// ChunkBatch applies an atomic batch of block operations to one chunk,
// unconditionally. Every coordinate must lie inside chunk (cx, cy).
func (c *Client) ChunkBatch(ctx context.Context, cx, cy int64, operations []BatchOperation) (MutationResult, error) {
	return c.chunkBatch(ctx, cx, cy, "-", operations)
}

// ChunkBatchIfVersion is [Client.ChunkBatch] conditioned on the chunk still
// having expectedVersion. On a mismatch the returned result has OK false and
// carries the current version; the chunk is unchanged.
func (c *Client) ChunkBatchIfVersion(ctx context.Context, cx, cy int64, expectedVersion uint64, operations []BatchOperation) (MutationResult, error) {
	return c.chunkBatch(ctx, cx, cy, strconv.FormatUint(expectedVersion, 10), operations)
}

func (c *Client) chunkBatch(ctx context.Context, cx, cy int64, version string, operations []BatchOperation) (MutationResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBATCH")
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	if len(operations) == 0 {
		return MutationResult{}, requestErrorf("CHUNKBATCH", "chunk batch requires at least one operation")
	}

	args := make([]string, 0, 3+len(operations)*4)
	args = append(args, coord(cx), coord(cy), version)
	for _, operation := range operations {
		switch operation.Type {
		case BatchSet:
			if !isBitString(operation.Bits) {
				return MutationResult{}, requestErrorf("CHUNKBATCH", "chunk batch set bits must contain only 0 and 1")
			}
			args = append(args, "SET", coord(operation.X), coord(operation.Y), operation.Bits)
		case BatchUnset:
			args = append(args, "UNSET", coord(operation.X), coord(operation.Y))
		default:
			return MutationResult{}, requestErrorf("CHUNKBATCH", "unknown chunk batch operation type: %d", operation.Type)
		}
	}

	frame, err := c.exec(ctx, "CHUNKBATCH", args...)
	if err != nil {
		return mutationFromError(err, "CHUNKBATCH")
	}
	return mutationFromFrame(frame, "CHUNKBATCH")
}

// WALFlush is an explicit durability barrier: it returns once every previously
// acknowledged write is durable, even when the server runs in relaxed mode.
func (c *Client) WALFlush(ctx context.Context) error {
	release, err := c.acquireSlot(ctx, "WALFLUSH")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "WALFLUSH")
	if err != nil {
		return err
	}
	return expectOK(frame, "WALFLUSH")
}

// Metrics returns runtime metrics in the Prometheus text exposition format.
func (c *Client) Metrics(ctx context.Context) (string, error) {
	release, err := c.acquireSlot(ctx, "METRICS")
	if err != nil {
		return "", err
	}
	defer release()

	frame, err := c.exec(ctx, "METRICS")
	if err != nil {
		return "", err
	}
	payload, err := expectBulk(frame, "METRICS")
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func coord(value int64) string { return strconv.FormatInt(value, 10) }

func isBitString(bits string) bool {
	if bits == "" {
		return false
	}
	return strings.IndexFunc(bits, func(r rune) bool { return r != '0' && r != '1' }) == -1
}

func expectSimple(frame Frame, command string) (string, error) {
	if frame.Kind != FrameSimple {
		return "", protocolErrorf(command, "expected simple response for %s", command)
	}
	return frame.Simple, nil
}

func expectBulk(frame Frame, command string) ([]byte, error) {
	if frame.Kind != FrameBulk {
		return nil, protocolErrorf(command, "expected bulk response for %s", command)
	}
	return frame.Bulk, nil
}

func expectArray(frame Frame, command string) ([][]byte, error) {
	if frame.Kind != FrameArray {
		return nil, protocolErrorf(command, "expected array response for %s", command)
	}
	return frame.Array, nil
}

func expectOK(frame Frame, command string) error {
	text, err := expectSimple(frame, command)
	if err != nil {
		return err
	}
	if text != "OK" {
		return protocolErrorf(command, "unexpected %s response: %s", command, text)
	}
	return nil
}

func expectBool(frame Frame, command string) (bool, error) {
	text, err := expectSimple(frame, command)
	if err != nil {
		return false, err
	}
	switch text {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, protocolErrorf(command, "unexpected %s response: %s", command, text)
	}
}

// parseChunkStateText splits a "<payload_bits>|<presence_bits>" payload.
func parseChunkStateText(text, command string) (bits, presence string, err error) {
	separator := strings.IndexByte(text, '|')
	if separator <= 0 || separator != strings.LastIndexByte(text, '|') || separator == len(text)-1 {
		return "", "", protocolErrorf(command, "unexpected %s STATE payload", command)
	}
	bits, presence = text[:separator], text[separator+1:]
	if !isBitString(bits) || !isBitString(presence) {
		return "", "", protocolErrorf(command, "unexpected %s STATE payload", command)
	}
	return bits, presence, nil
}

func parseRangeEntries(frame Frame, command string) ([]RangeEntry, error) {
	items, err := expectArray(frame, command)
	if err != nil {
		return nil, err
	}

	entries := make([]RangeEntry, 0, len(items))
	for _, item := range items {
		text := string(item)
		firstSpace := strings.IndexByte(text, ' ')
		if firstSpace <= 0 {
			return nil, protocolErrorf(command, "unexpected %s entry: %s", command, text)
		}
		secondSpace := strings.IndexByte(text[firstSpace+1:], ' ')
		if secondSpace < 0 {
			return nil, protocolErrorf(command, "unexpected %s entry: %s", command, text)
		}
		secondSpace += firstSpace + 1

		cx, err := parseCoordToken(text[:firstSpace], command)
		if err != nil {
			return nil, err
		}
		cy, err := parseCoordToken(text[firstSpace+1:secondSpace], command)
		if err != nil {
			return nil, err
		}
		bits, presence, err := parseChunkStateText(text[secondSpace+1:], command)
		if err != nil {
			return nil, err
		}
		entries = append(entries, RangeEntry{CX: cx, CY: cy, Bits: bits, Presence: presence})
	}
	return entries, nil
}

func parseCoordPair(text, command string) (cx, cy int64, err error) {
	first, second, found := strings.Cut(text, " ")
	if !found || strings.Contains(second, " ") {
		return 0, 0, protocolErrorf(command, "unexpected %s entry: %s", command, text)
	}
	if cx, err = parseCoordToken(first, command); err != nil {
		return 0, 0, err
	}
	if cy, err = parseCoordToken(second, command); err != nil {
		return 0, 0, err
	}
	return cx, cy, nil
}

func parseCoordToken(token, command string) (int64, error) {
	value, err := strconv.ParseInt(token, 10, 64)
	if err != nil || strconv.FormatInt(value, 10) != token {
		return 0, protocolErrorf(command, "invalid coordinate in %s response: %s", command, token)
	}
	return value, nil
}

func parseVersion(text, command string) (uint64, error) {
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != text {
		return 0, protocolErrorf(command, "invalid version in %s response: %s", command, text)
	}
	return value, nil
}

func mutationFromFrame(frame Frame, command string) (MutationResult, error) {
	payload, err := expectBulk(frame, command)
	if err != nil {
		return MutationResult{}, err
	}
	version, err := parseVersion(string(payload), command)
	if err != nil {
		return MutationResult{}, err
	}
	return MutationResult{OK: true, Version: version}, nil
}

// mutationFromError turns a VERSION_MISMATCH response into a non-OK result and
// passes every other error through unchanged.
func mutationFromError(err error, command string) (MutationResult, error) {
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != codeVersionMismatch {
		return MutationResult{}, err
	}

	_, current, found := strings.Cut(typed.ServerMessage, "current=")
	if !found {
		return MutationResult{}, protocolErrorf(command, "unexpected VERSION_MISMATCH payload for %s", command)
	}
	digits := current
	if end := strings.IndexFunc(current, func(r rune) bool { return r < '0' || r > '9' }); end >= 0 {
		digits = current[:end]
	}
	version, parseErr := parseVersion(digits, command)
	if parseErr != nil {
		return MutationResult{}, protocolErrorf(command, "unexpected VERSION_MISMATCH payload for %s", command)
	}
	return MutationResult{OK: false, Version: version}, nil
}

func positiveInfoInt(values map[string]string, field string) (int, error) {
	parsed, err := strconv.Atoi(values[field])
	if err != nil || parsed <= 0 {
		return 0, protocolErrorf("INFO", "INFO missing valid %s", field)
	}
	return parsed, nil
}
