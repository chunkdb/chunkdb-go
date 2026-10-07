package chunkdb

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

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

// Info returns runtime statistics of the selected table. Geometry and options
// come from [Client.ServerInfo], [Client.Use] and [Client.TableInfo].
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

// Get reads one block. An unset block reports Exists false; an explicitly
// stored all-zero block reports Exists true.
func (c *Client) Get(ctx context.Context, x, y int64) (BlockState, error) {
	release, err := c.acquireSlot(ctx, "GET")
	if err != nil {
		return BlockState{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "GET", coord(x), coord(y))
	if err != nil {
		return BlockState{}, err
	}
	return blockState(frame, "GET")
}

// Set writes one block. bits must contain only 0 and 1 and match the table's
// block_bits.
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

// Unset clears explicit presence for one block; a later [Client.Get] reports
// it unset.
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

// MGet reads many blocks in one round-trip, returning one [BlockState] per
// requested block in request order.
func (c *Client) MGet(ctx context.Context, blocks []BlockRef) ([]BlockState, error) {
	release, err := c.acquireSlot(ctx, "MGET")
	if err != nil {
		return nil, err
	}
	defer release()

	if len(blocks) == 0 {
		return []BlockState{}, nil
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
	if len(items) != len(blocks) {
		return nil, protocolErrorf("MGET", "MGET returned %d items for %d blocks", len(items), len(blocks))
	}

	states := make([]BlockState, 0, len(items))
	for _, item := range items {
		state, err := blockState(item, "MGET")
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
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

// GetChunk reads the chunk's packed block payload (see [ChunkState] for the
// layout). An absent chunk reads as zeros; use [Client.GetChunkState] or
// [Client.ChunkExists] to tell it from an all-zero chunk.
func (c *Client) GetChunk(ctx context.Context, cx, cy int64, opts GetOptions) ([]byte, error) {
	release, err := c.acquireSlot(ctx, "CHUNKGET")
	if err != nil {
		return nil, err
	}
	defer release()

	established, geo, err := c.chunkConnection(ctx, "CHUNKGET")
	if err != nil {
		return nil, err
	}
	args := []string{coord(cx), coord(cy)}
	if opts.ZRLE {
		args = append(args, "ZRLE")
	}
	frame, err := c.execOn(ctx, established, "CHUNKGET", args...)
	if err != nil {
		return nil, err
	}
	body, err := expectBulk(frame, "CHUNKGET")
	if err != nil {
		return nil, err
	}
	return decodeChunkBytes(body, geo.payloadBytes, opts.ZRLE, "CHUNKGET")
}

// GetChunkState reads the chunk's payload and presence bitmap. An absent chunk
// reports Exists false with zero payload and presence.
func (c *Client) GetChunkState(ctx context.Context, cx, cy int64, opts GetOptions) (ChunkState, error) {
	release, err := c.acquireSlot(ctx, "CHUNKGET")
	if err != nil {
		return ChunkState{}, err
	}
	defer release()

	established, geo, err := c.chunkConnection(ctx, "CHUNKGET")
	if err != nil {
		return ChunkState{}, err
	}
	args := []string{coord(cx), coord(cy), "STATE"}
	if opts.ZRLE {
		args = append(args, "ZRLE")
	}
	frame, err := c.execOn(ctx, established, "CHUNKGET", args...)
	if err != nil {
		return ChunkState{}, err
	}
	body, err := expectBulk(frame, "CHUNKGET")
	if err != nil {
		return ChunkState{}, err
	}
	state, err := decodeChunkBytes(body, geo.stateBytes(), opts.ZRLE, "CHUNKGET")
	if err != nil {
		return ChunkState{}, err
	}
	payload, presence := splitChunkState(state, geo)
	return ChunkState{Exists: anyBitSet(presence), Payload: payload, Presence: presence}, nil
}

// PutChunk replaces the chunk's payload; every block becomes explicitly
// present, including in an all-zero payload. payload must have exactly the
// table's chunk payload size.
//
// With [PutOptions.IfVersion], a version mismatch is not an error: the result
// has OK false and the chunk's current version, and the chunk is unchanged.
func (c *Client) PutChunk(ctx context.Context, cx, cy int64, payload []byte, opts PutOptions) (MutationResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKPUT")
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	established, geo, err := c.chunkConnection(ctx, "CHUNKPUT")
	if err != nil {
		return MutationResult{}, err
	}
	if len(payload) != geo.payloadBytes {
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT payload must be %d bytes, got %d", geo.payloadBytes, len(payload))
	}
	return c.putChunkBytes(ctx, established, cx, cy, payload, false, opts)
}

// PutChunkState replaces the chunk's payload and presence bitmap; payload bits
// of absent blocks are stored as zero, and an all-zero presence bitmap leaves
// the chunk absent. Both parts must have exactly the table's sizes.
// [PutOptions.IfVersion] works as for [Client.PutChunk].
func (c *Client) PutChunkState(ctx context.Context, cx, cy int64, state ChunkStateInput, opts PutOptions) (MutationResult, error) {
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
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT STATE payload must be %d bytes, got %d", geo.payloadBytes, len(state.Payload))
	}
	if len(state.Presence) != geo.presenceBytes {
		return MutationResult{}, requestErrorf("CHUNKPUT", "CHUNKPUT STATE presence must be %d bytes, got %d", geo.presenceBytes, len(state.Presence))
	}
	bytes := make([]byte, 0, geo.stateBytes())
	bytes = append(bytes, state.Payload...)
	bytes = append(bytes, state.Presence...)
	return c.putChunkBytes(ctx, established, cx, cy, bytes, true, opts)
}

// putChunkBytes sends CHUNKPUT. The server refuses a header it cannot parse
// without reading the bytes and closes the connection, so every argument is
// checked before anything is sent.
func (c *Client) putChunkBytes(ctx context.Context, established *conn, cx, cy int64, bytes []byte, state bool, opts PutOptions) (MutationResult, error) {
	args := []string{coord(cx), coord(cy)}
	if state {
		args = append(args, "STATE")
	}
	body := bytes
	if opts.ZRLE {
		if compressed := ZRLECompress(bytes); len(compressed) < len(bytes) {
			body = compressed
			args = append(args, "ZRLE")
		}
	}
	if opts.IfVersion != nil {
		args = append(args, "IF", strconv.FormatUint(*opts.IfVersion, 10))
	}
	args = append(args, strconv.Itoa(len(body)))

	frame, err := c.execPayloadOn(ctx, established, body, "CHUNKPUT", args...)
	if err != nil {
		return mutationFromError(err, "CHUNKPUT")
	}
	return mutationFromFrame(frame, "CHUNKPUT")
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

	texts := make([]string, 0, len(items))
	for _, item := range items {
		text, err := bulkText(item, "CHUNKSCAN")
		if err != nil {
			return ScanResult{}, err
		}
		texts = append(texts, text)
	}

	header := texts[0]
	if rest, found := strings.CutPrefix(header, "CURSOR "); found {
		cx, cy, err := parseCoordPair(rest, "CHUNKSCAN")
		if err != nil {
			return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN header: %s", header)
		}
		result.NextCursor = &CoordPair{CX: cx, CY: cy}
	} else if header != "END" {
		return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN header: %s", header)
	}

	for _, item := range texts[1:] {
		cx, cy, err := parseCoordPair(item, "CHUNKSCAN")
		if err != nil {
			return ScanResult{}, protocolErrorf("CHUNKSCAN", "unexpected CHUNKSCAN entry: %s", item)
		}
		result.Coords = append(result.Coords, CoordPair{CX: cx, CY: cy})
	}
	return result, nil
}

// ChunkRange reads the populated chunks in a rectangle, with their payload and
// presence, ordered by ascending cx then cy. A request may cover at most
// [HelloInfo.MaxAreaChunks] chunks (256) and its response is capped at
// [HelloInfo.MaxResponseBytes] (64 MiB).
func (c *Client) ChunkRange(ctx context.Context, cx0, cy0, cx1, cy1 int64, opts GetOptions) ([]RangeEntry, error) {
	release, err := c.acquireSlot(ctx, "CHUNKRANGE")
	if err != nil {
		return nil, err
	}
	defer release()

	return c.readArea(ctx, "CHUNKRANGE", opts, coord(cx0), coord(cy0), coord(cx1), coord(cy1))
}

// ChunkRadius reads the populated chunks whose coordinate lies within
// radiusChunks of (cx, cy), under the same limits and with the same result as
// [Client.ChunkRange].
func (c *Client) ChunkRadius(ctx context.Context, cx, cy int64, radiusChunks int, opts GetOptions) ([]RangeEntry, error) {
	release, err := c.acquireSlot(ctx, "CHUNKRADIUS")
	if err != nil {
		return nil, err
	}
	defer release()

	return c.readArea(ctx, "CHUNKRADIUS", opts, coord(cx), coord(cy), strconv.Itoa(radiusChunks))
}

// readArea runs CHUNKRANGE or CHUNKRADIUS, always with STATE. The reply holds
// two items per chunk: "<cx> <cy>", then the chunk bytes.
func (c *Client) readArea(ctx context.Context, command string, opts GetOptions, args ...string) ([]RangeEntry, error) {
	established, geo, err := c.chunkConnection(ctx, command)
	if err != nil {
		return nil, err
	}
	args = append(args, "STATE")
	if opts.ZRLE {
		args = append(args, "ZRLE")
	}
	frame, err := c.execOn(ctx, established, command, args...)
	if err != nil {
		return nil, err
	}
	items, err := expectArray(frame, command)
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, protocolErrorf(command, "%s returned an odd number of items", command)
	}

	entries := make([]RangeEntry, 0, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		text, err := bulkText(items[i], command)
		if err != nil {
			return nil, err
		}
		cx, cy, err := parseCoordPair(text, command)
		if err != nil {
			return nil, err
		}
		if items[i+1].Kind != FrameBulk {
			return nil, protocolErrorf(command, "%s returned a null chunk", command)
		}
		state, err := decodeChunkBytes(items[i+1].Bulk, geo.stateBytes(), opts.ZRLE, command)
		if err != nil {
			return nil, err
		}
		payload, presence := splitChunkState(state, geo)
		entries = append(entries, RangeEntry{CX: cx, CY: cy, Payload: payload, Presence: presence})
	}
	return entries, nil
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

// ChunkBatch applies an atomic batch of block operations to one chunk,
// unconditionally. Every coordinate must lie inside chunk (cx, cy).
func (c *Client) ChunkBatch(ctx context.Context, cx, cy int64, operations []BatchOperation) (MutationResult, error) {
	return c.chunkBatch(ctx, cx, cy, nil, operations)
}

// ChunkBatchIfVersion is [Client.ChunkBatch] conditioned on the chunk still
// having expectedVersion. On a mismatch the returned result has OK false and
// carries the current version; the chunk is unchanged.
func (c *Client) ChunkBatchIfVersion(ctx context.Context, cx, cy int64, expectedVersion uint64, operations []BatchOperation) (MutationResult, error) {
	return c.chunkBatch(ctx, cx, cy, &expectedVersion, operations)
}

func (c *Client) chunkBatch(ctx context.Context, cx, cy int64, ifVersion *uint64, operations []BatchOperation) (MutationResult, error) {
	release, err := c.acquireSlot(ctx, "CHUNKBATCH")
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	if len(operations) == 0 {
		return MutationResult{}, requestErrorf("CHUNKBATCH", "chunk batch requires at least one operation")
	}

	args := make([]string, 0, 4+len(operations)*4)
	args = append(args, coord(cx), coord(cy))
	if ifVersion != nil {
		args = append(args, "IF", strconv.FormatUint(*ifVersion, 10))
	}
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

func expectArray(frame Frame, command string) ([]Frame, error) {
	if frame.Kind != FrameArray {
		return nil, protocolErrorf(command, "expected array response for %s", command)
	}
	return frame.Array, nil
}

// bulkText reads a bulk array item as text; a null item is a protocol error.
func bulkText(item Frame, command string) (string, error) {
	if item.Kind != FrameBulk {
		return "", protocolErrorf(command, "unexpected null item in %s response", command)
	}
	return string(item.Bulk), nil
}

// blockState reads a GET reply or MGET item: bit text, or null when unset.
func blockState(frame Frame, command string) (BlockState, error) {
	switch frame.Kind {
	case FrameNull:
		return BlockState{}, nil
	case FrameBulk:
		return BlockState{Exists: true, Bits: string(frame.Bulk)}, nil
	default:
		return BlockState{}, protocolErrorf(command, "expected bulk or null response for %s", command)
	}
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

// mutationFromError turns a "VERSION_MISMATCH current=<version>" response into
// a non-OK result and passes every other error through unchanged.
func mutationFromError(err error, command string) (MutationResult, error) {
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != codeVersionMismatch {
		return MutationResult{}, err
	}

	current, found := strings.CutPrefix(typed.ServerMessage, "current=")
	if !found {
		return MutationResult{}, protocolErrorf(command, "unexpected VERSION_MISMATCH payload for %s: %s", command, typed.ServerMessage)
	}
	version, parseErr := parseVersion(current, command)
	if parseErr != nil {
		return MutationResult{}, protocolErrorf(command, "unexpected VERSION_MISMATCH payload for %s: %s", command, typed.ServerMessage)
	}
	return MutationResult{OK: false, Version: version}, nil
}

// decodeChunkBytes checks chunk bytes from the server against the table's
// sizes, decompressing zrle with the expected size as its bound.
func decodeChunkBytes(body []byte, expected int, zrle bool, command string) ([]byte, error) {
	if zrle {
		decoded, err := ZRLEDecompress(body, expected)
		if err != nil {
			return nil, protocolErrorf(command, "invalid %s ZRLE payload: %s", command, err)
		}
		return decoded, nil
	}
	if len(body) != expected {
		return nil, protocolErrorf(command, "%s returned %d bytes, expected %d", command, len(body), expected)
	}
	return body, nil
}

// splitChunkState splits state bytes into payload and presence. Both share the
// backing array of state, which the caller owns.
func splitChunkState(state []byte, geo geometry) (payload, presence []byte) {
	return state[:geo.payloadBytes:geo.payloadBytes], state[geo.payloadBytes:]
}

func anyBitSet(bits []byte) bool {
	for _, b := range bits {
		if b != 0 {
			return true
		}
	}
	return false
}
