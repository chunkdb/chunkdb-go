package chunkdb

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"iter"
	"strconv"
	"strings"
)

const (
	// maxTagBytes and maxHistoryLimit are the longest tag and the largest
	// history LIMIT of protocol 2, for a server whose HELLO does not report
	// max_tag_bytes and max_history_limit.
	maxTagBytes     = 255
	maxHistoryLimit = 1024
	// historyEventFields is the number of fields of a history event item.
	historyEventFields = 9
)

func writeOptionsOf(opts []WriteOption) writeOptions {
	var options writeOptions
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	return options
}

// tagArgs returns the "TAG <hex>" arguments of a write tagged with tag, none
// when it is empty.
func tagArgs(established *conn, command string, tag []byte) ([]string, error) {
	if len(tag) == 0 {
		return nil, nil
	}
	if err := requireHistory(established, command); err != nil {
		return nil, err
	}
	if len(tag) > established.maxTagBytes {
		return nil, requestErrorf(command, "%s tag of %d bytes exceeds the server's max_tag_bytes (%d)",
			command, len(tag), established.maxTagBytes)
	}
	return []string{"TAG", hex.EncodeToString(tag)}, nil
}

// requireHistory refuses to send a tag to a server whose HELLO reported no
// max_tag_bytes: a server without history refuses a tagged CHUNKPUT or XPUT
// without reading its bytes and closes the connection.
func requireHistory(established *conn, command string) error {
	if established.maxTagBytes <= 0 {
		return requestErrorf(command, "%s with a tag needs a server with history (its HELLO reported no max_tag_bytes)", command)
	}
	return nil
}

// args returns the "AT <revision>" or "AT TIME <ms>" arguments of a read at
// p, none for the present.
func (p HistoryPoint) args(command string) ([]string, error) {
	switch {
	case !p.set:
		return nil, nil
	case !p.byTime:
		return []string{"AT", strconv.FormatUint(p.revision, 10)}, nil
	case p.timeMs < 0:
		return nil, requestErrorf(command, "%s AT TIME must not be negative, got %d", command, p.timeMs)
	default:
		return []string{"AT", "TIME", strconv.FormatInt(p.timeMs, 10)}, nil
	}
}

// History reads one page of the history of block (x, y) on a table with
// history: its committed changes in the window opts describes, newest first
// unless [HistoryOptions.Ascending]. Continue with [HistoryPage.Cursor], or
// iterate with [Client.HistoryEvents]. A window that reaches below what the
// table's history keeps fails with [CodeNotRetained]: oldest first at once,
// newest first after the page that returns what is kept.
func (c *Client) History(ctx context.Context, x, y int64, opts HistoryOptions) (HistoryPage, error) {
	return c.history(ctx, "HISTORY", opts, coord(x), coord(y))
}

// ChunkHistory reads one page of the history of every block of chunk
// (cx, cy), as [Client.History] does for one block.
func (c *Client) ChunkHistory(ctx context.Context, cx, cy int64, opts HistoryOptions) (HistoryPage, error) {
	return c.history(ctx, "CHUNKHISTORY", opts, coord(cx), coord(cy))
}

// RangeHistory reads one page of the history of the chunks of a rectangle,
// at most [HelloInfo.MaxAreaChunks] (256), as [Client.History] does for one
// block. Events of different chunks are ordered by revision.
func (c *Client) RangeHistory(ctx context.Context, cx0, cy0, cx1, cy1 int64, opts HistoryOptions) (HistoryPage, error) {
	return c.history(ctx, "RANGEHISTORY", opts, coord(cx0), coord(cy0), coord(cx1), coord(cy1))
}

// HistoryEvents iterates over the history of block (x, y): it reads the
// pages of [Client.History] ([HistoryOptions.Limit] events each) and follows
// their cursors until the window ends, through short and empty pages. It
// stops at the first error, which it yields.
func (c *Client) HistoryEvents(ctx context.Context, x, y int64, opts HistoryOptions) iter.Seq2[HistoryEvent, error] {
	return historyEvents(ctx, "HISTORY", opts, func(ctx context.Context, opts HistoryOptions) (HistoryPage, error) {
		return c.History(ctx, x, y, opts)
	})
}

// ChunkHistoryEvents iterates over the history of chunk (cx, cy), as
// [Client.HistoryEvents] does with [Client.ChunkHistory].
func (c *Client) ChunkHistoryEvents(ctx context.Context, cx, cy int64, opts HistoryOptions) iter.Seq2[HistoryEvent, error] {
	return historyEvents(ctx, "CHUNKHISTORY", opts, func(ctx context.Context, opts HistoryOptions) (HistoryPage, error) {
		return c.ChunkHistory(ctx, cx, cy, opts)
	})
}

// RangeHistoryEvents iterates over the history of the chunks of a rectangle,
// as [Client.HistoryEvents] does with [Client.RangeHistory].
func (c *Client) RangeHistoryEvents(ctx context.Context, cx0, cy0, cx1, cy1 int64, opts HistoryOptions) iter.Seq2[HistoryEvent, error] {
	return historyEvents(ctx, "RANGEHISTORY", opts, func(ctx context.Context, opts HistoryOptions) (HistoryPage, error) {
		return c.RangeHistory(ctx, cx0, cy0, cx1, cy1, opts)
	})
}

// historyEvents yields the events of the pages read reads from opts on,
// passing each page's cursor to the next read.
func historyEvents(ctx context.Context, command string, opts HistoryOptions,
	read func(context.Context, HistoryOptions) (HistoryPage, error)) iter.Seq2[HistoryEvent, error] {
	return func(yield func(HistoryEvent, error) bool) {
		for {
			page, err := read(ctx, opts)
			if err != nil {
				yield(HistoryEvent{}, err)
				return
			}
			for _, event := range page.Events {
				if !yield(event, nil) {
					return
				}
			}
			if page.Cursor == "" {
				return
			}
			next := &opts.Before
			if opts.Ascending {
				next = &opts.After
			}
			// A page that neither returns events nor moves the cursor would
			// be read again forever.
			if len(page.Events) == 0 && page.Cursor == *next {
				yield(HistoryEvent{}, protocolErrorf(command, "%s returned an empty page at its own cursor %s", command, page.Cursor))
				return
			}
			*next = page.Cursor
		}
	}
}

func (c *Client) history(ctx context.Context, command string, opts HistoryOptions, args ...string) (HistoryPage, error) {
	release, err := c.acquireSlot(ctx, command)
	if err != nil {
		return HistoryPage{}, err
	}
	defer release()

	// Events carry block bits, checked against the table's block_bits.
	established, geo, err := c.chunkConnection(ctx, command)
	if err != nil {
		return HistoryPage{}, err
	}
	options, err := historyOptionArgs(established, command, opts)
	if err != nil {
		return HistoryPage{}, err
	}
	frame, err := c.execBoundedOn(ctx, established, historyItemBound(established, geo), command, append(args, options...)...)
	if err != nil {
		return HistoryPage{}, err
	}
	items, err := expectArray(frame, command)
	if err != nil {
		return HistoryPage{}, err
	}
	return parseHistoryPage(items, geo.blockBits, command)
}

// historyItemBound bounds one item of a history reply: an event whose block
// had extra data of up to max_extra_chunk_bytes before and after the change,
// in hex, can exceed MaxBulkBytes.
func historyItemBound(established *conn, geo geometry) int {
	return 4*established.maxExtraChunkBytes + 2*geo.blockBits + 2*cmp.Or(established.maxTagBytes, maxTagBytes) + 1024
}

func historyOptionArgs(established *conn, command string, opts HistoryOptions) ([]string, error) {
	limit := cmp.Or(established.maxHistoryLimit, maxHistoryLimit)
	if opts.Limit < 0 || opts.Limit > limit {
		return nil, requestErrorf(command, "%s limit must be between 1 and %d (0 for the server's default), got %d",
			command, limit, opts.Limit)
	}
	for _, cursor := range []HistoryCursor{opts.After, opts.Before} {
		if cursor != "" && !validHistoryCursor(string(cursor)) {
			return nil, requestErrorf(command, "%s cursor must be <revision> or <revision>:<block_index>, got %q",
				command, cursor)
		}
	}
	if opts.SinceMs < 0 || opts.UntilMs < 0 {
		return nil, requestErrorf(command, "%s SinceMs and UntilMs must not be negative, got %d and %d",
			command, opts.SinceMs, opts.UntilMs)
	}
	if tagLimit := cmp.Or(established.maxTagBytes, maxTagBytes); len(opts.Tag) > tagLimit {
		return nil, requestErrorf(command, "%s tag of %d bytes exceeds the server's max_tag_bytes (%d)",
			command, len(opts.Tag), tagLimit)
	}

	var args []string
	if opts.Limit != 0 {
		args = append(args, "LIMIT", strconv.Itoa(opts.Limit))
	}
	if opts.Ascending {
		args = append(args, "ASC")
	}
	if opts.After != "" {
		args = append(args, "AFTER", string(opts.After))
	}
	if opts.Before != "" {
		args = append(args, "BEFORE", string(opts.Before))
	}
	if opts.SinceMs != 0 {
		args = append(args, "SINCE", strconv.FormatInt(opts.SinceMs, 10))
	}
	if opts.UntilMs != 0 {
		args = append(args, "UNTIL", strconv.FormatInt(opts.UntilMs, 10))
	}
	if len(opts.Tag) != 0 {
		args = append(args, "TAG", hex.EncodeToString(opts.Tag))
	}
	return args, nil
}

// validHistoryCursor reports whether text is "<revision>" or
// "<revision>:<block_index>".
func validHistoryCursor(text string) bool {
	revision, block, hasBlock := strings.Cut(text, ":")
	if _, err := strconv.ParseUint(revision, 10, 64); err != nil {
		return false
	}
	if hasBlock {
		if _, err := strconv.ParseUint(block, 10, 32); err != nil {
			return false
		}
	}
	return true
}

// parseHistoryPage reads a history reply: "END" or "CURSOR <cursor>", then
// one item per event.
func parseHistoryPage(items []Frame, blockBits int, command string) (HistoryPage, error) {
	if len(items) == 0 {
		return HistoryPage{}, protocolErrorf(command, "empty %s response", command)
	}
	header, err := bulkText(items[0], command)
	if err != nil {
		return HistoryPage{}, err
	}
	page := HistoryPage{Events: make([]HistoryEvent, 0, len(items)-1)}
	if cursor, found := strings.CutPrefix(header, "CURSOR "); found && validHistoryCursor(cursor) {
		page.Cursor = HistoryCursor(cursor)
	} else if header != "END" {
		return HistoryPage{}, protocolErrorf(command, "unexpected %s header: %s", command, header)
	}
	for i, item := range items[1:] {
		text, err := bulkText(item, command)
		if err != nil {
			return HistoryPage{}, err
		}
		event, err := parseHistoryEvent(text, blockBits)
		if err != nil {
			return HistoryPage{}, protocolErrorf(command, "invalid %s event %d: %s", command, i, err)
		}
		page.Events = append(page.Events, event)
	}
	return page, nil
}

// parseHistoryEvent reads "<revision> <time_ms> <x> <y> <before> <after>
// <before_extra> <after_extra> <tag>".
func parseHistoryEvent(text string, blockBits int) (HistoryEvent, error) {
	fields := strings.Split(text, " ")
	if len(fields) != historyEventFields {
		return HistoryEvent{}, fmt.Errorf("%d fields, expected %d", len(fields), historyEventFields)
	}
	revision, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || strconv.FormatUint(revision, 10) != fields[0] {
		return HistoryEvent{}, fmt.Errorf("invalid revision %q", fields[0])
	}
	event := HistoryEvent{Revision: revision}
	if event.TimeMs, err = parseHistoryInt(fields[1]); err != nil || event.TimeMs < 0 {
		return HistoryEvent{}, fmt.Errorf("invalid time %q", fields[1])
	}
	// Chunks at the edge of the chunk coordinate range hold blocks beyond
	// the signed 64-bit range, which an int64 cannot hold.
	if event.X, err = parseHistoryInt(fields[2]); err != nil {
		return HistoryEvent{}, fmt.Errorf("block coordinate %q is not a signed 64-bit integer", fields[2])
	}
	if event.Y, err = parseHistoryInt(fields[3]); err != nil {
		return HistoryEvent{}, fmt.Errorf("block coordinate %q is not a signed 64-bit integer", fields[3])
	}
	if event.Before, err = parseHistoryBlock(fields[4], blockBits); err != nil {
		return HistoryEvent{}, fmt.Errorf("before: %w", err)
	}
	if event.After, err = parseHistoryBlock(fields[5], blockBits); err != nil {
		return HistoryEvent{}, fmt.Errorf("after: %w", err)
	}
	if event.BeforeExtra, err = parseHistoryExtra(fields[6], event.Before.Exists); err != nil {
		return HistoryEvent{}, fmt.Errorf("before extra data: %w", err)
	}
	if event.AfterExtra, err = parseHistoryExtra(fields[7], event.After.Exists); err != nil {
		return HistoryEvent{}, fmt.Errorf("after extra data: %w", err)
	}
	if fields[8] != "-" {
		tag, err := hex.DecodeString(fields[8])
		if err != nil || len(tag) == 0 {
			return HistoryEvent{}, fmt.Errorf("invalid tag %q", fields[8])
		}
		event.Tag = tag
	}
	return event, nil
}

// parseHistoryInt reads a decimal int64 in canonical form.
func parseHistoryInt(token string) (int64, error) {
	value, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return 0, err
	}
	if strconv.FormatInt(value, 10) != token {
		return 0, fmt.Errorf("%q is not canonical", token)
	}
	return value, nil
}

// parseHistoryBlock reads block bits as GET returns them, or "-" for an
// absent block.
func parseHistoryBlock(token string, blockBits int) (BlockState, error) {
	if token == "-" {
		return BlockState{}, nil
	}
	if len(token) != blockBits || !isBitString(token) {
		return BlockState{}, fmt.Errorf("expected %d bits or -, got %q", blockBits, token)
	}
	return BlockState{Exists: true, Bits: token}, nil
}

// parseHistoryExtra reads "<bit_length>:<hex>", or "-" for no extra data,
// which a block that is absent must have.
func parseHistoryExtra(token string, present bool) (*ExtraValue, error) {
	if token == "-" {
		return nil, nil
	}
	if !present {
		return nil, fmt.Errorf("an absent block has extra data")
	}
	lengthText, hexText, found := strings.Cut(token, ":")
	if !found {
		return nil, fmt.Errorf("expected <bit_length>:<hex>")
	}
	bitLength, err := strconv.ParseUint(lengthText, 10, 64)
	if err != nil || strconv.FormatUint(bitLength, 10) != lengthText || bitLength == 0 || bitLength > maxExtraBlockBits {
		return nil, fmt.Errorf("the bit length %q is not 1..%d", lengthText, maxExtraBlockBits)
	}
	value := ExtraValue{BitLength: int(bitLength)}
	if len(hexText) != 2*extraValueBytes(value.BitLength) {
		return nil, fmt.Errorf("a value of %d bits in %d hex digits", value.BitLength, len(hexText))
	}
	if value.Bytes, err = hex.DecodeString(hexText); err != nil {
		return nil, fmt.Errorf("invalid hex: %w", err)
	}
	if value.Bytes[len(value.Bytes)-1]&^extraPaddingMask(value.BitLength) != 0 {
		return nil, fmt.Errorf("the value has set bits past its bit length")
	}
	return &value, nil
}
