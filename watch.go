package chunkdb

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Area is an inclusive rectangle of chunk coordinates.
type Area struct{ CX0, CY0, CX1, CY1 int64 }

// Position identifies a frontier in a table's change stream.
type Position struct {
	Epoch    string
	Revision uint64
}

// WatchOptions select an area and an optional retained position to resume after.
type WatchOptions struct {
	// Slot selects a durable consumer. Empty keeps an in-memory watch.
	Slot  string
	Area  *Area
	After *Position
}

// Coordinate is an absolute int64 block coordinate, or a ChunkOffset when
// the absolute address does not fit int64.
type Coordinate = any

// ChunkOffset preserves an axis outside the int64 range without rounding.
type ChunkOffset struct {
	Chunk  int64
	Offset uint32
}

// Event is a change, schema change, or a request to rebuild local state.
type Event interface{ GetPosition() Position }

// BlockChange contains the complete rows before and after a changed block.
// Nil rows denote absence; a nil field within a row denotes NULL.
type BlockChange struct {
	X, Y          Coordinate
	Before, After Record
}

// ChangeEvent is one committed write or transaction, in revision order.
type ChangeEvent struct {
	Position      Position
	CommitTimeMs  uint64
	User          *string
	SchemaVersion uint64
	Blocks        []BlockChange
}

func (e *ChangeEvent) GetPosition() Position { return e.Position }

// SchemaEvent supplies the columns for subsequent events of Version.
type SchemaEvent struct {
	Position Position
	Version  uint64
	Columns  []Column
}

func (e *SchemaEvent) GetPosition() Position { return e.Position }

// ResyncEvent asks the consumer to re-read state on another connection.
type ResyncEvent struct{ Position Position }

func (e *ResyncEvent) GetPosition() Position { return e.Position }

// Watch owns a dedicated logged-in connection. Next calls are serialized;
// Close can interrupt a pending Next. Closing the originating Client or Pool
// does not close a Watch: callers must close each watch themselves.
type Watch struct {
	client         *Client
	metadataMu     sync.Mutex
	metadataCancel context.CancelFunc
	metadata       *Client
	conn           *conn
	table          string
	start          Position
	schemas        map[uint64][]Column
	pushes         chan Reply
	closing, done  chan struct{}
	nextGate       chan struct{}
	closeOnce      sync.Once
	closeErr       error
	slot           string
	controlGate    chan struct{}
	ackContext     context.Context
	ackCancel      context.CancelFunc
	ackMu          sync.Mutex
	delivered      Position
	acknowledged   uint64
}

// Watch opens a stream with the client's endpoint, TLS options and login.
// It does not occupy the client's connection or pipeline slots.
func (c *Client) Watch(ctx context.Context, table string, opts WatchOptions) (*Watch, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, closedError("WATCH")
	}
	name, err := c.tableName("WATCH", table)
	if err != nil {
		return nil, err
	}
	statement := "WATCH " + name
	if opts.Slot != "" {
		if err := slotName("WATCH", opts.Slot); err != nil {
			return nil, err
		}
		statement += " SLOT '" + opts.Slot + "'"
	}
	if a := opts.Area; a != nil {
		if a.CX0 > a.CX1 || a.CY0 > a.CY1 {
			return nil, requestErrorf("WATCH", "inverted area")
		}
		statement += " AREA " + coord(a.CX0) + " " + coord(a.CY0) + " TO " + coord(a.CX1) + " " + coord(a.CY1)
	}
	if p := opts.After; p != nil {
		if !validEpoch(p.Epoch) {
			return nil, requestErrorf("WATCH", "epoch must be 32 hex digits")
		}
		statement += " AFTER " + p.Epoch + " " + strconv.FormatUint(p.Revision, 10)
	}
	dedicated := c.copyOptions()
	w := &Watch{client: dedicated, table: name, schemas: make(map[uint64][]Column), pushes: make(chan Reply), closing: make(chan struct{}), done: make(chan struct{}), nextGate: make(chan struct{}, 1)}
	w.slot = opts.Slot
	w.controlGate = make(chan struct{}, 1)
	w.ackContext, w.ackCancel = context.WithCancel(context.Background())
	// Seed before WATCH, so schema changes racing with setup are retained in
	// the stream rather than decoded with a later DESCRIBE's columns.
	schema, err := w.describe(ctx)
	if err != nil {
		return nil, err
	}
	w.schemas[schema.Version] = schema.Columns
	cn, err := dedicated.connection(ctx)
	if err != nil {
		_ = dedicated.Close()
		return nil, err
	}
	w.conn = cn
	cn.mu.Lock()
	if cn.failed {
		err = cn.termErr
	} else {
		cn.watch = w
	}
	cn.mu.Unlock()
	if err != nil {
		_ = dedicated.Close()
		return nil, err
	}
	if err = checkLimits(cn.info, "WATCH", statement, nil); err == nil {
		var reply Reply
		reply, err = dedicated.execOn(ctx, cn, "WATCH", statement, nil)
		if err == nil {
			w.start, err = parseWatchStart(reply)
		}
	}
	if err != nil {
		_ = dedicated.Close()
		return nil, err
	}
	w.delivered = w.start
	w.acknowledged = w.start.Revision
	return w, nil
}

func (c *Client) copyOptions() *Client {
	return &Client{opts: c.opts, slots: make(chan struct{}, c.opts.pipelineDepth), dialGate: make(chan struct{}, 1), txGate: make(chan struct{}, 1), schemas: make(map[string]*Schema)}
}

// Watch opens a dedicated connection without consuming a pool lease.
func (p *Pool) Watch(ctx context.Context, table string, opts WatchOptions) (*Watch, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, closedError("WATCH")
	}
	c, err := NewClient(p.clientOpts)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Watch(ctx, table, opts)
}

// Start is the position reported by WATCH's initial +OK.
func (w *Watch) Start() Position { return w.start }

// Next waits for an event. Cancellation while waiting leaves the stream open;
// lookup or decoding failures end it. Idle watches have no command timeout.
func (w *Watch) Next(ctx context.Context) (Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, timeoutErrorf("WATCH", err, "%s", err)
	}
	select {
	case w.nextGate <- struct{}{}:
	case <-ctx.Done():
		return nil, timeoutErrorf("WATCH", ctx.Err(), "%s", ctx.Err())
	case <-w.closing:
		return nil, closedError("WATCH")
	}
	defer func() { <-w.nextGate }()
	select {
	case <-w.closing:
		return nil, closedError("WATCH")
	default:
	}
	select {
	case reply := <-w.pushes:
		if reply.Kind == ReplyError {
			return nil, replyError(PhaseResponse, "ACK", reply)
		}
		event, err := w.decode(ctx, reply)
		select {
		case <-w.closing:
			return nil, closedError("WATCH")
		default:
		}
		if err != nil {
			_ = w.conn.shutdown(err)
		} else {
			w.ackMu.Lock()
			w.delivered = event.GetPosition()
			w.ackMu.Unlock()
		}
		return event, err
	case <-ctx.Done():
		return nil, timeoutErrorf("WATCH", ctx.Err(), "%s", ctx.Err())
	case <-w.closing:
		return nil, closedError("WATCH")
	case <-w.done:
		return nil, w.conn.terminalError()
	}
}

// Close sends UNWATCH, discards queued pushes through its +OK and closes the
// dedicated connections. The client's CommandTimeout bounds draining (the
// default applies when it was disabled). Close is idempotent.
func (w *Watch) Close() error {
	w.closeOnce.Do(func() {
		close(w.closing)
		w.ackCancel()
		w.controlGate <- struct{}{}
		defer func() { <-w.controlGate }()
		w.metadataMu.Lock()
		lookupCancel := w.metadataCancel
		w.metadataMu.Unlock()
		if lookupCancel != nil {
			lookupCancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		if w.client.opts.commandTimeout > 0 {
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), w.client.opts.commandTimeout)
		}
		defer cancel()
		if terminal := w.conn.terminalError(); terminal != nil {
			w.closeErr = terminal
		} else {
			reply, err := w.client.execOn(ctx, w.conn, "UNWATCH", "UNWATCH", nil)
			if err == nil && (reply.Kind != ReplySimple || reply.Text != "OK") {
				err = protocolErrorf("UNWATCH", "expected +OK")
			}
			w.closeErr = err
		}
		_ = w.client.Close()
		w.metadataMu.Lock()
		metadata := w.metadata
		w.metadataMu.Unlock()
		if metadata != nil {
			_ = metadata.Close()
		}
	})
	return w.closeErr
}

func validEpoch(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func parseWatchStart(reply Reply) (Position, error) {
	fields := strings.Fields(reply.Text)
	if reply.Kind != ReplySimple || len(fields) != 3 || fields[0] != "OK" || !validEpoch(fields[1]) {
		return Position{}, protocolErrorf("WATCH", "expected +OK epoch revision")
	}
	rev, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return Position{}, protocolErrorf("WATCH", "invalid start revision")
	}
	return Position{Epoch: fields[1], Revision: rev}, nil
}

func (w *Watch) decode(ctx context.Context, r Reply) (Event, error) {
	bad := func() (Event, error) { return nil, protocolErrorf("WATCH", "malformed push") }
	if r.Kind != ReplyPush || len(r.Array) < 3 {
		return bad()
	}
	a := r.Array
	if a[0].Kind != ReplyBulk || a[1].Kind != ReplyBulk || !validEpoch(string(a[1].Bulk)) {
		return bad()
	}
	rev, ok := a[2].Uint64()
	if !ok {
		return bad()
	}
	pos := Position{Epoch: string(a[1].Bulk), Revision: rev}
	switch string(a[0].Bulk) {
	case "resync":
		if len(a) != 3 {
			return bad()
		}
		return &ResyncEvent{Position: pos}, nil
	case "schema":
		if len(a) != 5 || a[4].Kind != ReplyArray || len(a[4].Array) == 0 {
			return bad()
		}
		version, ok := a[3].Uint64()
		if !ok {
			return bad()
		}
		columns := make([]Column, 0, len(a[4].Array))
		names := make(map[string]bool)
		ids := make(map[uint32]bool)
		for _, item := range a[4].Array {
			col, err := parseColumn(item)
			if err != nil {
				return nil, annotate("WATCH", err)
			}
			if names[col.Name] || ids[col.ID] {
				return nil, protocolErrorf("WATCH", "duplicate schema column name or ID")
			}
			names[col.Name] = true
			ids[col.ID] = true
			columns = append(columns, col)
		}
		w.schemas[version] = columns
		// Keep cache ownership separate from mutable event values.
		eventColumns := (&Schema{Columns: columns}).clone().Columns
		return &SchemaEvent{Position: pos, Version: version, Columns: eventColumns}, nil
	case "change":
		if len(a) != 7 || a[6].Kind != ReplyArray {
			return bad()
		}
		ms, ok := a[3].Uint64()
		if !ok {
			return bad()
		}
		var user *string
		if a[4].Kind == ReplyBulk {
			s := string(a[4].Bulk)
			user = &s
		} else if a[4].Kind != ReplyNull {
			return bad()
		}
		version, ok := a[5].Uint64()
		if !ok {
			return bad()
		}
		columns, ok := w.schemas[version]
		if !ok {
			schema, err := w.describe(ctx)
			if err != nil {
				return nil, err
			}
			if schema.Version != version {
				return nil, protocolErrorf("WATCH", "schema version %d unavailable (DESCRIBE returned %d); resume or rebuild state", version, schema.Version)
			}
			columns = schema.Columns
			w.schemas[version] = columns
		}
		e := &ChangeEvent{Position: pos, CommitTimeMs: ms, User: user, SchemaVersion: version, Blocks: make([]BlockChange, 0, len(a[6].Array))}
		for _, item := range a[6].Array {
			if item.Kind != ReplyArray || len(item.Array) != 4 {
				return bad()
			}
			b := item.Array
			x, err := decodeCoordinate(b[0])
			if err != nil {
				return nil, err
			}
			y, err := decodeCoordinate(b[1])
			if err != nil {
				return nil, err
			}
			before, err := decodeWatchRow(columns, b[2])
			if err != nil {
				return nil, err
			}
			after, err := decodeWatchRow(columns, b[3])
			if err != nil {
				return nil, err
			}
			e.Blocks = append(e.Blocks, BlockChange{X: x, Y: y, Before: before, After: after})
		}
		return e, nil
	}
	return bad()
}

func decodeCoordinate(r Reply) (Coordinate, error) {
	if n, ok := r.Int64(); ok {
		return n, nil
	}
	if r.Kind == ReplyArray && len(r.Array) == 2 {
		chunk, ok := r.Array[0].Int64()
		offset, okOffset := r.Array[1].Uint64()
		if ok && okOffset && offset <= 1<<32-1 {
			return ChunkOffset{Chunk: chunk, Offset: uint32(offset)}, nil
		}
	}
	return nil, protocolErrorf("WATCH", "invalid block coordinate")
}
func decodeWatchRow(columns []Column, r Reply) (Record, error) {
	if r.Kind == ReplyNull {
		return nil, nil
	}
	if r.Kind != ReplyArray || len(r.Array) != len(columns) {
		return nil, protocolErrorf("WATCH", "row does not match schema columns")
	}
	out := make(Record, len(columns))
	for i, col := range columns {
		v, err := decodeValue(col.Type, r.Array[i])
		if err != nil {
			return nil, annotate("WATCH", err)
		}
		out[col.Name] = v
	}
	return out, nil
}

// Describe connections are short-lived: idle ordinary connections occupy
// statement workers, so holding one would starve WATCH setup on small servers.
func (w *Watch) describe(ctx context.Context) (*Schema, error) {
	lookupCtx, cancel := context.WithCancel(ctx)
	metadata := w.client.copyOptions()
	w.metadataMu.Lock()
	select {
	case <-w.closing:
		w.metadataMu.Unlock()
		cancel()
		_ = metadata.Close()
		return nil, closedError("WATCH")
	default:
	}
	w.metadata = metadata
	w.metadataCancel = cancel
	w.metadataMu.Unlock()
	defer func() {
		cancel()
		_ = metadata.Close()
		w.metadataMu.Lock()
		if w.metadata == metadata {
			w.metadata = nil
			w.metadataCancel = nil
		}
		w.metadataMu.Unlock()
	}()
	return metadata.Describe(lookupCtx, w.table)
}

// Ack acknowledges applied work on a slot watch. It completes after writing the
// request, not after persistence: success has no server reply. An asynchronous
// rejection is returned by Next; INVALID_ARGUMENT leaves the stream open.
// Revisions must not decrease or exceed the last returned event (or Start).
// A schema description prefacing a change must only be acknowledged after
// applying that change. Close flushes accepted acknowledgements via UNWATCH.
func (w *Watch) Ack(ctx context.Context, revision uint64) error {
	if err := ctx.Err(); err != nil {
		return timeoutErrorf("ACK", err, "%s", err)
	}
	select {
	case w.controlGate <- struct{}{}:
	case <-ctx.Done():
		return timeoutErrorf("ACK", ctx.Err(), "%s", ctx.Err())
	case <-w.closing:
		return closedError("ACK")
	}
	defer func() { <-w.controlGate }()
	select {
	case <-w.closing:
		return closedError("ACK")
	default:
	}
	if w.slot == "" {
		return requestErrorf("ACK", "ACK requires a slot watch")
	}
	w.ackMu.Lock()
	valid := revision >= w.acknowledged && revision <= w.delivered.Revision
	w.ackMu.Unlock()
	if !valid {
		return requestErrorf("ACK", "revision must be between acknowledged and last delivered revision")
	}
	statement := "ACK " + strconv.FormatUint(revision, 10)
	if err := checkLimits(w.conn.info, "ACK", statement, nil); err != nil {
		return err
	}
	wire, err := encodeRequest("ACK", statement, nil)
	if err != nil {
		return err
	}
	ackCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClose := context.AfterFunc(w.ackContext, cancel)
	defer stopClose()
	deadline := w.client.commandDeadline(ackCtx, "ACK")
	defer deadline.cancel()
	cn := w.conn
	cn.writeMu.Lock()
	defer cn.writeMu.Unlock()
	if err := cn.terminalError(); err != nil {
		return err
	}
	if deadline.ctx.Err() != nil {
		return deadline.err()
	}
	if at, ok := deadline.ctx.Deadline(); ok {
		_ = cn.netConn.SetWriteDeadline(at)
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(deadline.ctx, func() { _ = cn.netConn.SetWriteDeadline(time.Now()); close(interrupted) })
	_, err = cn.writer.Write(wire)
	if err == nil {
		err = cn.writer.Flush()
	}
	if !stop() {
		<-interrupted
	}
	_ = cn.netConn.SetWriteDeadline(time.Time{})
	if err != nil {
		var wrapped error
		if deadline.ctx.Err() != nil {
			wrapped = deadline.err()
		} else {
			wrapped = newError(KindConnection, PhaseRequest, "ACK", "write request: "+err.Error(), err)
		}
		_ = cn.shutdown(wrapped)
		return wrapped
	}
	w.ackMu.Lock()
	w.acknowledged = revision
	w.ackMu.Unlock()
	return nil
}
