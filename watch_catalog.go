package chunkdb

import "context"

// WatchCatalog opens a stream of catalog changes (WATCH CATALOG): tables,
// columns, CHECK constraints, users, grants and slots. It needs MONITOR or
// ADMIN on *; users, grants and slot owners appear only with MANAGES USERS.
// The server must advertise the watch_catalog feature; otherwise it returns an
// [ErrProtocol] error before WATCH is sent.
//
// [Watch.Start] is the catalog epoch and revision. [Watch.Next] returns
// [*CatalogEvent] values; the first holds the whole catalog, one create per
// table, user and slot. A [*ResyncEvent] means the watch fell behind: discard
// the catalog you built and rebuild it from the next event, which holds the
// whole catalog again. A [*ProgressEvent] moves the position without a change.
// The catalog epoch changes when the server restarts; watch again and rebuild.
// A catalog watch has no slot, so [Watch.Ack] is refused.
func (c *Client) WatchCatalog(ctx context.Context) (*Watch, error) {
	const command = "WATCH"
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, closedError(command)
	}
	dedicated := c.copyOptions()
	w := &Watch{client: dedicated, catalog: true, pushes: make(chan Reply), closing: make(chan struct{}), done: make(chan struct{}), nextGate: make(chan struct{}, 1)}
	w.controlGate = make(chan struct{}, 1)
	w.ackContext, w.ackCancel = context.WithCancel(context.Background())
	cn, err := dedicated.connection(ctx)
	if err != nil {
		_ = dedicated.Close()
		return nil, err
	}
	if !cn.info.Supports("watch_catalog") {
		_ = dedicated.Close()
		return nil, requestErrorf(command, "the server does not support watch_catalog")
	}
	w.conn = cn
	cn.mu.Lock()
	if cn.failed {
		err = cn.termErr
	} else {
		cn.watch = w
	}
	cn.mu.Unlock()
	if err == nil {
		const statement = "WATCH CATALOG"
		if err = checkLimits(cn.info, command, statement, nil); err == nil {
			var reply Reply
			reply, err = dedicated.execOn(ctx, cn, command, statement, nil)
			if err == nil {
				w.start, err = parseWatchStart(reply)
			}
		}
	}
	if err != nil {
		_ = dedicated.Close()
		return nil, err
	}
	w.delivered = w.start
	return w, nil
}

// WatchCatalog opens a catalog watch on a dedicated connection without
// consuming a pool lease; see [Client.WatchCatalog].
func (p *Pool) WatchCatalog(ctx context.Context) (*Watch, error) {
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
	return c.WatchCatalog(ctx)
}

// TableSchema decodes the data of a table entry: its DESCRIBE map without
// revision. It returns nil for a drop and an error for an entry of another
// kind.
func (c CatalogChange) TableSchema() (*Schema, error) {
	if err := c.expectKind("table"); err != nil || c.Data.Kind == ReplyNull {
		return nil, err
	}
	schema, err := parseSchema(c.Data)
	if err != nil {
		return nil, annotate("WATCH", err)
	}
	return schema, nil
}

// UserData decodes the data of a user entry: its SHOW USERS map. It returns nil
// for a drop and an error for an entry of another kind.
func (c CatalogChange) UserData() (*User, error) {
	if err := c.expectKind("user"); err != nil || c.Data.Kind == ReplyNull {
		return nil, err
	}
	user, err := parseUser("WATCH", c.Data)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// CatalogSlot is the data of a slot entry of a catalog event.
type CatalogSlot struct {
	// Filters are the slot's persisted filters, as SHOW SLOTS reports them.
	Filters *SlotFilters
	// Owner is the user the slot belongs to; nil without MANAGES USERS or for
	// a slot without a recorded owner.
	Owner *string
}

// SlotData decodes the data of a slot entry: its SHOW SLOTS filters and owner.
// The entry's Table names the slot's table. It returns nil for a drop and an
// error for an entry of another kind.
func (c CatalogChange) SlotData() (*CatalogSlot, error) {
	if err := c.expectKind("slot"); err != nil || c.Data.Kind == ReplyNull {
		return nil, err
	}
	filters, hasFilters := c.Data.Lookup("filters")
	owner, hasOwner := c.Data.Lookup("owner")
	if c.Data.Kind != ReplyMap || !hasFilters || !hasOwner {
		return nil, protocolErrorf("WATCH", "a catalog slot needs filters and owner")
	}
	parsed, err := parseSlotFilters(filters)
	if err != nil {
		return nil, annotate("WATCH", err)
	}
	slot := &CatalogSlot{Filters: parsed}
	if slot.Owner, err = nullableText(owner); err != nil {
		return nil, err
	}
	return slot, nil
}

func (c CatalogChange) expectKind(kind string) error {
	if c.Kind == nil || *c.Kind != kind {
		return requestErrorf("WATCH", "the catalog entry is not a %s", kind)
	}
	return nil
}
