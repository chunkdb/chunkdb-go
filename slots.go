package chunkdb

import "context"

// Slot is one durable consumer position returned by SHOW SLOTS.
type Slot struct {
	Table, Name, Epoch string
	Acked              uint64
	RetainedBytes      uint64
	// Lost means the retention limit was exceeded. Rebuild consumer state,
	// drop the slot, and create it again before resuming.
	Lost bool
}

func slotName(command, name string) error {
	if len(name) > 63 {
		return requestErrorf(command, "slot name exceeds 63 bytes")
	}
	return checkNames(command, "slot", name)
}

// CreateSlot creates a durable slot at the table's current frontier. An empty
// table uses the default table. This needs ADMIN on the table.
func (c *Client) CreateSlot(ctx context.Context, table, name string) error {
	return c.slotStatement(ctx, "CREATE SLOT", table, name)
}

// DropSlot removes a slot and invalidates its active watch. An empty table
// uses the default table. This needs ADMIN on the table.
func (c *Client) DropSlot(ctx context.Context, table, name string) error {
	return c.slotStatement(ctx, "DROP SLOT", table, name)
}

func (c *Client) slotStatement(ctx context.Context, command, table, name string) error {
	if err := slotName(command, name); err != nil {
		return err
	}
	table, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	reply, err := c.call(ctx, command+" '"+name+"' ON "+table, nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, command, "OK")
}

// Slots lists durable slots, including lost ones. An empty table lists all
// tables visible to the user; otherwise it runs SHOW SLOTS ON table.
func (c *Client) Slots(ctx context.Context, table string) ([]Slot, error) {
	const command = "SHOW SLOTS"
	statement := command
	if table != "" {
		name, err := c.tableName(command, table)
		if err != nil {
			return nil, err
		}
		statement += " ON " + name
	}
	reply, err := c.call(ctx, statement, nil)
	if err != nil {
		return nil, err
	}
	if reply.Kind != ReplyArray {
		return nil, protocolErrorf(command, "expected an array")
	}
	slots := make([]Slot, 0, len(reply.Array))
	for _, item := range reply.Array {
		if item.Kind != ReplyMap {
			return nil, protocolErrorf(command, "expected a map per slot")
		}
		seen := make(map[string]bool, len(item.Map))
		for _, pair := range item.Map {
			if pair.Key.Kind != ReplyBulk || seen[string(pair.Key.Bulk)] {
				return nil, protocolErrorf(command, "invalid or duplicate slot field")
			}
			seen[string(pair.Key.Bulk)] = true
		}
		t, _ := item.Lookup("table")
		n, _ := item.Lookup("name")
		e, _ := item.Lookup("epoch")
		a, _ := item.Lookup("acked")
		b, _ := item.Lookup("retained_bytes")
		l, _ := item.Lookup("lost")
		acked, ae := a.Uint64()
		bytes, be := b.Uint64()
		if t.Kind != ReplyBulk || !isName(string(t.Bulk)) || n.Kind != ReplyBulk || slotName(command, string(n.Bulk)) != nil || e.Kind != ReplyBulk || !validEpoch(string(e.Bulk)) || !ae || !be || l.Kind != ReplyBoolean {
			return nil, protocolErrorf(command, "malformed slot fields")
		}
		slots = append(slots, Slot{Table: string(t.Bulk), Name: string(n.Bulk), Epoch: string(e.Bulk), Acked: acked, RetainedBytes: bytes, Lost: l.Bool})
	}
	return slots, nil
}

// CreateSlot runs Client.CreateSlot on a leased client.
func (p *Pool) CreateSlot(ctx context.Context, table, name string) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.CreateSlot(ctx, table, name) })
}

// DropSlot runs Client.DropSlot on a leased client.
func (p *Pool) DropSlot(ctx context.Context, table, name string) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.DropSlot(ctx, table, name) })
}

// Slots runs Client.Slots on a leased client.
func (p *Pool) Slots(ctx context.Context, table string) ([]Slot, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]Slot, error) { return c.Slots(ctx, table) })
}
