package chunkdb

import (
	"context"
	"strconv"
)

// CurrentTable reports the table this client works on ("default" unless one
// was selected).
func (c *Client) CurrentTable() string {
	if table := c.selectedTable(); table != "" {
		return table
	}
	return "default"
}

// Tables lists the table names in ascending order.
func (c *Client) Tables(ctx context.Context) ([]string, error) {
	release, err := c.acquireSlot(ctx, "TABLES")
	if err != nil {
		return nil, err
	}
	defer release()

	frame, err := c.exec(ctx, "TABLES")
	if err != nil {
		return nil, err
	}
	items, err := expectArray(frame, "TABLES")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		name, err := bulkText(item, "TABLES")
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// TableInfo reports a table's geometry, options and store id.
func (c *Client) TableInfo(ctx context.Context, name string) (TableInfo, error) {
	release, err := c.acquireSlot(ctx, "TABLEINFO")
	if err != nil {
		return TableInfo{}, err
	}
	defer release()

	frame, err := c.exec(ctx, "TABLEINFO", name)
	if err != nil {
		return TableInfo{}, err
	}
	return parseTableInfo(frame, "TABLEINFO")
}

// Use selects the table for this client's connection, and for every
// reconnect. An unknown name fails with [CodeNoTable] and keeps the current
// table. Use waits for the requests in flight on this client and holds back
// new ones until it completes, so each runs entirely on one table.
func (c *Client) Use(ctx context.Context, name string) (TableInfo, error) {
	release, err := c.acquireAllSlots(ctx, "USE")
	if err != nil {
		return TableInfo{}, err
	}
	defer release()

	established, err := c.connection(ctx)
	if err != nil {
		return TableInfo{}, err
	}
	return c.useOn(ctx, established, name)
}

// useOn selects a table on a connection and records it as the client's
// selection, with its geometry.
func (c *Client) useOn(ctx context.Context, established *conn, name string) (TableInfo, error) {
	frame, err := c.execOn(ctx, established, "USE", name)
	if err != nil {
		return TableInfo{}, err
	}
	info, err := parseTableInfo(frame, "USE")
	if err != nil {
		return TableInfo{}, err
	}
	c.tableMu.Lock()
	c.table = info.Name
	c.tableMu.Unlock()
	established.setGeometry(geometryOf(info))
	return info, nil
}

// Table returns a new connected client on table name, with this client's
// options; its handshake names the table. It has its own connection; close it
// when done.
func (c *Client) Table(ctx context.Context, name string) (*Client, error) {
	options := c.options
	options.Table = name
	return Connect(ctx, options)
}

// CreateTable creates a table. Its geometry is fixed once created; its
// options can change with [Client.SetTableOptions].
func (c *Client) CreateTable(ctx context.Context, name string, spec TableSpec) error {
	release, err := c.acquireSlot(ctx, "TABLECREATE")
	if err != nil {
		return err
	}
	defer release()

	args := []string{name, "block_bits", strconv.Itoa(spec.BlockBits)}
	for _, field := range []struct {
		key   string
		value int
	}{
		{"chunk_width_blocks", spec.ChunkWidthBlocks},
		{"chunk_height_blocks", spec.ChunkHeightBlocks},
		{"large_chunk_width_chunks", spec.LargeChunkWidthChunks},
		{"large_chunk_height_chunks", spec.LargeChunkHeightChunks},
	} {
		if field.value != 0 {
			args = append(args, field.key, strconv.Itoa(field.value))
		}
	}
	args = append(args, tableOptionArgs(spec.Options)...)

	frame, err := c.exec(ctx, "TABLECREATE", args...)
	if err != nil {
		return err
	}
	return expectOK(frame, "TABLECREATE")
}

// SetTableOptions changes the non-zero options of a table; the server
// reopens the table.
func (c *Client) SetTableOptions(ctx context.Context, name string, options TableOptions) error {
	args := tableOptionArgs(options)
	if len(args) == 0 {
		return requestErrorf("TABLESET", "SetTableOptions needs at least one option")
	}
	release, err := c.acquireSlot(ctx, "TABLESET")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "TABLESET", append([]string{name}, args...)...)
	if err != nil {
		return err
	}
	return expectOK(frame, "TABLESET")
}

// DropTable deletes a table and its data. Irreversible. Connections working on
// it get [CodeNoTable] from then on.
func (c *Client) DropTable(ctx context.Context, name string) error {
	release, err := c.acquireSlot(ctx, "TABLEDROP")
	if err != nil {
		return err
	}
	defer release()

	frame, err := c.exec(ctx, "TABLEDROP", name)
	if err != nil {
		return err
	}
	return expectOK(frame, "TABLEDROP")
}

func tableOptionArgs(options TableOptions) []string {
	var args []string
	if options.DurabilityMode != "" {
		args = append(args, "durability_mode", options.DurabilityMode)
	}
	if options.CheckpointUpdates != 0 {
		args = append(args, "checkpoint_updates", strconv.Itoa(options.CheckpointUpdates))
	}
	if options.CheckpointWalBytes != 0 {
		args = append(args, "checkpoint_wal_bytes", strconv.Itoa(options.CheckpointWalBytes))
	}
	if options.WalGroupCommitUpdates != 0 {
		args = append(args, "wal_group_commit_updates", strconv.Itoa(options.WalGroupCommitUpdates))
	}
	if options.CheckpointCompression != "" {
		args = append(args, "checkpoint_compression", options.CheckpointCompression)
	}
	return args
}

func parseTableInfo(frame Frame, command string) (TableInfo, error) {
	payload, err := expectBulk(frame, command)
	if err != nil {
		return TableInfo{}, err
	}
	return parseTableValues(ParseInfo(payload), command)
}

// parseTableValues reads the TABLEINFO lines, which USE and HELLO carry too.
func parseTableValues(values map[string]string, command string) (TableInfo, error) {
	info := TableInfo{
		Name:    values["table"],
		StoreID: values["store_id"],
		Options: TableOptions{
			DurabilityMode:        values["durability_mode"],
			CheckpointCompression: values["checkpoint_compression"],
		},
		Values: values,
	}
	if info.Name == "" || info.StoreID == "" || info.Options.DurabilityMode == "" ||
		info.Options.CheckpointCompression == "" {
		return TableInfo{}, protocolErrorf(command, "%s reply is missing table fields", command)
	}
	for _, field := range []struct {
		key    string
		target *int
	}{
		{"block_bits", &info.BlockBits},
		{"chunk_width_blocks", &info.ChunkWidthBlocks},
		{"chunk_height_blocks", &info.ChunkHeightBlocks},
		{"large_chunk_width_chunks", &info.LargeChunkWidthChunks},
		{"large_chunk_height_chunks", &info.LargeChunkHeightChunks},
		{"checkpoint_updates", &info.Options.CheckpointUpdates},
		{"checkpoint_wal_bytes", &info.Options.CheckpointWalBytes},
		{"wal_group_commit_updates", &info.Options.WalGroupCommitUpdates},
	} {
		value, err := strconv.Atoi(values[field.key])
		if err != nil || value <= 0 {
			return TableInfo{}, protocolErrorf(command, "%s missing valid %s", command, field.key)
		}
		*field.target = value
	}
	return info, nil
}
