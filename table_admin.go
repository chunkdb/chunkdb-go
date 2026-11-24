package chunkdb

import (
	"context"
	"strconv"
	"strings"
)

// TableStats is the reply of SHOW TABLE STATS.
type TableStats struct {
	Epoch string
	// Revision is the completed commit revision; nil on a read-only server.
	Revision              *uint64
	Dims                  int
	Tombstones            uint64
	RetainedDeletionFloor uint64
	// Chunks counts populated chunks and LoadedChunks those in the chunk
	// cache; Files and DiskBytes count the files under the table's directory;
	// Slots counts durable slots and Watches open watches. They are zero for
	// servers that do not report them.
	Chunks, LoadedChunks, Files, DiskBytes, Slots, Watches uint64
	// Fields retains the raw map, including unknown fields.
	Fields map[string]Reply
}

func replyFields(command string, r Reply) (map[string]Reply, error) {
	if r.Kind != ReplyMap {
		return nil, protocolErrorf(command, "expected a map")
	}
	fields := make(map[string]Reply, len(r.Map))
	for _, pair := range r.Map {
		if pair.Key.Kind != ReplyBulk {
			return nil, protocolErrorf(command, "invalid map key")
		}
		fields[string(pair.Key.Bulk)] = cloneReply(pair.Value)
	}
	return fields, nil
}

func fieldUint64(command string, fields map[string]Reply, key string) (uint64, error) {
	n, ok := fields[key].Uint64()
	if !ok {
		return 0, protocolErrorf(command, "invalid %s", key)
	}
	return n, nil
}

func fieldText(command string, fields map[string]Reply, key string) (string, error) {
	value := fields[key]
	if value.Kind != ReplyBulk {
		return "", protocolErrorf(command, "invalid %s", key)
	}
	return string(value.Bulk), nil
}

func parseTableStats(reply Reply) (TableStats, error) {
	const command = "SHOW TABLE STATS"
	fields, err := replyFields(command, reply)
	if err != nil {
		return TableStats{}, err
	}
	stats := TableStats{Fields: fields}
	if stats.Epoch, err = fieldText(command, fields, "epoch"); err != nil {
		return TableStats{}, err
	}
	if revision := fields["revision"]; revision.Kind != ReplyNull {
		n, ok := revision.Uint64()
		if !ok {
			return TableStats{}, protocolErrorf(command, "invalid revision")
		}
		stats.Revision = &n
	}
	dims, err := fieldUint64(command, fields, "dims")
	if err != nil || (dims != 2 && dims != 3) {
		return TableStats{}, protocolErrorf(command, "invalid dims")
	}
	stats.Dims = int(dims)
	if stats.Tombstones, err = fieldUint64(command, fields, "tombstones"); err != nil {
		return TableStats{}, err
	}
	if stats.RetainedDeletionFloor, err = fieldUint64(command, fields, "retained_deletion_floor"); err != nil {
		return TableStats{}, err
	}
	for key, target := range map[string]*uint64{"chunks": &stats.Chunks, "loaded_chunks": &stats.LoadedChunks, "files": &stats.Files, "disk_bytes": &stats.DiskBytes, "slots": &stats.Slots, "watches": &stats.Watches} {
		if _, exists := fields[key]; !exists {
			continue
		}
		if *target, err = fieldUint64(command, fields, key); err != nil {
			return TableStats{}, err
		}
	}
	return stats, nil
}

// TableStats returns SHOW TABLE STATS: the epoch, completed revision,
// dimensions, retained tombstones and retained-deletion floor of table.
func (c *Client) TableStats(ctx context.Context, table string) (TableStats, error) {
	name, err := c.tableName("SHOW TABLE STATS", table)
	if err != nil {
		return TableStats{}, err
	}
	reply, err := c.call(ctx, "SHOW TABLE STATS "+name, nil)
	if err != nil {
		return TableStats{}, err
	}
	return parseTableStats(reply)
}

// CompactTombstones forgets retained tombstones of table below revision
// before (COMPACT TOMBSTONES ... BEFORE) and returns the retained-deletion
// floor. It requires ADMIN on the table. Watches that resume below the floor
// receive RESYNC_REQUIRED.
func (c *Client) CompactTombstones(ctx context.Context, table string, before uint64) (uint64, error) {
	const command = "COMPACT TOMBSTONES"
	name, err := c.tableName(command, table)
	if err != nil {
		return 0, err
	}
	reply, err := c.call(ctx, command+" "+name+" BEFORE "+strconv.FormatUint(before, 10), nil)
	if err != nil {
		return 0, err
	}
	floor, ok := reply.Uint64()
	if !ok {
		return 0, protocolErrorf(command, "expected an integer floor")
	}
	return floor, nil
}

// SwapTables exchanges the names of two tables in one atomic step. It
// requires ADMIN on both tables.
func (c *Client) SwapTables(ctx context.Context, first, second string) error {
	const command = "SWAP TABLES"
	if err := checkNames(command, "table", first, second); err != nil {
		return err
	}
	defer c.forgetSchema(second)
	return c.tableStatement(ctx, first, command+" "+first+", "+second)
}

// RecoverTable returns a fenced table to service (RECOVER TABLE).
func (c *Client) RecoverTable(ctx context.Context, table string) error {
	name, err := c.tableName("RECOVER TABLE", table)
	if err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "RECOVER TABLE "+name)
}

// MigrationRecord is one applied migration of SHOW MIGRATIONS.
type MigrationRecord struct {
	Name string
	// AppliedMS is the application time in Unix milliseconds.
	AppliedMS uint64
	// User is the applying user; empty when you lack MANAGES USERS.
	User      string
	Statement string
}

func parseMigrations(reply Reply) ([]MigrationRecord, error) {
	const command = "SHOW MIGRATIONS"
	if reply.Kind != ReplyArray {
		return nil, protocolErrorf(command, "expected an array")
	}
	records := make([]MigrationRecord, 0, len(reply.Array))
	for _, item := range reply.Array {
		fields, err := replyFields(command, item)
		if err != nil {
			return nil, err
		}
		var record MigrationRecord
		if record.Name, err = fieldText(command, fields, "name"); err != nil {
			return nil, err
		}
		if record.AppliedMS, err = fieldUint64(command, fields, "applied_ms"); err != nil {
			return nil, err
		}
		if user := fields["user"]; user.Kind != ReplyNull {
			if record.User, err = fieldText(command, fields, "user"); err != nil {
				return nil, err
			}
		}
		if record.Statement, err = fieldText(command, fields, "statement"); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// Migrations lists applied migrations within your rights, in applied order.
func (c *Client) Migrations(ctx context.Context) ([]MigrationRecord, error) {
	reply, err := c.call(ctx, "SHOW MIGRATIONS", nil)
	if err != nil {
		return nil, err
	}
	return parseMigrations(reply)
}

// BackupCut is the backed-up position of one table.
type BackupCut struct {
	Table    string
	Position Position
}

// BackupResult is the reply of BACKUP TO.
type BackupResult struct {
	Tables, Files, Bytes uint64
	Cuts                 []BackupCut
}

func parseBackup(reply Reply) (BackupResult, error) {
	const command = "BACKUP"
	fields, err := replyFields(command, reply)
	if err != nil {
		return BackupResult{}, err
	}
	var result BackupResult
	for key, target := range map[string]*uint64{"tables": &result.Tables, "files": &result.Files, "bytes": &result.Bytes} {
		if *target, err = fieldUint64(command, fields, key); err != nil {
			return BackupResult{}, err
		}
	}
	cuts := fields["cuts"]
	if cuts.Kind != ReplyArray {
		return BackupResult{}, protocolErrorf(command, "invalid cuts")
	}
	for _, item := range cuts.Array {
		cut, err := replyFields(command, item)
		if err != nil {
			return BackupResult{}, err
		}
		var entry BackupCut
		if entry.Table, err = fieldText(command, cut, "table"); err != nil {
			return BackupResult{}, err
		}
		if entry.Position.Epoch, err = fieldText(command, cut, "epoch"); err != nil {
			return BackupResult{}, err
		}
		if entry.Position.Revision, err = fieldUint64(command, cut, "revision"); err != nil {
			return BackupResult{}, err
		}
		result.Cuts = append(result.Cuts, entry)
	}
	return result, nil
}

func quoteName(command, what, value string) (string, error) {
	if value == "" || strings.ContainsAny(value, "\r\n\x00") {
		return "", requestErrorf(command, "%s must be a nonempty single line without NUL", what)
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'", nil
}

// Backup writes a backup of every table to destination under the server's
// --backup-dir (BACKUP TO). It requires MANAGES USERS.
func (c *Client) Backup(ctx context.Context, destination string) (BackupResult, error) {
	quoted, err := quoteName("BACKUP", "destination", destination)
	if err != nil {
		return BackupResult{}, err
	}
	reply, err := c.call(ctx, "BACKUP TO "+quoted, nil)
	if err != nil {
		return BackupResult{}, err
	}
	return parseBackup(reply)
}

// RestoreOptions selects the name of a restored table.
type RestoreOptions struct {
	// As names the restored table; empty keeps the backed-up name.
	As string
}

// RestoreResult is the reply of RESTORE TABLE.
type RestoreResult struct {
	Table string
	Epoch string
	// Parent is the backed-up epoch and the revision the restored table forks from.
	Parent       Position
	Files, Bytes uint64
}

func parseRestore(reply Reply) (RestoreResult, error) {
	const command = "RESTORE TABLE"
	fields, err := replyFields(command, reply)
	if err != nil {
		return RestoreResult{}, err
	}
	var result RestoreResult
	for key, target := range map[string]*string{"table": &result.Table, "epoch": &result.Epoch, "parent_epoch": &result.Parent.Epoch} {
		if *target, err = fieldText(command, fields, key); err != nil {
			return RestoreResult{}, err
		}
	}
	for key, target := range map[string]*uint64{"fork_revision": &result.Parent.Revision, "files": &result.Files, "bytes": &result.Bytes} {
		if *target, err = fieldUint64(command, fields, key); err != nil {
			return RestoreResult{}, err
		}
	}
	return result, nil
}

// RestoreTable restores table from backup as a new table (RESTORE TABLE). It
// never replaces a table; use [Client.SwapTables] to serve it under the live
// name. It requires ADMIN on * and MANAGES USERS.
func (c *Client) RestoreTable(ctx context.Context, backup, table string, opts RestoreOptions) (RestoreResult, error) {
	const command = "RESTORE TABLE"
	if err := checkNames(command, "table", table); err != nil {
		return RestoreResult{}, err
	}
	quoted, err := quoteName(command, "backup", backup)
	if err != nil {
		return RestoreResult{}, err
	}
	statement := command + " " + table + " FROM " + quoted
	target := table
	if opts.As != "" {
		if err := checkNames(command, "table", opts.As); err != nil {
			return RestoreResult{}, err
		}
		statement += " AS " + opts.As
		target = opts.As
	}
	defer c.forgetSchema(target)
	reply, err := c.call(ctx, statement, nil)
	if err != nil {
		return RestoreResult{}, err
	}
	return parseRestore(reply)
}

// ReloadTLS reads the server's certificate and key files again for new
// connections (RELOAD TLS). It requires ADMIN on *.
func (c *Client) ReloadTLS(ctx context.Context) error {
	reply, err := c.call(ctx, "RELOAD TLS", nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, "RELOAD TLS", "OK")
}

// TableStats runs [Client.TableStats] on a leased client.
func (p *Pool) TableStats(ctx context.Context, table string) (TableStats, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (TableStats, error) { return c.TableStats(ctx, table) })
}

// CompactTombstones runs [Client.CompactTombstones] on a leased client.
func (p *Pool) CompactTombstones(ctx context.Context, table string, before uint64) (uint64, error) {
	return withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (uint64, error) {
		return c.CompactTombstones(ctx, table, before)
	})
}

// SwapTables runs [Client.SwapTables] on a leased client.
func (p *Pool) SwapTables(ctx context.Context, first, second string) error {
	_, err := withPooledClient(ctx, p, func(ctx context.Context, c *Client) (struct{}, error) {
		return struct{}{}, c.SwapTables(ctx, first, second)
	})
	return err
}

// RecoverTable runs [Client.RecoverTable] on a leased client.
func (p *Pool) RecoverTable(ctx context.Context, table string) error {
	return p.withTableClient(ctx, table, func(ctx context.Context, c *Client) error { return c.RecoverTable(ctx, table) })
}

// Migrations runs [Client.Migrations] on a leased client.
func (p *Pool) Migrations(ctx context.Context) ([]MigrationRecord, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]MigrationRecord, error) { return c.Migrations(ctx) })
}

// Backup runs [Client.Backup] on a leased client.
func (p *Pool) Backup(ctx context.Context, destination string) (BackupResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (BackupResult, error) { return c.Backup(ctx, destination) })
}

// RestoreTable runs [Client.RestoreTable] on a leased client.
func (p *Pool) RestoreTable(ctx context.Context, backup, table string, opts RestoreOptions) (RestoreResult, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (RestoreResult, error) {
		return c.RestoreTable(ctx, backup, table, opts)
	})
}

// ReloadTLS runs [Client.ReloadTLS] on a leased client.
func (p *Pool) ReloadTLS(ctx context.Context) error {
	_, err := withPooledClient(ctx, p, func(ctx context.Context, c *Client) (struct{}, error) { return struct{}{}, c.ReloadTLS(ctx) })
	return err
}
