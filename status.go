package chunkdb

import (
	"context"
	"unicode/utf8"
)

// FeatureStatus reports known enabled feature names and exact manifest masks.
// Fields retains the raw map, including unknown fields for diagnostics.
type FeatureStatus struct {
	Enabled                          []string
	Compat, ReadOnlyCompat, Incompat uint64
	Fields                           map[string]Reply
}

// TableFeatureStatus reports one table's enabled storage features.
type TableFeatureStatus struct {
	Name string
	// State is "ok" or "fenced"; Reason is the fence reason, nil when the
	// table is not fenced. Both are empty for servers that do not report them.
	State  string
	Reason *string
	FeatureStatus
}

// FencedTable is a fenced table whose features cannot be read, because its
// store is not open or its feed cleanup failed.
type FencedTable struct {
	Name, Reason string
}

// ServerStatus is a protected storage feature and activation snapshot.
// An unhealthy server returns its recovery error instead of a guessed snapshot.
type ServerStatus struct {
	// Version is the server version HELLO reports, UptimeMS the time since
	// the server started, and ReadOnly whether it runs with --read-only.
	// They are zero for servers that do not report them.
	Version           string
	UptimeMS          uint64
	ReadOnly          bool
	Directory         FeatureStatus
	Tables            []TableFeatureStatus
	Fenced            []FencedTable
	ActivationPending bool
	Fields            map[string]Reply
}

func statusFields(r Reply) (map[string]Reply, error) {
	if r.Kind != ReplyMap {
		return nil, protocolErrorf("SHOW STATUS", "expected a map")
	}
	fields := make(map[string]Reply, len(r.Map))
	for _, pair := range r.Map {
		if pair.Key.Kind != ReplyBulk {
			return nil, protocolErrorf("SHOW STATUS", "invalid map key")
		}
		fields[string(pair.Key.Bulk)] = cloneReply(pair.Value)
	}
	return fields, nil
}

func parseFeatureStatus(r Reply) (FeatureStatus, error) {
	var status FeatureStatus
	fields, err := statusFields(r)
	if err != nil {
		return status, err
	}
	status.Fields = fields
	enabled, ok := r.Lookup("enabled")
	if !ok || enabled.Kind != ReplyArray {
		return status, protocolErrorf("SHOW STATUS", "invalid enabled features")
	}
	status.Enabled = make([]string, 0, len(enabled.Array))
	for _, value := range enabled.Array {
		if value.Kind != ReplyBulk || !utf8.Valid(value.Bulk) {
			return status, protocolErrorf("SHOW STATUS", "invalid feature name")
		}
		status.Enabled = append(status.Enabled, string(value.Bulk))
	}
	for key, target := range map[string]*uint64{"compat": &status.Compat, "ro_compat": &status.ReadOnlyCompat, "incompat": &status.Incompat} {
		value, exists := r.Lookup(key)
		n, ok := value.Uint64()
		if !exists || !ok {
			return status, protocolErrorf("SHOW STATUS", "invalid %s mask", key)
		}
		*target = n
	}
	return status, nil
}

func parseStatus(r Reply) (ServerStatus, error) {
	var status ServerStatus
	fields, err := statusFields(r)
	if err != nil {
		return status, err
	}
	status.Fields = fields
	directory, exists := r.Lookup("directory")
	if !exists {
		return status, protocolErrorf("SHOW STATUS", "missing directory")
	}
	status.Directory, err = parseFeatureStatus(directory)
	if err != nil {
		return status, err
	}
	pending, exists := r.Lookup("activation_pending")
	if !exists || pending.Kind != ReplyBoolean {
		return status, protocolErrorf("SHOW STATUS", "invalid activation_pending")
	}
	status.ActivationPending = pending.Bool
	tables, exists := r.Lookup("tables")
	if !exists || tables.Kind != ReplyArray {
		return status, protocolErrorf("SHOW STATUS", "invalid tables")
	}
	status.Tables = make([]TableFeatureStatus, 0, len(tables.Array))
	for _, entry := range tables.Array {
		name, exists := entry.Lookup("name")
		if !exists || name.Kind != ReplyBulk || !isName(string(name.Bulk)) {
			return status, protocolErrorf("SHOW STATUS", "invalid table name")
		}
		features, err := parseFeatureStatus(entry)
		if err != nil {
			return status, err
		}
		table := TableFeatureStatus{Name: string(name.Bulk), FeatureStatus: features}
		if state, exists := entry.Lookup("state"); exists {
			if state.Kind != ReplyBulk || (string(state.Bulk) != "ok" && string(state.Bulk) != "fenced") {
				return status, protocolErrorf("SHOW STATUS", "invalid table state")
			}
			table.State = string(state.Bulk)
			reason, _ := entry.Lookup("reason")
			if reason.Kind == ReplyBulk {
				text := string(reason.Bulk)
				table.Reason = &text
			} else if reason.Kind != ReplyNull {
				return status, protocolErrorf("SHOW STATUS", "invalid table reason")
			}
			if (table.State == "fenced") != (table.Reason != nil) {
				return status, protocolErrorf("SHOW STATUS", "table state and reason disagree")
			}
		}
		status.Tables = append(status.Tables, table)
	}
	if version, exists := r.Lookup("version"); exists {
		if version.Kind != ReplyBulk || !utf8.Valid(version.Bulk) {
			return status, protocolErrorf("SHOW STATUS", "invalid version")
		}
		status.Version = string(version.Bulk)
	}
	if uptime, exists := r.Lookup("uptime_ms"); exists {
		var ok bool
		if status.UptimeMS, ok = uptime.Uint64(); !ok {
			return status, protocolErrorf("SHOW STATUS", "invalid uptime_ms")
		}
	}
	if readOnly, exists := r.Lookup("read_only"); exists {
		if readOnly.Kind != ReplyBoolean {
			return status, protocolErrorf("SHOW STATUS", "invalid read_only")
		}
		status.ReadOnly = readOnly.Bool
	}
	if fenced, exists := r.Lookup("fenced"); exists {
		if fenced.Kind != ReplyArray {
			return status, protocolErrorf("SHOW STATUS", "invalid fenced tables")
		}
		for _, entry := range fenced.Array {
			name, _ := entry.Lookup("name")
			reason, _ := entry.Lookup("reason")
			if name.Kind != ReplyBulk || !isName(string(name.Bulk)) || reason.Kind != ReplyBulk || !utf8.Valid(reason.Bulk) {
				return status, protocolErrorf("SHOW STATUS", "invalid fenced table")
			}
			status.Fenced = append(status.Fenced, FencedTable{Name: string(name.Bulk), Reason: string(reason.Bulk)})
		}
	}
	return status, nil
}

// Status asks SHOW STATUS for enabled directory and table storage features.
// It requires ADMIN on *; limits and HELLO capabilities are separate metadata.
func (c *Client) Status(ctx context.Context) (ServerStatus, error) {
	reply, err := c.call(ctx, "SHOW STATUS", nil)
	if err != nil {
		return ServerStatus{}, err
	}
	return parseStatus(reply)
}

// Status runs Client.Status on a leased client.
func (p *Pool) Status(ctx context.Context) (ServerStatus, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (ServerStatus, error) { return c.Status(ctx) })
}
