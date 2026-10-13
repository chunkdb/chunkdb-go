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
	FeatureStatus
}

// ServerStatus is a protected storage feature and activation snapshot.
// An unhealthy server returns its recovery error instead of a guessed snapshot.
type ServerStatus struct {
	Directory         FeatureStatus
	Tables            []TableFeatureStatus
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
		status.Tables = append(status.Tables, TableFeatureStatus{Name: string(name.Bulk), FeatureStatus: features})
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
