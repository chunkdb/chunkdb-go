package chunkdb

import (
	"context"
	"strconv"
	"unicode/utf8"
)

// ConnectionInfo is one live connection visible to the requesting user.
// User and Statement are nil when the connection has no authenticated user or active statement.
type ConnectionInfo struct {
	ID        uint64
	User      *string
	Peer      string
	State     string
	Statement *string
}

// ShowConnections lists connections owned by the current user incarnation.
// ADMIN on * permits listing other users' connections.
func (c *Client) ShowConnections(ctx context.Context) ([]ConnectionInfo, error) {
	reply, err := c.call(ctx, "SHOW CONNECTIONS", nil)
	if err != nil {
		return nil, err
	}
	return parseConnections(reply)
}

func parseConnections(reply Reply) ([]ConnectionInfo, error) {
	bad := func() error { return protocolErrorf("SHOW CONNECTIONS", "invalid connection listing") }
	if reply.Kind != ReplyArray {
		return nil, bad()
	}
	connections := make([]ConnectionInfo, 0, len(reply.Array))
	seen := make(map[uint64]struct{}, len(reply.Array))
	for _, item := range reply.Array {
		id, exists := item.Lookup("id")
		n, valid := id.Uint64()
		if item.Kind != ReplyMap || !exists || !valid || n == 0 {
			return nil, bad()
		}
		if _, duplicate := seen[n]; duplicate {
			return nil, bad()
		}
		seen[n] = struct{}{}
		connection := ConnectionInfo{ID: n}
		for _, field := range []struct {
			name   string
			target **string
		}{{"user", &connection.User}, {"statement", &connection.Statement}} {
			value, exists := item.Lookup(field.name)
			if !exists || (value.Kind != ReplyNull && (value.Kind != ReplyBulk || !utf8.Valid(value.Bulk))) {
				return nil, bad()
			}
			if value.Kind == ReplyBulk {
				text := string(value.Bulk)
				*field.target = &text
			}
		}
		for _, field := range []struct {
			name   string
			target *string
		}{{"peer", &connection.Peer}, {"state", &connection.State}} {
			value, exists := item.Lookup(field.name)
			if !exists || value.Kind != ReplyBulk || !utf8.Valid(value.Bulk) {
				return nil, bad()
			}
			*field.target = string(value.Bulk)
		}
		connections = append(connections, connection)
	}
	return connections, nil
}

// Cancel asks the server to cancel the target connection's current statement.
// False means no cancellable statement was found, including one past its decision boundary.
func (c *Client) Cancel(ctx context.Context, connectionID uint64) (bool, error) {
	return c.controlConnection(ctx, "CANCEL", "cancelled", connectionID)
}

// KillConnection closes the target connection after its current work is stopped safely.
// Killing the calling connection returns the acknowledgment before the socket closes.
func (c *Client) KillConnection(ctx context.Context, connectionID uint64) (bool, error) {
	return c.controlConnection(ctx, "KILL CONNECTION", "killed", connectionID)
}

func (c *Client) controlConnection(ctx context.Context, command, field string, id uint64) (bool, error) {
	if id == 0 {
		return false, requestErrorf(command, "connection ID must be positive")
	}
	reply, err := c.call(ctx, command+" "+strconv.FormatUint(id, 10), nil)
	if err != nil {
		return false, err
	}
	value, exists := reply.Lookup(field)
	if reply.Kind != ReplyMap || !exists || value.Kind != ReplyBoolean {
		return false, protocolErrorf(command, "invalid %s acknowledgment", field)
	}
	return value.Bool, nil
}

// ShowConnections lists live connections using a leased client.
func (p *Pool) ShowConnections(ctx context.Context) ([]ConnectionInfo, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]ConnectionInfo, error) { return c.ShowConnections(ctx) })
}

// Cancel cancels the target connection's active statement using a leased client.
func (p *Pool) Cancel(ctx context.Context, id uint64) (bool, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (bool, error) { return c.Cancel(ctx, id) })
}

// KillConnection closes the target connection using a leased client.
func (p *Pool) KillConnection(ctx context.Context, id uint64) (bool, error) {
	return withPooledClient(ctx, p, func(ctx context.Context, c *Client) (bool, error) { return c.KillConnection(ctx, id) })
}
