package chunkdb

import "context"

// EnableFeature explicitly enables a monotonic storage feature in the server's
// data directory. It requires ADMIN on * and cannot disable a feature.
// Unknown names are sent to the server and rejected as INVALID_ARGUMENT.
func (c *Client) EnableFeature(ctx context.Context, name string) error {
	const command = "ENABLE FEATURE"
	if err := checkNames(command, "feature", name); err != nil {
		return err
	}
	reply, err := c.call(ctx, command+" "+name, nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, command, "OK")
}

// EnableFeature runs Client.EnableFeature on a leased client.
func (p *Pool) EnableFeature(ctx context.Context, name string) error {
	return p.WithClient(ctx, func(ctx context.Context, c *Client) error { return c.EnableFeature(ctx, name) })
}
