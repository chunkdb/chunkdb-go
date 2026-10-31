package chunkdb

import (
	"context"
	"strings"
)

// SlotOptions persists a consumer's fixed filters. Changing them requires a
// new slot. Columns retain their declared order; nil selects every column.
type SlotOptions struct {
	Area        *Area
	Area3D      *Area3D
	Columns     []string
	NoBefore    bool
	Chunks      bool
	IfNotExists bool
}

func feedFilterClause(command string, area *Area, area3D *Area3D, columns []string, noBefore, chunks bool) (string, error) {
	clause := ""
	if area != nil && area3D != nil {
		return "", requestErrorf(command, "AREA needs either two or three dimensions")
	}
	if area3D != nil {
		if area3D.CX0 > area3D.CX1 || area3D.CY0 > area3D.CY1 || area3D.CZ0 > area3D.CZ1 {
			return "", requestErrorf(command, "inverted area")
		}
		clause += " AREA " + areaRange3D(area3D.CX0, area3D.CY0, area3D.CZ0, area3D.CX1, area3D.CY1, area3D.CZ1)
	}
	if area != nil {
		if area.CX0 > area.CX1 || area.CY0 > area.CY1 {
			return "", requestErrorf(command, "inverted area")
		}
		clause += " AREA " + areaRange(area.CX0, area.CY0, area.CX1, area.CY1)
	}
	if columns != nil {
		if len(columns) == 0 {
			return "", requestErrorf(command, "COLUMNS requires at least one column")
		}
		if err := checkNames(command, "column", columns...); err != nil {
			return "", err
		}
		seen := map[string]bool{}
		for _, column := range columns {
			if seen[column] {
				return "", requestErrorf(command, "duplicate column %s", column)
			}
			seen[column] = true
		}
		clause += " COLUMNS (" + strings.Join(columns, ", ") + ")"
	}
	if noBefore {
		clause += " NO BEFORE"
	}
	if chunks {
		clause += " CHUNKS"
	}
	return clause, nil
}

// CreateSlotWithOptions creates a durable slot with fixed filters. A watch
// omitting filters inherits them; explicit watch filters must match them.
// IF NOT EXISTS also requires matching filters on an existing slot.
func (c *Client) CreateSlotWithOptions(ctx context.Context, table, name string, opts SlotOptions) error {
	const command = "CREATE SLOT"
	if err := slotName(command, name); err != nil {
		return err
	}
	table, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	filters, err := feedFilterClause(command, opts.Area, opts.Area3D, opts.Columns, opts.NoBefore, opts.Chunks)
	if err != nil {
		return err
	}
	statement := command
	if opts.IfNotExists {
		statement += " IF NOT EXISTS"
	}
	statement += " '" + name + "' ON " + table + filters
	reply, err := c.call(ctx, statement, nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, command, "OK")
}

// CreateSlotWithOptions runs Client.CreateSlotWithOptions on a leased client.
func (p *Pool) CreateSlotWithOptions(ctx context.Context, table, name string, opts SlotOptions) error {
	return p.withTableClient(ctx, table, func(ctx context.Context, c *Client) error { return c.CreateSlotWithOptions(ctx, table, name, opts) })
}
