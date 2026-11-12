package chunkdb

import "context"

// ChunkColumn returns a named column of the row attached to a chunk.
func (s *Schema) ChunkColumn(name string) (Column, bool) {
	for _, column := range s.ChunkColumns {
		if column.Name == name {
			return column, true
		}
	}
	return Column{}, false
}

// AddChunkColumn adds a field to the row attached to every chunk.
func (c *Client) AddChunkColumn(ctx context.Context, table string, column ColumnDef) error {
	name, err := c.tableName("ALTER TABLE", table)
	if err != nil {
		return err
	}
	definition, err := columnDefinition(column)
	if err != nil {
		return annotate("ALTER TABLE", err)
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" ADD CHUNK COLUMN "+definition)
}

// DropChunkColumn removes a chunk field and its values.
func (c *Client) DropChunkColumn(ctx context.Context, table, column string) error {
	name, err := c.tableName("ALTER TABLE", table)
	if err != nil {
		return err
	}
	if err := checkNames("ALTER TABLE", "column", column); err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" DROP CHUNK COLUMN "+column)
}

// RenameChunkColumn changes a chunk field's name while preserving its ID.
func (c *Client) RenameChunkColumn(ctx context.Context, table, oldName, newName string) error {
	name, err := c.tableName("ALTER TABLE", table)
	if err != nil {
		return err
	}
	if err := checkNames("ALTER TABLE", "column", oldName, newName); err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" RENAME CHUNK COLUMN "+oldName+" TO "+newName)
}

// AddChunkColumn runs Client.AddChunkColumn on a leased client.
func (p *Pool) AddChunkColumn(ctx context.Context, table string, column ColumnDef) error {
	return p.withTableClient(ctx, table, func(ctx context.Context, c *Client) error { return c.AddChunkColumn(ctx, table, column) })
}

// DropChunkColumn runs Client.DropChunkColumn on a leased client.
func (p *Pool) DropChunkColumn(ctx context.Context, table, column string) error {
	return p.withTableClient(ctx, table, func(ctx context.Context, c *Client) error { return c.DropChunkColumn(ctx, table, column) })
}

// RenameChunkColumn runs Client.RenameChunkColumn on a leased client.
func (p *Pool) RenameChunkColumn(ctx context.Context, table, oldName, newName string) error {
	return p.withTableClient(ctx, table, func(ctx context.Context, c *Client) error { return c.RenameChunkColumn(ctx, table, oldName, newName) })
}
