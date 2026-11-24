package chunkdb

import (
	"context"
	"strings"
)

// Check is a CHECK constraint of a table: a condition over a block's columns
// and position that no present block may make false; NULL passes.
type Check struct {
	Name, Expression string
}

func (c *Client) alterColumn(ctx context.Context, table, column, clause string) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "column", column); err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" ALTER COLUMN "+column+" "+clause)
}

// SetColumnDefault sets the default of a column of table ("" is the default
// table), the value a new block takes when a write gives none. The value is
// checked against the column type of the client's cached schema.
func (c *Client) SetColumnDefault(ctx context.Context, table, column string, value any) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if value == nil {
		return requestErrorf(command, "a default needs a value; DropColumnDefault removes it")
	}
	schema, err := c.Schema(ctx, name)
	if err != nil {
		return err
	}
	target, ok := schema.Column(column)
	if !ok {
		return requestErrorf(command, "table %s has no column %s", name, column)
	}
	normalized, err := normalize(column, target.Type, value)
	if err != nil {
		return err
	}
	return c.alterColumn(ctx, name, column, "SET DEFAULT "+literal(normalized))
}

// DropColumnDefault removes the default of a column.
func (c *Client) DropColumnDefault(ctx context.Context, table, column string) error {
	return c.alterColumn(ctx, table, column, "DROP DEFAULT")
}

// SetColumnNull lets a column hold NULL (SET NULL), or no longer (DROP NULL,
// refused while a present block holds NULL in it).
func (c *Client) SetColumnNull(ctx context.Context, table, column string, null bool) error {
	if null {
		return c.alterColumn(ctx, table, column, "SET NULL")
	}
	return c.alterColumn(ctx, table, column, "DROP NULL")
}

// SetColumnRequired makes every write of a new block give the column (SET
// REQUIRED), or no longer (DROP REQUIRED).
func (c *Client) SetColumnRequired(ctx context.Context, table, column string, required bool) error {
	if required {
		return c.alterColumn(ctx, table, column, "SET REQUIRED")
	}
	return c.alterColumn(ctx, table, column, "DROP REQUIRED")
}

// AddCheck adds a CHECK constraint (ALTER TABLE ... ADD CHECK) to table, named
// name or by the server when name is empty. It is refused when a present
// block makes the condition false; later writes that would are refused with
// [CodeCheckFailed].
func (c *Client) AddCheck(ctx context.Context, table, name, condition string) error {
	const command = "ALTER TABLE"
	tableName, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if strings.TrimSpace(condition) == "" {
		return requestErrorf(command, "a CHECK needs a condition")
	}
	statement := "ALTER TABLE " + tableName + " ADD CHECK "
	if name != "" {
		if err := checkNames(command, "check", name); err != nil {
			return err
		}
		statement += name + " "
	}
	return c.tableStatement(ctx, tableName, statement+"("+condition+")")
}

// DropCheck drops a CHECK constraint by name; with ifExists an absent one is
// no error.
func (c *Client) DropCheck(ctx context.Context, table, name string, ifExists bool) error {
	const command = "ALTER TABLE"
	tableName, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "check", name); err != nil {
		return err
	}
	statement := "ALTER TABLE " + tableName + " DROP CHECK "
	if ifExists {
		statement += "IF EXISTS "
	}
	return c.tableStatement(ctx, tableName, statement+name)
}

// RenameTable renames table (ALTER TABLE ... RENAME TO). Its specific grants
// move to the new name; it needs ADMIN on the table and on the new name.
func (c *Client) RenameTable(ctx context.Context, table, newName string) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "table", newName); err != nil {
		return err
	}
	defer c.forgetSchema(newName)
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" RENAME TO "+newName)
}

func (p *Pool) poolTable(ctx context.Context, table string, fn func(ctx context.Context, c *Client) error) error {
	_, err := withPooledTableClient(ctx, p, table, func(ctx context.Context, c *Client) (struct{}, error) { return struct{}{}, fn(ctx, c) })
	return err
}

// SetColumnDefault runs [Client.SetColumnDefault] on a leased client.
func (p *Pool) SetColumnDefault(ctx context.Context, table, column string, value any) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.SetColumnDefault(ctx, table, column, value) })
}

// DropColumnDefault runs [Client.DropColumnDefault] on a leased client.
func (p *Pool) DropColumnDefault(ctx context.Context, table, column string) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.DropColumnDefault(ctx, table, column) })
}

// SetColumnNull runs [Client.SetColumnNull] on a leased client.
func (p *Pool) SetColumnNull(ctx context.Context, table, column string, null bool) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.SetColumnNull(ctx, table, column, null) })
}

// SetColumnRequired runs [Client.SetColumnRequired] on a leased client.
func (p *Pool) SetColumnRequired(ctx context.Context, table, column string, required bool) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error {
		return c.SetColumnRequired(ctx, table, column, required)
	})
}

// AddCheck runs [Client.AddCheck] on a leased client.
func (p *Pool) AddCheck(ctx context.Context, table, name, condition string) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.AddCheck(ctx, table, name, condition) })
}

// DropCheck runs [Client.DropCheck] on a leased client.
func (p *Pool) DropCheck(ctx context.Context, table, name string, ifExists bool) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.DropCheck(ctx, table, name, ifExists) })
}

// RenameTable runs [Client.RenameTable] on a leased client.
func (p *Pool) RenameTable(ctx context.Context, table, newName string) error {
	return p.poolTable(ctx, table, func(ctx context.Context, c *Client) error { return c.RenameTable(ctx, table, newName) })
}
