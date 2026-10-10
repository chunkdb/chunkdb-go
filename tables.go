package chunkdb

import (
	"context"
	"reflect"
	"strconv"
	"strings"
)

// Tables lists the table names (SHOW TABLES).
func (c *Client) Tables(ctx context.Context) ([]string, error) {
	const command = "SHOW TABLES"
	reply, err := c.call(ctx, command, nil)
	if err != nil {
		return nil, err
	}
	if reply.Kind != ReplyArray {
		return nil, protocolErrorf(command, "expected an array, got a reply of kind %d", reply.Kind)
	}
	names := make([]string, 0, len(reply.Array))
	for _, item := range reply.Array {
		name, err := expectBulk(item, command)
		if err != nil {
			return nil, err
		}
		names = append(names, string(name))
	}
	return names, nil
}

// CreateTable creates table ("" is the default table). Its chunk and
// large-chunk sizes are fixed once it exists; its columns and options change
// with the ALTER methods.
func (c *Client) CreateTable(ctx context.Context, table string, spec TableSpec) error {
	const command = "CREATE TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if len(spec.Columns) == 0 {
		return requestErrorf(command, "a table needs at least one column")
	}
	if spec.ChunkWidth <= 0 || spec.ChunkHeight <= 0 {
		return requestErrorf(command, "the chunk size must be positive, got %dx%d", spec.ChunkWidth, spec.ChunkHeight)
	}
	if spec.LargeWidth < 0 || spec.LargeHeight < 0 || (spec.LargeWidth == 0) != (spec.LargeHeight == 0) {
		return requestErrorf(command, "the large-chunk size must be both zero or both positive, got %dx%d",
			spec.LargeWidth, spec.LargeHeight)
	}
	definitions := make([]string, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		definition, err := columnDefinition(column)
		if err != nil {
			return annotate(command, err)
		}
		definitions = append(definitions, definition)
	}
	statement := "CREATE TABLE " + name + " (" + strings.Join(definitions, ", ") + ") CHUNK " +
		strconv.Itoa(spec.ChunkWidth) + " x " + strconv.Itoa(spec.ChunkHeight)
	if spec.LargeWidth > 0 {
		statement += " LARGE " + strconv.Itoa(spec.LargeWidth) + " x " + strconv.Itoa(spec.LargeHeight)
	}
	if options := optionAssignments(spec.Options); len(options) > 0 {
		statement += " WITH " + strings.Join(options, ", ")
	}
	return c.tableStatement(ctx, name, statement)
}

// columnDefinition formats a column of CREATE TABLE or ADD COLUMN.
func columnDefinition(column ColumnDef) (string, error) {
	if err := checkNames("", "column", column.Name); err != nil {
		return "", err
	}
	if err := column.Type.validate(); err != nil {
		return "", err
	}
	definition := column.Name + " " + column.Type.String()
	if column.Null {
		definition += " NULL"
	}
	if column.Required {
		definition += " REQUIRED"
	}
	if column.Default != nil {
		value, err := normalize(column.Name, column.Type, column.Default)
		if err != nil {
			return "", err
		}
		definition += " DEFAULT " + literal(value)
	}
	return definition, nil
}

func optionAssignments(options TableOptions) []string {
	var out []string
	text := func(name, value string) {
		if value != "" {
			out = append(out, name+" = "+literal(value))
		}
	}
	number := func(name string, value uint64) {
		if value != 0 {
			out = append(out, name+" = "+strconv.FormatUint(value, 10))
		}
	}
	text("durability_mode", options.DurabilityMode)
	number("checkpoint_updates", options.CheckpointUpdates)
	number("checkpoint_wal_bytes", options.CheckpointWalBytes)
	number("wal_group_commit_updates", options.WalGroupCommitUpdates)
	text("checkpoint_compression", options.CheckpointCompression)
	number("var_max_chunk_bytes", options.VarMaxChunkBytes)
	return out
}

// AddColumn adds a column to table ("" is the default table). A REQUIRED
// column needs a default when the table has present blocks. Chunks written
// before take its default, or NULL.
func (c *Client) AddColumn(ctx context.Context, table string, column ColumnDef) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	definition, err := columnDefinition(column)
	if err != nil {
		return annotate(command, err)
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" ADD COLUMN "+definition)
}

// DropColumn drops a column of table ("" is the default table). The last
// fixed-width column cannot be dropped.
func (c *Client) DropColumn(ctx context.Context, table, column string) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "column", column); err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" DROP COLUMN "+column)
}

// RenameColumn renames a column of table ("" is the default table).
func (c *Client) RenameColumn(ctx context.Context, table, column, newName string) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "column", column, newName); err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" RENAME COLUMN "+column+" TO "+newName)
}

// AlterColumnType changes the type of a column of table ("" is the default
// table) within its family (integers, floats, text, bytes, bits). A type that
// holds every value changes at once; for a narrower one, conversion says what
// happens to stored values that do not fit ([ConvertNone] fails on the first).
func (c *Client) AlterColumnType(ctx context.Context, table, column string, t ColumnType, conversion Conversion) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "column", column); err != nil {
		return err
	}
	if err := t.validate(); err != nil {
		return annotate(command, err)
	}
	statement := "ALTER TABLE " + name + " ALTER COLUMN " + column + " TYPE " + t.String()
	switch conversion {
	case ConvertNone:
	case ConvertClamp:
		statement += " USING CLAMP"
	case ConvertDefault:
		statement += " USING DEFAULT"
	case ConvertTruncate:
		statement += " USING TRUNCATE"
	default:
		return requestErrorf(command, "unknown conversion %d", conversion)
	}
	return c.tableStatement(ctx, name, statement)
}

// SetTableOption sets one option of table ("" is the default table), by its
// name as [TableOptions] documents it (for example "durability_mode" or
// "var_max_chunk_bytes"). value is a string or a non-negative integer.
func (c *Client) SetTableOption(ctx context.Context, table, option string, value any) error {
	const command = "ALTER TABLE"
	name, err := c.tableName(command, table)
	if err != nil {
		return err
	}
	if err := checkNames(command, "option", option); err != nil {
		return err
	}
	var text string
	if reflect.ValueOf(value).Kind() == reflect.String {
		text = literal(reflect.ValueOf(value).String())
	} else if negative, magnitude, ok := integerOf(value); ok && !negative {
		text = strconv.FormatUint(magnitude, 10)
	} else {
		return requestErrorf(command, "option %s takes a string or a non-negative integer, got %T", option, value)
	}
	return c.tableStatement(ctx, name, "ALTER TABLE "+name+" SET "+option+" = "+text)
}

// DropTable drops table ("" is the default table) and its data.
func (c *Client) DropTable(ctx context.Context, table string) error {
	name, err := c.tableName("DROP TABLE", table)
	if err != nil {
		return err
	}
	return c.tableStatement(ctx, name, "DROP TABLE "+name)
}

// tableStatement sends a statement that changes table and drops the cached
// schema of it, whatever the outcome.
func (c *Client) tableStatement(ctx context.Context, table, statement string) error {
	defer c.forgetSchema(table)
	reply, err := c.call(ctx, statement, nil)
	if err != nil {
		return err
	}
	return expectSimple(reply, commandOf(statement), "OK")
}
