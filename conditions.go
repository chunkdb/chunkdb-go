package chunkdb

import (
	"strings"
)

// Arg is a $n parameter of a condition or a computed value. The client
// encodes Value with the type of Column, the column the parameter meets on the
// server: a block column, or a chunk column written as "chunk.name". nil is
// NULL.
type Arg struct {
	Column string
	Value  any
}

// Condition is a CQL condition, such as "height > $1 AND kind = $2", with its
// parameters: Args[0] is $1. See the server's CQL reference for the grammar.
type Condition struct {
	Text string
	Args []Arg
}

// Where returns a [Condition] with the given parameters.
func Where(text string, args ...Arg) *Condition {
	return &Condition{Text: text, Args: args}
}

// Expr is a computed value in a [Client.SetBlock] record, such as
// Expr("coins + $1"): the server evaluates it against the block as it is when
// the write applies. Its $n parameters are the ones given with [WithArgs].
type Expr string

// If makes a block write apply only when condition holds for the block as it
// is when the write applies. A false or NULL condition writes nothing and
// fails with server code [CodeConditionFailed], whose metadata Current is
// the chunk's version. Its $n parameters are the ones given with [WithArgs].
// It excludes [IfVersion].
func If(condition string) WriteOption {
	return func(o *writeOptions) { o.condition = &condition }
}

// WithArgs gives the $n parameters of a block write's [If] condition and
// [Expr] values: args[0] is $1. The written plain values are numbered after
// them. Each $n appears once in the statement, so a value used twice is
// passed as two arguments.
func WithArgs(args ...Arg) WriteOption {
	return func(o *writeOptions) { o.args = append(o.args, args...) }
}

// blockClause is the text after the assignments of a block write and the
// parameters it numbers first.
type blockClause struct {
	text string
	args []Arg
}

func blockWriteClause(command string, opts []WriteOption) (blockClause, error) {
	options := resolveWriteOptions(opts)
	if options.ifAbsent {
		return blockClause{}, requestErrorf(command, "IF ABSENT needs a chunk write without IF VERSION")
	}
	if options.condition != nil {
		if options.ifVersion != nil {
			return blockClause{}, requestErrorf(command, "IF VERSION and an IF condition exclude each other")
		}
		if strings.TrimSpace(*options.condition) == "" {
			return blockClause{}, requestErrorf(command, "the IF condition is empty")
		}
		return blockClause{text: " IF " + *options.condition, args: options.args}, nil
	}
	clause, err := writeClause(command, opts, false)
	return blockClause{text: clause, args: options.args}, err
}

// encodeArgs encodes parameters with the types of the columns they meet.
func encodeArgs(command string, schema *Schema, args []Arg) ([][]byte, error) {
	params := make([][]byte, 0, len(args))
	for i, arg := range args {
		var column Column
		var ok bool
		if name, chunk := strings.CutPrefix(arg.Column, "chunk."); chunk {
			for _, c := range schema.ChunkColumns {
				if c.Name == name {
					column, ok = c, true
				}
			}
		} else {
			column, ok = schema.Column(arg.Column)
		}
		if !ok {
			return nil, &staleSchemaError{err: requestErrorf(command, "parameter $%d names column %q, which table %s does not have", i+1, arg.Column, schema.Table), local: true}
		}
		value, err := normalize(arg.Column, column.Type, arg.Value)
		if err != nil {
			return nil, err
		}
		var param []byte
		if value != nil {
			param = encodeNormalized(value)
		}
		params = append(params, param)
	}
	return params, nil
}

func conditionText(command string, where *Condition) (string, error) {
	if where == nil {
		return "", nil
	}
	if strings.TrimSpace(where.Text) == "" {
		return "", requestErrorf(command, "the WHERE condition is empty")
	}
	return " WHERE " + where.Text, nil
}

func conditionArgs(where *Condition) []Arg {
	if where == nil {
		return nil
	}
	return where.Args
}

// txBlockClause is blockWriteClause for a transaction, which takes no IF
// VERSION or IF ABSENT.
func txBlockClause(command string, opts []WriteOption) (blockClause, error) {
	options := resolveWriteOptions(opts)
	if options.ifVersion != nil || options.ifAbsent {
		return blockClause{}, requestErrorf(command, "a transaction takes no IfVersion or IfAbsent: COMMIT checks every chunk it read or wrote")
	}
	return blockWriteClause(command, opts)
}
