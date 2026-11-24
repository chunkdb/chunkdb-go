package chunkdb

// Guard is the optional existence clause of a create, add or drop helper.
// [IfNotExists] skips an object that already exists, and [IfExists] one that
// is absent; the server answers OK either way, still checks rights, and does
// not compare definitions.
type Guard string

const (
	// IfNotExists is the guard of CreateTable, AddColumn, AddChunkColumn and
	// CreateUser.
	IfNotExists Guard = "IF NOT EXISTS"
	// IfExists is the guard of DropTable, DropColumn, DropChunkColumn,
	// DropUser and DropSlot.
	IfExists Guard = "IF EXISTS"
)

// guardClause returns " IF NOT EXISTS" or " IF EXISTS" when guards holds the
// one guard the statement takes, and "" when it holds none.
func guardClause(command string, want Guard, guards []Guard) (string, error) {
	switch {
	case len(guards) == 0:
		return "", nil
	case len(guards) == 1 && guards[0] == want:
		return " " + string(want), nil
	}
	return "", requestErrorf(command, "%s takes no guard but %s, once", command, want)
}
