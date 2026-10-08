package chunkdb

import (
	"context"
)

// Right is a user's right on a table. Rights are ordered: [RightAdmin]
// includes [RightWrite], which includes [RightRead].
type Right string

const (
	// RightRead reads blocks, chunks and areas, scans and DESCRIBE.
	RightRead Right = "READ"
	// RightWrite also writes blocks and chunks.
	RightWrite Right = "WRITE"
	// RightAdmin also alters and drops the table; on [AllTables] it creates
	// tables and reads metrics.
	RightAdmin Right = "ADMIN"
)

// AllTables in [Client.Grant] and [Client.Revoke] stands for every table,
// those created later included.
const AllTables = "*"

// User is one user as [Client.Users] lists it.
type User struct {
	Name         string
	ManagesUsers bool
	// Grants maps a table name, or [AllTables], to the user's right on it.
	Grants map[string]Right
}

// CreateUserOptions configure [Client.CreateUser].
type CreateUserOptions struct {
	// ManagesUsers lets the user create, change and drop users and grant and
	// revoke rights.
	ManagesUsers bool
}

// CreateUser creates a user with password. The client computes the user's
// SCRAM verifier ([ComputeVerifier], [Options.VerifierIterations]) and sends
// only that; the server never receives the password. It needs MANAGES USERS.
func (c *Client) CreateUser(ctx context.Context, name, password string, opts CreateUserOptions) error {
	const command = "CREATE USER"
	if err := checkNames(command, "user", name); err != nil {
		return err
	}
	verifier, err := ComputeVerifier(password, c.opts.verifierIterations)
	if err != nil {
		return annotate(command, err)
	}
	statement := "CREATE USER " + name + " VERIFIER $1"
	if opts.ManagesUsers {
		statement += " MANAGES USERS"
	}
	return c.userStatement(ctx, statement, []byte(verifier))
}

// SetPassword changes the password of a user, sending a new verifier as
// [Client.CreateUser] does. A user may change their own password; another
// user's needs MANAGES USERS. Connections already logged in stay logged in.
func (c *Client) SetPassword(ctx context.Context, name, password string) error {
	const command = "ALTER USER"
	if err := checkNames(command, "user", name); err != nil {
		return err
	}
	verifier, err := ComputeVerifier(password, c.opts.verifierIterations)
	if err != nil {
		return annotate(command, err)
	}
	return c.userStatement(ctx, "ALTER USER "+name+" VERIFIER $1", []byte(verifier))
}

// SetManagesUsers gives or takes away a user's right to manage users. The
// last user who manages users cannot lose it.
func (c *Client) SetManagesUsers(ctx context.Context, name string, manages bool) error {
	if err := checkNames("ALTER USER", "user", name); err != nil {
		return err
	}
	statement := "ALTER USER " + name + " MANAGES USERS"
	if !manages {
		statement = "ALTER USER " + name + " NO MANAGES USERS"
	}
	return c.userStatement(ctx, statement, nil)
}

// DropUser drops a user. The last user who manages users cannot be dropped.
func (c *Client) DropUser(ctx context.Context, name string) error {
	if err := checkNames("DROP USER", "user", name); err != nil {
		return err
	}
	return c.userStatement(ctx, "DROP USER "+name, nil)
}

// Grant gives user right on table: a table name, [AllTables], or "" for the
// client's default table. A grant on a table goes when the table is dropped.
func (c *Client) Grant(ctx context.Context, right Right, table, user string) error {
	return c.rightStatement(ctx, "GRANT", right, table, "TO", user)
}

// Revoke takes away right on table (as for [Client.Grant]) from user, and the
// rights above it: after revoking [RightWrite], a user who had [RightAdmin]
// keeps [RightRead].
func (c *Client) Revoke(ctx context.Context, right Right, table, user string) error {
	return c.rightStatement(ctx, "REVOKE", right, table, "FROM", user)
}

// Users lists the users and their rights (SHOW USERS). It needs MANAGES
// USERS.
func (c *Client) Users(ctx context.Context) ([]User, error) {
	const command = "SHOW USERS"
	reply, err := c.call(ctx, command, nil)
	if err != nil {
		return nil, err
	}
	if reply.Kind != ReplyArray {
		return nil, protocolErrorf(command, "expected an array, got a reply of kind %d", reply.Kind)
	}
	users := make([]User, 0, len(reply.Array))
	for _, item := range reply.Array {
		if item.Kind != ReplyMap {
			return nil, protocolErrorf(command, "expected a map per user, got a reply of kind %d", item.Kind)
		}
		name, _ := item.Lookup("name")
		manages, _ := item.Lookup("manages_users")
		grants, _ := item.Lookup("grants")
		if name.Kind != ReplyBulk || manages.Kind != ReplyBoolean || grants.Kind != ReplyMap {
			return nil, protocolErrorf(command, "a user needs name, manages_users and grants")
		}
		user := User{Name: string(name.Bulk), ManagesUsers: manages.Bool, Grants: make(map[string]Right, len(grants.Map))}
		for _, grant := range grants.Map {
			if grant.Key.Kind != ReplyBulk || grant.Value.Kind != ReplyBulk {
				return nil, protocolErrorf(command, "the grants of user %s are not table names and rights", user.Name)
			}
			user.Grants[string(grant.Key.Bulk)] = Right(grant.Value.Bulk)
		}
		users = append(users, user)
	}
	return users, nil
}

func (c *Client) rightStatement(ctx context.Context, verb string, right Right, table, preposition, user string) error {
	switch right {
	case RightRead, RightWrite, RightAdmin:
	default:
		return requestErrorf(verb, "unknown right %q: rights are READ, WRITE and ADMIN", right)
	}
	if table != AllTables {
		var err error
		if table, err = c.tableName(verb, table); err != nil {
			return err
		}
	}
	if err := checkNames(verb, "user", user); err != nil {
		return err
	}
	return c.userStatement(ctx, verb+" "+string(right)+" ON "+table+" "+preposition+" "+user, nil)
}

// userStatement sends a user or right statement, which answers +OK.
func (c *Client) userStatement(ctx context.Context, statement string, param []byte) error {
	var params [][]byte
	if param != nil {
		params = [][]byte{param}
	}
	reply, err := c.call(ctx, statement, params)
	if err != nil {
		return err
	}
	return expectSimple(reply, commandOf(statement), "OK")
}
