package chunkdb

import (
	"context"
	"strconv"
	"strings"
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
	// RightMonitor reads server status: SHOW METRICS, SHOW STATUS, every
	// user's connections, and table stats and slots of every table. It is a
	// server right, granted on [AllTables] only, as are the next two.
	RightMonitor Right = "MONITOR"
	// RightBackup takes backups.
	RightBackup Right = "BACKUP"
	// RightReplication follows the change feed of every table for replication.
	RightReplication Right = "REPLICATION"
)

// AllTables in [Client.Grant] and [Client.Revoke] stands for every table,
// those created later included.
const AllTables = "*"

// User is one user as [Client.Users] lists it.
type User struct {
	Name         string
	ManagesUsers bool
	// RequiresChannelBinding reports that the user logs in only with a login
	// bound to its TLS connection; false from a server that does not show it.
	RequiresChannelBinding bool
	// Grants maps a table name, a prefix such as "tenant1_*", or [AllTables]
	// to the user's right on it.
	Grants map[string]Right
	// ServerRights are the user's server rights: [RightMonitor],
	// [RightBackup] and [RightReplication].
	ServerRights []Right
	// Verifiers is 2 between [Client.AddVerifier] and
	// [Client.DropOldVerifier], else 1; 0 from a server that does not show it.
	Verifiers int
}

// CreateUserOptions configure [Client.CreateUser].
type CreateUserOptions struct {
	// ManagesUsers lets the user create, change and drop users and grant and
	// revoke rights.
	ManagesUsers bool
	// RequiresChannelBinding lets the user log in only with a login bound to
	// its TLS connection ([Options.ChannelBinding]).
	RequiresChannelBinding bool
}

// CreateUser creates a user with password. The client computes the user's
// SCRAM verifier ([ComputeVerifier], [Options.VerifierIterations]) and sends
// only that; the server never receives the password. It needs MANAGES USERS.
// An empty password returns an [ErrProtocol] error before a request is sent.
// With [IfNotExists] an existing user is left as it is, password included.
func (c *Client) CreateUser(ctx context.Context, name, password string, opts CreateUserOptions, guard ...Guard) error {
	const command = "CREATE USER"
	if err := checkNames(command, "user", name); err != nil {
		return err
	}
	clause, err := guardClause(command, IfNotExists, guard)
	if err != nil {
		return err
	}
	if password == "" {
		return requestErrorf(command, "empty password; a user logs in with a password")
	}
	verifier, err := ComputeVerifier(password, c.opts.verifierIterations)
	if err != nil {
		return annotate(command, err)
	}
	statement := "CREATE USER" + clause + " " + name + " VERIFIER $1"
	if opts.ManagesUsers {
		statement += " MANAGES USERS"
	}
	if opts.RequiresChannelBinding {
		statement += " REQUIRE CHANNEL BINDING"
	}
	return c.userStatement(ctx, statement, []byte(verifier))
}

// SetPassword changes the password of a user, sending a new verifier as
// [Client.CreateUser] does. A user may change their own password; another
// user's needs MANAGES USERS. Connections already logged in stay logged in.
// An empty password returns an [ErrProtocol] error before a request is sent.
func (c *Client) SetPassword(ctx context.Context, name, password string) error {
	const command = "ALTER USER"
	if err := checkNames(command, "user", name); err != nil {
		return err
	}
	if password == "" {
		return requestErrorf(command, "empty password; a user logs in with a password")
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

// SetRequiresChannelBinding sets whether a user logs in only with a login
// bound to its TLS connection ([Options.ChannelBinding]); an unbound login of
// such a user fails like a wrong password. It needs MANAGES USERS.
func (c *Client) SetRequiresChannelBinding(ctx context.Context, name string, require bool) error {
	if err := checkNames("ALTER USER", "user", name); err != nil {
		return err
	}
	statement := "ALTER USER " + name + " REQUIRE CHANNEL BINDING"
	if !require {
		statement = "ALTER USER " + name + " NO REQUIRE CHANNEL BINDING"
	}
	return c.userStatement(ctx, statement, nil)
}

// UserLimits are the limits of [Client.SetUserLimits]. A nil field keeps its
// limit; zero disables it.
type UserLimits struct {
	// MaxConnections bounds the user's logged-in connections. Lowering it
	// leaves open connections open and refuses new logins until fewer remain.
	MaxConnections *uint64
	// StatementsPerSec bounds the user's statements in a rolling second, at
	// most 65536; a statement over it fails with RATE_LIMITED.
	StatementsPerSec *uint64
}

// SetUserLimits sets the connection and statement limits of a user (ALTER
// USER ... LIMIT). It needs MANAGES USERS and at least one limit.
func (c *Client) SetUserLimits(ctx context.Context, name string, limits UserLimits) error {
	const command = "ALTER USER"
	if err := checkNames(command, "user", name); err != nil {
		return err
	}
	var settings []string
	if limits.MaxConnections != nil {
		settings = append(settings, "max_connections = "+strconv.FormatUint(*limits.MaxConnections, 10))
	}
	if limits.StatementsPerSec != nil {
		settings = append(settings, "statements_per_sec = "+strconv.FormatUint(*limits.StatementsPerSec, 10))
	}
	if len(settings) == 0 {
		return requestErrorf(command, "LIMIT needs max_connections or statements_per_sec")
	}
	return c.userStatement(ctx, "ALTER USER "+name+" LIMIT "+strings.Join(settings, ", "), nil)
}

// DropUser drops a user. The last user who manages users cannot be dropped.
// With [IfExists] an absent user is not an error.
func (c *Client) DropUser(ctx context.Context, name string, guard ...Guard) error {
	if err := checkNames("DROP USER", "user", name); err != nil {
		return err
	}
	clause, err := guardClause("DROP USER", IfExists, guard)
	if err != nil {
		return err
	}
	return c.userStatement(ctx, "DROP USER"+clause+" "+name, nil)
}

// Grant gives user right on table: a table name, a prefix such as
// "tenant1_*" for every table whose name starts with it, [AllTables], or ""
// for the client's default table. A grant on a table goes when the table is
// dropped; prefix and [AllTables] grants stay. [RightMonitor], [RightBackup]
// and [RightReplication] are granted on [AllTables] only.
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
		user, err := parseUser(command, item)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

// parseUser decodes one SHOW USERS map, which catalog user entries also carry.
func parseUser(command string, item Reply) (User, error) {
	if item.Kind != ReplyMap {
		return User{}, protocolErrorf(command, "expected a map per user, got a reply of kind %d", item.Kind)
	}
	name, _ := item.Lookup("name")
	manages, _ := item.Lookup("manages_users")
	grants, _ := item.Lookup("grants")
	if name.Kind != ReplyBulk || manages.Kind != ReplyBoolean || grants.Kind != ReplyMap {
		return User{}, protocolErrorf(command, "a user needs name, manages_users and grants")
	}
	user := User{Name: string(name.Bulk), ManagesUsers: manages.Bool, Grants: make(map[string]Right, len(grants.Map))}
	if requires, ok := item.Lookup("requires_channel_binding"); ok {
		if requires.Kind != ReplyBoolean {
			return User{}, protocolErrorf(command, "requires_channel_binding of user %s is not a boolean", user.Name)
		}
		user.RequiresChannelBinding = requires.Bool
	}
	for _, grant := range grants.Map {
		if grant.Key.Kind != ReplyBulk || grant.Value.Kind != ReplyBulk {
			return User{}, protocolErrorf(command, "the grants of user %s are not table names and rights", user.Name)
		}
		user.Grants[string(grant.Key.Bulk)] = Right(grant.Value.Bulk)
	}
	if rights, ok := item.Lookup("server_rights"); ok {
		if rights.Kind != ReplyArray {
			return User{}, protocolErrorf(command, "server_rights of user %s is not a list", user.Name)
		}
		for _, right := range rights.Array {
			if right.Kind != ReplyBulk {
				return User{}, protocolErrorf(command, "server_rights of user %s holds a non-text right", user.Name)
			}
			user.ServerRights = append(user.ServerRights, Right(right.Bulk))
		}
	}
	if verifiers, ok := item.Lookup("verifiers"); ok {
		n, valid := verifiers.Uint64()
		if !valid || n < 1 || n > 2 {
			return User{}, protocolErrorf(command, "verifiers of user %s is not 1 or 2", user.Name)
		}
		user.Verifiers = int(n)
	}
	return user, nil
}

func (c *Client) rightStatement(ctx context.Context, verb string, right Right, table, preposition, user string) error {
	switch right {
	case RightRead, RightWrite, RightAdmin:
	case RightMonitor, RightBackup, RightReplication:
		if table != AllTables {
			return requestErrorf(verb, "%s is a server right, granted on * only", right)
		}
	default:
		return requestErrorf(verb, "unknown right %q: rights are READ, WRITE, ADMIN, MONITOR, BACKUP and REPLICATION", right)
	}
	if prefix, ok := strings.CutSuffix(table, "*"); ok && prefix != "" {
		if err := checkNames(verb, "table prefix", prefix); err != nil {
			return err
		}
	} else if table != AllTables {
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
