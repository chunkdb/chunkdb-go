# Give a user table access

Create a user and grant access to one table.
You need an administrator who can manage users and grant rights on that table.

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first to create `world_go`.
Imports are in the [checked examples](../example_test.go).

<!-- go-example: ExampleClient_CreateUser -->
```go
if err := client.CreateUser(ctx, "bot", "bot-password",
	chunkdb.CreateUserOptions{}); err != nil {
	log.Fatal(err)
}
if err := client.Grant(ctx, chunkdb.RightWrite, "world_go", "bot"); err != nil {
	log.Fatal(err)
}
users, err := client.Users(ctx)
if err != nil {
	log.Fatal(err)
}
for _, user := range users {
	if user.Name == "bot" {
		fmt.Println(user.Name, user.ManagesUsers)
	}
}
```

```output
bot false
```

The name `bot` must be unused.
A password must not be empty; `CreateUser` and `SetPassword` return `ErrProtocol` for an empty one before sending a request.
Connect it with `chunk://bot:bot-password@127.0.0.1:4242/world_go`.
URI-escape reserved password characters, or use `Options.User` and `Options.Password`.
Use TLS over an untrusted network.

## Rights

| Right or method | Behavior |
|---|---|
| `RightAdmin` | Includes `RightWrite`, which includes `RightRead`. |
| `AllTables` | Grants a right on all tables, including future tables. |
| Prefix grant | `Grant(ctx, right, "tenant1_*", user)` covers every table whose name starts with `tenant1_`, future tables included; it stays when a table is dropped. |
| `RightMonitor`, `RightBackup`, `RightReplication` | Server rights, granted on `AllTables` only: MONITOR reads metrics, status, every connection, every table's stats and slots, and the catalog watch; BACKUP takes backups and restores a table under a name it has ADMIN on; REPLICATION is stored and listed, and no statement requires it. `User.ServerRights` lists them. |
| Table-specific grant | Disappears when that table is dropped. |
| `Revoke` | Removes the named right and rights above it. |
| MANAGES USERS | Permits user administration and grants, independently of table rights. |
| `SetManagesUsers`, `DropUser`, `Users` | Require MANAGES USERS. |
| `SetPassword` | Changes your password; another user's requires MANAGES USERS. |
| `RequiresChannelBinding`, `SetRequiresChannelBinding` | The user logs in only with a login bound to its TLS connection; an unbound login fails like a wrong password. |

## Rotate a password

A user can hold two verifiers, so clients move to a new password while the old one still logs in.
Compute the new verifier with the salt and iterations of the current one: `ParseVerifier` reads them from a verifier, and a login as the user receives them as `s=` and `i=` in the SCRAM server-first message.

```go
salt, iterations, err := chunkdb.ParseVerifier(currentVerifier)
if err != nil {
	log.Fatal(err)
}
next, err := chunkdb.ComputeVerifierWithSalt("new-password", salt, iterations)
if err != nil {
	log.Fatal(err)
}
if err := client.AddVerifier(ctx, "bot", next); err != nil {
	log.Fatal(err)
}
// Move every client to the new password, then:
if err := client.DropOldVerifier(ctx, "bot"); err != nil {
	log.Fatal(err)
}
```

`User.Verifiers` is 2 between `AddVerifier` and `DropOldVerifier`.
`ComputeVerifier` picks a new salt, so its verifiers fit `SetPassword` but not `AddVerifier`.

The last manager cannot be dropped or lose that capability.
Already authenticated connections remain logged in after password changes.
The client sends verifiers when creating users or changing passwords.
A local `--auth none` server grants all rights to connections without credentials.

## If it fails

| Error | What you do |
|---|---|
| `ErrAuth` | Check credentials and URI escaping. |
| `ErrPermissionDenied` | Ask an administrator for the required capability or table right. `PermissionDeniedError.Rights` lists the rights of which any one would do, such as MONITOR and ADMIN. |
| `CodeNoTable` | Check the table name and rights; inaccessible tables are hidden. |

See [Users](https://github.com/chunkdb/chunkdb/blob/main/docs/USERS.md) for server administration and password changes.
