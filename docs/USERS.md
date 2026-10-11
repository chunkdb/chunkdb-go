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
Connect it with `chunk://bot:bot-password@127.0.0.1:4242/world_go`.
URI-escape reserved password characters, or use `Options.User` and `Options.Password`.
Use TLS over an untrusted network.

## Rights

| Right or method | Behavior |
|---|---|
| `RightAdmin` | Includes `RightWrite`, which includes `RightRead`. |
| `AllTables` | Grants a right on all tables, including future tables. |
| Table-specific grant | Disappears when that table is dropped. |
| `Revoke` | Removes the named right and rights above it. |
| MANAGES USERS | Permits user administration and grants, independently of table rights. |
| `SetManagesUsers`, `DropUser`, `Users` | Require MANAGES USERS. |
| `SetPassword` | Changes your password; another user's requires MANAGES USERS. |

The last manager cannot be dropped or lose that capability.
Already authenticated connections remain logged in after password changes.
The client sends verifiers when creating users or changing passwords.
A local `--auth none` server grants all rights to connections without credentials.

## If it fails

| Error | What you do |
|---|---|
| `ErrAuth` | Check credentials and URI escaping. |
| `ErrPermissionDenied` | Ask an administrator for the required capability or table right. |
| `CodeNoTable` | Check the table name and rights; inaccessible tables are hidden. |

See [Users](https://github.com/chunkdb/chunkdb/blob/main/docs/USERS.md) for server administration and password changes.
