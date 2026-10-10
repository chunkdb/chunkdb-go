# Users and rights

Connections authenticate with SCRAM-SHA-256 from `Options.User`/`Password` or URI userinfo.
For a local `--auth none` server omit credentials; it grants every connection every right.
URI-escape `:`, `@`, `/` and other reserved password characters.
Use TLS when connecting over an untrusted network; SCRAM does not encrypt subsequent data.

With an administrator `client`, after the [world example](../examples/world/main.go), create a user who can read and write that table:

```go
if err := client.CreateUser(ctx, "bot", "bot-password", chunkdb.CreateUserOptions{}); err != nil { return err }
if err := client.Grant(ctx, chunkdb.RightWrite, "world_go", "bot"); err != nil { return err }
users, err := client.Users(ctx)
if err != nil { return err }
for _, user := range users { fmt.Println(user.Name, user.ManagesUsers, user.Grants) }
```

The snippet runs inside a function returning `error`; `bot` must not already exist.
Connect that user with `chunk://bot:bot-password@127.0.0.1:4242/world_go`.
The client computes verifiers from passwords and sends only the verifier for user creation or password changes.

`RightAdmin` includes `RightWrite`, which includes `RightRead`.
Use `AllTables` for a grant on every table, including future tables; a table-specific grant disappears when the table is dropped.
`Revoke` removes the named right and rights above it.
MANAGES USERS permits user administration and grants, independently of table rights.
`SetManagesUsers`, `DropUser` and `Users` require MANAGES USERS; the last manager cannot be dropped or lose that capability.
`SetPassword` permits changing one's own password; changing another user's requires MANAGES USERS.
Already authenticated connections stay logged in after a password change.
The server hides a table as `NO_TABLE` from a user with no rights on it.
See the [server user guide](https://github.com/chunkdb/chunkdb/blob/main/docs/USERS.md) for administration.
