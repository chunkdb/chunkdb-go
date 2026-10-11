# Set up and back up application data

Use named migrations to repeat schema setup safely.

Use `ctx` and an administrator `client` from [Connections](CONNECTIONS.md).

<!-- go-example: ExampleClient_Migrate -->
```go
steps := []chunkdb.Migration{
	{Name: "notes_create", Statement: "CREATE TABLE notes (v u8) CHUNK 2 x 2"},
	{Name: "notes_label", Statement: "ALTER TABLE notes ADD COLUMN label text(16) NULL"},
}
for range 2 {
	results, err := client.Migrate(ctx, steps)
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(result.Name, result.Status)
	}
}
```

```output
notes_create applied
notes_label applied
notes_create skipped
notes_label skipped
```

- Repeating a recorded name with identical text skips that step.
- Different text returns `ErrConflict`; case and interior spacing matter.
- The client trims leading and trailing spaces and tabs.
- Each step is independently durable; earlier successes remain after a later failure.
- `MigrationError` identifies the failed Name and zero-based Index, and wraps its cause.
- Completed results accompany the error; retry the same names and text after an uncertain connection failure.
- A recovery-required error needs a server restart before retrying.
- `Pool.Migrate` leases one connection for the list; concurrent starts can share it.

| Limit or requirement | Value |
|---|---|
| Name | `[a-z_][a-z0-9_]*`, at most 63 bytes. |
| Statement | CREATE/ALTER/DROP TABLE, GRANT/REVOKE, or CREATE/DROP SLOT. |
| Input | No parameters, CR, LF, or NUL; the inner statement's rights apply. |
| Server | One process, read-write mode; no enclosing transaction. |
| `SHOW MIGRATIONS` | Requires MANAGES USERS unless authentication is disabled. |

## Online backup

Backup is disabled unless you start the server with `--backup-dir <path>`.
Use a client with MANAGES USERS, or a server with authentication disabled.

<!-- go-example: ExampleClient_Do_backup -->
```go
backupName := "go-docs-" + time.Now().UTC().Format("20060102T150405.000000000Z")
backup, err := client.Do(ctx, fmt.Sprintf("BACKUP TO '%s'", backupName))
if err != nil {
	log.Fatal(err)
}
fmt.Println("backup complete:", backup.Kind == chunkdb.ReplyMap)
```

```output
backup complete: true
```

The backup destination must be absent or empty.
Only one backup runs at a time; another receives `CodeBusy`.
The reply describes each table's cut; different tables need not share one simultaneous cut.

After connection failure, inspect the server-side result before assuming completion.
See [Backup](https://github.com/chunkdb/chunkdb/blob/main/docs/BACKUP.md) for verification and restore.

## Durability and metrics

<!-- go-example: ExampleClient_FlushWAL -->
```go
if err := client.FlushWAL(ctx); err != nil {
	log.Fatal(err)
}
if err := client.Ping(ctx); err != nil {
	log.Fatal(err)
}
metrics, err := client.Metrics(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println("metrics available:", len(metrics) > 0)
```

```output
metrics available: true
```

`FlushWAL` makes previously acknowledged writes durable.
Ordinary write durability depends on server and table settings.
`Metrics` returns Prometheus text; `ServerInfo()` reports server version and request limits.
Use `Do` for other CQL statements, with parameters from `EncodeValue`.

See [Durability](https://github.com/chunkdb/chunkdb/blob/main/docs/DURABILITY_CONTRACT.md) for configuration and crash guarantees.

Storage features require explicit activation on existing server directories.
`EnableFeature(ctx, name)` sends that request and requires ADMIN on `*`.
Activation is idempotent and monotonic; it cannot disable a feature.
Unknown names return `INVALID_ARGUMENT reason=unknown_feature`.
