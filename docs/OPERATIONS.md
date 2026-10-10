# Migrations, backup and server operations

Examples run inside a function returning `error`, with `ctx` and an administrator `client`.

## Named migrations

```go
results, err := client.Migrate(ctx, []chunkdb.Migration{
    {Name: "notes_create", Statement: "CREATE TABLE notes (v u8) CHUNK 2 x 2"},
    {Name: "notes_label", Statement: "ALTER TABLE notes ADD COLUMN label text(16) NULL"},
})
if err != nil { return err }
for _, result := range results { fmt.Println(result.Name, result.Status) }
records, err := client.Do(ctx, "SHOW MIGRATIONS")
if err != nil { return err }
fmt.Println("recorded:", len(records.Array))
```

Run the same list at application startup; a recorded name with the same statement is `skipped`.
Different text for that name returns `ErrConflict`; case and interior spacing matter, while the Go client trims leading/trailing spaces and tabs.
Each step is durable independently; earlier successes remain when a later step fails.
Completed results are returned alongside the error; `*MigrationError` gives the failed `Name`, zero-based `Index` and wrapped cause for `errors.Is`/`errors.As`.
Concurrent starts can share a list; `Pool.Migrate` leases one connection for it.
After a connection failure, retry the same names and text because the outcome may be unknown; a recovery-required error requires a server restart first.
Names are `[a-z_][a-z0-9_]*`, at most 63 bytes.
Steps support CREATE/ALTER/DROP TABLE, GRANT/REVOKE and CREATE/DROP SLOT, with the inner statement's rights, no parameters, and no CR/LF or NUL.
SHOW MIGRATIONS requires MANAGES USERS unless authentication is disabled.
Migrations require a single-process read-write server and cannot run inside a transaction.

## Online backup

```go
backup, err := client.Do(ctx, "BACKUP TO 'go-docs'")
if err != nil { return err }
fmt.Println("backup complete:", backup.Kind == chunkdb.ReplyMap)
```

BACKUP uses server-local storage under `--backup-dir`; its destination must be absent or empty, and needs MANAGES USERS unless authentication is disabled.
The relative name above must be unused; use a new name for another snapshot.
The reply carries per-table cuts and counts; data across tables need not represent one simultaneous cut.
Only one backup runs at a time; another receives `CodeBusy`.
If the connection fails, inspect the server-side result before assuming completion: a write may have succeeded before its reply was lost.
Verification and restore are server-tool operations; use the [backup guide](https://github.com/chunkdb/chunkdb/blob/main/docs/BACKUP.md).

## Durability, health and metrics

```go
if err := client.FlushWAL(ctx); err != nil { return err }
if err := client.Ping(ctx); err != nil { return err }
metrics, err := client.Metrics(ctx)
if err != nil { return err }
fmt.Println("metrics available:", len(metrics) > 0)
```

`FlushWAL` waits until previously acknowledged writes are durable; ordinary write durability depends on server/table configuration.
`Metrics` returns Prometheus text; `ServerInfo()` returns version and request/area limits from the latest handshake.
Use `Do(ctx, statement, params...)` for CQL without a dedicated method; parameters are typed binary values from `EncodeValue`.
The [server durability contract](https://github.com/chunkdb/chunkdb/blob/main/docs/DURABILITY_CONTRACT.md) describes configuration and crash guarantees.
