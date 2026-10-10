# chunkdb-go

Official Go client for [`chunkdb`](https://github.com/chunkdb/chunkdb). It speaks protocol 3: one CQL statement per request, values sent as binary parameters, typed replies. It does not connect to servers of an earlier protocol; see the engine's [compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).

## World in five minutes

Start a server using the [server quick start](https://github.com/chunkdb/chunkdb/blob/main/docs/QUICK_START.md), then run the [world example](examples/world/main.go) with Go 1.25 or newer:

```bash
git clone https://github.com/chunkdb/chunkdb-go.git
cd chunkdb-go
CHUNKDB_URI='chunk://admin:your-password@127.0.0.1:4242/' go run ./examples/world
```

Use the administrator login from your server and URI-escape special characters in its password. The example creates `world_go` with `tile u8` and `label text(16)`, writes a 4×4 area, reads its blocks and chunks, then subscribes before updating `(0,0)` and prints the observed change:

```text
schema: applied
area: 4 chunks, 16 blocks written
block (0,0): tile=1 label=grass
change (0,0): tile=2 label=water
```

Run the same command again: the named schema migration is `skipped`, and the example overwrites the same 4×4 area before watching another update. It needs ADMIN on `*` to create the table and READ/WRITE on `world_go`. For a server started with `--auth none`, use `CHUNKDB_URI='chunk://127.0.0.1:4242/'`.

Requirements: Go 1.25 or newer and a reachable chunkdb server of protocol 3. Standard library only.

```bash
go get github.com/chunkdb/chunkdb-go/v2
```

## Quick start

```go
ctx := context.Background()

// Log in as bot; the URI path is the client's default table ("" in a method).
client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
if err != nil {
	log.Fatal(err)
}
defer client.Close()

err = client.CreateTable(ctx, "world", chunkdb.TableSpec{
	Columns: []chunkdb.ColumnDef{
		{Name: "id", Type: chunkdb.TypeUint(10), Required: true},
		{Name: "light", Type: chunkdb.TypeUint(4), Default: 15},
		{Name: "sign", Type: chunkdb.TypeText(256), Null: true},
	},
	ChunkWidth: 16, ChunkHeight: 16,
})
if err != nil {
	log.Fatal(err)
}

version, err := client.SetBlock(ctx, "", 10, 4, chunkdb.Record{"id": 23, "sign": "hello"})
if err != nil {
	log.Fatal(err)
}
block, err := client.GetBlock(ctx, "", 10, 4) // nil when the block is absent
fmt.Println(version, block["id"], block["light"], block["sign"]) // <chunk version> 23 15 hello
```

## Logging in

Every connection logs in with a user and password, from the URI (`chunk://user:password@host:4242/`, with `%XX` escapes for `:`, `@` or `/` in the password) or `Options.User` and `Options.Password`. The login is SCRAM-SHA-256: the password never crosses the network, and the client refuses a server that cannot prove it holds the user's verifier. Without a user the client sends `HELLO 3` alone, which only a server started with `--auth none` accepts.

## Values

Every method names its table; `""` is the default table: `Options.Table`, else the URI path, else `default`. Values are typed by the table's columns, and the client caches each table's schema (`Schema`, `Describe`) to encode and decode them.

| Column type | Go value written | Go value read |
|---|---|---|
| `uN` | any integer type | `uint64` (the full range, above `math.MaxInt64` too) |
| `iN` | any integer type | `int64` |
| `bool` | `bool` | `bool` |
| `f32`, `f64` | `float32` or `float64` | `float32`, `float64` (inf and NaN included) |
| `bits(N)` | `chunkdb.Bits` (`ParseBits("1010")`, lowest bit first) | `chunkdb.Bits` |
| `text(max)` | `string`, valid UTF-8 | `string` |
| `bytes(max)` | `[]byte` | `[]byte` |
| `NULL` | `nil` | `nil` |

A value that does not fit its column (out of range, too long, wrong type) fails with `ErrProtocol` before anything is sent. Values always travel as parameters, so text and bytes may hold any byte, CR and LF included.

## Watching changes

`Client.Watch` and `Pool.Watch` open a dedicated connection with the same login and TLS options. Ordinary requests remain available, including with a pool of one connection. Close each watch separately; closing its originating client or pool does not close the stream. WATCH needs server support for change feeds and READ on the table.

```go
watch, err := client.Watch(ctx, "world", chunkdb.WatchOptions{
    Area: &chunkdb.Area{CX0: 0, CY0: 0, CX1: 3, CY1: 3},
})
if err != nil {
    log.Fatal(err)
}
defer watch.Close()
last := watch.Start()
for {
    event, err := watch.Next(ctx)
    if err != nil {
        log.Printf("watch ended after %v: %v", last, err)
        break
    }
    switch e := event.(type) {
    case *chunkdb.ChangeEvent:
        fmt.Println(e.User, e.Blocks) // complete Before/After records, nil for absence
    case *chunkdb.SchemaEvent:
        fmt.Println(e.Version, e.Columns)
    case *chunkdb.ResyncEvent:
        // Re-read state on client before applying subsequent changes.
        fmt.Println("rebuild state through", e.Position)
    }
    last = event.GetPosition() // persist only after handling the event
}
```

AREA bounds are inclusive **chunk** coordinates. Block axes are `int64`, or `ChunkOffset{Chunk, Offset}` when the absolute address does not fit int64. Row values have the same types as `GetBlock`; NULL fields are nil, and an absent Before or After is a nil record. User is a `*string`, nil for anonymous writes. Revisions order events; timestamps may move backwards.

After a lost connection, open another watch with `WatchOptions{After: &last}`, where `last` is the last successfully applied position. There is no automatic reconnect or replay of user work. On `ResyncEvent`, keep reading while another connection re-reads state with `Describe`, `ScanChunks`/`GetChunk` or `GetArea`; replace the scanned state, including disappeared chunks, and apply later changes to a chunk only above its read version. Persist the resync frontier with the rebuilt state. See the server's [resync guide](https://github.com/chunkdb/chunkdb/blob/feat/change-feed/docs/CHANGE_FEED.md#resynchronizing).

The stream caches columns by schema version and updates them on schema events. An uncached version triggers DESCRIBE on another short-lived connection; schema lookups release their connections before WATCH setup and after each lookup. If that version is no longer available, Next ends with `ErrProtocol` rather than decoding old rows with new columns; rebuild state and resume. Cancelling Next while waiting leaves the watch open; a failure during schema lookup or decoding ends it. `Close` sends UNWATCH, drains queued pushes through its reply, and closes the dedicated connections. The command timeout bounds closing (five seconds if disabled); idle Next calls use only their context.

### Durable slots

`Client.CreateSlot(ctx, table, name)` and `DropSlot` need ADMIN. `Client.Slots(ctx, table)` returns `[]Slot` with `Table`, `Name`, `Epoch`, `Acked`, `RetainedBytes` and `Lost`; an empty table lists all visible tables. Create/Drop use the default table when empty. Pool exposes the same methods. Names are `[a-z_][a-z0-9_]*`, at most 63 bytes.

Set `WatchOptions.Slot` to resume a durable consumer. The initial position is the stored ACK, or a later `After` position. AFTER does not acknowledge data. Slots replay archived changes, send historical schema descriptions before their rows, then join the live stream. Only changes through the server's persisted durable frontier are sent, including in relaxed mode. A slot allows one active watch (`CodeBusy`).

`watch.Ack(ctx, revision)` writes an ACK without waiting for a reply. It rejects revisions beyond the last `ChangeEvent` returned by `Next` (or the initial position before any change) and use on an ordinary watch. Schema and resync events do not advance that bound; the server validates decreasing revisions. Successful writing does not confirm persistence: the server batches ACKs across the table at most every 100 ms; `Close` sends UNWATCH and waits for persistence. Server ACK `INVALID_ARGUMENT` is returned by the next `Next` call; that stream remains usable. Recoverable ACK rejections received while closing are discarded. Other stream errors end it. Close can also report a terminal asynchronous server error. Context cancellation during a partial ACK write closes the connection because the request may be incomplete. `Close` cancels an in-flight ACK; if it interrupts the write, both calls report a write or connection error and UNWATCH is skipped. Cancellation by `Close` does not cause `ErrTimeout`; caller cancellation and command deadlines still do.

For exactly-once effects, atomically store your output **and** its `Position` in the same external transaction, then ACK. On reconnect, pass the saved position as `After`, and ignore already applied revisions in that epoch. ACK alone cannot make a separate sink exactly-once: a crash may repeat events after the server's last persisted ACK. For example, after creating the slot and initializing your sink's position:

```go
last := loadSavedPosition() // your sink's last committed Position
watch, err := client.Watch(ctx, "world", chunkdb.WatchOptions{
    Slot: "consumer", After: &last,
})
if err != nil { log.Fatal(err) }
defer watch.Close()
for {
    event, err := watch.Next(ctx)
    if err != nil { log.Fatal(err) }
    switch e := event.(type) {
    case *chunkdb.SchemaEvent:
        // Columns describe later rows; do not ACK a description prefacing a change.
        continue
    case *chunkdb.ResyncEvent:
        log.Fatal("rebuild and save consumer state before resuming")
    case *chunkdb.ChangeEvent:
        if e.Position.Epoch == last.Epoch && e.Position.Revision <= last.Revision {
            continue
        }
        // This application function commits output and Position atomically.
        if err := saveOutputAndPosition(e.Blocks, e.Position); err != nil { log.Fatal(err) }
        last = e.Position
        if err := watch.Ack(ctx, last.Revision); err != nil { log.Fatal(err) }
    }
}
```

A schema description prefacing a historical change uses that change's position; wait until the change is returned and applied before acknowledging it. Schema and resync positions cannot raise the client's ACK bound. If retained history exceeds the server's slot limit, `Lost` becomes true and WATCH ends or fails with `CodeSlotLost`. Rebuild consumer state, drop the lost slot and create it again; creating its existing name fails until it is dropped.

## API

- blocks: `GetBlock(ctx, table, x, y, columns...)`, `SetBlock(ctx, table, x, y, Record, opts...)`, `DeleteBlock(ctx, table, x, y, opts...)`; writes return the chunk's new version
- chunks: `GetChunk(ctx, table, cx, cy, columns...)` returns a `*Chunk` (`Version`, `SchemaVersion`, `Present` per block, `Columns` of values per block); `SetChunk(ctx, table, cx, cy, *Chunk, opts...)` replaces every column; `NewChunk(schema)`, `Chunk.Block` / `SetBlock` / `DeleteBlock` and `Schema.Locate(x, y)` build one
- raw chunk forms: `GetChunkRaw` and `SetChunkRaw` copy chunks as bytes; `DecodeChunk` and `EncodeChunk` convert them
- areas: `GetArea(ctx, table, cx0, cy0, cx1, cy1, columns...)` and `GetAreaAround(ctx, table, cx, cy, radius, columns...)` return the chunks with a present block (at most `ServerInfo().MaxAreaChunks` per read)
- scans: `ScanChunks(ctx, table, after, limit)` returns one page (`Chunks`, `More`); `AllChunks(ctx, table, limit)` iterates every page
- tables: `CreateTable`, `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType` (`ConvertNone`, `ConvertClamp`, `ConvertDefault`, `ConvertTruncate`), `SetTableOption`, `DropTable`, `Tables`, `Describe`, `Schema`
- users: `CreateUser(ctx, name, password, CreateUserOptions{ManagesUsers})`, `SetPassword`, `SetManagesUsers`, `DropUser`, `Grant(ctx, right, table, user)`, `Revoke`, `Users` (name, `ManagesUsers`, `Grants` per table); see below
- server: `Ping`, `FlushWAL` (returns once every acknowledged write is durable), `Metrics` (Prometheus text), `ServerInfo()` (version and limits from `HELLO 3`)
- transactions: `Transaction(ctx, func(tx *Tx) error, opts...)` returns the commit version; see below
- migrations: `Migrate(ctx, []Migration)` applies named schema steps in order and returns each name with `Status` (`applied` or `skipped`); see below
- any statement: `Do(ctx, statement, params...)` sends raw parameter frames (`EncodeValue` builds them; `nil` is NULL) and returns the decoded `Reply`

`Pool` mirrors the statement methods and `Transaction`, and adds `WithClient(ctx, fn)`.

## Users

```go
err := admin.CreateUser(ctx, "bot", password, chunkdb.CreateUserOptions{})
err = admin.Grant(ctx, chunkdb.RightWrite, "world", "bot") // chunkdb.AllTables is every table
```

The client computes the user's SCRAM verifier from the password and sends only that. Rights are `RightRead`, `RightWrite` (includes read) and `RightAdmin` (includes write); user statements and grants need a user who manages users, and a user may change their own password with `SetPassword`. `ComputeVerifier(password, iterations)` returns the verifier for `CREATE USER ... VERIFIER $1` sent with `Do`; `Options.VerifierIterations` raises the PBKDF2 iterations from 4096.

## Conditional writes

`IfVersion(v)` writes only while the chunk is still at version `v`; the version belongs to the whole chunk. A mismatch changes nothing and returns a `*VersionMismatchError` with the current version:

```go
chunk, err := client.GetChunk(ctx, "", 0, 0, "light")
if err != nil {
	return err
}
_, err = client.SetBlock(ctx, "", 0, 0, chunkdb.Record{"light": 9}, chunkdb.IfVersion(chunk.Version))
var mismatch *chunkdb.VersionMismatchError
if errors.As(err, &mismatch) {
	// mismatch.Current is the chunk's version now: read again and retry.
}
```

## Transactions

`Transaction` reads one snapshot of one table and writes several chunks together, or not at all:

```go
version, err := client.Transaction(ctx, func(tx *chunkdb.Tx) error {
	from, err := tx.GetBlock(ctx, "", 10, 4, "gold")
	if err != nil {
		return err
	}
	to, err := tx.GetBlock(ctx, "", 300, 7, "gold")
	if err != nil {
		return err
	}
	if err := tx.SetBlock(ctx, "", 10, 4, chunkdb.Record{"gold": from["gold"].(uint64) - 10}); err != nil {
		return err
	}
	return tx.SetBlock(ctx, "", 300, 7, chunkdb.Record{"gold": to["gold"].(uint64) + 10})
})
```

- `tx` has the block, chunk and area methods of the client; reads see the snapshot taken by the first statement plus the transaction's own writes, and writes return no version: `Transaction` returns the version every written chunk has after `COMMIT`, or 0 when nothing was written
- `IfVersion` is not offered inside a transaction: `COMMIT` checks every chunk the transaction read or wrote
- when another write changed one of them, or the server ends the transaction for another reason, it answers `CONFLICT` and nothing is written; `Transaction` then runs the function again after a pause of a few milliseconds, up to 5 times (`TxRetries(n)` changes the limit), and returns a `*ConflictError` (`Reason`: `chunk_changed`, `duration`, `history_limit` or `table_changed`) after the last one. Keep the function free of other side effects
- after a statement answers `CONFLICT`, the transaction's other statements return the same error and send nothing; returning it from the function is enough
- when the function returns an error, the transaction rolls back and `Transaction` returns that error without running it again; other statement errors leave the transaction open
- the transaction holds the client's connection until it ends, and the client's other requests wait for it; `Pool.Transaction` runs it on a leased connection while the pool serves other requests. A dropped connection rolls the transaction back

## Schema changes

The client refreshes a table's cached schema after its own `CREATE`, `ALTER` or `DROP TABLE` (also through `Do`), on `Describe`, and when the server shows the cache is out of date:

- a `SetBlock` parameter whose size no longer fits its column (another client changed the type): the client fetches the schema and sends the write once more
- a chunk form carries the schema version it was encoded for; `SetChunk` refused with `SCHEMA_MISMATCH` fetches the schema, encodes the chunk again and sends it once more, and fails to encode a chunk whose columns no longer match the table's. `SetChunkRaw` returns the `*SchemaMismatchError`
- a parameter longer than the column now holds makes the server close the connection; that write fails and the next statement uses a fresh schema

## Migrations

Run the same migration list at every start of your app, before serving requests:

```go
results, err := client.Migrate(ctx, []chunkdb.Migration{
    {Name: "create_world", Statement: "CREATE TABLE world (v u8) CHUNK 2 x 2"},
    {Name: "add_label", Statement: "ALTER TABLE world ADD COLUMN label text(16) NULL"},
})
if err != nil { log.Fatal(err) }
for _, result := range results { log.Printf("%s: %s", result.Name, result.Status) }
```

`Pool.Migrate` uses one leased connection for the list. Each step is committed independently; the list is not a transaction. Concurrent application starts may use the same list: one applies each step and the others skip it. A name already recorded with different statement text returns `CodeConflict` (`ErrConflict`); preserve interior spacing and case when editing application code. Leading and trailing spaces/tabs are ignored. Names are `[a-z_][a-z0-9_]*`, at most 63 bytes.

Steps support `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `GRANT`, `REVOKE`, `CREATE SLOT` and `DROP SLOT`, with the rights of the inner statement. Statements must be single lines without parameters. On failure, the returned results contain completed steps, and `*MigrationError` gives the failed `Name`, zero-based `Index`, and wrapped cause for `errors.Is`/`errors.As`. After a connection failure the step may have completed; retry the same list with the same names and text. If the server reports that recovery is required, restart it before retrying. `Do(ctx, "SHOW MIGRATIONS")` lists recorded steps and requires MANAGES USERS unless authentication is disabled.

## Options

```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
	URI:            "chunks://bot:secret@127.0.0.1:4242/world",
	CA:             caPEM,
	ConnectTimeout: 2 * time.Second,
	CommandTimeout: 3 * time.Second,
	PipelineDepth:  8,
})
```

- explicit `Host`, `Port`, `User`, `Password` and `Table` win over the URI
- `ConnectTimeout` bounds dialing, TLS and `HELLO`; `CommandTimeout` bounds one reply; both default to 5 seconds, and a negative value leaves deadlines to the context
- `PipelineDepth` keeps that many requests in flight on one connection (default 1); replies come back in request order
- TLS: `chunks://` or `TLS: true`; `CA`, `Cert`, `Key` take PEM bytes; `TLSServerName` overrides SNI; `TLSInsecure` skips verification (local testing only); TLS 1.2 or newer

## Connections and pooling

A `Client` is one socket, safe for concurrent use. It connects lazily and reconnects on the next request after a transport failure; the request that hit the failure returns an error. Cancelling a context aborts the call and drops the connection, since replies cannot be skipped. After an error the server closes the connection on (`BAD_REQUEST`, or `SYNTAX`, `NO_TABLE` or an unknown column in a statement with parameters), the client drops it too; requests pipelined behind it fail with `ErrConnection` and were not executed.

`ConnectPool(ctx, PoolOptions{Options, MaxConnections, MinConnections, AcquireTimeout})` leases clients per operation and discards one after a transport failure. The server occupies a worker per open connection, so keep `MaxConnections` at or below its `--workers`.

## Errors

Statement failures carry an `*Error`, directly or wrapped, with its phase, command and, for `-ERR` replies, the server's `ServerCode` (`CodeSyntax`, `CodeInvalidArgument`, `CodeNoTable`, ...) and message. Classify with `errors.Is`: `ErrConnection`, `ErrTimeout`, `ErrProtocol` (malformed replies, client-side validation, a server of an older protocol), `ErrServer`, `ErrAuth` (`AUTH_FAILED` for a wrong user or password, `AUTH_REQUIRED` for no user; also `ErrServer`), `ErrPermissionDenied` (`*PermissionDeniedError` with the `Right` and `Table` the statement needs; also `ErrServer`), `ErrVersionMismatch` (`*VersionMismatchError`), `ErrSchemaMismatch` (`*SchemaMismatchError`), `ErrConflict` (`*ConflictError` for transactions; `*Error` for a migration name with different statement text, wrapped by `*MigrationError` when using `Migrate`), `ErrTLS`, `ErrClosed`. A table the user has no right on reads as `NO_TABLE`. A server whose SCRAM signature does not match fails the login with `ErrConnection`.

## Limits

- a statement is one line of at most `ServerInfo().MaxLineBytes`, CRLF included; a statement with CR or LF is refused before sending
- a bulk reply is capped at `MaxBulkBytes` (160 MiB); a larger declared length is a protocol error
- a chunk's text and bytes values take at most the table's `var_max_chunk_bytes`, counting 12 bytes per value
