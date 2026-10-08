# chunkdb-go

Official Go client for [`chunkdb`](https://github.com/chunkdb/chunkdb). It speaks protocol 3: one CQL statement per request, values sent as binary parameters, typed replies. It does not connect to servers of an earlier protocol; see the engine's [compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).

Requirements: Go 1.25 or newer and a reachable chunkdb server of protocol 3. Standard library only.

```bash
go get github.com/chunkdb/chunkdb-go/v2
```

## Quick start

```go
ctx := context.Background()

// The URI path is the client's default table; "" in a method means that table.
client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/world")
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

## API

- blocks: `GetBlock(ctx, table, x, y, columns...)`, `SetBlock(ctx, table, x, y, Record, opts...)`, `DeleteBlock(ctx, table, x, y, opts...)`; writes return the chunk's new version
- chunks: `GetChunk(ctx, table, cx, cy, columns...)` returns a `*Chunk` (`Version`, `SchemaVersion`, `Present` per block, `Columns` of values per block); `SetChunk(ctx, table, cx, cy, *Chunk, opts...)` replaces every column; `NewChunk(schema)`, `Chunk.Block` / `SetBlock` / `DeleteBlock` and `Schema.Locate(x, y)` build one
- raw chunk forms: `GetChunkRaw` and `SetChunkRaw` copy chunks as bytes; `DecodeChunk` and `EncodeChunk` convert them
- areas: `GetArea(ctx, table, cx0, cy0, cx1, cy1, columns...)` and `GetAreaAround(ctx, table, cx, cy, radius, columns...)` return the chunks with a present block (at most `ServerInfo().MaxAreaChunks` per read)
- scans: `ScanChunks(ctx, table, after, limit)` returns one page (`Chunks`, `More`); `AllChunks(ctx, table, limit)` iterates every page
- tables: `CreateTable`, `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType` (`ConvertNone`, `ConvertClamp`, `ConvertDefault`, `ConvertTruncate`), `SetTableOption`, `DropTable`, `Tables`, `Describe`, `Schema`
- server: `Ping`, `FlushWAL` (returns once every acknowledged write is durable), `Metrics` (Prometheus text), `ServerInfo()` (version and limits from `HELLO 3`)
- any statement: `Do(ctx, statement, params...)` sends raw parameter frames (`EncodeValue` builds them; `nil` is NULL) and returns the decoded `Reply`

`Pool` mirrors the statement methods and adds `WithClient(ctx, fn)`.

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

## Schema changes

The client refreshes a table's cached schema after its own `CREATE`, `ALTER` or `DROP TABLE` (also through `Do`), on `Describe`, and when the server shows the cache is out of date:

- a `SetBlock` parameter whose size no longer fits its column (another client changed the type): the client fetches the schema and sends the write once more
- a chunk form carries the schema version it was encoded for; `SetChunk` refused with `SCHEMA_MISMATCH` fetches the schema, encodes the chunk again and sends it once more, and fails to encode a chunk whose columns no longer match the table's. `SetChunkRaw` returns the `*SchemaMismatchError`
- a parameter longer than the column now holds makes the server close the connection; that write fails and the next statement uses a fresh schema

## Options

```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
	URI:            "chunks://chunk-token@127.0.0.1:4242/world",
	CA:             caPEM,
	ConnectTimeout: 2 * time.Second,
	CommandTimeout: 3 * time.Second,
	PipelineDepth:  8,
})
```

- explicit `Host`, `Port`, `Token` and `Table` win over the URI
- `ConnectTimeout` bounds dialing, TLS and `HELLO`; `CommandTimeout` bounds one reply; both default to 5 seconds, and a negative value leaves deadlines to the context
- `PipelineDepth` keeps that many requests in flight on one connection (default 1); replies come back in request order
- TLS: `chunks://` or `TLS: true`; `CA`, `Cert`, `Key` take PEM bytes; `TLSServerName` overrides SNI; `TLSInsecure` skips verification (local testing only); TLS 1.2 or newer

## Connections and pooling

A `Client` is one socket, safe for concurrent use. It connects lazily and reconnects on the next request after a transport failure; the request that hit the failure returns an error. Cancelling a context aborts the call and drops the connection, since replies cannot be skipped. After an error the server closes the connection on (`BAD_REQUEST`, or `SYNTAX`, `NO_TABLE` or an unknown column in a statement with parameters), the client drops it too; requests pipelined behind it fail with `ErrConnection` and were not executed.

`ConnectPool(ctx, PoolOptions{Options, MaxConnections, MinConnections, AcquireTimeout})` leases clients per operation and discards one after a transport failure. The server occupies a worker per open connection, so keep `MaxConnections` at or below its `--workers`.

## Errors

Every failure is an `*Error` with its phase, command and, for `-ERR` replies, the server's `ServerCode` (`CodeSyntax`, `CodeInvalidArgument`, `CodeNoTable`, ...) and message. Classify with `errors.Is`: `ErrConnection`, `ErrTimeout`, `ErrProtocol` (malformed replies, client-side validation, a server of an older protocol), `ErrServer`, `ErrAuth` (`AUTH_FAILED` or `AUTH_REQUIRED`, also `ErrServer`), `ErrVersionMismatch` (`*VersionMismatchError`), `ErrSchemaMismatch` (`*SchemaMismatchError`), `ErrTLS`, `ErrClosed`.

## Limits

- a statement is one line of at most `ServerInfo().MaxLineBytes`, CRLF included; a statement with CR or LF is refused before sending
- a bulk reply is capped at `MaxBulkBytes` (160 MiB); a larger declared length is a protocol error
- a chunk's text and bytes values take at most the table's `var_max_chunk_bytes`, counting 12 bytes per value
