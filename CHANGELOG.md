# Changelog

All notable changes to this project will be documented in this file.

This client follows [Semantic Versioning](https://semver.org/). Version 2.x
uses protocol 3 (CQL); see the engine's
[compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).

## Unreleased

### Documentation
- Rewrite the README and package overview for chunkdb 2.0, with separate data, values, users, transactions, feed/slots, migrations, backup and connection guides.

### Breaking
- Users replace the token: a connection logs in with SCRAM-SHA-256 (`HELLO 3 USER <name> $1`, then `AUTH $1`), from `chunk://user:password@host:4242/` or `Options.User` and `Options.Password`, and checks the server's signature; a mismatch is an `ErrConnection` error. Without a user the client sends `HELLO 3`, for servers started with `--auth none`. `Options.Token`, `URI.Token` and the token in the URI are removed; `URI` has `User` and `Password`
- The module path is `github.com/chunkdb/chunkdb-go/v2`
- Protocol 3 (CQL). Every connection starts with `HELLO 3`; `ServerInfo()` returns a `ServerInfo` (server version and the limits `max_line_bytes`, `max_parameters`, `max_area_chunks`, `max_response_bytes`, `max_scan_limit`). A server of protocol 2 or 1.x is refused with an `ErrProtocol` error saying it speaks an older protocol
- Every method names its table; `""` is the client's default table (`Options.Table`, else the URI path, else `default`). HELLO no longer carries a table
- Removed the protocol 2 and 1.x API: `Get`, `Set`, `Unset`, `MGet`, `MSet`, `BlockState`, `Block`, `BlockRef`, `ChunkExists`, `GetChunk` / `GetChunkState` / `PutChunk` / `PutChunkState` on bit-packed payloads, `ChunkState`, `ChunkStateInput`, `GetOptions`, `PutOptions`, `MutationResult`, `ChunkScan`, `ChunkRange`, `ChunkRadius`, `RangeEntry`, `CoordPair`, `ScanResult`, `ChunkVersion`, `ChunkBatch`, `ChunkBatchIfVersion`, `BatchOperation`, `SetOp`, `UnsetOp`, `Info`, `WALFlush`, `Use`, `Table`, `CurrentTable`, `TableInfo`, `TableSpec.BlockBits` and the geometry fields, `SetTableOptions`, `HelloInfo`, bit-string block values, ZRLE on the wire and `ZRLECompress` / `ZRLEDecompress`, and the frame helpers `SerializeCommand`, `ReadFrame`, `Frame`, `ParseInfo`
- A version mismatch is an error: `*VersionMismatchError` with the current version, matching `ErrVersionMismatch`
- Chunk forms carry the schema version after the chunk version (`Chunk.SchemaVersion`); `SetChunk` re-encodes once on `SCHEMA_MISMATCH`, which is a `*SchemaMismatchError` matching `ErrSchemaMismatch`

### Added
- A runnable `examples/world` creates a typed table, fills and reads an area, and observes an update through WATCH; the integration suite runs it twice to check its repeatable named migration
- Client/Pool `Migrate` apply named schema steps in order with `applied`/`skipped` results, partial results and a wrapping `MigrationError` identifying the first failed step; migration `CONFLICT` preserves the server error without transaction retry semantics
- Durable slots: Client/Pool `CreateSlot`, `DropSlot`, typed `Slots`, `WatchOptions.Slot`, and cancellable write-only `Watch.Ack`; slot ACK rejections are recoverable through `Next`, with `CodeSlotLost` for expired history
- `Client.Watch` and `Pool.Watch` stream typed change, schema and resync events over dedicated logged-in connections, with AREA/AFTER, schema-version decoding, exact overflow coordinates and draining UNWATCH close
- Transactions: `Client.Transaction(ctx, func(tx *Tx) error, opts...)` and `Pool.Transaction` run `BEGIN` ... `COMMIT` on one connection held for the whole transaction and return the commit version (0 when nothing was written); `Tx` has the block, chunk and area methods without `IfVersion`. On a transaction `CONFLICT` the function runs again after a short pause, up to `DefaultTxRetries` (5) times (`TxRetries`); an error from the function rolls back. A transaction `CONFLICT` is a `*ConflictError` with its `Reason`, matching `ErrConflict` and `ErrServer`
- Users: `CreateUser` (`CreateUserOptions`), `SetPassword`, `SetManagesUsers`, `DropUser`, `Grant`, `Revoke` (`Right`, `AllTables`) and `Users` (`User`); the client computes verifiers from passwords (`ComputeVerifier`, `Options.VerifierIterations`, 4096 by default)
- `PERMISSION_DENIED` is a `*PermissionDeniedError` matching `ErrPermissionDenied` and `ErrServer`; `ServerInfo.ServerSignature`
- Typed values by column type (`uN`, `iN`, `bool`, `f32`, `f64`, `bits(N)`, `text(max)`, `bytes(max)`, NULL), sent as parameters and checked before sending; `Record`, `Bits`, `ParseBits`, `EncodeValue`, `ColumnType`, `ParseColumnType`
- Blocks: `GetBlock`, `SetBlock`, `DeleteBlock`; `IfVersion` makes writes conditional
- Chunks: `GetChunk` / `SetChunk` on a decoded `Chunk`, `GetChunkRaw` / `SetChunkRaw` on the chunk form, `NewChunk`, `DecodeChunk`, `EncodeChunk`, `Schema.Locate`
- Areas and scans: `GetArea`, `GetAreaAround`, `ScanChunks` (`ScanPage`), `AllChunks`
- Tables: `CreateTable` with `ColumnDef` columns, `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType` (`Conversion`), `SetTableOption`, `DropTable`, `Tables`, `Describe` and the cached `Schema`, refreshed after the client's own table statements and once when a write's parameter has the wrong size for its column
- `Do(ctx, statement, params...)` sends any statement and returns the decoded RESP3 `Reply`
- `FlushWAL`, `Metrics` (`SHOW METRICS`), `DefaultTable`; error codes as `Code*` constants
- The client drops the connection after the error replies on which the server closes it; the next plain call reconnects even while socket teardown is still in progress. Transaction statements remain on the connection that began the transaction

### Fixed
- Connection errors identify refused addresses and give server, URI scheme, TLS trust and timeout guidance while preserving error types and wrapped causes
- Typed permission and schema mismatch errors retain server guidance without including it in right or table fields
- Slot ACK validation uses the last returned change or initial position and lets the server validate decreasing revisions, including retries after rejected ACKs
- Closing during an ACK write reports the interrupted write or connection error instead of a timeout caused by Close; recoverable ACK rejections received while closing are discarded

## 1.1.0 - 2026-09-03

### Added
- `SetChunkBin(ctx, cx, cy, payload)` and `SetChunkBinState(ctx, cx, cy, state)`:
  binary chunk writes over the new `CHUNKSETBIN` command (chunkdb server
  1.3+), taking exactly the byte layouts `ChunkBin` / `ChunkBinState` return.
  Lengths are validated against the server geometry before sending; `Pool`
  mirrors both

## 1.0.0

First release of `github.com/chunkdb/chunkdb-go`, covering the same protocol
surface as `@chunkdb/client` 1.1.0.

### Added
- `Client`: one long-lived connection with lazy connect, automatic `AUTH`, and
  reconnect on the request after a transport failure
- point commands: `Get`, `ReadBlock`, `Exists`, `Set`, `Unset`, `MSet`, `MGet`
- chunk commands: `ChunkExists`, `Chunk`, `ReadChunk`, `SetChunk`,
  `SetChunkState`, `ChunkBin`, `ChunkBinState`
- world reads: `ChunkScan`, `ChunkRange`, `ChunkRadius`
- chunk concurrency: `ChunkVersion`, `ChunkCompareAndSet`, `ChunkBatch`,
  `ChunkBatchIfVersion`; a version mismatch is reported as a `MutationResult`
  with `OK` false rather than an error
- `WALFlush` durability barrier and `Metrics` Prometheus text output
- `ChunkBinCompressed` / `ChunkBinStateCompressed` using the server's `zrle`
  codec, plus exported `ZRLECompress` / `ZRLEDecompress`
- `Pool`: fixed-size connection pool with warm connections, acquire timeout,
  `WithClient`, and mirrors of every high-level `Client` operation
- `context.Context` on every request, for per-call deadlines and cancellation
- typed `*Error` with `errors.Is` sentinels (`ErrConnection`, `ErrTimeout`,
  `ErrProtocol`, `ErrServer`, `ErrAuth`, `ErrTLS`, `ErrClosed`) and the server's
  protocol error code
- TLS transport with custom trust roots, client certificates, SNI override, and
  an insecure mode for local testing
- request pipelining via `PipelineDepth` (default 1)
- `int64` block and chunk coordinates across the full signed 64-bit domain, and
  `uint64` chunk versions
- exported protocol helpers: `ParseURI`, `SerializeCommand`, `ReadFrame`,
  `ParseInfo`
