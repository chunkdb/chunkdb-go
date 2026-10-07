# chunkdb-go

Official Go client for [`chunkdb`](https://github.com/chunkdb/chunkdb).

Speaks `chunkdb` protocol 2, which chunkdb 2.0 servers serve; see the engine's
[compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).
It does not connect to 1.x servers: use `github.com/chunkdb/chunkdb-go` 1.x
with those.

This package is intentionally small:

- `Client` = one long-lived socket
- sequential request/response per client by default, with opt-in pipelining (`PipelineDepth`)
- opt-in pooling via `Pool`
- no automatic retries or background reconnect loops
- standard library only, no third-party dependencies

## Features

- `chunk://` and `chunks://` URI support
- `net` / `crypto/tls` transport
- `Connect`, `ConnectURI`, `ConnectPool`, `NewClient`, `NewPool`
- `ServerInfo`: the server version, capabilities, limits, and the table's
  geometry and options, from the `HELLO` handshake
- blocks: `Get` (reports unset blocks), `Set`, `Unset`, batch `MSet` / `MGet`
  (single round-trip for many blocks)
- binary chunks: `GetChunk`, `GetChunkState`, `PutChunk`, `PutChunkState`,
  optionally zrle-compressed on the wire
- world reads: `ChunkScan`, `ChunkRange`, `ChunkRadius`
- optimistic concurrency: `ChunkVersion`, conditional `PutChunk` /
  `PutChunkState` (`IfVersion`), and atomic single-chunk `ChunkBatch` /
  `ChunkBatchIfVersion`
- `WALFlush` durability barrier and `Metrics` (Prometheus text format)
- tables: `CreateTable`, `DropTable`, `Tables`, `TableInfo`,
  `SetTableOptions`, `Use`, per-table clients (`client.Table(ctx, name)`), and
  the table named in the URI path (`chunk://host:4242/terrain`)
- per-block extra data: `XGet`, `XPut`, `XDel`, `GetChunkStateExtra`, `PutChunkStateExtra`, and `XPutOp` / `XDelOp` in `ChunkBatch`
- configurable request pipelining for high-latency links
- `context.Context` on every request, for per-call deadlines and cancellation
- typed errors with `errors.Is` sentinels and protocol error codes
- `int64` block and chunk coordinates across the full signed 64-bit domain
- configurable connect and command timeouts

## Requirements

- Go 1.25 or newer
- a reachable `chunkdb` 2.0 server

## Install

```bash
go get github.com/chunkdb/chunkdb-go/v2
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/chunkdb/chunkdb-go/v2"
)

func main() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Set(ctx, 0, 0, "1011001110110011"); err != nil {
		log.Fatal(err)
	}

	block, err := client.Get(ctx, 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(block.Exists, block.Bits) // true 1011001110110011

	unset, err := client.Get(ctx, 1, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(unset.Exists) // false

	chunk, err := client.GetChunkState(ctx, 0, 0, chunkdb.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(chunk.Exists, len(chunk.Payload), len(chunk.Presence))
}
```

Connecting sends `HELLO 2` with the token and table from the URI. The reply is
available as `client.ServerInfo()`: the server version, capabilities, limits,
and the table's geometry and options.

## Options

`Connect` takes `Options` when a URI alone is not enough:

```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
	URI:            "chunk://chunk-token@127.0.0.1:4242/",
	ConnectTimeout: 2 * time.Second,
	CommandTimeout: 3 * time.Second,
})
```

Explicit `Host`, `Port`, and `Token` win over the values parsed from `URI`.

- `ConnectTimeout`: maximum time to establish the socket and complete TLS setup
- `CommandTimeout`: maximum time to wait for one command response, including
  `HELLO`
- both default to 5 seconds; a negative value disables the client-side deadline
  and leaves cancellation to the context
- `PipelineDepth`: maximum concurrent in-flight requests per connection (default 1)

## Context And Cancellation

Every request takes a `context.Context`. Cancelling it aborts the call and
returns an error that matches both `chunkdb.ErrTimeout` and the underlying
`context.Canceled` or `context.DeadlineExceeded`.

The protocol has no request identifiers, so a response cannot be skipped without
desynchronizing every later response. Aborting an in-flight request therefore
drops the connection; the next request opens a fresh one. Requests are never
retried automatically.

## TLS

```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
	URI: "chunks://chunk-token@127.0.0.1:4242/",
	CA:  caPEM,
})
```

- `chunks://` or `TLS: true` enables TLS
- `TLSInsecure: true` skips certificate verification, for local testing only
- `TLSServerName` overrides the SNI and hostname-verification target
- `CA`, `Cert`, and `Key` take PEM bytes for custom trust roots and client certificates
- the minimum negotiated version is TLS 1.2

## Connection Model

- Reuse one `Client` for low-concurrency code paths. It keeps one socket open
  and sends one request at a time. It is safe for concurrent use; calls are
  serialized.
- Use one shared `Pool` for concurrent workloads. It keeps several warm clients
  and leases them per operation.
- Single-socket multiplexing is out of scope. Parallelism comes from multiple
  sockets, not request IDs on one socket.
- The connection is opened lazily and re-established on the next request after a
  transport failure. The request that hit the failure still returns an error.

## Pooling

```go
pool, err := chunkdb.ConnectPool(ctx, chunkdb.PoolOptions{
	Options:        chunkdb.Options{URI: "chunk://chunk-token@127.0.0.1:4242/"},
	MaxConnections: 4,
	MinConnections: 1,
	AcquireTimeout: 2 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
defer pool.Close()

if err := pool.Set(ctx, 0, 0, "1011001110110011"); err != nil {
	log.Fatal(err)
}

err = pool.WithClient(ctx, func(ctx context.Context, client *chunkdb.Client) error {
	if err := client.Set(ctx, 1, 0, "0000111100001111"); err != nil {
		return err
	}
	return client.WALFlush(ctx)
})
```

`PoolOptions` embeds `Options` and adds:

- `MaxConnections`: maximum number of clients the pool opens
- `MinConnections`: warm connections opened by `ConnectPool`
- `AcquireTimeout`: maximum time to wait for a free pooled client

A leased client is returned to the pool when the operation finishes, or
discarded when it fails with a transport error. `Close` waits for outstanding
leases before closing the connections.

The server occupies one worker for as long as a client connection is open, so
keep `MaxConnections` at or below the server's `--workers` setting. Extra
connections wait in the server's pending queue instead of being served.

## Chunks

A chunk's data is binary:

- `Payload`: the packed block bits,
  `ceil(ChunkWidthBlocks * ChunkHeightBlocks * BlockBits / 8)` bytes; bit `i`
  of the chunk is `payload[i/8] >> (i%8) & 1`. Block `b` (row-major inside the
  chunk) holds bits `b*BlockBits` to `(b+1)*BlockBits - 1`, and character `j`
  of its `Get` bit text is bit `b*BlockBits + j`
- `Presence`: one bit per block, laid out the same way, set when the block is
  explicitly present; `ceil(ChunkWidthBlocks * ChunkHeightBlocks / 8)` bytes

The sizes come from the connection's table (`client.ServerInfo().Table`, or the
`TableInfo` that `Use` returns). The client checks every chunk it sends or
receives against them; a write of the wrong size fails before anything is
sent.

```go
version, err := client.ChunkVersion(ctx, 0, 0)
if err != nil {
	return err
}
state, err := client.GetChunkState(ctx, 0, 0, chunkdb.GetOptions{})
if err != nil {
	return err
}
state.Payload[0] ^= 0xff
state.Presence[0] |= 0x01

result, err := client.PutChunkState(ctx, 0, 0,
	chunkdb.ChunkStateInput{Payload: state.Payload, Presence: state.Presence},
	chunkdb.PutOptions{IfVersion: &version})
if err != nil {
	return err
}
if !result.OK {
	// Someone else wrote the chunk; result.Version is its current version.
}
```

- `GetChunk` returns the payload only; an absent chunk reads as zeros, so use
  `GetChunkState(...).Exists` or `ChunkExists` to tell it from an all-zero
  chunk.
- `PutChunk` makes every block present; `PutChunkState` writes the presence
  bitmap too, and payload bits of absent blocks are stored as zero. An
  all-zero presence bitmap leaves the chunk absent.
- `GetOptions{ZRLE: true}` and `PutOptions{ZRLE: true}` compress the transfer.
  Reads are decompressed and size-checked; writes are compressed only when
  that makes them smaller. Sparse chunks shrink a lot, dense ones not at all.
- `PutOptions{IfVersion: &v}` applies the write only if the chunk's version is
  still `v`.
- Writes return a `MutationResult`: the chunk's version after the write, or
  with `OK` false the current version when `IfVersion` did not match.

## Tables

A chunkdb server holds named tables, each with its own geometry and options.
A client works on one table: `Options.Table`, else the URI path, else the
server's `default` table. The client names it again in `HELLO` on every
reconnect.

```go
admin, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
if err != nil {
	log.Fatal(err)
}
defer admin.Close()

err = admin.CreateTable(ctx, "terrain", chunkdb.TableSpec{
	BlockBits:         4,
	ChunkWidthBlocks:  32,
	ChunkHeightBlocks: 32,
	Options:           chunkdb.TableOptions{DurabilityMode: "fsync-wal"},
})
if err != nil {
	log.Fatal(err)
}

// A per-table client has its own connection.
terrain, err := admin.Table(ctx, "terrain")
if err != nil {
	log.Fatal(err)
}
defer terrain.Close()
if err := terrain.Set(ctx, 0, 0, "1011"); err != nil {
	log.Fatal(err)
}

// Or name the table in the URI.
same, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/terrain")
if err != nil {
	log.Fatal(err)
}
defer same.Close()
```

- Geometry is fixed when a table is created; `SetTableOptions` changes its
  options (zero fields stay unchanged).
- A pool works on one table: set `Options.Table` or the URI path in
  `PoolOptions`. Use one pool per table, and do not call `Use` on a client
  from `WithClient`: the pooled connection would keep that table.
- After `DropTable`, commands from clients on that table fail with an `*Error`
  whose `ServerCode` is `CodeNoTable`, even if a table of the same name is
  created again; `Use` selects a table again.
- `Use` changes the table for every later command on the client. It waits
  for the requests in flight and holds back new ones until it completes, so
  each request runs entirely on the old or the new table.
- If the server has no `default` table and none is named, the connection has
  no table: `ServerInfo().Table` is nil and the chunk methods fail until `Use`
  selects one.
- An unknown table in `Options.Table` or the URI path fails `Connect` with
  `CodeNoTable`.

## Extra Data

A table can let each present block carry one opaque value of 1 or more bits next to its payload: an owner, a label, an object's state. Values can differ in length from block to block, and blocks without one cost nothing.

Extra data is off until a table sets `TableOptions.ExtraMaxBlockBits`, the longest value in bits. `ExtraMaxChunkBytes` caps the extra data of one chunk, where each value costs 8 bytes plus `ceil(bits/8)` (server default 65536). Enabling is permanent and both limits can only be raised; `TableInfo` reports them, `0` for a table without extra data.

```go
err := client.CreateTable(ctx, "world", chunkdb.TableSpec{
	BlockBits: 16,
	Options:   chunkdb.TableOptions{ExtraMaxBlockBits: 4096},
})
if err != nil {
	log.Fatal(err)
}
world, err := client.Table(ctx, "world")
if err != nil {
	log.Fatal(err)
}
defer world.Close()

// A value belongs to a present block. Bit n is Bytes[n/8] >> (n%8) & 1.
if err := world.Set(ctx, 10, 4, "0000000000000101"); err != nil {
	log.Fatal(err)
}
if err := world.XPut(ctx, 10, 4, chunkdb.ExtraValue{BitLength: 12, Bytes: []byte{0xab, 0x0c}}); err != nil {
	log.Fatal(err)
}
value, err := world.XGet(ctx, 10, 4) // nil when the block has no value
if err != nil {
	log.Fatal(err)
}
fmt.Println(value.BitLength, value.Bytes) // 12 [171 12]

// Every value of a chunk by block index: local_y * ChunkWidthBlocks + local_x.
chunk, err := world.GetChunkStateExtra(ctx, 0, 0, chunkdb.GetOptions{})
if err != nil {
	log.Fatal(err)
}
fmt.Println(chunk.Extra[4*16+10].BitLength) // 12
```

- `XPut` replaces the value of a present block and `XDel` removes it (also when there is none). `Unset` removes the value with the block; `Set` keeps it.
- `GetChunkStateExtra` reads a chunk's payload, presence and values in one request. Block indexes use local coordinates, the block coordinates modulo the chunk size (never negative). `PutChunkStateExtra` replaces the state and all values in one write, with `IfVersion` and `ZRLE` as for `PutChunkState`; each value must belong to a block the new presence bitmap marks present.
- In `ChunkBatch`, `XPutOp(x, y, bits)` (`0`/`1` text, character `n` is bit `n`) and `XDelOp(x, y)` apply in order with `SetOp` and `UnsetOp`; a block must be present at its `XPutOp`. Batch values are text, so they count against the server's `MaxLineBytes`.
- `XPut` and `XDel` take no version condition: use `ChunkBatchIfVersion` or `PutChunkStateExtra` with `IfVersion`. Every change advances the chunk version.
- Padding bits past `BitLength` in a value's last byte are ignored on write and zero on read. `EncodeExtraSection` and `DecodeExtraSection` convert values to and from the wire's EXTRA section.
- The client checks value lengths (1 to 134217664 bits in `ceil(bits/8)` bytes) and block indexes before sending. The server checks the table's limits: a value over them, a value for an unset block, or a table without extra data fails with an `*Error` whose `ServerCode` is `INVALID_ARGUMENT`, and the connection stays usable.
- `Get`, `MGet`, `MSet`, `ChunkScan`, `ChunkRange` and `ChunkRadius` do not carry extra data.

## API

Package functions:

- `Connect(ctx, Options) (*Client, error)`
- `ConnectURI(ctx, uri) (*Client, error)`
- `NewClient(Options) (*Client, error)` — build without connecting
- `ConnectPool(ctx, PoolOptions) (*Pool, error)`
- `NewPool(PoolOptions) (*Pool, error)`
- `ParseURI(string) (URI, error)`; `URI.Table()` and `TableFromPath(path)`
  report the table a path names (empty for `/`)
- `SerializeCommand(parts ...string) ([]byte, error)`, `ReadFrame(*bufio.Reader) (Frame, error)`,
  `ParseInfo([]byte) map[string]string`; `$-1` reads as a `FrameNull` frame,
  and array items are `FrameBulk` or `FrameNull` frames
- `ZRLECompress([]byte) []byte`, `ZRLEDecompress([]byte, int) ([]byte, error)`
- `EncodeExtraSection(map[int]ExtraValue, blockCount) ([]byte, error)`, `DecodeExtraSection([]byte, blockCount) (map[int]ExtraValue, error)`

`Client` methods:

- `Connect(ctx)` / `Close()` / `URI()` — the URI path is the selected table
- `ServerInfo() *HelloInfo` — the `HELLO` reply of the most recent
  connection, nil before the first: `ServerVersion`, `Capabilities`,
  `MaxLineBytes`, `MaxAreaChunks`, `MaxResponseBytes`, `MaxScanLimit`,
  `MaxBatchOps`, `MaxExtraChunkBytes`, and `Table` (geometry and options, or nil)
- `CurrentTable()` — the table this client works on
- `Tables(ctx) ([]string, error)`
- `TableInfo(ctx, name) (TableInfo, error)` — geometry, options and store id
- `Use(ctx, name) (TableInfo, error)` — selects a table for this client; an
  unknown name fails with `CodeNoTable` and keeps the current one
- `Table(ctx, name) (*Client, error)` — a new connected client on `name`
- `CreateTable(ctx, name, TableSpec)`, `SetTableOptions(ctx, name, TableOptions)`,
  `DropTable(ctx, name)`
- `Ping(ctx)`
- `Info(ctx)` — runtime statistics of the selected table
- `Get(ctx, x, y) (BlockState, error)` — `{Exists: false}` for an unset block
- `Set(ctx, x, y, bits)`
- `Unset(ctx, x, y)`
- `MSet(ctx, blocks []Block)` — batch write, one round-trip; items apply in order
  and are not atomic as a group (on error, earlier items may already be
  applied) — use `ChunkBatch` for an atomic single-chunk update
- `MGet(ctx, blocks []BlockRef) ([]BlockState, error)` — batch read, one round-trip
- `ChunkExists(ctx, cx, cy)`
- `GetChunk(ctx, cx, cy, GetOptions) ([]byte, error)` — the payload
- `GetChunkState(ctx, cx, cy, GetOptions) (ChunkState, error)` — `Exists`,
  `Payload`, `Presence`
- `PutChunk(ctx, cx, cy, payload, PutOptions) (MutationResult, error)`
- `PutChunkState(ctx, cx, cy, ChunkStateInput, PutOptions) (MutationResult, error)`
- `XGet(ctx, x, y) (*ExtraValue, error)` — a block's extra data, nil when it has none
- `XPut(ctx, x, y, ExtraValue)`, `XDel(ctx, x, y)`
- `GetChunkStateExtra(ctx, cx, cy, GetOptions) (ChunkStateExtra, error)` — `Exists`, `Payload`, `Presence`, `Extra`
- `PutChunkStateExtra(ctx, cx, cy, ChunkStateExtraInput, PutOptions) (MutationResult, error)`
- `ChunkScan(ctx, limit, cursor)` — enumerate populated chunks in deterministic
  `(cx, cy)` order; returns `Coords` and `NextCursor`, pass `NextCursor` back to
  continue (limit 1..1024 per page)
- `ChunkRange(ctx, cx0, cy0, cx1, cy1, GetOptions)` — bounded rectangular
  multi-chunk read (max 256 chunks, 64 MiB response cap); returns a
  `RangeEntry{CX, CY, Payload, Presence}` per populated chunk
- `ChunkRadius(ctx, cx, cy, radiusChunks, GetOptions)` — bounded radius/disc
  multi-chunk read with the same limits and result shape as `ChunkRange`
- `ChunkVersion(ctx, cx, cy) (uint64, error)` — opaque chunk version token
- `ChunkBatch(ctx, cx, cy, operations)` / `ChunkBatchIfVersion(ctx, cx, cy, expectedVersion, operations)` —
  atomic single-chunk batch of `SetOp` / `UnsetOp` / `XPutOp` / `XDelOp`
  operations; returns a `MutationResult`
- `WALFlush(ctx)` — explicit durability barrier: returns once every previously
  acknowledged write is durable, even when the server runs in `relaxed` mode
- `Metrics(ctx)` — Prometheus text-format runtime metrics

`Pool` mirrors the same data methods and adds `Close()` and
`WithClient(ctx, fn)`.

## Versions

Chunk versions are opaque tokens: they change on every content mutation and
survive eviction and restart. A write that does not change the chunk keeps its
version.

Conditional writes (`PutOptions.IfVersion`, `ChunkBatchIfVersion`) report a
version mismatch as a normal result rather than an error:

```go
result, err := client.ChunkBatchIfVersion(ctx, 0, 0, version, operations)
if err != nil {
	return err
}
if !result.OK {
	// result.Version is the chunk's current version; re-read, reconcile, retry.
}
```

## Errors

Every failure is an `*Error` carrying the phase, command, and — for `-ERR ...`
responses — the server's code and message. Classify with `errors.Is` against:

- `ErrConnection`
- `ErrTimeout`
- `ErrProtocol` — also when the server does not speak protocol 2
- `ErrServer`
- `ErrAuth` (also matches `ErrServer`) — a wrong (`AUTH_FAILED`) or missing
  (`AUTH_REQUIRED`) token
- `ErrTLS`
- `ErrClosed`

A wrong token, a missing token, or an unknown table fails `Connect`.

```go
client, err := chunkdb.ConnectURI(ctx, "chunk://wrong-token@127.0.0.1:4242/")
if err != nil {
	var chunkErr *chunkdb.Error
	switch {
	case errors.Is(err, chunkdb.ErrAuth):
		errors.As(err, &chunkErr)
		log.Println("auth failed:", chunkErr.ServerCode, chunkErr.ServerMessage)
	case errors.Is(err, chunkdb.ErrTimeout):
		log.Println("timed out:", err)
	default:
		return err
	}
}
```

Client-side argument validation (a bit string containing anything but `0` and
`1`, a chunk of the wrong size, a malformed extra data value, an empty batch, a
chunk method without a table) fails with `ErrProtocol` before anything is
written to the socket.

## Limits

- a single bulk payload is capped at `MaxBulkBytes` (64 MiB); a larger declared length is rejected as a protocol error instead of being allocated. A chunk read is capped by the size its table's geometry gives (plus `max_extra_chunk_bytes` with extra data), which can be larger
- a request line longer than the server's `max_line_bytes` (for example a long `XPutOp` value in `ChunkBatch`) is refused before it is sent; the server would close the connection
- ZRLE reads bound decompression by the size the table's geometry gives and
  reject any payload that declares or produces a different size
- ZRLE reads of `GetChunkStateExtra` bound decompression by the state size plus `MaxExtraChunkBytes` (16 MiB)
