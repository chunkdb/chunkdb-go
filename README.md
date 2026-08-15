# chunkdb-go

Official Go client for [`chunkdb`](https://github.com/chunkdb/chunkdb).

Targets the stable `chunkdb` 1.x protocol; see the engine's
[compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).

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
- `Auth`, `Ping`, `Info`, `Get`, `ReadBlock`, `Exists`, `Set`, `Unset`, `MSet`, `MGet`, `ChunkExists`, `ReadChunk`, `SetChunk`, `SetChunkState`, `Chunk`, `ChunkBin`, `ChunkBinState`
- batch `MSet` / `MGet` (single round-trip for many blocks) and configurable request pipelining for high-latency links
- `context.Context` on every request, for per-call deadlines and cancellation
- typed errors with `errors.Is` sentinels and protocol error codes
- `int64` block and chunk coordinates across the full signed 64-bit domain
- configurable connect and command timeouts

## Requirements

- Go 1.25 or newer
- a reachable `chunkdb` 1.x server

## Install

```bash
go get github.com/chunkdb/chunkdb-go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/chunkdb/chunkdb-go"
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

	state, err := client.ReadBlock(ctx, 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(state.Exists, state.Bits)
}
```

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
- `CommandTimeout`: maximum time to wait for one command response
- both default to 5 seconds; a negative value disables the client-side deadline
  and leaves cancellation to the context
- `DisableAutoAuth`: skip the implicit `AUTH` sent after connecting
- `PipelineDepth`: maximum concurrent in-flight requests per connection (default 1)

## Context And Cancellation

Every request takes a `context.Context`. Cancelling it aborts the call and
returns an error that matches both `chunkdb.ErrTimeout` and the underlying
`context.Canceled` or `context.DeadlineExceeded`.

Protocol v1 has no request identifiers, so a response cannot be skipped without
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
- True single-socket multiplexing is intentionally out of scope for protocol v1.
  Parallelism comes from multiple sockets, not request IDs on one socket.
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

## API

Package functions:

- `Connect(ctx, Options) (*Client, error)`
- `ConnectURI(ctx, uri) (*Client, error)`
- `NewClient(Options) (*Client, error)` — build without connecting
- `ConnectPool(ctx, PoolOptions) (*Pool, error)`
- `NewPool(PoolOptions) (*Pool, error)`
- `ParseURI(string) (URI, error)`
- `SerializeCommand(parts ...string) ([]byte, error)`, `ReadFrame(*bufio.Reader) (Frame, error)`, `ParseInfo([]byte) map[string]string`
- `ZRLECompress([]byte) []byte`, `ZRLEDecompress([]byte, int) ([]byte, error)`

`Client` methods:

- `Connect(ctx)` / `Close()` / `URI()`
- `Auth(ctx, token)` — an empty token uses the configured one
- `Ping(ctx)`
- `Info(ctx)`
- `Get(ctx, x, y)`
- `ReadBlock(ctx, x, y)`
- `Exists(ctx, x, y)`
- `Set(ctx, x, y, bits)`
- `Unset(ctx, x, y)`
- `MSet(ctx, blocks []Block)` — batch write, one round-trip; items apply in order
  and are not atomic as a group (on error, earlier items may already be
  applied) — use `ChunkBatch` for an atomic single-chunk update
- `MGet(ctx, blocks []BlockRef) ([]string, error)` — batch read, one round-trip
- `ChunkExists(ctx, cx, cy)`
- `ReadChunk(ctx, cx, cy)`
- `SetChunk(ctx, cx, cy, bits)`
- `SetChunkState(ctx, cx, cy, ChunkStateInput)`
- `Chunk(ctx, cx, cy)`
- `ChunkBin(ctx, cx, cy)`
- `ChunkBinState(ctx, cx, cy)`
- `ChunkBinCompressed(ctx, cx, cy)` / `ChunkBinStateCompressed(ctx, cx, cy)` — same
  payloads as `ChunkBin`/`ChunkBinState`, transferred compressed and
  decompressed client-side
- `ChunkScan(ctx, limit, cursor)` — enumerate populated chunks in deterministic
  `(cx, cy)` order; returns `Coords` and `NextCursor`, pass `NextCursor` back to
  continue (limit 1..1024 per page)
- `ChunkRange(ctx, cx0, cy0, cx1, cy1)` — bounded rectangular multi-chunk read
  (max 256 chunks, 64 MiB response cap); returns one entry per populated chunk
- `ChunkRadius(ctx, cx, cy, radiusChunks)` — bounded radius/disc multi-chunk read
  with the same limits and result shape as `ChunkRange`
- `ChunkVersion(ctx, cx, cy) (uint64, error)` — opaque chunk version token
- `ChunkCompareAndSet(ctx, cx, cy, expectedVersion, ChunkStateInput)` —
  conditional full-chunk replace; returns `MutationResult`
- `ChunkBatch(ctx, cx, cy, operations)` / `ChunkBatchIfVersion(ctx, cx, cy, expectedVersion, operations)` —
  atomic single-chunk batch of `SetOp` / `UnsetOp` operations; same result type
- `WALFlush(ctx)` — explicit durability barrier: returns once every previously
  acknowledged write is durable, even when the server runs in `relaxed` mode
- `Metrics(ctx)` — Prometheus text-format runtime metrics

`Pool` mirrors the same high-level data methods and adds `Close()` and
`WithClient(ctx, fn)`.

## Reads

`ReadBlock` is the preferred high-level read API:

```go
type BlockState struct {
	Exists bool
	Bits   string
}
```

- unset block -> `{Exists: false, Bits: ""}`
- explicit zero block -> `{Exists: true, Bits: "000...0"}`

`Get` is the lower-level read and still returns the configured zero-bit payload
when a block is unset. Use `Exists` only when you specifically want the
protocol-style presence check.

Chunk-level presence uses the same pattern:

- `ChunkExists` tells you whether the chunk is explicitly present
- `ReadChunk` is the preferred high-level chunk read API and returns:

```go
type ChunkState struct {
	Exists   bool
	Bits     string
	Presence string
}
```

- absent chunk -> `{Exists: false, Bits: "000...0", Presence: "000...0"}`
- explicit zero chunk -> `{Exists: true, Bits: "000...0", Presence: "111...1"}`
- `Chunk` is the lower-level chunk read and still returns the zero-bit payload
  for an absent chunk
- `SetChunk` explicitly replaces the full chunk payload, including an all-zero chunk
- `SetChunkState` writes mixed present/absent block state in one request
- `ChunkBinState` returns `[payload bytes][presence bytes]` for exact chunk-state transfer

`Info` returns:

```go
type Info struct {
	Raw    string
	Values map[string]string
}
```

`Values` contains the parsed `INFO` key/value pairs exactly as reported by the
server. `SetChunkState` and the `ChunkBin*` size checks derive the chunk
geometry from `INFO` once per client and cache it.

## Versions

Chunk versions are opaque: they change on every content mutation and whenever
the server reloads the chunk (eviction or restart), so a stale version can never
silently match after recovery.

`ChunkCompareAndSet` and `ChunkBatchIfVersion` report a version mismatch as a
normal result rather than an error:

```go
result, err := client.ChunkCompareAndSet(ctx, 0, 0, version, next)
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
- `ErrProtocol`
- `ErrServer`
- `ErrAuth` (also matches `ErrServer`)
- `ErrTLS`
- `ErrClosed`

```go
if err := client.Ping(ctx); err != nil {
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

Client-side argument validation (a payload containing anything but `0` and `1`,
a chunk-state payload of the wrong length, an empty batch) fails with
`ErrProtocol` before anything is written to the socket.

## Limits

- a single bulk payload is capped at `MaxBulkBytes` (64 MiB), matching the
  server's response-size limit; a larger declared length is rejected as a
  protocol error instead of being allocated
- `ChunkBinCompressed` and `ChunkBinStateCompressed` bound decompression by the
  geometry-derived expected size and reject any payload that declares or
  produces a different size

## Local Development

```bash
go test ./...
go test -race ./...
```

Integration tests run against a real server binary and are skipped when none is
found. They look for it as:

1. `$CHUNKDB_SERVER_BIN`
2. `$CHUNKDB_REPO_ROOT/build-js-tests/chunkdb_server`
3. `../chunkdb/build-js-tests/chunkdb_server`

TLS fixtures are generated at test time, so no certificates are checked in.
