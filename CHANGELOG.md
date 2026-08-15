# Changelog

All notable changes to this project will be documented in this file.

This client follows [Semantic Versioning](https://semver.org/) and targets the
stable `chunkdb` 1.x protocol; see the engine's
[compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md).

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
