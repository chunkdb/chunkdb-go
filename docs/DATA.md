# Read and write blocks

A block holds typed column values at one grid coordinate.
A chunk groups a fixed rectangle or three-dimensional box of blocks.
An area selects a rectangle, box, or radius of chunk coordinates.

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first to create `world_go`.

<!-- go-example: ExampleClient_SetBlock -->
```go
version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 3, "label": "road"})
if err != nil {
	log.Fatal(err)
}
block, err := client.GetBlock(ctx, "world_go", 0, 0)
if err != nil {
	log.Fatal(err)
}
fmt.Println(block["tile"], block["label"])
_, err = client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 4}, chunkdb.IfVersion(version))
if err != nil {
	log.Fatal(err)
}
fmt.Println("conditional write: applied")
```

```output
3 road
conditional write: applied
```

- `GetBlock` returns a nil record for an absent block; a nil field means NULL.
- `SetBlock` changes supplied columns; new blocks use schema defaults, NULL, or zero values.
- A version identifies a chunk's state for conditional writes; compare it for equality.
- `IfVersion` checks the whole chunk, including other blocks; rejection returns `VersionMismatchError`.
- `DeleteBlock` removes a block and returns the new chunk version.

## Chunks and areas

<!-- go-example: ExampleClient_GetChunk -->
```go
schema, err := client.Schema(ctx, "world_go")
if err != nil {
	log.Fatal(err)
}
chunk := chunkdb.NewChunk(schema)
chunk.SetBlock(0, 0, chunkdb.Record{"tile": 5, "label": "lake"})
if _, err := client.SetChunk(ctx, "world_go", 2, 0, chunk); err != nil {
	log.Fatal(err)
}
read, err := client.GetChunk(ctx, "world_go", 2, 0)
if err != nil {
	log.Fatal(err)
}
fmt.Println(read.Block(0, 0)["label"])
area, err := client.GetArea(ctx, "world_go", 2, 0, 2, 0)
if err != nil {
	log.Fatal(err)
}
fmt.Println("chunks:", len(area))
```

```output
lake
chunks: 1
```

- `SetChunk` replaces the whole chunk; `Chunk.Present` identifies present blocks.
- `GetChunk` and `GetChunkRaw` return `nil, nil` for a never-written chunk.
- Create its first form with `NewChunk(schema)` and an unconditional write.
- Collection keeps an empty written chunk's version and commit time in a retained tombstone.
- Typed chunk decoding and raw writes require the checked CKCF form for both table dimensions.
- `Schema.Locate` maps block coordinates to chunk and local coordinates, including negative addresses.
- `GetArea` uses inclusive chunk bounds and returns chunks with present blocks or nonbaseline chunk metadata.
- `GetAreaAround` selects chunks within a radius.

`GetAreaPage` accepts the same rectangle followed by `AreaPageOptions{Limit, After}` and optional column names.
`GetAreaAroundPage` selects a disc; `GetAreaPage3D` and `GetAreaAroundPage3D` use `AreaPageOptions3D` for boxes and balls.
Client, Pool and Tx expose these methods and require the advertised `area_paging` feature.
Each page contains `Chunks`, `SchemaVersion`, `More` and `After`; pass its cursor with the same selector and projection to continue.
Limit zero uses the negotiated maximum.
The cursor advances across requested empty cells, so an unfinished page may contain no populated chunks.
A completed page always has a nil cursor, including when its final page contains populated chunks.
The server can shorten pages at its response-byte limit.
Page calls return only one page and do not accumulate earlier pages or provide a snapshot across calls.
The original command and context deadlines also cover typed area validation and decoding; expiry closes that response's connection and ends an active transaction.

All GET AREA replies enforce negotiated encoded-response bytes while parsing, before allocating declared bulk bodies.
Known raw and typed area shapes also check the chunk-array count before allocating its initial storage.
Aggregate reply storage is bounded to 64 MiB of reply slots; server errors and unsolicited pushes keep their separate framing.
An oversized or malformed response closes the connection; a later independent call may reconnect.
Typed area methods admit at most 4194304 combined block-column and chunk-metadata values across the complete area before allocating chunk value arrays.
Select fewer columns or request smaller pages when this limit is exceeded.
Use `Do` with a bounded GET AREA page to retain checked raw forms without dense typed decoding.

## Three-dimensional tables

Set `TableSpec.ChunkDepth` to a positive side length when creating a three-dimensional table.
When specifying large-chunk geometry, supply `LargeWidth`, `LargeHeight`, and `LargeDepth` together.
`Schema.Dims` is fixed at table creation, and `ChunkDepth` is one for a two-dimensional table.

Client, Pool, and Tx provide `GetBlock3D`, `SetBlock3D`, `DeleteBlock3D`, `GetChunk3D`, `GetChunkRaw3D`, `SetChunk3D`, and `SetChunkRaw3D` with explicit x, y, and z coordinates.
`GetBlock3DWithOptions` and `GetBlock3DWithMetadata` select read metadata.
`GetArea3D` and `GetArea3DWithSchema` take inclusive lower and upper corners; `GetAreaAround3D` selects a ball in chunk coordinates.
Their results use `AreaChunk3D`, preserving all three coordinates, including an explicit zero z.
The existing two-axis methods and coordinate types retain their signatures.

`Schema.Locate3D` applies floor division to negative block coordinates.
`Chunk.Index3D`, `Block3D`, `SetBlock3D`, and `DeleteBlock3D` address local blocks with x varying fastest, then y, then z.

## Chunk columns and partial writes

`TableSpec.ChunkColumns` declares a separate column namespace for one metadata row per chunk.
`Schema.ChunkColumns` describes that row, and `Chunk.Metadata` contains its values.
Block-column projections retain the complete metadata row.
Typed chunk reads admit at most 4194304 block-column cells before allocating dense value arrays.
Select fewer columns or read the raw form when a chunk exceeds that limit.
`AddChunkColumn`, `DropChunkColumn`, and `RenameChunkColumn` change that namespace.

`SetChunk` and `SetChunk3D` replace both the block values and the metadata row.
Omitted metadata uses DEFAULT, otherwise NULL for nullable columns, otherwise zero or empty.
An explicit nil metadata value remains NULL and is refused for a nonnullable column.
A REQUIRED chunk column without DEFAULT must be supplied when first initializing a chunk.

`SetChunkMetadata` and `SetChunkMetadata3D` update the supplied chunk columns while retaining block values.
`SetChunkColumns` and `SetChunkColumns3D` replace selected block columns while preserving metadata, even when every block column is selected.
The incoming presence removes absent blocks, retains unnamed values on continuing blocks, and initializes unnamed values of newly present blocks from defaults.
Tx stages these updates for COMMIT; Pool exposes the same helpers through leased clients.

`IfAbsent()` permits a chunk write only when the chunk is not populated; retained tombstones count as absent and metadata-only populated chunks do not.
An occupied chunk returns `VersionMismatchError` with its current version and leaves the chunk unchanged.
It cannot be combined with `IfVersion` or used for block writes.
These conditions report `VersionMismatchError` when refused.
Stored variable data stays readable after lowering its table write quota.
Chunk write requests enforce the hard format limit and leave growth-aware quota admission to the server, which can accept non-growing replacements of older data above that quota.
Standalone encoding uses the supplied schema's configured budget.

## Scans and schemas

| Method | Result or behavior |
|---|---|
| `ScanChunks` | One page ordered by CX, then CY; pass the last coordinate as `after`. |
| `AllChunks` | Iterates pages; zero uses the server's maximum page size. |
| `ScanChunks3D`, `AllChunks3D` | Preserve all three coordinates and reject a nonadvancing page. |
| `ScanChunksWithOptions`, `ScanChunks3DWithOptions` | Retained versions, optional data, and table lifetime/frontier metadata. |
| `GetChunkRaw`, `SetChunkRaw` | Retain encoded forms and write forms whose column types the client knows. |
| `Tables`, `Describe`, `Schema` | List tables, refresh a schema, or use the cached schema. |
| `CreateTable`, `DropTable` | Create an explicit table or remove it. |
| `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType` | Change columns; narrowing conversions require a supported `Conversion`. |
| `SetTableOption` | Change runtime options; chunk geometry stays fixed. |

Scans across several requests do not share a snapshot.
The option-based scan helpers return Epoch, nullable Position, and RetainedDeletionFloor with each page.
Pass a saved Epoch to bind subsequent pages to the same table lifetime.
Since selects versions strictly greater than that revision and includes retained deletions.
History below RetainedDeletionFloor returns RESYNC_REQUIRED with reason=history_lost and Metadata.Floor; rebuild the snapshot before resuming.
WithData supplies decoded forms, and Deleted distinguishes a tombstone from a live form.
Deleted is nil without WithData, because version-only entries do not report whether they were deleted.
Columns requires WithData and preserves complete chunk metadata.
Ordinary scans omit tombstones, while GET CHUNK still returns their retained empty form and version.
The client refreshes schemas after its own table changes and when a typed write detects stale column sizes.
`SetChunk` re-encodes once after a schema mismatch; `SetChunkRaw` returns `SchemaMismatchError`.
Raw reads retain forms with unknown column types.
Raw writes need known column widths for the size check and refuse unknown types, unsupported mutation sections, or oversized forms before sending.
Use a client that knows every column type to write those forms.

Coordinates use `int64`; block methods count blocks, while chunk, area, and scan methods count chunks.
Read request limits through `client.ServerInfo()`.
`TableOptions.FeedBufferBytes` and `SlotMaxBytes` set positive per-table byte budgets.
Zero uses server defaults, normally 64 MiB and 1 GiB; `Describe` reports effective values.

Use [Migrations](OPERATIONS.md) for repeatable schema setup.
See the [package reference](https://pkg.go.dev/github.com/chunkdb/chunkdb-go/v2) for method arguments.

## Read metadata

`GetBlockWithMetadata` returns a BlockRead with Record, nullable Version and SchemaVersion.
`GetBlockWithOptions` selects individual metadata fields with ReadOptions.WithVersion and WithSchema.
An absent block with a retained commit retains its version.
A never-written chunk or transaction-private modified chunk has a nil Version.
Collected empty chunks retain their producing version until explicit tombstone compaction forgets them.
The schema version remains available for absent rows.

`GetAreaWithSchema` returns AreaRead.Chunks and SchemaVersion, including for an empty area.
Client, Tx and Pool expose these helpers.
Rows and requested metadata share the server's read boundary; area reads retain their existing consistency across chunks.
DESCRIBE supplies Schema.Epoch, Revision, Dims and ChunkColumns independently of Schema.Version.
Revision is a nullable completed frontier; nil means the server could not establish it.
A numeric zero is a known frontier; never substitute zero for nil.

Unknown column types retain their type name and decode row/default values as raw bytes.
Known neighboring columns remain typed.
Checked chunk descriptors preserve opaque unknown fixed values without changing known neighboring columns.
Mutations refuse unknown types and sections instead of republishing data they cannot validate.
