# Read and write blocks

A block holds typed column values at one grid coordinate.
A chunk groups a fixed rectangle of blocks.
An area selects a rectangle of chunks.

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
- An empty written chunk can retain its version until stored artifacts and cached state are removed.
- `Schema.Locate` maps block coordinates to chunk and local coordinates, including negative addresses.
- `GetArea` uses inclusive chunk bounds and returns chunks with present blocks.
- `GetAreaAround` selects chunks within a radius.

## Scans and schemas

| Method | Result or behavior |
|---|---|
| `ScanChunks` | One page ordered by CX, then CY; pass the last coordinate as `after`. |
| `AllChunks` | Iterates pages; zero uses the server's maximum page size. |
| `GetChunkRaw`, `SetChunkRaw` | Transfer encoded chunk forms; use `DecodeChunk` and `EncodeChunk` to convert. |
| `Tables`, `Describe`, `Schema` | List tables, refresh a schema, or use the cached schema. |
| `CreateTable`, `DropTable` | Create an explicit table or remove it. |
| `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType` | Change columns; narrowing conversions require a supported `Conversion`. |
| `SetTableOption` | Change runtime options; chunk geometry stays fixed. |

Scans across several requests do not share a snapshot.
The client refreshes schemas after its own table changes and when a typed write detects stale column sizes.
`SetChunk` re-encodes once after a schema mismatch; `SetChunkRaw` returns `SchemaMismatchError`.

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
A never-written chunk, collected empty chunk without a retained commit, or transaction-private modified chunk has a nil Version.
The schema version remains available for absent rows.

`GetAreaWithSchema` returns AreaRead.Chunks and SchemaVersion, including for an empty area.
Client, Tx and Pool expose these helpers.
Rows and requested metadata share the server's read boundary; area reads retain their existing consistency across chunks.
DESCRIBE supplies Schema.Epoch, Revision, Dims and ChunkColumns independently of Schema.Version.
Revision is a nullable completed frontier: read-only opens return nil because the writer frontier is unknown.
A numeric zero is a known frontier; never substitute zero for nil.

Unknown column types retain their type name and decode row/default values as raw bytes.
Known neighboring columns remain typed.
Typed legacy chunk forms with unknown-width columns fail explicitly; use raw forms or a projection of known columns.
