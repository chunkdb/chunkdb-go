# Data API

These examples go inside a function returning `error`, with `ctx` and an administrator `client` from `ConnectURI`.
Run the [world example](../examples/world/main.go) first to create `world_go` with `tile u8` and `label text(16)`.
Block coordinates are `int64`; area and scan coordinates count chunks.

## Blocks and conditional writes

```go
version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 3, "label": "road"})
if err != nil { return err }
block, err := client.GetBlock(ctx, "world_go", 0, 0)
if err != nil { return err }
fmt.Println(block["tile"], block["label"])
_, err = client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 4}, chunkdb.IfVersion(version))
if err != nil { return err }
```

`GetBlock` returns a nil record for an absent block; a nil field means NULL.
A block write changes only the supplied columns; new blocks take defaults, NULL or zero/empty as their schema permits.
`IfVersion` checks the entire chunk, including changes to other blocks; rejection returns `*VersionMismatchError`.
`DeleteBlock` removes a block and returns the new chunk version.

## Chunks, areas and scans

```go
schema, err := client.Schema(ctx, "world_go")
if err != nil { return err }
chunk := chunkdb.NewChunk(schema)
chunk.SetBlock(0, 0, chunkdb.Record{"tile": 5, "label": "lake"})
if _, err := client.SetChunk(ctx, "world_go", 2, 0, chunk); err != nil { return err }
read, err := client.GetChunk(ctx, "world_go", 2, 0)
if err != nil { return err }
if read != nil { fmt.Println(read.Block(0, 0)["label"]) }
area, err := client.GetArea(ctx, "world_go", 0, 0, 2, 1)
if err != nil { return err }
fmt.Println("chunks:", len(area))
for coord, err := range client.AllChunks(ctx, "world_go", 0) {
    if err != nil { return err }
    fmt.Println(coord.CX, coord.CY)
}
```

`GetChunk` and `GetChunkRaw` return `nil, nil` for a never-written chunk. Build a fresh form with `NewChunk(schema)` and perform an ordinary first write; NULL supplies no version for `IfVersion`. A written chunk with all blocks deleted retains its empty form and version until collection removes it.

`SetChunk` replaces the entire chunk; `Chunk.Present` distinguishes absent blocks.
`Schema.Locate(x, y)` maps absolute blocks to chunk and local coordinates, including negative addresses.
`GetArea` uses inclusive chunk bounds and returns only chunks with present blocks; `GetAreaAround` selects a radius.
`ScanChunks` returns a page in ascending CX then CY order; use its last coordinate as `after` while `More` is true.
`AllChunks` handles paging; zero uses the server's maximum scan limit.
`GetChunkRaw`/`SetChunkRaw` transfer chunk forms, and `DecodeChunk`/`EncodeChunk` convert them.
Area limits come from `client.ServerInfo()`; multi-request scans are not a snapshot.

## Tables and schema

```go
err := client.CreateTable(ctx, "scratch", chunkdb.TableSpec{
    Columns: []chunkdb.ColumnDef{{Name: "v", Type: chunkdb.TypeUint(8), Required: true}},
    ChunkWidth: 2, ChunkHeight: 2,
})
if err != nil { return err }
if err := client.AddColumn(ctx, "scratch", chunkdb.ColumnDef{Name: "label", Type: chunkdb.TypeText(16), Null: true}); err != nil { return err }
if err := client.DropTable(ctx, "scratch"); err != nil { return err }
```

`Tables` lists tables, `Describe` refreshes a schema and `Schema` uses the client's cache.
Chunk and large-chunk geometry is fixed at creation.
`RenameColumn`, `DropColumn`, `AlterColumnType` and `SetTableOption` change schema or runtime options; the server validates each operation.
A narrowing conversion uses `ConvertNone`, `ConvertClamp`, `ConvertDefault` or `ConvertTruncate` as appropriate to the type.
The client invalidates schema caches after its own table statements, including those inside `Migrate`, and refreshes when a typed write detects stale schema.
`SetChunk` re-encodes once on schema mismatch; `SetChunkRaw` returns `*SchemaMismatchError`.
Use [named migrations](OPERATIONS.md) for repeatable application startup.
