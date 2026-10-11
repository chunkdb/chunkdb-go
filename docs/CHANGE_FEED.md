# Watch block changes

A watch delivers committed changes while your ordinary requests continue on another connection.
Close each watch yourself; closing its Client or Pool leaves it open.

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first to create `world_go`.
Imports are in the [checked examples](../example_test.go).

## Live changes

<!-- go-example: ExampleClient_Watch -->
```go
watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{
	Area: &chunkdb.Area{CX0: 0, CY0: 0, CX1: 1, CY1: 1},
})
if err != nil {
	log.Fatal(err)
}
defer watch.Close()
if _, err := client.SetBlock(ctx, "world_go", 0, 0,
	chunkdb.Record{"tile": 6}); err != nil {
	log.Fatal(err)
}
event, err := watch.Next(ctx)
if err != nil {
	log.Fatal(err)
}
change, ok := event.(*chunkdb.ChangeEvent)
if !ok {
	log.Fatalf("expected a change, got %T", event)
}
fmt.Println("tile:", change.Blocks[0].After["tile"])
if err := watch.Close(); err != nil {
	log.Fatal(err)
}
```

```output
tile: 6
```

- Watch returns when the subscription is ready; later writes are observable.
- Area bounds are inclusive chunk coordinates; you need READ on the table.
- `ChangeEvent.Blocks` has complete Before and After records; nil means absence, while a nil field means NULL.
- A revision orders committed changes within a table; timestamps can move backwards.
- An epoch identifies the table's lifetime and changes when you drop and recreate it.
- Save `event.GetPosition()` after applying the event; resume with `WatchOptions.After`.
- `ResyncEvent` requires rebuilding local state; `SchemaEvent` supplies changed columns.

Use [Slots](SLOTS.md) to resume a durable consumer.

Idle Next waits use the context without a command timeout; cancelling that wait keeps the watch open.
Lookup or decoding failures end the stream, including unavailable schemas with `ErrProtocol`.
User is nil for anonymous writes; `ChunkOffset` represents coordinates outside int64.
Close drains within CommandTimeout, using five seconds when that timeout is disabled.

See [Change feed](https://github.com/chunkdb/chunkdb/blob/main/docs/CHANGE_FEED.md) for resynchronization and slot recovery.
