# Resume a durable consumer

A slot retains changes for a named consumer across disconnections.
Use `ctx` and an administrator `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first; imports are in the [checked examples](../example_test.go).

<!-- go-example: ExampleClient_CreateSlot -->
```go
if err := client.CreateSlot(ctx, "world_go", "consumer"); err != nil {
	log.Fatal(err)
}
watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{Slot: "consumer"})
if err != nil {
	log.Fatal(err)
}
defer watch.Close()
if _, err := client.SetBlock(ctx, "world_go", 0, 0,
	chunkdb.Record{"tile": 7}); err != nil {
	log.Fatal(err)
}
for {
	event, err := watch.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, ok := event.(*chunkdb.SchemaEvent); ok {
		continue
	}
	change, ok := event.(*chunkdb.ChangeEvent)
	if !ok {
		log.Fatalf("rebuild consumer state after %T", event)
	}
	fmt.Println("apply:", change.Blocks[0].After["tile"])
	if err := watch.Ack(ctx, change.Position.Revision); err != nil {
		log.Fatal(err)
	}
	break
}
if err := watch.Close(); err != nil {
	log.Fatal(err)
}
if err := client.DropSlot(ctx, "world_go", "consumer"); err != nil {
	log.Fatal(err)
}
```

```output
apply: 7
```

Apply changes and save their Position atomically in your destination before acknowledging them.
After a crash, skip already applied revisions; an ACK alone cannot prevent duplicate external effects.
`Ack` sends the request without waiting for a success reply; `Close` waits for persistence unless an interrupted write closes the connection.
`Next` reports recoverable ACK rejections without ending the stream.

| Limit or requirement | Value |
|---|---|
| Slot names | `[a-z_][a-z0-9_]*`, at most 63 bytes. |
| Create or drop | ADMIN on the table. |
| Active watches | One per slot; another receives `CodeBusy`. |
| Retention | Table's `slot_max_bytes`; exceeded history makes the slot lost. |
| ACK bound | Last delivered ChangeEvent or initial position. |

AFTER selects a later starting point without acknowledging it.
Slots replay durable archived changes, including in relaxed durability mode.
`Slots(ctx, "")` lists visible tables; empty table arguments elsewhere select the default table.
A lost slot returns `CodeSlotLost`; rebuild its state before continuing.


See [Change feed](CHANGE_FEED.md) for events and [server recovery](https://github.com/chunkdb/chunkdb/blob/main/docs/CHANGE_FEED.md) for lost slots.
