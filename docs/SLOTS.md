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
version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 7})
if err != nil {
	log.Fatal(err)
}
for {
	event, err := watch.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	switch event.(type) {
	case *chunkdb.ProgressEvent, *chunkdb.SchemaEvent:
		continue
	}
	change, ok := event.(*chunkdb.ChangeEvent)
	if !ok || change.Position.Epoch != watch.Start().Epoch || change.Position.Revision != version || len(change.Blocks) != 1 {
		log.Fatalf("expected the written change; rebuild after resync: %#v", event)
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
| Active watches | One per slot; another receives `CodeSlotBusy`. |
| Retention | Table's `slot_max_bytes`; exceeded history makes the slot lost. |
| ACK bound | Last delivered change/chunks/progress event or initial position. |

AFTER selects a later starting point without acknowledging it.
Slots replay durable archived changes, including in relaxed durability mode.
`Slots(ctx, "")` lists visible tables; empty table arguments elsewhere require Options.Table or a URI table path.
A lost slot returns `CodeSlotLost`.
Rebuild your consumer state, drop the lost slot, and create it again before resuming.

See [Change feed](CHANGE_FEED.md) for events and [server recovery](https://github.com/chunkdb/chunkdb/blob/main/docs/FEED_RESUME.md) for lost slots.

`CreateSlotWithOptions` persists Area, Columns, NoBefore and Chunks filters.
Omitted watch filters inherit the slot's filters; explicitly supplied filters must match.
Changing filters requires a different slot, and IfNotExists also checks the existing filters.
Dropping a projected column makes the slot lost instead of broadening its projection.

WatchOptions.Takeover requires a slot and revokes its previous owner with `KILLED reason=slot_takeover`.
Taking over another user's connection additionally requires ADMIN on `*`.
WatchOptions.AckTimeout takes positive whole milliseconds; zero disables that watch deadline.
The deadline starts with the first unacknowledged change or chunks mutation and is not refreshed by later mutations or progress.

The example tolerates periodic progress and schema events while waiting for its exact write revision.
It uses the same bounded ctx for the whole wait and treats resync or reserved catalog events as unexpected.
Next must return a progress event before Ack can cover its revision.
Schema, resync and unknown events never raise that bound.
Takeover resumes from the persisted ACK, so unacknowledged events remain replayable.

Slots reports typed Filters and LostReason metadata.
Filters.Columns aligns current display names with stable ColumnIDs; a dropped column keeps its ID and has a nil name.
A rename changes the display name without changing that persisted identity.
Area stores lower coordinates followed by upper coordinates, with its length determined by dimensions.

Unknown loss reasons remain diagnostic strings.
A nil Filters value identifies a legacy response without the added metadata; nil projection lists within Filters mean all columns.
Fields preserves raw map fields for additional diagnostics.
