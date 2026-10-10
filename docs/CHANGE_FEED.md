# Change feeds and durable slots

A watch owns a separate logged-in connection; ordinary client and pool requests remain available.
Close each watch explicitly: closing its originating client or pool does not close it.
The examples run inside a function returning `error`, after the [world example](../examples/world/main.go).

## Live changes

```go
watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{
    Area: &chunkdb.Area{CX0: 0, CY0: 0, CX1: 1, CY1: 1},
})
if err != nil { return err }
if _, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 6}); err != nil { watch.Close(); return err }
event, err := watch.Next(ctx)
if err != nil { watch.Close(); return err }
fmt.Printf("event: %T\n", event)
if err := watch.Close(); err != nil { return err }
```

WATCH returns after subscription setup; writes made afterwards are observable.
AREA bounds are inclusive chunk coordinates; watches need READ on the table.
`ChangeEvent.Blocks` contains complete Before/After records: nil means absence, while a nil field means NULL.
`User` is nil for anonymous writes; coordinates may be `ChunkOffset` if the absolute block address exceeds int64.
Revisions order events; timestamps may move backwards.

Save `event.GetPosition()` only after applying the event, and use `WatchOptions.After` when reconnecting.
An ordinary watch has bounded in-memory history and may produce `ResyncEvent`; this requests a state rebuild, not an ordinary change.
See the server's [resynchronization procedure](https://github.com/chunkdb/chunkdb/blob/main/docs/CHANGE_FEED.md#resynchronizing).
`SchemaEvent` supplies columns for following changes; the client caches schema versions and uses short-lived lookups for missing versions.
If a needed schema is unavailable, `Next` returns `ErrProtocol` instead of decoding with another schema.

## Durable consumer slots

```go
if err := client.CreateSlot(ctx, "world_go", "consumer"); err != nil { return err }
slotWatch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{Slot: "consumer"})
if err != nil { return err }
if _, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 7}); err != nil { slotWatch.Close(); return err }
for {
    event, err := slotWatch.Next(ctx)
    if err != nil { slotWatch.Close(); return err }
    if change, ok := event.(*chunkdb.ChangeEvent); ok {
        fmt.Println("apply:", change.Blocks[0].After["tile"])
        if err := slotWatch.Ack(ctx, change.Position.Revision); err != nil { slotWatch.Close(); return err }
        break
    }
    if _, resync := event.(*chunkdb.ResyncEvent); resync { slotWatch.Close(); return fmt.Errorf("consumer state needs rebuilding") }
}
if err := slotWatch.Close(); err != nil { return err }
slots, err := client.Slots(ctx, "world_go")
if err != nil { return err }
fmt.Println("slots:", len(slots))
if err := client.DropSlot(ctx, "world_go", "consumer"); err != nil { return err }
```

Create/Drop slot require ADMIN; names are `[a-z_][a-z0-9_]*`, at most 63 bytes.
An empty table in `Slots` lists every visible table; in Create/Drop it means the client's default table.
A slot has one active watch (`CodeBusy`); its start is its persisted ACK or a later `After` position.
AFTER does not acknowledge work.
Slots replay archived changes, including historical schemas, and then join the live stream; they send only persisted durable changes, even in relaxed mode.
`Ack` returns after writing the request, without a success reply; `Close` waits for persistence through UNWATCH unless closing interrupts an in-flight write.
See [server ACK persistence](https://github.com/chunkdb/chunkdb/blob/main/docs/CHANGE_FEED.md#durable-slots) for batching and durability.
The client accepts ACKs only through its last returned ChangeEvent or initial position; SchemaEvent and ResyncEvent cannot raise that bound.
An asynchronous `INVALID_ARGUMENT` rejection appears in `Next` and leaves the stream usable; decreasing ACKs are validated by the server.
To avoid duplicate external effects, commit output and its Position atomically in your sink, then ACK; ACK alone does not provide exactly-once effects.
When retention exceeds the server's limit, `Slot.Lost` is true and WATCH returns `CodeSlotLost`; follow the server's [slot recovery guidance](https://github.com/chunkdb/chunkdb/blob/main/docs/CHANGE_FEED.md#durable-slots).

Idle `Next` waits use their context, without a command timeout; cancelling a wait leaves the watch open.
Lookup or decoding failures end the stream; reconnect and resumption are application-controlled.
Close drains queued pushes within `CommandTimeout` (five seconds if disabled), discards recoverable ACK rejections while closing, and may report a terminal server error.
Cancellation during a partial ACK write closes the connection; Close-induced interruption is a write/connection error, while caller cancellation still matches `ErrTimeout`.
