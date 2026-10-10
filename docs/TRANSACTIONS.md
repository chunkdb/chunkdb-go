# Transactions

A transaction reads one snapshot of one table, sees its own writes, and publishes its chunk writes together at COMMIT.
In a function returning `error`, after the [world example](../examples/world/main.go), increment two blocks in different chunks:

```go
version, err := client.Transaction(ctx, func(tx *chunkdb.Tx) error {
    for _, x := range []int64{0, 2} {
        block, err := tx.GetBlock(ctx, "world_go", x, 0)
        if err != nil { return err }
        if block == nil { return fmt.Errorf("missing block at %d", x) }
        if err := tx.SetBlock(ctx, "world_go", x, 0, chunkdb.Record{"tile": block["tile"].(uint64) + 1}); err != nil { return err }
    }
    return nil
})
if err != nil { return err }
fmt.Println("committed:", version > 0)
```

`Transaction` returns the shared version of its written chunks, or zero when it writes nothing.
A server CONFLICT applies no writes; the client retries the callback up to five times after the first attempt (`TxRetries(n)` changes this).
Keep the callback free of external side effects, since it may run again, and return any statement error.
Other callback errors roll back and return without retrying; a transport failure is not retried because the outcome may be unknown.
A dropped connection rolls back any still-open transaction, but cannot undo a commit that already completed.
`*ConflictError` exposes `Reason` and matches `ErrConflict`; reasons are `chunk_changed`, `duration`, `history_limit` and `table_changed`.

Use only `tx` methods inside the callback: the originating client's other requests wait until the transaction ends.
`Pool.Transaction` leases one connection while other pool connections remain available.
The first data statement selects the table; another table is refused.
Tx methods include block, chunk and area operations; `IfVersion` is unavailable because COMMIT checks every read or written chunk.
A `Tx` must not be used after its callback returns.
The server bounds duration, write size and retained history; see the [transaction guide](https://github.com/chunkdb/chunkdb/blob/main/docs/TRANSACTIONS.md).
