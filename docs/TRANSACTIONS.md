# Write blocks together

A transaction reads one table snapshot and publishes its writes together.
You can update two distant blocks without exposing half the change.

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first to create `world_go`.
Imports are in the [checked examples](../example_test.go).

## Commit a transaction

<!-- go-example: ExampleClient_Transaction -->
```go
version, err := client.Transaction(ctx, func(tx *chunkdb.Tx) error {
	for _, x := range []int64{0, 2} {
		block, err := tx.GetBlock(ctx, "world_go", x, 0)
		if err != nil {
			return err
		}
		if block == nil {
			return fmt.Errorf("missing block at %d", x)
		}
		if err := tx.SetBlock(ctx, "world_go", x, 0,
			chunkdb.Record{"tile": block["tile"].(uint64) + 1}); err != nil {
			return err
		}
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("committed:", version > 0)
```

```output
committed: true
```

- Reads see the snapshot plus your transaction's writes.
- `Transaction` returns the shared version of written chunks, or zero when nothing changes.
- A conflict applies no writes and retries the callback up to five times after the first attempt.
- `TxRetries(n)` changes that retry limit.
- Keep external effects outside the callback because it can run again.
- Return statement errors; other callback errors roll back without retrying.

## If it fails

| Error | What you do |
|---|---|
| `ErrConflict` | Inspect `ConflictError.Reason`; retry later if automatic attempts run out. |
| `ErrConnection`, `ErrTimeout` | Check application state before retrying; a completed commit can have lost its reply. |
| Another callback error | Correct the cause; the callback's writes roll back. |

Conflict reasons are `chunk_changed`, `duration`, `history_limit`, and `table_changed`.
A dropped connection rolls back an open transaction; it cannot undo a completed commit.

## Connection use

Use only `tx` methods inside the callback; the originating client's other requests wait for the transaction.
`Pool.Transaction` leases one connection while other pool connections remain available.
The first data statement selects the table; the server refuses another table.
Stop using `Tx` when its callback returns.

Tx supports block, chunk, and area methods.
It omits `IfVersion` because commit checks every chunk you read or write.
Server limits bound duration, write size, and retained history.
See [Transactions](https://github.com/chunkdb/chunkdb/blob/main/docs/TRANSACTIONS.md) for those limits and guarantees.
