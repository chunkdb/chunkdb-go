# Watch the catalog

A catalog watch delivers changes to tables, users and slots, so a tool can keep its own copy of the catalog.

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).

`WatchCatalog` streams changes to tables, columns, CHECK constraints, users, grants and slots (WATCH CATALOG).
It needs MONITOR or ADMIN on `*`, and a server that reports `info.Supports("watch_catalog")`; otherwise it returns `ErrProtocol` before sending WATCH.

```go
catalog, err := client.WatchCatalog(ctx)
if err != nil {
	log.Fatal(err)
}
defer catalog.Close()
tables := map[string]*chunkdb.Schema{}
for {
	event, err := catalog.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	switch event := event.(type) {
	case *chunkdb.ResyncEvent:
		tables = map[string]*chunkdb.Schema{} // the next event holds the whole catalog
	case *chunkdb.CatalogEvent:
		for _, change := range event.Events {
			if *change.Kind != "table" {
				continue
			}
			schema, err := change.TableSchema()
			if err != nil {
				log.Fatal(err)
			}
			if schema == nil {
				delete(tables, *change.Name)
			} else {
				tables[*change.Name] = schema
			}
		}
	}
}
```

- The first `CatalogEvent` holds the whole catalog: one `create` for each table, user and slot.
- Each entry has `Kind` (`table`, `user` or `slot`), `Action` (`create`, `alter` or `drop`), `Name`, `Table` for a slot, and `Data`; `TableSchema`, `UserData` and `SlotData` decode `Data`, and return nil for a drop.
- `ResyncEvent` means the watch fell behind: discard your catalog and rebuild it from the next event, which holds the whole catalog again.
- Users, grants and slot owners appear only with MANAGES USERS; `CatalogEvent.User` is nil in the whole-catalog event.
- `RenameTable` arrives as a drop and a create; `SwapTables` as an alter of both names.
- The catalog epoch changes when the server restarts: watch again and rebuild from the first event.
- A catalog watch has no slot, so `Ack` is refused.

See [WATCH CATALOG](https://github.com/chunkdb/chunkdb/blob/main/docs/reference/FEED.md#watch-catalog) for the event contents.
