# chunkdb-go

[![CI](https://github.com/chunkdb/chunkdb-go/actions/workflows/ci.yml/badge.svg)](https://github.com/chunkdb/chunkdb-go/actions/workflows/ci.yml)
[![Go reference](https://pkg.go.dev/badge/github.com/chunkdb/chunkdb-go/v2.svg)](https://pkg.go.dev/github.com/chunkdb/chunkdb-go/v2)

Go client for [chunkdb](https://github.com/chunkdb/chunkdb), a storage engine for typed grid data.
You need Go 1.25 or newer; the client uses only the standard library.

## Install

```bash
go get github.com/chunkdb/chunkdb-go/v2
```

## Store and watch blocks

Start a server with [Quick start](https://github.com/chunkdb/chunkdb/blob/main/docs/QUICK_START.md), then run:

```bash
export CHUNKDB_URI="chunk://admin:$CHUNKDB_PASSWORD@127.0.0.1:4242/"
go run github.com/chunkdb/chunkdb-go/v2/examples/world@latest
```

```output
schema: applied
area: 4 chunks, 16 blocks written
block (0,0): tile=1 label=grass
change (0,0): tile=2 label=water
```

`CHUNKDB_PASSWORD` holds the administrator password from Quick start; URI-escape reserved characters in other passwords.
The [example](examples/world/main.go) creates its table with a named migration, writes 16 blocks, reads them back and watches one change:

```go
results, err := client.Migrate(ctx, []chunkdb.Migration{{
	Name: "world_go_schema", Statement: "CREATE TABLE world_go (tile u8, label text(16)) CHUNK 2 x 2",
}})
```

A rerun skips the applied migration and prints `schema: skipped`.
See [Operations](docs/OPERATIONS.md) for migration rules.

## Data layout

A block holds typed column values at a grid coordinate.
A chunk groups a fixed rectangle or box of blocks; an area selects chunk coordinates.
A version identifies chunk state for conditional writes; compare it for equality.
A revision orders committed changes within a table's change feed.

## Guides

- [Data](docs/DATA.md): blocks, chunks, areas, scans, and schemas.
- [Values](docs/VALUES.md): column types, NULL, and parameters.
- [Transactions](docs/TRANSACTIONS.md): publish several writes together.
- [Users](docs/USERS.md): grant table access.
- [Change feed](docs/CHANGE_FEED.md) and [Slots](docs/SLOTS.md): live changes and durable consumers.
- [Journal example](examples/journal/main.go): store every slot change and schema description in a file before acknowledging it; see [Event journal](https://github.com/chunkdb/chunkdb/blob/main/docs/EVENT_JOURNAL.md) for rebuilds and retention.
- [Operations](docs/OPERATIONS.md): migrations, backup, durability, and metrics.
- [Connections](docs/CONNECTIONS.md): authentication, TLS, channel binding, pooling, timeouts, and errors.

The [package reference](https://pkg.go.dev/github.com/chunkdb/chunkdb-go/v2) lists exported methods and types.
See [CQL](https://github.com/chunkdb/chunkdb/blob/main/docs/CQL.md) for statements sent through `Do`.
See [Compatibility](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md) for supported server and data formats.
