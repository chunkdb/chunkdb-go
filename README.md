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
git clone https://github.com/chunkdb/chunkdb-go.git
cd chunkdb-go
export CHUNKDB_URI='chunk://admin:your-password@127.0.0.1:4242/'
go run ./examples/world
```

```output
schema: applied
area: 4 chunks, 16 blocks written
block (0,0): tile=1 label=grass
change (0,0): tile=2 label=water
```

Use your administrator password and URI-escape reserved characters.
The example creates its table; a fresh server has no tables.
Run it again and the schema step prints `skipped`.

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
- [Operations](docs/OPERATIONS.md): migrations, backup, durability, and metrics.
- [Connections](docs/CONNECTIONS.md): authentication, TLS, pooling, timeouts, and errors.

The [package reference](https://pkg.go.dev/github.com/chunkdb/chunkdb-go/v2) lists exported methods and types.
See [CQL](https://github.com/chunkdb/chunkdb/blob/main/docs/CQL.md) for statements sent through `Do`.
See [Compatibility](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md) for supported server and data formats.
