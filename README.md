# chunkdb-go

Go client for [chunkdb 2.0](https://github.com/chunkdb/chunkdb): tables of chunks containing typed blocks, accessed through CQL and protocol 3.
Requires Go 1.25 or newer; uses only the standard library.

```bash
go get github.com/chunkdb/chunkdb-go/v2
```

## World in five minutes

Start a server with the [server quick start](https://github.com/chunkdb/chunkdb/blob/main/docs/QUICK_START.md), then run:

```bash
git clone https://github.com/chunkdb/chunkdb-go.git
cd chunkdb-go
CHUNKDB_URI='chunk://admin:your-password@127.0.0.1:4242/' go run ./examples/world
```

Use your administrator login and URI-escape special characters in the password.
For a local server started with `--auth none`, use `chunk://127.0.0.1:4242/`.
The [example](examples/world/main.go) creates `world_go`, writes sixteen typed blocks in four chunks, reads the area, then subscribes before updating one block:

```text
schema: applied
area: 4 chunks, 16 blocks written
block (0,0): tile=1 label=grass
change (0,0): tile=2 label=water
```

Run it again: the named schema step is `skipped`, and the data is written again.
Creating its table requires ADMIN on `*`; reading and writing require READ and WRITE on `world_go`.

## Use the client

Connect with `ConnectURI(ctx, uri)` and close the client when finished.
Data operations take a context and a table name; `""` uses `Options.Table`, then the URI path, then `default`.
A `Client` is safe for concurrent use; `Pool` provides multiple connections.

| Task | Guide with examples |
|---|---|
| Tables, blocks, chunks, areas, scans and conditional writes | [Data API](docs/DATA.md) |
| Column types, NULL and raw parameters | [Values](docs/VALUES.md) |
| Atomic writes across chunks | [Transactions](docs/TRANSACTIONS.md) |
| Users and rights | [Users](docs/USERS.md) |
| Live changes and durable consumer slots | [Change feeds](docs/CHANGE_FEED.md) |
| Named migrations, backup, durability and metrics | [Operations](docs/OPERATIONS.md) |
| Authentication, TLS, timeouts, pooling and errors | [Connections](docs/CONNECTIONS.md) |

[Package reference](https://pkg.go.dev/github.com/chunkdb/chunkdb-go/v2) lists the exported methods and types.
The server's [CQL reference](https://github.com/chunkdb/chunkdb/blob/main/docs/CQL.md) covers statements sent through `Do`.
The [2.x compatibility policy](https://github.com/chunkdb/chunkdb/blob/main/docs/COMPATIBILITY.md) keeps the 2.0 protocol and data usable throughout 2.x; breaking changes wait for 3.0.
