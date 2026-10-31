# Contributing

Use the [organization contribution guide](https://github.com/chunkdb/.github/blob/main/CONTRIBUTING.md) for reporting issues and proposing changes.

## Local checks

Use the Go toolchain declared in `go.mod`.
Run these checks from the repository root:

```bash
gofmt -l .
go vet ./...
go test -race ./...
go build ./...
```

`gofmt -l .` must print no paths.
Integration tests skip when no server binary is found.
To include them, build the server with TLS support and set `CHUNKDB_SERVER_BIN` to its absolute path:

```bash
CHUNKDB_SERVER_BIN=/path/to/chunkdb/build/chunkdb_server go test -race -count=1 ./...
```

Update exported doc comments and the relevant guide in `docs/` when public behavior changes.
