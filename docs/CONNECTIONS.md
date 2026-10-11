# Connections and errors

Connect using the same URI as the [world example](../examples/world/main.go).
`chunk://` selects a plain listener and `chunks://` selects TLS; both default to port 4242.
The URI path selects the client's default table; `Options.Table` overrides it.
Without either, the client selects the name `default`; selecting it does not create a table. A fresh server has no tables.
Nonempty explicit Host, Port, User and Password fields override URI values.
Every connection authenticates separately; use URI-escaped credentials or Options fields for unescaped values.

In a function returning `error`, with `ctx` and the URI in `CHUNKDB_URI`:

```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
    URI: os.Getenv("CHUNKDB_URI"),
    ConnectTimeout: 2 * time.Second,
    CommandTimeout: 3 * time.Second,
    PipelineDepth: 4,
})
if err != nil { return err }
defer client.Close()
if err := client.Ping(ctx); err != nil { return err }
```

`NewClient` is lazy; `Connect`/`ConnectURI` perform the connection and handshake.
ConnectTimeout bounds dialing and TLS; CommandTimeout bounds each HELLO, AUTH or statement reply.
Both default to five seconds; a negative value disables the client deadline while the context still applies.
A Client is safe for concurrent use; pipelining keeps ordered requests/replies on one socket, and its default depth is one.
After transport failure the failed request is not resent; the next request reconnects.
Cancelling an in-flight ordinary request drops the socket to prevent reply-stream misalignment; queued pipelined requests may also fail.
A transport error after a write leaves its outcome uncertain; reconnecting does not resolve it.

## TLS and pooling

For TLS set `chunks://` or `Options.TLS`; certificate verification is on by default and TLS 1.2 is the minimum.
`CA`, `Cert` and `Key` hold PEM bytes; `TLSServerName` selects the verified hostname and SNI.
`TLSInsecure` disables certificate verification for local testing.

```go
pool, err := chunkdb.ConnectPool(ctx, chunkdb.PoolOptions{
    Options: chunkdb.Options{URI: os.Getenv("CHUNKDB_URI")},
    MaxConnections: 2, MinConnections: 1,
})
if err != nil { return err }
defer pool.Close()
if err := pool.Ping(ctx); err != nil { return err }
```

Pool mirrors statement methods and Transaction; `WithClient` holds one lease for several calls.
AcquireTimeout bounds waiting for a lease; its zero default follows CommandTimeout, and a negative value leaves the deadline to the context.
Size the pool for the server's worker count; each open ordinary connection occupies a worker.

## Errors

Use `errors.Is` for `ErrConnection`, `ErrTimeout`, `ErrTLS`, `ErrProtocol`, `ErrAuth`, `ErrPermissionDenied`, `ErrServer`, `ErrVersionMismatch`, `ErrSchemaMismatch`, `ErrConflict` and `ErrClosed`.
Use `errors.As` for details: `*Error` carries Phase, Command, ServerCode and ServerMessage; permission, version, schema and transaction-conflict errors have their corresponding typed fields.
`*MigrationError` wraps the failed step's original error.
Server guidance remains in ServerMessage without changing structured permission or schema fields.
Client validation and malformed replies match ErrProtocol; authentication and permission failures also match ErrServer.
A table hidden by permissions returns CodeNoTable.
The client closes its socket for protocol errors on which the server closes it; it does not automatically retry application work.
