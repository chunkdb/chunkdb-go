# Connect to a server

Connect with the URI printed or configured for your server.
Set `CHUNKDB_URI`, then use a `ctx` from `context.WithTimeout` in these snippets.
Imports are in the [checked examples](../example_test.go).

<!-- go-example: ExampleConnect -->
```go
client, err := chunkdb.Connect(ctx, chunkdb.Options{
	URI:            os.Getenv("CHUNKDB_URI"),
	ConnectTimeout: 2 * time.Second,
	CommandTimeout: 3 * time.Second,
	PipelineDepth:  4,
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()
if err := client.Ping(ctx); err != nil {
	log.Fatal(err)
}
fmt.Println("connected")
```

```output
connected
```

## Addresses and authentication

| Setting | Behavior |
|---|---|
| `chunk://`, `chunks://` | Plain TCP or TLS; port defaults to 4242. |
| URI path | Selects the default table; `Options.Table` overrides it. |
| Empty table argument | Uses `Options.Table`, the URI path, then the name `default`. |
| Explicit Host, Port, User, Password | Nonempty values override URI values. |
| No credentials | Requires a server started with `--auth none`. |

Selecting a table does not create it; a fresh server has no tables.
URI-escape reserved password characters, or supply unescaped credentials through `Options`.
Every connection authenticates separately with SCRAM-SHA-256.
Use TLS over an untrusted network because authentication does not encrypt subsequent data.

## Timeouts and retries

- `NewClient` connects on its first request; `Connect` and `ConnectURI` connect immediately.
- ConnectTimeout bounds dialing and TLS; CommandTimeout bounds authentication and statement replies.
- Both default to five seconds; negative values leave deadlines to the context.
- A Client supports concurrent use; PipelineDepth defaults to one request at a time.
- Failed requests are never replayed; the next ordinary request reconnects after transport failure.
- Cancelling an in-flight request closes the connection; queued requests may also fail.
- A failed write can have succeeded before its reply was lost.

## TLS

`Options.TLS` also enables TLS; certificate verification is on, with TLS 1.2 as the minimum.
`CA`, `Cert`, and `Key` hold PEM bytes; `TLSServerName` sets the verified hostname and SNI.
Use `TLSInsecure` only for local testing.

## Pooling

<!-- go-example: ExampleConnectPool -->
```go
pool, err := chunkdb.ConnectPool(ctx, chunkdb.PoolOptions{
	Options:        chunkdb.Options{URI: os.Getenv("CHUNKDB_URI")},
	MaxConnections: 2, MinConnections: 1,
})
if err != nil {
	log.Fatal(err)
}
defer pool.Close()
if err := pool.Ping(ctx); err != nil {
	log.Fatal(err)
}
fmt.Println("pool connected")
```

```output
pool connected
```

Pool mirrors statement methods and Transaction; `WithClient` holds one lease for several calls.
AcquireTimeout bounds waiting for a lease; zero follows CommandTimeout, and a negative value leaves deadlines to the context.
Each ordinary connection occupies one server worker; size the pool accordingly.

## Errors

| Sentinel | Meaning |
|---|---|
| `ErrConnection`, `ErrTimeout`, `ErrTLS` | Transport, deadline, or TLS failure. |
| `ErrAuth`, `ErrPermissionDenied` | Login or required rights failed; these also match `ErrServer`. |
| `ErrVersionMismatch`, `ErrSchemaMismatch`, `ErrConflict` | Conditional write, encoded schema, or transaction conflict. |
| `ErrProtocol` | Invalid client input or malformed server reply. |
| `ErrServer`, `ErrClosed` | Server rejection or a closed client. |

Use `errors.Is` for classification and `errors.As` for typed details.
`Error` carries Phase, Command, ServerCode, and ServerMessage; specialized errors add structured fields.
`MigrationError` wraps the failed step's cause.
A table hidden by permissions returns `CodeNoTable`.
