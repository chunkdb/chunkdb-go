# Values

Values travel as binary parameters, checked against the table's schema before sending.

| Column | Write | Read |
|---|---|---|
| `uN` | Any integer type in range | `uint64` |
| `iN` | Any integer type in range | `int64` |
| `bool` | `bool` | `bool` |
| `f32`, `f64` | `float32` or `float64` | `float32`, `float64` |
| `bits(N)` | `chunkdb.Bits` | `chunkdb.Bits` |
| `text(max)` | Valid UTF-8 `string` | `string` |
| `bytes(max)` | `[]byte` | `[]byte` |
| NULL | `nil` for a nullable column | `nil` |

Integers retain their full signed or unsigned range; floating-point values support infinities and NaN.
Text limits count UTF-8 bytes; bytes values need not be UTF-8.
The server's `var_max_chunk_bytes` limit counts variable values and twelve bytes of overhead per value.
An out-of-range, oversized or wrong-type input returns an error matching `ErrProtocol` before the request is sent.

```go
bits, err := chunkdb.ParseBits("1010")
if err != nil { return err }
fmt.Println(bits.Bit(0), bits.Bit(1))
parameter, err := chunkdb.EncodeValue(chunkdb.TypeText(16), "river")
if err != nil { return err }
if _, err := client.Do(ctx, "SET BLOCK 0 0 IN world_go label=$1", parameter); err != nil { return err }
```

This block runs inside a function returning `error`, with `ctx` and a connected `client`; first create `world_go` with the [world example](../examples/world/main.go).
`ParseBits` uses lowest-bit-first order: the first digit is bit zero.
`Do` accepts raw parameter bytes from `EncodeValue`; a nil parameter is NULL.
Statements are single lines; CR and LF are permitted in parameter values.
The client caps one bulk reply at `MaxBulkBytes` (160 MiB); the server also supplies request and area limits through `ServerInfo`.
