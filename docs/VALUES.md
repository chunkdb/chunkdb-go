# Choose column values

A schema defines each column's type; the client checks values before sending them.

| Column | Write | Read |
|---|---|---|
| `uN` | Integer in range | `uint64` |
| `iN` | Integer in range | `int64` |
| `bool` | `bool` | `bool` |
| `f32`, `f64` | `float32` or `float64` | `float32`, `float64` |
| `bits(N)` | `chunkdb.Bits` | `chunkdb.Bits` |
| `text(max)` | Valid UTF-8 `string` | `string` |
| `bytes(max)` | `[]byte` | `[]byte` |
| NULL | `nil` for a nullable column | `nil` |

Integers retain their full range; floating-point values support infinities and NaN.
Text limits count UTF-8 bytes; bytes values need not be UTF-8.
Wrong types, oversized values, and out-of-range inputs return `ErrProtocol` before sending a request.

## Bits and raw parameters

Use `ctx` and a connected `client` from [Connections](CONNECTIONS.md).
Run the [world example](../examples/world/main.go) first to create `world_go`.
Imports are in the [checked examples](../example_test.go).

<!-- go-example: ExampleParseBits -->
```go
bits, err := chunkdb.ParseBits("1010")
if err != nil {
	log.Fatal(err)
}
fmt.Println(bits.Bit(0), bits.Bit(1))
parameter, err := chunkdb.EncodeValue(chunkdb.TypeText(16), "river")
if err != nil {
	log.Fatal(err)
}
if _, err := client.Do(ctx, "SET BLOCK 0 0 IN world_go label=$1", parameter); err != nil {
	log.Fatal(err)
}
block, err := client.GetBlock(ctx, "world_go", 0, 0, "label")
if err != nil {
	log.Fatal(err)
}
fmt.Println(block["label"])
```

```output
true false
river
```

`ParseBits` reads the first digit as bit zero.
`Do` accepts encoded parameter bytes; a nil parameter means NULL.
Statements use one line; parameter values can contain CR and LF.

## Limits

| Value | Limit |
|---|---|
| `uN` | N is 1–64 bits. |
| `iN` | N is 2–64 bits. |
| `bits(N)` | N is 1–65535 bits. |
| Text and bytes per chunk | `var_max_chunk_bytes`, counting 12 overhead bytes per value. |
| One bulk reply | `MaxBulkBytes`: 160 MiB. |

Read server request and area limits through `ServerInfo()`.
See [Data](DATA.md) for reading values from blocks and chunks.
