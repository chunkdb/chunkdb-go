package chunkdb_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chunkdb/chunkdb-go/v2"
)

func Example() {
	ctx := context.Background()

	// The path names the default table: "" in a method means "world".
	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/world")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	version, err := client.SetBlock(ctx, "", 10, 4, chunkdb.Record{"id": 23, "light": 7})
	if err != nil {
		log.Fatal(err)
	}
	block, err := client.GetBlock(ctx, "", 10, 4, "id", "light")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(version, block["id"], block["light"])
}

func ExampleConnect() {
	ctx := context.Background()

	client, err := chunkdb.Connect(ctx, chunkdb.Options{
		Host:           "127.0.0.1",
		Port:           4242,
		Token:          "chunk-token",
		ConnectTimeout: 2 * time.Second,
		CommandTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		log.Fatal(err)
	}
}

// Connecting over TLS with a custom trust root.
func ExampleConnect_tls() {
	ctx := context.Background()

	ca, err := os.ReadFile("/etc/chunkdb/ca.pem")
	if err != nil {
		log.Fatal(err)
	}

	client, err := chunkdb.Connect(ctx, chunkdb.Options{
		URI:           "chunks://chunk-token@chunkdb.local:4242/",
		CA:            ca,
		TLSServerName: "chunkdb.local",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleConnectPool() {
	ctx := context.Background()

	pool, err := chunkdb.ConnectPool(ctx, chunkdb.PoolOptions{
		Options:        chunkdb.Options{URI: "chunk://chunk-token@127.0.0.1:4242/world"},
		MaxConnections: 4,
		MinConnections: 1,
		AcquireTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.SetBlock(ctx, "", 0, 0, chunkdb.Record{"id": 1}); err != nil {
		log.Fatal(err)
	}

	// Several statements on one leased connection.
	err = pool.WithClient(ctx, func(ctx context.Context, client *chunkdb.Client) error {
		if _, err := client.SetBlock(ctx, "", 1, 0, chunkdb.Record{"id": 2}); err != nil {
			return err
		}
		return client.FlushWAL(ctx)
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_CreateTable() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.CreateTable(ctx, "world", chunkdb.TableSpec{
		Columns: []chunkdb.ColumnDef{
			{Name: "id", Type: chunkdb.TypeUint(10), Required: true},
			{Name: "light", Type: chunkdb.TypeUint(4), Default: 15},
			{Name: "sign", Type: chunkdb.TypeText(256), Null: true},
		},
		ChunkWidth:  16,
		ChunkHeight: 16,
	})
	if err != nil {
		log.Fatal(err)
	}
}

// Paging through every chunk with a present block.
func ExampleClient_AllChunks() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/world")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for coord, err := range client.AllChunks(ctx, "", 0) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(coord.CX, coord.CY)
	}
}

// Read, compute, write IF VERSION, and retry on a mismatch.
func ExampleIfVersion() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/world")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for range 5 {
		chunk, err := client.GetChunk(ctx, "", 0, 0, "light")
		if err != nil {
			log.Fatal(err)
		}
		light := uint64(0)
		if value, ok := chunk.Columns["light"][0].(uint64); ok {
			light = value
		}

		_, err = client.SetBlock(ctx, "", 0, 0, chunkdb.Record{"light": min(light+1, 15)},
			chunkdb.IfVersion(chunk.Version))
		var mismatch *chunkdb.VersionMismatchError
		if errors.As(err, &mismatch) {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}
		return
	}
}

// Server errors carry their code, and the sentinels classify failures without
// unwrapping.
func ExampleError() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://wrong-token@127.0.0.1:4242/")
	if err == nil {
		defer client.Close()
		err = client.Ping(ctx)
	}

	switch {
	case err == nil:
	case errors.Is(err, chunkdb.ErrAuth):
		var typed *chunkdb.Error
		errors.As(err, &typed)
		fmt.Println("auth failed:", typed.ServerCode, typed.ServerMessage)
	case errors.Is(err, chunkdb.ErrServer):
		fmt.Println("server rejected the statement:", err)
	case errors.Is(err, chunkdb.ErrTimeout):
		fmt.Println("timed out:", err)
	case errors.Is(err, chunkdb.ErrConnection):
		fmt.Println("transport failure:", err)
	default:
		log.Fatal(err)
	}
}
