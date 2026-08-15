package chunkdb_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chunkdb/chunkdb-go"
)

func Example() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Set(ctx, 0, 0, "1011001110110011"); err != nil {
		log.Fatal(err)
	}

	state, err := client.ReadBlock(ctx, 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(state.Exists, state.Bits)
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
		Options:        chunkdb.Options{URI: "chunk://chunk-token@127.0.0.1:4242/"},
		MaxConnections: 4,
		MinConnections: 1,
		AcquireTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Set(ctx, 0, 0, "1011001110110011"); err != nil {
		log.Fatal(err)
	}

	// Several commands on one leased connection.
	err = pool.WithClient(ctx, func(ctx context.Context, client *chunkdb.Client) error {
		if err := client.Set(ctx, 1, 0, "0000111100001111"); err != nil {
			return err
		}
		return client.WALFlush(ctx)
	})
	if err != nil {
		log.Fatal(err)
	}
}

// Paging through every populated chunk.
func ExampleClient_ChunkScan() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var cursor *chunkdb.CoordPair
	for {
		page, err := client.ChunkScan(ctx, 256, cursor)
		if err != nil {
			log.Fatal(err)
		}
		for _, coordinate := range page.Coords {
			fmt.Println(coordinate.CX, coordinate.CY)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
}

// Read-modify-write against an opaque chunk version. On a mismatch, re-read the
// chunk, reconcile, and retry with the fresh version.
func ExampleClient_ChunkCompareAndSet() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for attempt := range 5 {
		version, err := client.ChunkVersion(ctx, 0, 0)
		if err != nil {
			log.Fatal(err)
		}

		state, err := client.ReadChunk(ctx, 0, 0)
		if err != nil {
			log.Fatal(err)
		}

		next := chunkdb.ChunkStateInput{Bits: state.Bits, Presence: state.Presence}
		// ... derive next from state ...

		result, err := client.ChunkCompareAndSet(ctx, 0, 0, version, next)
		if err != nil {
			log.Fatal(err)
		}
		if result.OK {
			fmt.Println("applied at version", result.Version)
			return
		}
		fmt.Println("retrying after mismatch on attempt", attempt)
	}
}

// An atomic multi-block update inside one chunk.
func ExampleClient_ChunkBatch() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.ChunkBatch(ctx, 0, 0, []chunkdb.BatchOperation{
		chunkdb.SetOp(0, 0, "1011001110110011"),
		chunkdb.SetOp(1, 0, "0000111100001111"),
		chunkdb.UnsetOp(2, 0),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.OK, result.Version)
}

// Server errors carry their protocol code, and the sentinels classify failures
// without unwrapping.
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
		fmt.Println("server rejected the command:", err)
	case errors.Is(err, chunkdb.ErrTimeout):
		fmt.Println("timed out:", err)
	case errors.Is(err, chunkdb.ErrConnection):
		fmt.Println("transport failure:", err)
	default:
		log.Fatal(err)
	}
}
