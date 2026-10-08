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
	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
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
		User:           "bot",
		Password:       os.Getenv("CHUNKDB_PASSWORD"),
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
		URI:           "chunks://bot:secret@chunkdb.local:4242/",
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
		Options:        chunkdb.Options{URI: "chunk://bot:secret@127.0.0.1:4242/world"},
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

	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/")
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

	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
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

	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
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

// Moving gold between two blocks in different chunks: both writes apply
// together, and a conflict with another write runs the function again.
func ExampleClient_Transaction() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	version, err := client.Transaction(ctx, func(tx *chunkdb.Tx) error {
		from, err := tx.GetBlock(ctx, "", 10, 4, "gold")
		if err != nil {
			return err
		}
		to, err := tx.GetBlock(ctx, "", 300, 7, "gold")
		if err != nil {
			return err
		}
		if err := tx.SetBlock(ctx, "", 10, 4, chunkdb.Record{"gold": from["gold"].(uint64) - 10}); err != nil {
			return err
		}
		return tx.SetBlock(ctx, "", 300, 7, chunkdb.Record{"gold": to["gold"].(uint64) + 10})
	})
	if errors.Is(err, chunkdb.ErrConflict) {
		log.Fatal("still conflicting after the retries: ", err)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(version)
}

// An administrator creates a user who reads and writes one table.
func ExampleClient_CreateUser() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://admin:secret@127.0.0.1:4242/")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.CreateUser(ctx, "bot", "bot-password", chunkdb.CreateUserOptions{}); err != nil {
		log.Fatal(err)
	}
	if err := client.Grant(ctx, chunkdb.RightWrite, "world", "bot"); err != nil {
		log.Fatal(err)
	}
	users, err := client.Users(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, user := range users {
		fmt.Println(user.Name, user.ManagesUsers, user.Grants)
	}
}

// Server errors carry their code, and the sentinels classify failures without
// unwrapping.
func ExampleError() {
	ctx := context.Background()

	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:wrong@127.0.0.1:4242/")
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
	case errors.Is(err, chunkdb.ErrPermissionDenied):
		var denied *chunkdb.PermissionDeniedError
		errors.As(err, &denied)
		fmt.Println("needs", denied.Right, "on", denied.Table)
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
