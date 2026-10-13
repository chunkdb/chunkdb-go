package chunkdb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/chunkdb/chunkdb-go/v2"
)

// documentationClient connects to the server selected by CHUNKDB_URI.
// The integration runner prepares world_go before executing these examples.
func documentationClient() (context.Context, *chunkdb.Client, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := chunkdb.ConnectURI(ctx, os.Getenv("CHUNKDB_URI"))
	if err != nil {
		cancel()
		log.Fatal(err)
	}
	return ctx, client, func() { client.Close(); cancel() }
}

// ExampleClient_SetBlock is checked against docs/DATA.md by the integration runner.
func ExampleClient_SetBlock() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_SetBlock
	version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 3, "label": "road"})
	if err != nil {
		log.Fatal(err)
	}
	block, err := client.GetBlock(ctx, "world_go", 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(block["tile"], block["label"])
	_, err = client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 4}, chunkdb.IfVersion(version))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("conditional write: applied")
	// docs:end ExampleClient_SetBlock
}

// ExampleClient_GetChunk is checked against docs/DATA.md by the integration runner.
func ExampleClient_GetChunk() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_GetChunk
	schema, err := client.Schema(ctx, "world_go")
	if err != nil {
		log.Fatal(err)
	}
	chunk := chunkdb.NewChunk(schema)
	chunk.SetBlock(0, 0, chunkdb.Record{"tile": 5, "label": "lake"})
	if _, err := client.SetChunk(ctx, "world_go", 2, 0, chunk); err != nil {
		log.Fatal(err)
	}
	read, err := client.GetChunk(ctx, "world_go", 2, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(read.Block(0, 0)["label"])
	area, err := client.GetArea(ctx, "world_go", 2, 0, 2, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("chunks:", len(area))
	// docs:end ExampleClient_GetChunk
}

// ExampleClient_Transaction is checked against docs/TRANSACTIONS.md by the integration runner.
func ExampleClient_Transaction() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_Transaction
	version, err := client.Transaction(ctx, func(tx *chunkdb.Tx) error {
		for _, x := range []int64{0, 2} {
			block, err := tx.GetBlock(ctx, "world_go", x, 0)
			if err != nil {
				return err
			}
			if block == nil {
				return fmt.Errorf("missing block at %d", x)
			}
			if err := tx.SetBlock(ctx, "world_go", x, 0,
				chunkdb.Record{"tile": block["tile"].(uint64) + 1}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("committed:", version > 0)
	// docs:end ExampleClient_Transaction
}

// ExampleClient_Watch is checked against docs/CHANGE_FEED.md by the integration runner.
func ExampleClient_Watch() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_Watch
	watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{
		Area: &chunkdb.Area{CX0: 0, CY0: 0, CX1: 1, CY1: 1},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watch.Close()
	version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 6})
	if err != nil {
		log.Fatal(err)
	}
	for {
		event, err := watch.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		switch event.(type) {
		case *chunkdb.ProgressEvent, *chunkdb.SchemaEvent:
			continue
		}
		change, ok := event.(*chunkdb.ChangeEvent)
		if !ok || change.Position.Epoch != watch.Start().Epoch || change.Position.Revision != version || len(change.Blocks) != 1 {
			log.Fatalf("expected the written block change, got %#v", event)
		}
		fmt.Println("tile:", change.Blocks[0].After["tile"])
		break
	}
	if err := watch.Close(); err != nil {
		log.Fatal(err)
	}
	// docs:end ExampleClient_Watch
}

// ExampleClient_CreateSlot is checked against docs/SLOTS.md by the integration runner.
func ExampleClient_CreateSlot() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_CreateSlot
	if err := client.CreateSlot(ctx, "world_go", "consumer"); err != nil {
		log.Fatal(err)
	}
	watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{Slot: "consumer"})
	if err != nil {
		log.Fatal(err)
	}
	defer watch.Close()
	version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 7})
	if err != nil {
		log.Fatal(err)
	}
	for {
		event, err := watch.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		switch event.(type) {
		case *chunkdb.ProgressEvent, *chunkdb.SchemaEvent:
			continue
		}
		change, ok := event.(*chunkdb.ChangeEvent)
		if !ok || change.Position.Epoch != watch.Start().Epoch || change.Position.Revision != version || len(change.Blocks) != 1 {
			log.Fatalf("expected the written change; rebuild after resync: %#v", event)
		}
		fmt.Println("apply:", change.Blocks[0].After["tile"])
		if err := watch.Ack(ctx, change.Position.Revision); err != nil {
			log.Fatal(err)
		}
		break
	}
	if err := watch.Close(); err != nil {
		log.Fatal(err)
	}
	if err := client.DropSlot(ctx, "world_go", "consumer"); err != nil {
		log.Fatal(err)
	}
	// docs:end ExampleClient_CreateSlot
}

// ExampleConnect is checked against docs/CONNECTIONS.md by the integration runner.
func ExampleConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// docs:begin ExampleConnect
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
	// docs:end ExampleConnect
}

// ExampleConnectPool is checked against docs/CONNECTIONS.md by the integration runner.
func ExampleConnectPool() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// docs:begin ExampleConnectPool
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
	// docs:end ExampleConnectPool
}

// ExampleClient_CreateUser is checked against docs/USERS.md by the integration runner.
func ExampleClient_CreateUser() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_CreateUser
	if err := client.CreateUser(ctx, "bot", "bot-password",
		chunkdb.CreateUserOptions{}); err != nil {
		log.Fatal(err)
	}
	if err := client.Grant(ctx, chunkdb.RightWrite, "world_go", "bot"); err != nil {
		log.Fatal(err)
	}
	users, err := client.Users(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, user := range users {
		if user.Name == "bot" {
			fmt.Println(user.Name, user.ManagesUsers)
		}
	}
	// docs:end ExampleClient_CreateUser
}

// ExampleParseBits is checked against docs/VALUES.md by the integration runner.
func ExampleParseBits() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleParseBits
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
	// docs:end ExampleParseBits
}

// ExampleClient_Migrate is checked against docs/OPERATIONS.md by the integration runner.
func ExampleClient_Migrate() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_Migrate
	steps := []chunkdb.Migration{
		{Name: "notes_create", Statement: "CREATE TABLE notes (v u8) CHUNK 2 x 2"},
		{Name: "notes_label", Statement: "ALTER TABLE notes ADD COLUMN label text(16) NULL"},
	}
	for range 2 {
		results, err := client.Migrate(ctx, steps)
		if err != nil {
			log.Fatal(err)
		}
		for _, result := range results {
			fmt.Println(result.Name, result.Status)
		}
	}
	// docs:end ExampleClient_Migrate
}

// ExampleClient_Do_backup is checked against docs/OPERATIONS.md by the integration runner.
func ExampleClient_Do_backup() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_Do_backup
	backupName := "go-docs-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	backup, err := client.Do(ctx, fmt.Sprintf("BACKUP TO '%s'", backupName))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("backup complete:", backup.Kind == chunkdb.ReplyMap)
	// docs:end ExampleClient_Do_backup
}

// ExampleClient_FlushWAL is checked against docs/OPERATIONS.md by the integration runner.
func ExampleClient_FlushWAL() {
	ctx, client, close := documentationClient()
	defer close()
	// docs:begin ExampleClient_FlushWAL
	if err := client.FlushWAL(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	metrics, err := client.Metrics(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("metrics available:", len(metrics) > 0)
	// docs:end ExampleClient_FlushWAL
}

var documentationExamples = map[string]func(){
	"ExampleClient_SetBlock":    ExampleClient_SetBlock,
	"ExampleClient_GetChunk":    ExampleClient_GetChunk,
	"ExampleClient_Transaction": ExampleClient_Transaction,
	"ExampleClient_Watch":       ExampleClient_Watch,
	"ExampleClient_CreateSlot":  ExampleClient_CreateSlot,
	"ExampleConnect":            ExampleConnect,
	"ExampleConnectPool":        ExampleConnectPool,
	"ExampleClient_CreateUser":  ExampleClient_CreateUser,
	"ExampleParseBits":          ExampleParseBits,
	"ExampleClient_Migrate":     ExampleClient_Migrate,
	"ExampleClient_Do_backup":   ExampleClient_Do_backup,
	"ExampleClient_FlushWAL":    ExampleClient_FlushWAL,
}

func TestDocumentationExampleProcess(t *testing.T) {
	name := os.Getenv("CHUNKDB_DOCUMENTATION_EXAMPLE")
	if name == "" {
		t.Skip("only invoked by documentation integration runner")
	}
	example := documentationExamples[name]
	if example == nil {
		t.Fatal(fmt.Sprintf("unknown documentation example %q", name))
	}
	example()
}
