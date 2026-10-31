// Run with CHUNKDB_URI=chunk://admin:password@127.0.0.1:4242/ go run ./examples/world.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	chunkdb "github.com/chunkdb/chunkdb-go/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	uri := os.Getenv("CHUNKDB_URI")
	if uri == "" {
		return fmt.Errorf("set CHUNKDB_URI to your server URI, for example chunk://admin:password@127.0.0.1:4242/")
	}
	client, err := chunkdb.ConnectURI(ctx, uri)
	if err != nil {
		return err
	}
	defer client.Close()
	results, err := client.Migrate(ctx, []chunkdb.Migration{{
		Name: "world_go_schema", Statement: "CREATE TABLE world_go (tile u8, label text(16)) CHUNK 2 x 2",
	}})
	if err != nil {
		return err
	}
	fmt.Printf("schema: %s\n", results[0].Status)
	for y := int64(0); y < 4; y++ {
		for x := int64(0); x < 4; x++ {
			if _, err := client.SetBlock(ctx, "world_go", x, y, chunkdb.Record{"tile": 1, "label": "grass"}); err != nil {
				return err
			}
		}
	}
	area, err := client.GetArea(ctx, "world_go", 0, 0, 1, 1)
	if err != nil {
		return err
	}
	fmt.Printf("area: %d chunks, 16 blocks written\n", len(area))
	block, err := client.GetBlock(ctx, "world_go", 0, 0)
	if err != nil {
		return err
	}
	fmt.Printf("block (0,0): tile=%v label=%v\n", block["tile"], block["label"])
	// Watch returns only after the subscription is ready; write after that.
	watch, err := client.Watch(ctx, "world_go", chunkdb.WatchOptions{Area: &chunkdb.Area{CX0: 0, CY0: 0, CX1: 1, CY1: 1}})
	if err != nil {
		return err
	}
	defer watch.Close()
	version, err := client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 2, "label": "water"})
	if err != nil {
		return err
	}
	watchCtx, stopWatching := context.WithTimeout(ctx, 5*time.Second)
	defer stopWatching()
	for {
		event, err := watch.Next(watchCtx)
		if err != nil {
			return fmt.Errorf("waiting for the world write at revision %d: %w", version, err)
		}
		switch event.(type) {
		case *chunkdb.ProgressEvent, *chunkdb.SchemaEvent:
			continue
		}
		change, ok := event.(*chunkdb.ChangeEvent)
		if !ok || change.Position.Epoch != watch.Start().Epoch || change.Position.Revision != version || len(change.Blocks) != 1 {
			return fmt.Errorf("expected the written block change, got %#v", event)
		}
		fmt.Printf("change (0,0): tile=%v label=%v\n", change.Blocks[0].After["tile"], change.Blocks[0].After["label"])
		break
	}
	return watch.Close()
}
