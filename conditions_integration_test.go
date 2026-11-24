package chunkdb

import (
	"errors"
	"reflect"
	"testing"
)

func TestIntegrationConditionsAndComputedValues(t *testing.T) {
	server := startServer(t, serverConfig{})
	c := connectIntegration(t, server, nil)
	ctx := t.Context()
	if _, err := c.Do(ctx, "CREATE TABLE wallet (coins u16, kind u8) CHUNK 2 x 2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": 5, "kind": 1}); err != nil {
		t.Fatal(err)
	}
	version, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": Expr("coins + $1")}, WithArgs(Arg{"coins", 3}))
	if err != nil {
		t.Fatal(err)
	}
	if block, err := c.GetBlock(ctx, "wallet", 0, 0); err != nil || block["coins"] != uint64(8) {
		t.Fatalf("%v %v", block, err)
	}
	_, err = c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": Expr("coins - $1")}, If("coins >= $2"), WithArgs(Arg{"coins", 10}, Arg{"coins", 10}))
	var refused *Error
	if !errors.As(err, &refused) || refused.ServerCode != CodeConditionFailed || refused.Metadata.Current == nil || *refused.Metadata.Current != version {
		t.Fatalf("condition not refused: %v", err)
	}
	if _, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"kind": 2}, If("coins = $1"), WithArgs(Arg{"coins", 8})); err != nil {
		t.Fatal(err)
	}
	if block, err := c.GetBlock(ctx, "wallet", 0, 0); err != nil || block["kind"] != uint64(2) || block["coins"] != uint64(8) {
		t.Fatalf("%v %v", block, err)
	}
	if _, err := c.SetBlock(ctx, "wallet", 6, 0, Record{"coins": 1, "kind": 4}); err != nil {
		t.Fatal(err)
	}

	where := Where("kind > $1", Arg{"kind", 2})
	all := ChunkSelector{Area: &Area{0, 0, 3, 0}}
	result, err := c.AggregateWhere(ctx, "wallet", all, where, CountAll(), SumOf("coins"))
	if err != nil || !reflect.DeepEqual(result.Values, []any{int64(1), int64(1)}) {
		t.Fatalf("%+v %v", result, err)
	}
	rows, err := c.AggregateByChunkWhere(ctx, "wallet", all, where, CountAll())
	if err != nil || !reflect.DeepEqual(rows, []AggregateRow{{CX: 3, CY: 0, Values: []any{int64(1)}}}) {
		t.Fatalf("%+v %v", rows, err)
	}

	first, err := c.ScanChunksWithOptions(ctx, "wallet", ScanOptions{Limit: 1, Where: where})
	if err != nil || len(first.Chunks) != 0 || !first.More || !reflect.DeepEqual(first.After, &ChunkCoord{0, 0}) {
		t.Fatalf("%+v %v", first, err)
	}
	next, err := c.ScanChunksWithOptions(ctx, "wallet", ScanOptions{After: first.After, Where: where, WithData: true})
	if err != nil || len(next.Chunks) != 1 || next.More || next.After != nil || next.Chunks[0].CX != 3 || next.Chunks[0].Chunk.Block(0, 0)["kind"] != uint64(4) {
		t.Fatalf("%+v %v", next, err)
	}

	if _, err := c.DeleteBlock(ctx, "wallet", 6, 0, If("kind = $1"), WithArgs(Arg{"kind", 9})); !IsCode(err, CodeConditionFailed) {
		t.Fatal(err)
	}
	if _, err := c.DeleteBlock(ctx, "wallet", 6, 0, If("kind = $1"), WithArgs(Arg{"kind", 4})); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": Expr("coins - $1")}, WithArgs(Arg{"coins", 9})); !IsCode(err, CodeOutOfRange) {
		t.Fatal(err)
	}
}
