package chunkdb

import (
	"math/big"
	"path/filepath"
	"reflect"
	"testing"
)

func TestIntegrationAggregate(t *testing.T) {
	server := startServer(t, serverConfig{})
	c := connectIntegration(t, server, nil)
	ctx := t.Context()
	for _, statement := range []string{
		"CREATE TABLE agg (h i16, d f64 NULL, u u64) CHUNK 2 x 2",
		"SET BLOCK 0 0 IN agg h = 12, d = 1.5, u = 18446744073709551615",
		"SET BLOCK 1 0 IN agg h = 5, u = 18446744073709551615",
		"SET BLOCK 2 0 IN agg h = -3, d = 0.5",
		"CREATE TABLE agg3 (h i16) CHUNK 2 x 2 x 2",
		"SET BLOCK 0 0 2 IN agg3 h = 4",
	} {
		if _, err := c.Do(ctx, statement); err != nil {
			t.Fatal(statement, err)
		}
	}
	area := ChunkSelector{Area: &Area{0, 0, 1, 0}}
	result, err := c.Aggregate(ctx, "agg", area, CountAll(), SumOf("h"), MinOf("h"), MaxOf("d"), AvgOf("d"), SumOf("u"))
	if err != nil {
		t.Fatal(err)
	}
	sum, _ := new(big.Int).SetString("36893488147419103230", 10)
	if want := []any{int64(3), int64(14), int64(-3), 1.5, 1.0, sum}; !reflect.DeepEqual(result.Values, want) {
		t.Fatalf("%+v", result)
	}
	rows, err := c.AggregateByChunk(ctx, "agg", area, CountAll(), SumOf("h"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []AggregateRow{{CX: 0, CY: 0, Values: []any{int64(2), int64(17)}}, {CX: 1, CY: 0, Values: []any{int64(1), int64(-3)}}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("%+v", rows)
	}
	empty, err := c.Aggregate(ctx, "agg", ChunkSelector{Chunk: &ChunkCoord{5, 5}}, CountAll(), SumOf("h"))
	if err != nil || !reflect.DeepEqual(empty.Values, []any{int64(0), nil}) {
		t.Fatalf("%+v %v", empty, err)
	}
	around, err := c.AggregateByChunk(ctx, "agg", ChunkSelector{Around: &Around{0, 0, 1}}, CountAll())
	if err != nil || len(around) != 2 {
		t.Fatalf("%+v %v", around, err)
	}
	rows3D, err := c.AggregateByChunk(ctx, "agg3", ChunkSelector{Area3D: &Area3D{0, 0, 0, 1, 1, 1}}, MaxOf("h"))
	if err != nil || !reflect.DeepEqual(rows3D, []AggregateRow{{CX: 0, CY: 0, CZ: 1, Values: []any{int64(4)}}}) {
		t.Fatalf("%+v %v", rows3D, err)
	}
	one3D, err := c.Aggregate(ctx, "agg3", ChunkSelector{Around3D: &Around3D{0, 0, 1, 0}}, CountAll())
	if err != nil || one3D.Values[0] != int64(1) {
		t.Fatalf("%+v %v", one3D, err)
	}
	if _, err := c.Aggregate(ctx, "agg", area, SumOf("missing")); !IsCode(err, "INVALID_ARGUMENT") {
		t.Fatal(err)
	}

	pool, err := NewPool(PoolOptions{Options: Options{URI: server.uri}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	pooled, err := pool.Aggregate(ctx, "agg", area, CountAll())
	if err != nil || pooled.Values[0] != int64(3) {
		t.Fatalf("%+v %v", pooled, err)
	}
}

func TestIntegrationTableAdminStatements(t *testing.T) {
	backups, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := startServer(t, serverConfig{args: []string{"--backup-dir", backups}})
	c := connectIntegration(t, server, nil)
	ctx := t.Context()
	if _, err := c.Migrate(ctx, []Migration{{Name: "admin_first", Statement: "CREATE TABLE first (n u8) CHUNK 2 x 2"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "CREATE TABLE second (m u8) CHUNK 2 x 2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBlock(ctx, "first", 0, 0, Record{"n": 1}); err != nil {
		t.Fatal(err)
	}
	deleted, err := c.DeleteBlock(ctx, "first", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := c.TableStats(ctx, "first")
	if err != nil || stats.Dims != 2 || stats.Revision == nil || *stats.Revision < deleted || !validEpoch(stats.Epoch) {
		t.Fatalf("%+v %v", stats, err)
	}
	floor, err := c.CompactTombstones(ctx, "first", deleted+1)
	if err != nil || floor > deleted {
		t.Fatalf("floor %d, %v", floor, err)
	}
	if stats, err = c.TableStats(ctx, "first"); err != nil || stats.Tombstones != 0 || stats.RetainedDeletionFloor != floor {
		t.Fatalf("%+v %v", stats, err)
	}

	migrations, err := c.Migrations(ctx)
	if err != nil || len(migrations) != 1 || migrations[0].Name != "admin_first" || migrations[0].User != testAdmin || migrations[0].AppliedMS == 0 {
		t.Fatalf("%+v %v", migrations, err)
	}

	backup, err := c.Backup(ctx, "snapshot")
	if err != nil || backup.Tables != 2 || len(backup.Cuts) != 2 || backup.Files == 0 {
		t.Fatalf("%+v %v", backup, err)
	}
	restored, err := c.RestoreTable(ctx, "snapshot", "first", RestoreOptions{As: "first_copy"})
	if err != nil || restored.Table != "first_copy" || restored.Parent.Epoch != stats.Epoch || restored.Epoch == stats.Epoch {
		t.Fatalf("%+v %v", restored, err)
	}

	if err := c.SwapTables(ctx, "first", "second"); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Describe(ctx, "first")
	if err != nil || len(schema.Columns) != 1 || schema.Columns[0].Name != "m" {
		t.Fatalf("%+v %v", schema, err)
	}
	if err := c.RecoverTable(ctx, "first"); !IsCode(err, "INVALID_ARGUMENT") {
		t.Fatal(err)
	}
	if err := c.ReloadTLS(ctx); !IsCode(err, "INVALID_ARGUMENT") {
		t.Fatal(err)
	}
}

func TestIntegrationWatchSeveralAreas(t *testing.T) {
	s := startServer(t, serverConfig{workers: 2, args: []string{"--feed-linger-ms", "0"}})
	c := connectIntegration(t, s, nil)
	createTable(t, c, "multi", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(64)}}, ChunkWidth: 2, ChunkHeight: 2})
	w, err := c.Watch(t.Context(), "multi", WatchOptions{Area: &Area{0, 0, 0, 0}, Areas: []Area{{4, 4, 4, 4}}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := c.SetBlock(t.Context(), "multi", 4, 4, Record{"v": 9}); err != nil {
		t.Fatal(err)
	}
	first, err := c.SetBlock(t.Context(), "multi", 0, 0, Record{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.SetBlock(t.Context(), "multi", 8, 8, Record{"v": 2})
	if err != nil {
		t.Fatal(err)
	}
	inside, err := c.SetBlock(t.Context(), "multi", 9, 9, Record{"v": 3})
	if err != nil {
		t.Fatal(err)
	}
	e := nextExpectedWatch(t, w, "change", first, 1).(*ChangeEvent)
	if len(e.Blocks) != 1 || e.Blocks[0].After["v"] != uint64(1) {
		t.Fatal(e)
	}
	e = nextExpectedWatch(t, w, "change", second, 1, e.Position).(*ChangeEvent)
	if e.Blocks[0].After["v"] != uint64(2) {
		t.Fatal(e)
	}
	e = nextExpectedWatch(t, w, "change", inside, 1, e.Position).(*ChangeEvent)
	if e.Blocks[0].After["v"] != uint64(3) {
		t.Fatal(e)
	}
}
