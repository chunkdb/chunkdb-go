package chunkdb

import (
	"errors"
	"math/big"
	"reflect"
	"testing"
)

func bulkReply(s string) Reply { return Reply{Kind: ReplyBulk, Bulk: []byte(s)} }

func intReply(n int64) Reply {
	if n < 0 {
		return Reply{Kind: ReplyInteger, negative: true, magnitude: uint64(-n)}
	}
	return Reply{Kind: ReplyInteger, magnitude: uint64(n)}
}

func mapReply(pairs ...any) Reply {
	r := Reply{Kind: ReplyMap}
	for i := 0; i < len(pairs); i += 2 {
		r.Map = append(r.Map, MapEntry{Key: bulkReply(pairs[i].(string)), Value: pairs[i+1].(Reply)})
	}
	return r
}

func TestAggregateStatement(t *testing.T) {
	for _, tc := range []struct {
		selector ChunkSelector
		byChunk  bool
		want     string
	}{
		{ChunkSelector{Chunk: &ChunkCoord{1, -2}}, false, "AGGREGATE CHUNK 1 -2 FROM t COUNT(*), SUM(h)"},
		{ChunkSelector{Chunk3D: &ChunkCoord3D{1, 2, 3}}, false, "AGGREGATE CHUNK 1 2 3 FROM t COUNT(*), SUM(h)"},
		{ChunkSelector{Area: &Area{0, 0, 1, 1}}, true, "AGGREGATE AREA 0 0 TO 1 1 FROM t COUNT(*), SUM(h) BY CHUNK"},
		{ChunkSelector{Area3D: &Area3D{0, 0, 0, 1, 1, 1}}, false, "AGGREGATE AREA 0 0 0 TO 1 1 1 FROM t COUNT(*), SUM(h)"},
		{ChunkSelector{Around: &Around{0, 0, 2}}, false, "AGGREGATE AREA AROUND 0 0 RADIUS 2 FROM t COUNT(*), SUM(h)"},
		{ChunkSelector{Around3D: &Around3D{0, 0, 1, 2}}, true, "AGGREGATE AREA AROUND 0 0 1 RADIUS 2 FROM t COUNT(*), SUM(h) BY CHUNK"},
	} {
		got, err := aggregateStatement("t", tc.selector, []AggregateFunction{CountAll(), SumOf("h")}, nil, tc.byChunk)
		if err != nil || got != tc.want {
			t.Fatalf("got %q, %v; want %q", got, err, tc.want)
		}
	}
	got, _ := aggregateStatement("t", ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{MinOf("a"), MaxOf("b"), AvgOf("c")}, nil, false)
	if got != "AGGREGATE CHUNK 0 0 FROM t MIN(a), MAX(b), AVG(c)" {
		t.Fatal(got)
	}
	for _, bad := range []struct {
		selector  ChunkSelector
		functions []AggregateFunction
	}{
		{ChunkSelector{}, []AggregateFunction{CountAll()}},
		{ChunkSelector{Chunk: &ChunkCoord{}, Area: &Area{}}, []AggregateFunction{CountAll()}},
		{ChunkSelector{Around: &Around{0, 0, -1}}, []AggregateFunction{CountAll()}},
		{ChunkSelector{Chunk: &ChunkCoord{}}, nil},
		{ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{{Op: AggregateCount, Column: "h"}}},
		{ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{SumOf("Bad Name")}},
		{ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{{Op: "MEDIAN", Column: "h"}}},
	} {
		if _, err := aggregateStatement("t", bad.selector, bad.functions, nil, false); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
}

func TestParseAggregate(t *testing.T) {
	huge, _ := new(big.Int).SetString("36893488147419103230", 10)
	functions := []AggregateFunction{CountAll(), SumOf("u"), MinOf("h"), AvgOf("d"), MaxOf("d")}
	reply := mapReply("count(*)", intReply(3), "sum(u)", Reply{Kind: ReplyBigNumber, BigNumber: huge},
		"min(h)", intReply(-3), "avg(d)", Reply{Kind: ReplyDouble, Double: 1}, "max(d)", Reply{Kind: ReplyNull})
	result, err := parseAggregate(reply, functions)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{int64(3), huge, int64(-3), 1.0, nil}
	if !reflect.DeepEqual(result.Values, want) || result.Names[1] != "sum(u)" {
		t.Fatalf("%+v", result)
	}
	if v, ok := result.Value("min(h)"); !ok || v != int64(-3) {
		t.Fatal(v, ok)
	}
	if _, ok := result.Value("max(x)"); ok {
		t.Fatal("unknown name found")
	}
	for _, bad := range []Reply{
		{Kind: ReplyArray},
		mapReply("count(*)", intReply(1)),
		mapReply("sum(u)", intReply(1), "count(*)", intReply(1), "min(h)", intReply(1), "avg(d)", intReply(1), "max(d)", intReply(1)),
		mapReply("count(*)", bulkReply("x"), "sum(u)", intReply(1), "min(h)", intReply(1), "avg(d)", intReply(1), "max(d)", intReply(1)),
	} {
		if _, err := parseAggregate(bad, functions); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
}

func TestParseAggregateRows(t *testing.T) {
	huge, _ := new(big.Int).SetString("-9223372036854775809", 10)
	row := func(values ...Reply) Reply { return Reply{Kind: ReplyArray, Array: values} }
	reply := row(row(intReply(0), intReply(-1), intReply(2), Reply{Kind: ReplyBigNumber, BigNumber: huge}),
		row(intReply(1), intReply(0), intReply(1), Reply{Kind: ReplyNull}))
	rows, err := parseAggregateRows(reply, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []AggregateRow{{CX: 0, CY: -1, Values: []any{int64(2), huge}}, {CX: 1, Values: []any{int64(1), nil}}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("%+v", rows)
	}
	rows, err = parseAggregateRows(row(row(intReply(1), intReply(2), intReply(3), intReply(4))), 3, 1)
	if err != nil || rows[0].CX != 1 || rows[0].CY != 2 || rows[0].CZ != 3 || rows[0].Values[0] != int64(4) {
		t.Fatalf("%+v %v", rows, err)
	}
	for _, bad := range []Reply{mapReply(), row(row(intReply(0))), row(row(bulkReply("x"), intReply(0), intReply(1)))} {
		if _, err := parseAggregateRows(bad, 2, 1); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
}

func TestParseTableAdminReplies(t *testing.T) {
	epoch := "7675c5a0be0425d049073e695f8f7614"
	stats, err := parseTableStats(mapReply("epoch", bulkReply(epoch), "revision", intReply(3), "dims", intReply(2), "tombstones", intReply(1), "retained_deletion_floor", intReply(0)))
	if err != nil || stats.Epoch != epoch || stats.Revision == nil || *stats.Revision != 3 || stats.Dims != 2 || stats.Tombstones != 1 {
		t.Fatalf("%+v %v", stats, err)
	}
	stats, err = parseTableStats(mapReply("epoch", bulkReply(epoch), "revision", Reply{Kind: ReplyNull}, "dims", intReply(3), "tombstones", intReply(0), "retained_deletion_floor", intReply(2)))
	if err != nil || stats.Revision != nil || stats.RetainedDeletionFloor != 2 {
		t.Fatalf("%+v %v", stats, err)
	}
	if _, err := parseTableStats(mapReply("epoch", bulkReply(epoch), "dims", intReply(4))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}

	migrations, err := parseMigrations(Reply{Kind: ReplyArray, Array: []Reply{
		mapReply("name", bulkReply("m1"), "applied_ms", intReply(5), "user", Reply{Kind: ReplyNull}, "statement", bulkReply("DROP TABLE t")),
	}})
	if err != nil || !reflect.DeepEqual(migrations, []MigrationRecord{{Name: "m1", AppliedMS: 5, Statement: "DROP TABLE t"}}) {
		t.Fatalf("%+v %v", migrations, err)
	}
	if _, err := parseMigrations(Reply{Kind: ReplyArray, Array: []Reply{mapReply("name", intReply(1))}}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}

	backup, err := parseBackup(mapReply("tables", intReply(1), "files", intReply(2), "bytes", intReply(3), "cuts",
		Reply{Kind: ReplyArray, Array: []Reply{mapReply("table", bulkReply("a"), "epoch", bulkReply(epoch), "revision", intReply(7))}}))
	if err != nil || backup.Tables != 1 || backup.Bytes != 3 || !reflect.DeepEqual(backup.Cuts, []BackupCut{{"a", Position{epoch, 7}}}) {
		t.Fatalf("%+v %v", backup, err)
	}
	if _, err := parseBackup(mapReply("tables", intReply(1), "files", intReply(2), "bytes", intReply(3))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}

	restore, err := parseRestore(mapReply("table", bulkReply("a2"), "epoch", bulkReply(epoch), "parent_epoch", bulkReply(epoch),
		"fork_revision", intReply(9), "files", intReply(4), "bytes", intReply(8)))
	if err != nil || restore.Table != "a2" || restore.Parent != (Position{epoch, 9}) || restore.Files != 4 {
		t.Fatalf("%+v %v", restore, err)
	}
	if _, err := quoteName("BACKUP", "destination", "a\nb"); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if quoted, _ := quoteName("BACKUP", "destination", "it's"); quoted != "'it''s'" {
		t.Fatal(quoted)
	}
}

func TestWatchSeveralAreasClause(t *testing.T) {
	clause, err := feedFilterClause("WATCH", []Area{{0, 0, 1, 1}, {4, 4, 5, 5}}, nil, nil, false, false)
	if err != nil || clause != " AREA 0 0 TO 1 1, AREA 4 4 TO 5 5" {
		t.Fatalf("%q %v", clause, err)
	}
	clause, err = feedFilterClause("WATCH", nil, []Area3D{{0, 0, 0, 1, 1, 1}, {2, 2, 2, 3, 3, 3}}, []string{"a"}, true, false)
	if err != nil || clause != " AREA 0 0 0 TO 1 1 1, AREA 2 2 2 TO 3 3 3 COLUMNS (a) NO BEFORE" {
		t.Fatalf("%q %v", clause, err)
	}
	if _, err := feedFilterClause("WATCH", []Area{{0, 0, 0, 0}}, []Area3D{{}}, nil, false, false); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
