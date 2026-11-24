package chunkdb

import (
	"errors"
	"reflect"
	"testing"
)

func TestBlockWriteClause(t *testing.T) {
	clause, err := blockWriteClause("SET BLOCK", []WriteOption{If("coins >= $1"), WithArgs(Arg{"coins", 3})})
	if err != nil || clause.text != " IF coins >= $1" || !reflect.DeepEqual(clause.args, []Arg{{"coins", 3}}) {
		t.Fatalf("%+v %v", clause, err)
	}
	clause, err = blockWriteClause("SET BLOCK", []WriteOption{IfVersion(7)})
	if err != nil || clause.text != " IF VERSION 7" {
		t.Fatalf("%+v %v", clause, err)
	}
	for _, opts := range [][]WriteOption{
		{If("a = 1"), IfVersion(1)},
		{If(" ")},
		{IfAbsent()},
	} {
		if _, err := blockWriteClause("SET BLOCK", opts); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted %v", err)
		}
	}
	if _, err := writeClause("SET CHUNK", []WriteOption{If("a = 1")}, true); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if _, err := writeClause("SET CHUNK", []WriteOption{WithArgs(Arg{"a", 1})}, true); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestEncodeArgs(t *testing.T) {
	schema := &Schema{Table: "t", Columns: []Column{{Name: "h", Type: TypeInt(16)}, {Name: "n", Type: TypeUint(8), Null: true}},
		ChunkColumns: []Column{{Name: "biome", Type: TypeUint(32)}}}
	params, err := encodeArgs("AGGREGATE", schema, []Arg{{"h", -2}, {"n", nil}, {"chunk.biome", 7}})
	if err != nil || !reflect.DeepEqual(params, [][]byte{{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, nil, {7, 0, 0, 0, 0, 0, 0, 0}}) {
		t.Fatalf("%v %v", params, err)
	}
	if _, err := encodeArgs("AGGREGATE", schema, []Arg{{"missing", 1}}); err == nil {
		t.Fatal("accepted an unknown column")
	}
	if _, err := encodeArgs("AGGREGATE", schema, []Arg{{"h", 1 << 20}}); err == nil {
		t.Fatal("accepted a value outside the column")
	}
}

func TestAggregateStatementWhere(t *testing.T) {
	got, err := aggregateStatement("t", ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{CountAll()}, Where("h > $1", Arg{"h", 1}), true)
	if err != nil || got != "AGGREGATE CHUNK 0 0 FROM t COUNT(*) WHERE h > $1 BY CHUNK" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := aggregateStatement("t", ChunkSelector{Chunk: &ChunkCoord{}}, []AggregateFunction{CountAll()}, Where(""), false); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestDecodeFilteredScanPage(t *testing.T) {
	schema, err := parseSchema(readOne(t, modelDescription(3, false)))
	if err != nil {
		t.Fatal(err)
	}
	base := readOne(t, scanReply(respArray(), false, respInt(5), 2))
	epoch, _ := base.Lookup("epoch")
	point := func(axes ...int64) Reply {
		r := Reply{Kind: ReplyArray}
		for _, a := range axes {
			r.Array = append(r.Array, intReply(a))
		}
		return r
	}
	page := func(more bool, after Reply, rows ...Reply) Reply {
		return mapReply("chunks", Reply{Kind: ReplyArray, Array: rows}, "more", Reply{Kind: ReplyBoolean, Bool: more}, "after", after,
			"epoch", epoch, "position", intReply(5), "retained_deletion_floor", intReply(2))
	}
	empty, err := decodeScanPage(schema, page(true, point(1, 2, 0)), 3, nil, 1, nil, false, nil, "", true)
	if err != nil || len(empty.Chunks) != 0 || !empty.More || !reflect.DeepEqual(empty.After, &ChunkCoord3D{1, 2, 0}) {
		t.Fatalf("%+v %v", empty, err)
	}
	last, err := decodeScanPage(schema, page(false, Reply{Kind: ReplyNull}, point(3, 0, 0, 4)), 3, []int64{1, 2, 0}, 0, nil, false, nil, "", true)
	if err != nil || len(last.Chunks) != 1 || last.After != nil {
		t.Fatalf("%+v %v", last, err)
	}
	for name, reply := range map[string]Reply{
		"more without after":  page(true, Reply{Kind: ReplyNull}),
		"after without more":  page(false, point(2, 0, 0)),
		"after not advancing": page(true, point(1, 2, 0)),
		"after before a row":  page(true, point(2, 0, 0), point(3, 0, 0, 4)),
		"wrong arity":         page(true, point(2, 0)),
	} {
		if _, err := decodeScanPage(schema, reply, 3, []int64{1, 2, 0}, 0, nil, false, nil, "", true); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	missing := readOne(t, scanReply(respArray(), false, respInt(5), 2))
	if _, err := decodeScanPage(schema, missing, 3, nil, 0, nil, false, nil, "", true); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestExprOnlyInSetBlockValues(t *testing.T) {
	if _, err := normalize("label", TypeText(16), Expr("label")); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a text column took an Expr: %v", err)
	}
	schema := &Schema{Table: "t", Columns: []Column{{Name: "label", Type: TypeText(16)}}}
	if _, err := encodeArgs("SET BLOCK", schema, []Arg{{"label", Expr("label")}}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("an argument took an Expr: %v", err)
	}
	if _, err := normalizedFormValue(schema.Columns[0], Expr("label")); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a chunk block took an Expr: %v", err)
	}
}

func TestTxBlockClause(t *testing.T) {
	if clause, err := txBlockClause("SET BLOCK", []WriteOption{If("a = $1"), WithArgs(Arg{"a", 1})}); err != nil || clause.text != " IF a = $1" {
		t.Fatalf("%+v %v", clause, err)
	}
	for _, opt := range []WriteOption{IfVersion(1), IfAbsent()} {
		if _, err := txBlockClause("SET BLOCK", []WriteOption{opt}); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
}
