package chunkdb

import (
	"errors"
	"reflect"
	"testing"
)

func TestIntegrationReturningReadOnlyChecksAndAlter(t *testing.T) {
	server := startServer(t, serverConfig{})
	c := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, c, "wallet", TableSpec{ChunkWidth: 2, ChunkHeight: 2, Columns: []ColumnDef{{Name: "coins", Type: TypeUint(16)}, {Name: "note", Type: TypeText(8), Null: true}}})
	if _, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": 5}); err != nil {
		t.Fatal(err)
	}

	result, err := c.SetBlockReturning(ctx, "wallet", 0, 0, Record{"coins": Expr("coins + $1")}, Returning{Old: true, Columns: []string{"coins"}}, WithArgs(Arg{"coins", 2}))
	if err != nil || !result.Applied || result.Version == nil || !reflect.DeepEqual(result.Rows, []ReturnedRow{{X: 0, Y: 0, Values: Record{"coins": uint64(5)}}}) {
		t.Fatalf("%+v %v", result, err)
	}
	refused, err := c.SetBlockReturning(ctx, "wallet", 0, 0, Record{"coins": 0}, Returning{}, If("coins > $1"), WithArgs(Arg{"coins", 100}))
	if err != nil || refused.Applied || len(refused.Rows) != 0 || *refused.Version != *result.Version {
		t.Fatalf("%+v %v", refused, err)
	}
	fresh, err := c.SetBlockReturning(ctx, "wallet", 3, 3, Record{"coins": 1}, Returning{})
	if err != nil || !reflect.DeepEqual(fresh.Rows, []ReturnedRow{{X: 3, Y: 3, Values: Record{"coins": uint64(1), "note": nil}}}) {
		t.Fatalf("%+v %v", fresh, err)
	}
	deleted, err := c.DeleteBlockReturning(ctx, "wallet", 3, 3, Returning{Old: true, Columns: []string{"coins"}})
	if err != nil || !deleted.Applied || deleted.Rows[0].Values["coins"] != uint64(1) {
		t.Fatalf("%+v %v", deleted, err)
	}
	if _, err := c.DeleteBlockReturning(ctx, "wallet", 3, 3, Returning{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("DELETE without OLD: %v", err)
	}

	if _, err := c.Transaction(ctx, func(tx *Tx) error {
		block, err := tx.GetBlock(ctx, "wallet", 0, 0)
		if err != nil || block["coins"] != uint64(7) {
			t.Fatalf("%v %v", block, err)
		}
		return nil
	}, TxReadOnly()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Transaction(ctx, func(tx *Tx) error {
		return tx.SetBlock(ctx, "wallet", 0, 0, Record{"coins": 1})
	}, TxReadOnly()); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("a read-only transaction wrote: %v", err)
	}

	if err := c.AddCheck(ctx, "wallet", "coins_cap", "coins <= 100"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBlock(ctx, "wallet", 0, 0, Record{"coins": 101}); !IsCode(err, CodeCheckFailed) {
		t.Fatalf("CHECK did not refuse: %v", err)
	}
	if err := c.SetColumnDefault(ctx, "wallet", "note", "none"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetColumnRequired(ctx, "wallet", "coins", true); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Describe(ctx, "wallet")
	if err != nil || !reflect.DeepEqual(schema.Checks, []Check{{Name: "coins_cap", Expression: schema.Checks[0].Expression}}) || schema.Columns[1].Default != "none" || !schema.Columns[0].Required {
		t.Fatalf("%+v %v", schema, err)
	}
	if err := c.DropColumnDefault(ctx, "wallet", "note"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetColumnRequired(ctx, "wallet", "coins", false); err != nil {
		t.Fatal(err)
	}
	if err := c.SetColumnNull(ctx, "wallet", "coins", true); err != nil {
		t.Fatal(err)
	}
	if err := c.DropCheck(ctx, "wallet", "coins_cap", false); err != nil {
		t.Fatal(err)
	}
	if err := c.DropCheck(ctx, "wallet", "coins_cap", true); err != nil {
		t.Fatal(err)
	}
	if err := c.RenameTable(ctx, "wallet", "purse"); err != nil {
		t.Fatal(err)
	}
	if block, err := c.GetBlock(ctx, "purse", 0, 0, "coins"); err != nil || block["coins"] != uint64(7) {
		t.Fatalf("%v %v", block, err)
	}
	if _, err := c.GetBlock(ctx, "wallet", 0, 0); !IsCode(err, CodeNoTable) {
		t.Fatalf("the old name still serves: %v", err)
	}
}
