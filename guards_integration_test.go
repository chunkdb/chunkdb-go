package chunkdb

import "testing"

func TestIntegrationGuardClauses(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()
	spec := TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2}
	column := ColumnDef{Name: "c", Type: TypeUint(8)}
	for range 2 {
		if err := client.CreateTable(ctx, "guarded", spec, IfNotExists); err != nil {
			t.Fatal(err)
		}
		if err := client.AddColumn(ctx, "guarded", column, IfNotExists); err != nil {
			t.Fatal(err)
		}
		if err := client.AddChunkColumn(ctx, "guarded", column, IfNotExists); err != nil {
			t.Fatal(err)
		}
		if err := client.CreateUser(ctx, "guard_user", "secret-password", CreateUserOptions{}, IfNotExists); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.CreateTable(ctx, "guarded", spec); !IsCode(err, CodeTableExists) {
		t.Fatalf("an unguarded create of an existing table: %v", err)
	}
	schema, err := client.Describe(ctx, "guarded")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := schema.Column("c"); !ok || len(schema.ChunkColumns) != 1 {
		t.Fatalf("%+v", schema)
	}
	for range 2 {
		if err := client.DropSlot(ctx, "guarded", "absent", IfExists); err != nil {
			t.Fatal(err)
		}
		if err := client.DropColumn(ctx, "guarded", "c", IfExists); err != nil {
			t.Fatal(err)
		}
		if err := client.DropChunkColumn(ctx, "guarded", "c", IfExists); err != nil {
			t.Fatal(err)
		}
		if err := client.DropUser(ctx, "guard_user", IfExists); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := client.DropTable(ctx, "guarded", IfExists); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.DropTable(ctx, "guarded"); !IsCode(err, CodeNoTable) {
		t.Fatalf("an unguarded drop of an absent table: %v", err)
	}
}
