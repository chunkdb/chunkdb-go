package chunkdb

import (
	"errors"
	"net"
	"testing"
)

func TestGuardClauses(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) { writeSimple(conn, "OK") }))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	spec := TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2}
	column := ColumnDef{Name: "c", Type: TypeUint(8)}
	for _, step := range []struct {
		run  func(...Guard) error
		bare string
		want Guard
	}{
		{func(g ...Guard) error { return client.CreateTable(ctx, "t", spec, g...) }, "CREATE TABLE t (v u8) CHUNK 2 x 2", IfNotExists},
		{func(g ...Guard) error { return client.DropTable(ctx, "t", g...) }, "DROP TABLE t", IfExists},
		{func(g ...Guard) error { return client.AddColumn(ctx, "t", column, g...) }, "ALTER TABLE t ADD COLUMN c u8", IfNotExists},
		{func(g ...Guard) error { return client.DropColumn(ctx, "t", "c", g...) }, "ALTER TABLE t DROP COLUMN c", IfExists},
		{func(g ...Guard) error { return client.AddChunkColumn(ctx, "t", column, g...) }, "ALTER TABLE t ADD CHUNK COLUMN c u8", IfNotExists},
		{func(g ...Guard) error { return client.DropChunkColumn(ctx, "t", "c", g...) }, "ALTER TABLE t DROP CHUNK COLUMN c", IfExists},
		{func(g ...Guard) error { return client.CreateUser(ctx, "bot", "secret", CreateUserOptions{}, g...) }, "CREATE USER bot VERIFIER $1", IfNotExists},
		{func(g ...Guard) error { return client.DropUser(ctx, "bot", g...) }, "DROP USER bot", IfExists},
		{func(g ...Guard) error { return client.DropSlot(ctx, "t", "consumer", g...) }, "DROP SLOT 'consumer' ON t", IfExists},
	} {
		if err := step.run(); err != nil {
			t.Fatal(err)
		}
		if got := server.lastCommand(t); got != step.bare {
			t.Fatalf("got %q, want %q", got, step.bare)
		}
		if err := step.run(step.want); err != nil {
			t.Fatal(err)
		}
		verb, rest := guardedParts(step.bare)
		if got, want := server.lastCommand(t), verb+" "+string(step.want)+" "+rest; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		other := IfExists
		if step.want == IfExists {
			other = IfNotExists
		}
		sent := len(server.commands())
		for _, wrong := range [][]Guard{{other}, {step.want, step.want}, {Guard("IF MAYBE")}} {
			if err := step.run(wrong...); !errors.Is(err, ErrProtocol) {
				t.Fatalf("%s with %q: %v", step.bare, wrong, err)
			}
		}
		if len(server.commands()) != sent {
			t.Fatalf("%s: a refused guard sent a statement", step.bare)
		}
	}
}

// guardedParts splits a statement where its guard goes: after the object
// keyword of the verb.
func guardedParts(statement string) (string, string) {
	for _, verb := range []string{"ALTER TABLE t ADD CHUNK COLUMN", "ALTER TABLE t DROP CHUNK COLUMN", "ALTER TABLE t ADD COLUMN",
		"ALTER TABLE t DROP COLUMN", "CREATE TABLE", "DROP TABLE", "CREATE USER", "DROP USER", "DROP SLOT"} {
		if len(statement) > len(verb) && statement[:len(verb)+1] == verb+" " {
			return verb, statement[len(verb)+1:]
		}
	}
	return statement, ""
}
