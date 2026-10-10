package chunkdb

import (
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
)

func TestMigrateStatementsAndPool(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if strings.Contains(command, "'second'") {
			writeSimple(conn, "skipped")
		} else {
			writeSimple(conn, "applied")
		}
	}))
	c := newTestClient(t, s, nil)
	steps := []Migration{{"first", " \tCREATE TABLE world (v u8) CHUNK 2 x 2\t "}, {"second", "ALTER  TABLE world ADD COLUMN label text(16) NULL"}}
	want := []MigrationResult{{"first", "applied"}, {"second", "skipped"}}
	got, err := c.Migrate(t.Context(), steps)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	commands := s.commands()
	if !reflect.DeepEqual(commands[1:], []string{"MIGRATE 'first' CREATE TABLE world (v u8) CHUNK 2 x 2", "MIGRATE 'second' ALTER  TABLE world ADD COLUMN label text(16) NULL"}) {
		t.Fatal(commands)
	}
	p, err := NewPool(PoolOptions{Options: Options{URI: s.uri("")}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if got, err := p.Migrate(t.Context(), steps); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	if s.acceptedConns() != 2 {
		t.Fatal("pool did not reuse its leased connection", s.acceptedConns())
	}
}

func TestMigrateStopsAndPreservesConflict(t *testing.T) {
	s := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		if strings.Contains(command, "'changed'") {
			writeServerError(conn, "ERR CONFLICT migration 'changed' has a different statement")
		} else {
			writeSimple(conn, "applied")
		}
	}))
	c := newTestClient(t, s, nil)
	got, err := c.Migrate(t.Context(), []Migration{{"first", "DROP TABLE old"}, {"changed", "DROP TABLE world"}, {"later", "DROP TABLE later"}})
	var step *MigrationError
	var underlying *Error
	var transaction *ConflictError
	if !reflect.DeepEqual(got, []MigrationResult{{"first", "applied"}}) || !errors.As(err, &step) || step.Index != 1 || step.Name != "changed" || !errors.Is(err, ErrConflict) || !errors.Is(err, ErrServer) || !errors.As(err, &underlying) || underlying.ServerCode != CodeConflict || errors.As(err, &transaction) {
		t.Fatal(got, err)
	}
	if len(s.commands()) != 3 || strings.Contains(s.lastCommand(t), "later") {
		t.Fatal(s.commands())
	}
}

func TestMigrateRejectsInvalidStepBeforeSending(t *testing.T) {
	invalid := []Migration{{"", "DROP TABLE world"}, {"Upper", "DROP TABLE world"}, {"bad'name", "DROP TABLE world"}, {strings.Repeat("a", 64), "DROP TABLE world"}, {"valid", " \t"}, {"valid", "\nDROP TABLE world"}, {"valid", "DROP TABLE world\r"}, {"valid", "DROP TABLE world\x00"}}
	for _, step := range invalid {
		c, err := NewClient(Options{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Migrate(t.Context(), []Migration{step, {"later", "DROP TABLE later"}})
		var named *MigrationError
		if len(got) != 0 || !errors.Is(err, ErrProtocol) || !errors.As(err, &named) || named.Index != 0 || named.Name != step.Name {
			t.Fatal(step, got, err)
		}
		c.Close()
	}
}

func TestMigrateMalformedRepliesAndEmptyList(t *testing.T) {
	for _, frame := range []string{"+OK\r\n", "+APPLIED\r\n", respBulk("applied"), respNull} {
		s := newFakeServer(t, respondWith(frame))
		c := newTestClient(t, s, nil)
		got, err := c.Migrate(t.Context(), []Migration{{"first", "DROP TABLE world"}, {"later", "DROP TABLE later"}})
		if len(got) != 0 || !errors.Is(err, ErrProtocol) || len(s.commands()) != 2 {
			t.Fatal(got, err, s.commands())
		}
	}
	c, _ := NewClient(Options{})
	defer c.Close()
	if got, err := c.Migrate(t.Context(), nil); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	p, _ := NewPool(PoolOptions{MaxConnections: 1})
	defer p.Close()
	if got, err := p.Migrate(t.Context(), nil); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	p.Close()
	got, err := p.Migrate(t.Context(), []Migration{{"first", "DROP TABLE world"}})
	var named *MigrationError
	if len(got) != 0 || !errors.Is(err, ErrClosed) || !errors.As(err, &named) || named.Name != "first" || named.Index != 0 {
		t.Fatal(got, err)
	}
}

func TestMigrateInvalidatesSchemaIncludingSkippedAndRaw(t *testing.T) {
	s := newFakeServer(t, respondWith("+skipped\r\n"))
	c := newTestClient(t, s, nil)
	for _, statement := range []string{"ALTER TABLE world ADD COLUMN v u8", "DROP TABLE world", "CREATE TABLE world (v u8) CHUNK 2 x 2"} {
		c.schemas["world"] = &Schema{}
		if _, err := c.Migrate(t.Context(), []Migration{{"first", statement}}); err != nil || c.cachedSchema("world") != nil {
			t.Fatal(statement, err)
		}
	}
	c.schemas["world"] = &Schema{}
	if _, err := c.Do(t.Context(), "MIGRATE 'raw' ALTER TABLE world ADD COLUMN v u8"); err != nil || c.cachedSchema("world") != nil {
		t.Fatal(err)
	}
}
