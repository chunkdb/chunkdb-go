package chunkdb

import (
	"errors"
	"testing"
)

func TestURITable(t *testing.T) {
	for path, want := range map[string]string{"/": "", "": "", "/terrain": "terrain"} {
		got, err := URI{Path: path}.Table()
		if err != nil || got != want {
			t.Fatalf("Table(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
	if _, err := TableFromPath("/a/b"); !errors.Is(err, ErrConnection) {
		t.Fatalf("got %v, want a connection error for a two-segment path", err)
	}
	if _, err := NewClient(Options{URI: "chunk://h:1/a/b"}); err == nil {
		t.Fatal("NewClient accepted a two-segment path")
	}
}

func TestTableStatementTarget(t *testing.T) {
	cases := map[string]string{
		"CREATE TABLE land (id u8) CHUNK 4 x 4": "land",
		"create table land(id u8) CHUNK 4 x 4":  "land",
		"ALTER TABLE world DROP COLUMN d":       "world",
		"DROP TABLE world":                      "world",
		"DESCRIBE world":                        "",
		"DROP COLUMN x y":                       "",
	}
	for statement, want := range cases {
		got, ok := tableStatementTarget(statement)
		if got != want || ok != (want != "") {
			t.Fatalf("%q: got %q, %v; want %q", statement, got, ok, want)
		}
	}
}
