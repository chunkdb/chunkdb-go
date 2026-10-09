package chunkdb

import (
	"errors"
	"reflect"
	"testing"
)

func TestIntegrationWatch(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			s := startServer(t, serverConfig{tls: secure, workers: 4})
			c := connectIntegration(t, s, func(o *Options) {
				if secure {
					o.CA = s.caPEM
				}
			})
			createTable(t, c, "watched", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(64)}}, ChunkWidth: 2, ChunkHeight: 2})
			w, err := c.Watch(t.Context(), "watched", WatchOptions{Area: &Area{0, 0, 0, 0}})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			start := w.Start()
			if _, err := c.SetBlock(t.Context(), "watched", 8, 8, Record{"v": 90}); err != nil {
				t.Fatal(err)
			}
			first, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 1})
			if err != nil {
				t.Fatal(err)
			}
			second, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 2})
			if err != nil {
				t.Fatal(err)
			}
			e := nextWatch(t, w).(*ChangeEvent)
			if e.Position.Revision != first || e.Position.Epoch != start.Epoch || e.User == nil || *e.User != testAdmin || len(e.Blocks) != 1 || e.Blocks[0].Before != nil || e.Blocks[0].After["v"] != uint64(1) {
				t.Fatal(e)
			}
			last := e.Position
			e = nextWatch(t, w).(*ChangeEvent)
			if e.Position.Revision != second || e.Blocks[0].Before["v"] != uint64(1) || e.Blocks[0].After["v"] != uint64(2) {
				t.Fatal(e)
			}
			// Keep the original feed active while the resumed subscriber catches up.
			resumed, err := c.Watch(t.Context(), "watched", WatchOptions{Area: &Area{0, 0, 0, 0}, After: &last})
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.Close()
			if nextWatch(t, resumed).(*ChangeEvent).Position.Revision != second {
				t.Fatal("resume lost retained change")
			}
			if err := resumed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.AddColumn(t.Context(), "watched", ColumnDef{Name: "label", Type: TypeText(16), Null: true}); err != nil {
				t.Fatal(err)
			}
			schema := nextWatch(t, w).(*SchemaEvent)
			if len(schema.Columns) != 2 || schema.Columns[1].Name != "label" {
				t.Fatal(schema)
			}
			if _, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 3, "label": "new"}); err != nil {
				t.Fatal(err)
			}
			e = nextWatch(t, w).(*ChangeEvent)
			if e.SchemaVersion != schema.Version || !reflect.DeepEqual(e.Blocks[0].Before, Record{"v": uint64(2), "label": nil}) || e.Blocks[0].After["label"] != "new" {
				t.Fatal(e)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Closing the last subscriber frees retained history; AFTER now resyncs.
			fresh, err := c.Watch(t.Context(), "watched", WatchOptions{After: &last})
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			if nextWatch(t, fresh).(*ResyncEvent).Position.Epoch != start.Epoch {
				t.Fatal("resync epoch")
			}
			if err := c.DropTable(t.Context(), "watched"); err != nil {
				t.Fatal(err)
			}
			if _, err := fresh.Next(t.Context()); !isServerCode(err, CodeNoTable) {
				t.Fatal(err)
			}
		})
	}
}
func TestIntegrationPoolWatch(t *testing.T) {
	s := startServer(t, serverConfig{workers: 3})
	c := connectIntegration(t, s, nil)
	createTable(t, c, "watched", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeInt(8)}}, ChunkWidth: 2, ChunkHeight: 2})
	pool, err := NewPool(PoolOptions{Options: Options{URI: s.uri}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	w, err := pool.Watch(t.Context(), "watched", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.SetBlock(t.Context(), "watched", 0, 0, Record{"v": -3}); err != nil {
		t.Fatal(err)
	}
	if nextWatch(t, w).(*ChangeEvent).Blocks[0].After["v"] != int64(-3) {
		t.Fatal("pool change")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	// The stream owns its connections independently of the originating pool.
	if _, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": -4}); err != nil {
		t.Fatal(err)
	}
	if nextWatch(t, w).(*ChangeEvent).Blocks[0].After["v"] != int64(-4) {
		t.Fatal("closed pool interrupted watch")
	}
	if _, err := pool.Watch(t.Context(), "watched", WatchOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
