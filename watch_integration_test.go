package chunkdb

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestIntegrationWatch(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			s := startServer(t, serverConfig{tls: secure, workers: 2, args: []string{"--feed-linger-ms", "0"}})
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
			e := nextExpectedWatch(t, w, "change", first, 1).(*ChangeEvent)
			if e.Position.Revision != first || e.Position.Epoch != start.Epoch || e.User == nil || *e.User != testAdmin || len(e.Blocks) != 1 || e.Blocks[0].Before != nil || e.Blocks[0].After["v"] != uint64(1) {
				t.Fatal(e)
			}
			last := e.Position
			e = nextExpectedWatch(t, w, "change", second, 1, last).(*ChangeEvent)
			if e.Position.Revision != second || e.Blocks[0].Before["v"] != uint64(1) || e.Blocks[0].After["v"] != uint64(2) {
				t.Fatal(e)
			}
			// Keep the original feed active while the resumed subscriber catches up.
			resumed, err := c.Watch(t.Context(), "watched", WatchOptions{Area: &Area{0, 0, 0, 0}, After: &last})
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.Close()
			if nextExpectedWatch(t, resumed, "change", second, 1, last).(*ChangeEvent).Position.Revision != second {
				t.Fatal("resume lost retained change")
			}
			if err := resumed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.AddColumn(t.Context(), "watched", ColumnDef{Name: "label", Type: TypeText(16), Null: true}); err != nil {
				t.Fatal(err)
			}
			described, err := c.Describe(t.Context(), "watched")
			if err != nil || described.Revision == nil {
				t.Fatal(described, err)
			}
			schema := nextExpectedWatch(t, w, "schema", *described.Revision, described.Version, Position{start.Epoch, second}).(*SchemaEvent)
			if len(schema.Columns) != 2 || schema.Columns[1].Name != "label" {
				t.Fatal(schema)
			}
			third, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 3, "label": "new"})
			if err != nil {
				t.Fatal(err)
			}
			e = nextExpectedWatch(t, w, "change", third, schema.Version, Position{start.Epoch, second}).(*ChangeEvent)
			if e.SchemaVersion != schema.Version || !reflect.DeepEqual(e.Blocks[0].Before, Record{"v": uint64(2), "label": nil}) || e.Blocks[0].After["label"] != "new" {
				t.Fatal(e)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			// With linger disabled, closing the last subscriber frees retained history.
			fresh, err := c.Watch(t.Context(), "watched", WatchOptions{After: &last})
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			// WATCH AFTER returns the requested position; resync carries the
			// completed producer frontier, which can include reserved-clock gaps.
			// No mutation occurs between this DESCRIBE and the resync assertion.
			frontier, err := c.Describe(t.Context(), "watched")
			if err != nil || frontier.Revision == nil || frontier.Epoch != start.Epoch || frontier.Version != schema.Version || *frontier.Revision < third {
				t.Fatal("completed frontier for resync", frontier, err)
			}
			resync := nextExpectedWatch(t, fresh, "resync", *frontier.Revision, frontier.Version).(*ResyncEvent)
			if resync.Position.Epoch != start.Epoch {
				t.Fatal("resync epoch")
			}
			if err := c.DropTable(t.Context(), "watched"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			for {
				event, err := fresh.Next(ctx)
				if err != nil {
					if !isServerCode(err, CodeNoTable) {
						t.Fatal("waiting for NO_TABLE after drop", err)
					}
					break
				}
				progress, ok := event.(*ProgressEvent)
				if !ok || progress.Position.Epoch != start.Epoch || progress.Position.Revision != resync.Position.Revision {
					t.Fatalf("unexpected event while waiting for NO_TABLE: %#v", event)
				}
			}
		})
	}
}
func TestIntegrationPoolWatch(t *testing.T) {
	s := startServer(t, serverConfig{workers: 2})
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
	first, err := pool.SetBlock(t.Context(), "watched", 0, 0, Record{"v": -3})
	if err != nil {
		t.Fatal(err)
	}
	if nextExpectedWatch(t, w, "change", first, 1).(*ChangeEvent).Blocks[0].After["v"] != int64(-3) {
		t.Fatal("pool change")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	// The stream owns its connections independently of the originating pool.
	second, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": -4})
	if err != nil {
		t.Fatal(err)
	}
	if nextExpectedWatch(t, w, "change", second, 1, Position{w.Start().Epoch, first}).(*ChangeEvent).Blocks[0].After["v"] != int64(-4) {
		t.Fatal("closed pool interrupted watch")
	}
	if _, err := pool.Watch(t.Context(), "watched", WatchOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestIntegrationWatchLinger(t *testing.T) {
	s := startServer(t, serverConfig{workers: 2})
	c := connectIntegration(t, s, nil)
	createTable(t, c, "watched", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2})
	w, err := c.Watch(t.Context(), "watched", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	firstRevision, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	first, ok := nextExpectedWatch(t, w, "change", firstRevision, 1).(*ChangeEvent)
	if !ok {
		t.Fatal("first write did not produce a change")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	version, err := c.SetBlock(t.Context(), "watched", 0, 0, Record{"v": 2})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := c.Watch(t.Context(), "watched", WatchOptions{After: &first.Position})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	event := nextExpectedWatch(t, resumed, "change", version, 1, first.Position)
	change, ok := event.(*ChangeEvent)
	if !ok || change.Position.Revision != version || change.Blocks[0].Before["v"] != uint64(1) || change.Blocks[0].After["v"] != uint64(2) {
		t.Fatalf("default linger lost the disconnected write: %#v", event)
	}
}
