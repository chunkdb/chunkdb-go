package chunkdb

import (
	"context"
	"testing"
	"time"
)

func nextSlotChange(t *testing.T, w *Watch) *ChangeEvent {
	t.Helper()
	for {
		e := nextWatch(t, w)
		if change, ok := e.(*ChangeEvent); ok {
			return change
		}
		if _, ok := e.(*SchemaEvent); !ok {
			t.Fatalf("unexpected event %#v", e)
		}
	}
}
func TestIntegrationSlotsResume(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[secure], func(t *testing.T) {
			s := startServer(t, serverConfig{tls: secure, workers: 4, args: []string{"--checkpoint-updates", "1"}})
			c := connectIntegration(t, s, func(o *Options) {
				if secure {
					o.CA = s.caPEM
				}
			})
			createTable(t, c, "slotted", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(64)}}, ChunkWidth: 2, ChunkHeight: 2})
			if err := c.CreateSlot(t.Context(), "slotted", "consumer"); err != nil {
				t.Fatal(err)
			}
			listed, err := c.Slots(t.Context(), "slotted")
			if err != nil || len(listed) != 1 || listed[0].Name != "consumer" || listed[0].Lost {
				t.Fatal(listed, err)
			}
			w, err := c.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if w.Start() != (Position{listed[0].Epoch, listed[0].Acked}) {
				t.Fatal(w.Start(), listed)
			}
			if _, err := c.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer"}); !isServerCode(err, CodeBusy) {
				t.Fatal(err)
			}
			first, err := c.SetBlock(t.Context(), "slotted", 0, 0, Record{"v": 1})
			if err != nil {
				t.Fatal(err)
			}
			e := nextSlotChange(t, w)
			if e.Position.Revision != first || e.Blocks[0].After["v"] != uint64(1) {
				t.Fatal(e)
			}
			if err := w.Ack(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			listed, err = c.Slots(t.Context(), "slotted")
			if err != nil || listed[0].Acked != first {
				t.Fatal(listed, err)
			}
			second, err := c.SetBlock(t.Context(), "slotted", 0, 0, Record{"v": 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.AddColumn(t.Context(), "slotted", ColumnDef{Name: "label", Type: TypeText(16), Null: true}); err != nil {
				t.Fatal(err)
			}
			third, err := c.SetBlock(t.Context(), "slotted", 0, 0, Record{"v": 3, "label": "new"})
			if err != nil {
				t.Fatal(err)
			}
			// A new client starts with the current schema, but the slot archive describes
			// old layouts before its historical rows. All watchers were closed meanwhile.
			resumedClient := connectIntegration(t, s, func(o *Options) {
				if secure {
					o.CA = s.caPEM
				}
			})
			resumed, err := resumedClient.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer resumed.Close()
			e = nextSlotChange(t, resumed)
			if e.Position.Revision != second || e.SchemaVersion != 1 || len(e.Blocks[0].Before) != 1 || e.Blocks[0].Before["v"] != uint64(1) || e.Blocks[0].After["v"] != uint64(2) {
				t.Fatal(e)
			}
			saved := e.Position
			e = nextSlotChange(t, resumed)
			if e.Position.Revision != third || e.SchemaVersion != 2 || e.Blocks[0].After["label"] != "new" {
				t.Fatal(e)
			}
			if err := resumed.Close(); err != nil {
				t.Fatal(err)
			} // no ACK: still stored at first
			after, err := resumedClient.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer", After: &saved})
			if err != nil {
				t.Fatal(err)
			}
			defer after.Close()
			if after.Start() != saved {
				t.Fatal(after.Start(), saved)
			}
			e = nextSlotChange(t, after)
			if e.Position.Revision != third {
				t.Fatal(e)
			}
			listed, err = c.Slots(t.Context(), "slotted")
			if err != nil || listed[0].Acked != first {
				t.Fatal("AFTER acknowledged data", listed, err)
			}
			if err := after.Ack(t.Context(), third); err != nil {
				t.Fatal(err)
			}
			if err := after.Close(); err != nil {
				t.Fatal(err)
			}
			listed, err = c.Slots(t.Context(), "")
			if err != nil || len(listed) != 1 || listed[0].Acked != third {
				t.Fatal(listed, err)
			}
			if err := c.DropSlot(t.Context(), "slotted", "consumer"); err != nil {
				t.Fatal(err)
			}
			listed, err = c.Slots(t.Context(), "slotted")
			if err != nil || len(listed) != 0 {
				t.Fatal(listed, err)
			}
		})
	}
}
func TestIntegrationSlotLost(t *testing.T) {
	s := startServer(t, serverConfig{workers: 4, args: []string{"--slot-max-bytes", "1", "--checkpoint-updates", "1"}})
	c := connectIntegration(t, s, nil)
	createTable(t, c, "slotted", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(64)}}, ChunkWidth: 2, ChunkHeight: 2})
	if err := c.CreateSlot(t.Context(), "slotted", "consumer"); err != nil {
		t.Fatal(err)
	}
	w, err := c.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := c.SetBlock(t.Context(), "slotted", 0, 0, Record{"v": 1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		_, err = w.Next(ctx)
		if err != nil {
			break
		}
	}
	if !isServerCode(err, CodeSlotLost) {
		t.Fatal(err)
	}
	listed, err := c.Slots(t.Context(), "slotted")
	if err != nil || len(listed) != 1 || !listed[0].Lost {
		t.Fatal(listed, err)
	}
	if _, err := c.Watch(t.Context(), "slotted", WatchOptions{Slot: "consumer"}); !isServerCode(err, CodeSlotLost) {
		t.Fatal(err)
	}
}
