package chunkdb

import (
	"strings"
	"testing"
)

func TestIntegrationStatusStatsAndSlotFields(t *testing.T) {
	server := startServer(t, serverConfig{})
	c := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, c, "fields", TableSpec{ChunkWidth: 2, ChunkHeight: 2, Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}}})
	for _, x := range []int64{0, 4} {
		if _, err := c.SetBlock(ctx, "fields", x, 0, Record{"n": 1}); err != nil {
			t.Fatal(err)
		}
	}

	status, err := c.Status(ctx)
	if err != nil || status.Version == "" || status.Version != c.ServerInfo().ServerVersion || status.ReadOnly || len(status.Fenced) != 0 {
		t.Fatalf("%+v %v", status, err)
	}
	found := false
	for _, table := range status.Tables {
		if table.Name == "fields" {
			found = table.State == "ok" && table.Reason == nil
		}
	}
	if !found {
		t.Fatalf("table state missing: %+v", status.Tables)
	}

	stats, err := c.TableStats(ctx, "fields")
	if err != nil || stats.Chunks != 2 || stats.Files == 0 || stats.DiskBytes == 0 || stats.Slots != 0 || stats.Watches != 0 {
		t.Fatalf("%+v %v", stats, err)
	}

	for _, name := range []string{"app_one", "app_two", "other"} {
		if err := c.CreateSlot(ctx, "fields", name); err != nil {
			t.Fatal(err)
		}
	}
	slots, err := c.SlotsWithPrefix(ctx, "fields", "app_")
	if err != nil || len(slots) != 2 {
		t.Fatalf("%+v %v", slots, err)
	}
	for _, slot := range slots {
		if !strings.HasPrefix(slot.Name, "app_") || slot.Attached {
			t.Fatalf("%+v", slot)
		}
	}
	if stats, err := c.TableStats(ctx, "fields"); err != nil || stats.Slots != 3 {
		t.Fatalf("%+v %v", stats, err)
	}
	if _, err := c.SlotsWithPrefix(ctx, "fields", "bad'name"); err == nil {
		t.Fatal("sent an invalid prefix")
	}
}
