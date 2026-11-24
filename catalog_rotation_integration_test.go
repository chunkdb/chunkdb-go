package chunkdb

import (
	"context"
	"testing"
	"time"
)

// nextCatalogEvent skips progress events, which carry no change.
func nextCatalogEvent(t *testing.T, w *Watch) *CatalogEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		event, err := w.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		switch event := event.(type) {
		case *ProgressEvent:
			continue
		case *CatalogEvent:
			return event
		default:
			t.Fatalf("unexpected catalog watch event %#v", event)
		}
	}
}

// catalogEntryOf waits for the entry of kind and name, skipping other events.
func catalogEntryOf(t *testing.T, w *Watch, kind, name string) CatalogChange {
	t.Helper()
	for {
		for _, change := range nextCatalogEvent(t, w).Events {
			if *change.Kind == kind && *change.Name == name {
				return change
			}
		}
	}
}

func TestIntegrationWatchCatalog(t *testing.T) {
	server := startServer(t, serverConfig{workers: 2})
	ctx := t.Context()
	admin := connectIntegration(t, server, nil)
	if !admin.ServerInfo().Supports("watch_catalog") {
		t.Fatal("the server does not advertise watch_catalog")
	}
	createTable(t, admin, "catalog_before", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2})
	watch, err := admin.WatchCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()

	whole := nextCatalogEvent(t, watch)
	if whole.Position.Epoch != watch.Start().Epoch {
		t.Fatalf("whole catalog at %+v, watch started at %+v", whole.Position, watch.Start())
	}
	seen := map[string]bool{}
	for _, change := range whole.Events {
		if *change.Action != "create" {
			t.Fatalf("the whole catalog holds %s", *change.Action)
		}
		switch *change.Kind {
		case "table":
			schema, err := change.TableSchema()
			if err != nil || schema.Table != *change.Name {
				t.Fatalf("%+v %v", schema, err)
			}
		case "user":
			user, err := change.UserData()
			if err != nil || user.Name != *change.Name {
				t.Fatalf("%+v %v", user, err)
			}
		}
		seen[*change.Kind+" "+*change.Name] = true
	}
	if !seen["table catalog_before"] || !seen["user "+testAdmin] {
		t.Fatalf("the whole catalog lacks the table or the admin: %v", seen)
	}

	createTable(t, admin, "catalog_after", TableSpec{Columns: []ColumnDef{{Name: "v", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2})
	created := nextCatalogEvent(t, watch)
	if created.User == nil || *created.User != testAdmin || len(created.Events) == 0 || *created.Events[0].Action != "create" || *created.Events[0].Name != "catalog_after" {
		t.Fatalf("%+v", created)
	}
	if created.Position.Revision <= whole.Position.Revision {
		t.Fatalf("revision %d does not follow %d", created.Position.Revision, whole.Position.Revision)
	}
	if err := admin.CreateSlot(ctx, "catalog_after", "consumer"); err != nil {
		t.Fatal(err)
	}
	slotEntry := catalogEntryOf(t, watch, "slot", "consumer")
	slot, err := slotEntry.SlotData()
	if err != nil || *slotEntry.Action != "create" || *slotEntry.Table != "catalog_after" || slot.Filters == nil {
		t.Fatalf("%+v %v", slot, err)
	}
	if err := admin.DropTable(ctx, "catalog_before"); err != nil {
		t.Fatal(err)
	}
	dropped := catalogEntryOf(t, watch, "table", "catalog_before")
	if schema, err := dropped.TableSchema(); *dropped.Action != "drop" || schema != nil || err != nil {
		t.Fatalf("%+v %+v %v", dropped, schema, err)
	}
	if err := watch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationRotatePasswordAndUserLimits(t *testing.T) {
	server := startServer(t, serverConfig{workers: 4})
	ctx := t.Context()
	admin := connectIntegration(t, server, nil)
	if err := admin.CreateUser(ctx, "rot", "old-password", CreateUserOptions{}); err != nil {
		t.Fatal(err)
	}
	rot := connectIntegration(t, server, func(o *Options) { o.URI = server.uriAs("rot", "old-password") })
	if err := rot.RotatePassword(ctx, "new-password"); err != nil {
		t.Fatal(err)
	}
	if rot.URI().Password != "new-password" {
		t.Fatal("the client keeps the old password")
	}
	for _, password := range []string{"old-password", "new-password"} {
		_ = connectIntegration(t, server, func(o *Options) { o.URI = server.uriAs("rot", password) }).Close()
	}
	if err := rot.DropOldVerifier(ctx, "rot"); err != nil {
		t.Fatal(err)
	}
	if c, err := ConnectURI(ctx, server.uriAs("rot", "old-password")); err == nil {
		_ = c.Close()
		t.Fatal("the old password still logs in")
	}
	if err := rot.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	one, off := uint64(1), uint64(0)
	if err := admin.SetUserLimits(ctx, "rot", UserLimits{MaxConnections: &one}); err != nil {
		t.Fatal(err)
	}
	if c, err := ConnectURI(ctx, server.uriAs("rot", "new-password")); err == nil {
		_ = c.Close()
		t.Fatal("a second connection passed max_connections = 1")
	}
	if err := admin.SetUserLimits(ctx, "rot", UserLimits{MaxConnections: &off, StatementsPerSec: &off}); err != nil {
		t.Fatal(err)
	}
	_ = connectIntegration(t, server, func(o *Options) { o.URI = server.uriAs("rot", "new-password") }).Close()
}
