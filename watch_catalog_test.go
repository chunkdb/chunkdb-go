package chunkdb

import (
	"errors"
	"net"
	"slices"
	"strings"
	"testing"
)

// catalogHello is a HELLO reply that advertises watch_catalog.
var catalogHello = strings.Replace(helloReply, "%8\r\n", "%9\r\n", 1) + respBulk("features") + respArray(respBulk("watch_catalog"))

func catalogEntry(kind, action, name, table, data string) string {
	return respMap("kind", respBulk(kind), "action", respBulk(action), "name", respBulk(name), "table", table, "data", data)
}

func fakeCatalogServer(t *testing.T, hello string, frames string) *fakeServer {
	t.Helper()
	return newFakeServer(t, func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "HELLO":
			writeRaw(cn, hello)
		case "WATCH":
			writeRaw(cn, respMap("epoch", respBulk(watchEpoch), "revision", respInt(5))+frames)
		case "UNWATCH":
			writeSimple(cn, "OK")
		default:
			writeServerError(cn, "ERR INVALID_ARGUMENT reason=invalid_argument unexpected "+command)
		}
	})
}

func TestWatchCatalogDecodesEvents(t *testing.T) {
	user := respMap("name", respBulk("bot"), "manages_users", respBool(false), "grants", respMap("world", respBulk("READ")),
		"server_rights", respArray(respBulk("MONITOR")), "verifiers", respInt(1))
	filters := respMap("area", respArray(respInt(0), respInt(0), respInt(1), respInt(1)), "column_ids", respNull, "columns", respNull,
		"no_before", respBool(true), "chunks", respBool(false))
	slot := respMap("filters", filters, "owner", respBulk("bot"))
	whole := push(respBulk("catalog"), respBulk(watchEpoch), respInt(6), respInt(1000), respNull, respArray(
		catalogEntry("table", "create", "world", respNull, describeReply("world", 1, worldColumns)),
		catalogEntry("user", "create", "bot", respNull, user),
		catalogEntry("slot", "create", "consumer", respBulk("world"), slot),
	), respNull)
	dropped := push(respBulk("catalog"), respBulk(watchEpoch), respInt(7), respInt(1001), respBulk("admin"), respArray(
		catalogEntry("table", "drop", "world", respNull, respNull),
	), respNull)
	progress := push(respBulk("progress"), respBulk(watchEpoch), respInt(8))
	resync := push(respBulk("resync"), respBulk(watchEpoch), respInt(9), respNull, respNull)
	tableChange := changePush(1, respNull, respNull)
	server := fakeCatalogServer(t, catalogHello, whole+dropped+progress+resync+tableChange)
	client := newTestClient(t, server, nil)
	watch, err := client.WatchCatalog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	if watch.Start() != (Position{watchEpoch, 5}) {
		t.Fatal(watch.Start())
	}

	event := nextWatch(t, watch).(*CatalogEvent)
	if event.Position.Revision != 6 || event.CommitTimeMs != 1000 || event.User != nil || event.Txn != nil || len(event.Events) != 3 {
		t.Fatalf("%+v", event)
	}
	table, err := event.Events[0].TableSchema()
	if err != nil || table.Table != "world" || len(table.Columns) != len(worldColumns) {
		t.Fatalf("%+v %v", table, err)
	}
	if _, err := event.Events[0].UserData(); err == nil {
		t.Fatal("a table entry decoded as a user")
	}
	bot, err := event.Events[1].UserData()
	if err != nil || bot.Name != "bot" || bot.Grants["world"] != RightRead || !slices.Equal(bot.ServerRights, []Right{RightMonitor}) || bot.Verifiers != 1 {
		t.Fatalf("%+v %v", bot, err)
	}
	consumer, err := event.Events[2].SlotData()
	if err != nil || *event.Events[2].Table != "world" || *consumer.Owner != "bot" || !consumer.Filters.NoBefore || !slices.Equal(consumer.Filters.Area, []int64{0, 0, 1, 1}) {
		t.Fatalf("%+v %v", consumer, err)
	}

	event = nextWatch(t, watch).(*CatalogEvent)
	if *event.User != "admin" || *event.Events[0].Action != "drop" {
		t.Fatalf("%+v", event)
	}
	if table, err := event.Events[0].TableSchema(); table != nil || err != nil {
		t.Fatalf("a drop decoded as %+v %v", table, err)
	}
	if nextWatch(t, watch).(*ProgressEvent).Position.Revision != 8 {
		t.Fatal("progress")
	}
	if resync := nextWatch(t, watch).(*ResyncEvent); resync.Position.Revision != 9 || resync.ParentEpoch != nil {
		t.Fatalf("%+v", resync)
	}
	if err := watch.Ack(t.Context(), 8); err == nil {
		t.Fatal("a catalog watch accepted ACK")
	}
	// A table change has no place in a catalog stream.
	if _, err := watch.Next(t.Context()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

func TestWatchCatalogNeedsTheFeature(t *testing.T) {
	server := fakeCatalogServer(t, helloReply, "")
	client := newTestClient(t, server, nil)
	if _, err := client.WatchCatalog(t.Context()); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "watch_catalog") {
		t.Fatalf("got %v, want a watch_catalog error", err)
	}
	if slices.Contains(server.commands(), "WATCH CATALOG") {
		t.Fatal("WATCH CATALOG was sent to a server without the feature")
	}
}

func TestWatchCatalogClosesWithUnwatch(t *testing.T) {
	server := fakeCatalogServer(t, catalogHello, "")
	client := newTestClient(t, server, nil)
	watch, err := client.WatchCatalog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Close(); err != nil {
		t.Fatal(err)
	}
	if commands := server.commands(); !slices.Contains(commands, "UNWATCH") {
		t.Fatalf("got statements %q", commands)
	}
}
