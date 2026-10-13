package chunkdb

import (
	"bufio"
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func replaceMapValue(r Reply, key string, value Reply) Reply {
	r = cloneReply(r)
	for i := range r.Map {
		if string(r.Map[i].Key.Bulk) == key {
			r.Map[i].Value = value
			return r
		}
	}
	r.Map = append(r.Map, MapEntry{Key: Reply{Kind: ReplyBulk, Bulk: []byte(key)}, Value: value})
	return r
}

func TestFoundationNullableCompletedFrontier(t *testing.T) {
	r := readOne(t, describeReply("world", 1, []string{fakeColumn(1, "id", "u8", false, false, respInt(0))}))
	for _, wire := range []string{respNull, respInt(0), "(18446744073709551615\r\n"} {
		schema, err := parseSchema(replaceMapValue(r, "revision", readOne(t, wire)))
		if err != nil {
			t.Fatal(err)
		}
		if wire == respNull {
			if schema.Revision != nil {
				t.Fatal("unknown frontier became numeric", schema.Revision)
			}
		} else {
			expected, _ := readOne(t, wire).Uint64()
			if schema.Revision == nil || *schema.Revision != expected {
				t.Fatal(schema)
			}
			clone := schema.clone()
			*clone.Revision = 17
			if *schema.Revision != expected {
				t.Fatal("revision aliases cache")
			}
		}
	}
	for _, wire := range []string{respInt(-1), "(18446744073709551616\r\n", respBulk("0")} {
		if _, err := parseSchema(replaceMapValue(r, "revision", readOne(t, wire))); !errors.Is(err, ErrProtocol) {
			t.Fatal(wire, err)
		}
	}
}

func featureStatusReply(name string) string {
	fields := []string{"enabled", respArray(respBulk("feed_slots"), respBulk("future_storage")), "compat", "(18446744073709551615\r\n", "ro_compat", respInt(2), "incompat", respInt(3), "future", respMap("nested", respNull)}
	if name != "" {
		fields = append(fields, "name", respBulk(name))
	}
	return respMap(fields...)
}

func TestFoundationStatusMetadata(t *testing.T) {
	wire := respMap("directory", featureStatusReply(""), "tables", respArray(featureStatusReply("world")), "activation_pending", respBool(true), "future_root", respArray(respNull))
	server := newFakeServer(t, respondWith(wire))
	c := newTestClient(t, server, nil)
	got, err := c.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !got.ActivationPending || got.Directory.Compat != math.MaxUint64 || got.Directory.Enabled[1] != "future_storage" || got.Tables[0].Name != "world" || got.Tables[0].ReadOnlyCompat != 2 || got.Fields["future_root"].Kind != ReplyArray || got.Directory.Fields["future"].Kind != ReplyMap {
		t.Fatal(got)
	}
	pool, err := NewPool(PoolOptions{Options: Options{URI: server.uri("")}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Status(t.Context()); err != nil {
		t.Fatal(err)
	}
	r := readOne(t, wire)
	for _, key := range []string{"directory", "tables", "activation_pending"} {
		if _, err := parseStatus(replaceMapValue(r, key, Reply{Kind: ReplyNull})); !errors.Is(err, ErrProtocol) {
			t.Fatal(key, err)
		}
	}
	directory, _ := r.Lookup("directory")
	for _, key := range []string{"compat", "ro_compat", "incompat"} {
		for _, invalid := range []string{respInt(-1), "(18446744073709551616\r\n", respBulk("1")} {
			if _, err := parseStatus(replaceMapValue(r, "directory", replaceMapValue(directory, key, readOne(t, invalid)))); !errors.Is(err, ErrProtocol) {
				t.Fatal(key, invalid, err)
			}
		}
	}
	if _, err := parseFeatureStatus(replaceMapValue(directory, "enabled", readOne(t, respArray(respInt(1))))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func slotFiltersReply() string {
	return respMap("area", respArray(respInt(-1), respInt(-2), respInt(-3), respInt(4), respInt(5), respInt(6)), "columns", respArray(respBulk("renamed"), respNull), "column_ids", respArray(respInt(7), "(18446744073709551615\r\n"), "no_before", respBool(true), "chunks", respBool(true), "future", respArray(respNull))
}

func TestFoundationPersistedSlotMetadata(t *testing.T) {
	slot := respMap("table", respBulk("world"), "name", respBulk("consumer"), "epoch", respBulk(watchEpoch), "acked", respInt(12), "retained_bytes", respInt(50), "lost", respBool(true), "filters", slotFiltersReply(), "lost_reason", respBulk("future_loss"), "future", respMap())
	server := newFakeServer(t, respondWith(respArray(slot)))
	c := newTestClient(t, server, nil)
	slots, err := c.Slots(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	got := slots[0]
	if got.LostReason == nil || *got.LostReason != "future_loss" || len(got.Filters.Area) != 6 || *got.Filters.Columns[0] != "renamed" || got.Filters.Columns[1] != nil || got.Filters.ColumnIDs[1] != math.MaxUint64 || !got.Filters.Chunks || !got.Filters.NoBefore || got.Fields["future"].Kind != ReplyMap {
		t.Fatal(got)
	}
	r := readOne(t, slotFiltersReply())
	for key, value := range map[string]string{"area": respArray(respInt(1), respInt(2), respInt(3)), "columns": respNull, "column_ids": respArray(respInt(7)), "no_before": respNull, "chunks": respInt(1)} {
		if _, err := parseSlotFilters(replaceMapValue(r, key, readOne(t, value))); !errors.Is(err, ErrProtocol) {
			t.Fatal(key, err)
		}
	}
	noProjection := replaceMapValue(replaceMapValue(replaceMapValue(r, "area", Reply{Kind: ReplyNull}), "columns", Reply{Kind: ReplyNull}), "column_ids", Reply{Kind: ReplyNull})
	filters, err := parseSlotFilters(noProjection)
	if err != nil || filters.Columns != nil || filters.ColumnIDs != nil || filters.Area != nil {
		t.Fatal(filters, err)
	}
}

func TestFoundationChunksApplicationDeliveryAck(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	c, err := NewClient(Options{})
	if err != nil {
		t.Fatal(err)
	}
	w := &Watch{client: c, slot: "consumer", start: Position{watchEpoch, 10}, delivered: Position{watchEpoch, 10}, schemas: map[watchSchemaKey][]Column{{watchEpoch, 1}: {{ID: 1, Name: "id", Type: TypeUint(8)}}}, pushes: make(chan Reply, 5), closing: make(chan struct{}), done: make(chan struct{}), nextGate: make(chan struct{}, 1), controlGate: make(chan struct{}, 1)}
	w.ackContext, w.ackCancel = context.WithCancel(context.Background())
	defer w.ackCancel()
	w.conn = &conn{client: c, netConn: left, writer: bufio.NewWriter(left)}
	frames := []string{
		push(respBulk("chunks"), respBulk(watchEpoch), respInt(11), respInt(123), respNull, respInt(1), respArray(respArray(respInt(0), respInt(0), respInt(11), respBulk("form"), respNull)), respNull, respNull, respNull),
		push(respBulk("catalog"), respBulk(watchEpoch), respInt(12), respInt(123), respNull, respArray(), respNull),
		foundationSchemaPush(2),
		push(respBulk("resync"), respBulk(watchEpoch), respInt(14), respNull, respNull),
		push(respBulk("progress"), respBulk(watchEpoch), respInt(15)),
	}
	for _, frame := range frames {
		w.pushes <- readOne(t, frame)
	}
	if len(w.pushes) != 5 {
		t.Fatal("fixture did not queue decoded events")
	}
	if err := w.Ack(t.Context(), 11); !errors.Is(err, ErrProtocol) {
		t.Fatal("queued CHUNKS advanced ACK", err)
	}
	if _, ok := nextWatch(t, w).(*ChunksEvent); !ok {
		t.Fatal("expected chunks")
	}
	lines := make(chan string, 2)
	go func() {
		reader := bufio.NewReader(right)
		for range 2 {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			lines <- line
		}
	}()
	if err := w.Ack(t.Context(), 11); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-lines:
		if line != "ACK 11\r\n" {
			t.Fatal(line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivered CHUNKS ACK not received")
	}
	if _, ok := nextWatch(t, w).(*CatalogEvent); !ok {
		t.Fatal("catalog")
	}
	if err := w.Ack(t.Context(), 12); !errors.Is(err, ErrProtocol) {
		t.Fatal("reserved catalog advanced ACK", err)
	}
	if _, ok := nextWatch(t, w).(*SchemaEvent); !ok {
		t.Fatal("schema")
	}
	if err := w.Ack(t.Context(), 12); !errors.Is(err, ErrProtocol) {
		t.Fatal("schema advanced ACK", err)
	}
	if _, ok := nextWatch(t, w).(*ResyncEvent); !ok {
		t.Fatal("resync")
	}
	if err := w.Ack(t.Context(), 14); !errors.Is(err, ErrProtocol) {
		t.Fatal("resync advanced ACK", err)
	}
	nextWatch(t, w)
	if err := w.Ack(t.Context(), 15); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-lines:
		if line != "ACK 15\r\n" {
			t.Fatal(line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivered progress ACK not received")
	}
}

func TestFoundationExplicitTableSelection(t *testing.T) {
	server := newFakeServer(t, withHello(func(s *fakeServer, conn net.Conn, statement string) {
		if statement == "BEGIN" || statement == "ROLLBACK" {
			writeSimple(conn, "OK")
			return
		}
		genericHandler(s, conn, statement)
	}))
	c, err := NewClient(Options{URI: server.uri("")})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.DefaultTable() != "" {
		t.Fatal(c.DefaultTable())
	}
	if _, err := c.GetBlock(t.Context(), "", 0, 0); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolOptions{Options: Options{URI: server.uri("")}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.GetBlock(t.Context(), "", 0, 0); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if err := pool.CreateSlot(t.Context(), "", "consumer"); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if len(server.commands()) != 0 {
		t.Fatal("unselected helpers dialed server", server.commands())
	}
	_, err = c.Transaction(t.Context(), func(tx *Tx) error { _, err := tx.GetBlock(t.Context(), "", 0, 0); return err })
	if !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	for _, statement := range server.commands() {
		if strings.Contains(statement, "default") || strings.HasPrefix(statement, "GET") || strings.HasPrefix(statement, "DESCRIBE") {
			t.Fatal("unselected transaction sent data request", server.commands())
		}
	}
	selected, err := NewClient(Options{URI: server.uri("") + "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	if selected.DefaultTable() != "default" {
		t.Fatal(selected.DefaultTable())
	}
	if _, err := selected.DeleteBlock(t.Context(), "", 0, 0); err != nil {
		t.Fatal(err)
	}
	if server.lastCommand(t) != "DELETE BLOCK 0 0 FROM default" {
		t.Fatal(server.commands())
	}
}
