package chunkdb

import (
	"bufio"
	"errors"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestIntegrationFoundationMetadata(t *testing.T) {
	server := startServer(t, serverConfig{workers: 4})
	c := connectIntegration(t, server, func(o *Options) {
		o.SessionOptions = map[string]any{"client_name": "metadata-check", "statement_timeout_ms": uint64(0), "default_tag": []byte{}}
	})
	info := c.ServerInfo()
	if info.ConnectionID == 0 || !info.Supports("protocol_foundation") || !info.Supports("read_metadata") || info.NotApplied["default_tag"] != "implementation_pending" {
		t.Fatal(info)
	}
	if info.Supports("statement_timeout") {
		if value, ok := info.Options["statement_timeout_ms"].Uint64(); !ok || value != 0 {
			t.Fatal(info.Options)
		}
	} else if info.NotApplied["statement_timeout_ms"] != "implementation_pending" {
		t.Fatal(info.NotApplied)
	}
	if string(info.Options["client_name"].Bulk) != "metadata-check" {
		t.Fatal(info.Options)
	}
	createTable(t, c, "foundation", TableSpec{Columns: []ColumnDef{{Name: "id", Type: TypeUint(8)}, {Name: "other", Type: TypeBool()}}, ChunkWidth: 2, ChunkHeight: 2})
	schema, err := c.Describe(t.Context(), "foundation")
	if err != nil || schema.Revision == nil || schema.Dims != 2 {
		t.Fatal(schema, err)
	}
	absent, err := c.GetBlockWithMetadata(t.Context(), "foundation", 100, 100)
	if err != nil || absent.Record != nil || absent.Version != nil || absent.SchemaVersion != schema.Version {
		t.Fatal(absent, err)
	}
	version, err := c.SetBlock(t.Context(), "foundation", 0, 0, Record{"id": 7})
	if err != nil {
		t.Fatal(err)
	}
	read, err := c.GetBlockWithMetadata(t.Context(), "foundation", 0, 0, "id")
	if err != nil || read.Version == nil || *read.Version != version || read.Record["id"] != uint64(7) {
		t.Fatal(read, err)
	}
	absent, err = c.GetBlockWithMetadata(t.Context(), "foundation", 1, 0)
	if err != nil || absent.Record != nil || absent.Version == nil || *absent.Version != version {
		t.Fatal(absent, err)
	}
	_, err = c.Transaction(t.Context(), func(tx *Tx) error {
		read, err := tx.GetBlockWithMetadata(t.Context(), "foundation", 0, 0, "id")
		if err != nil {
			return err
		}
		if read.Version == nil || *read.Version != version || read.Record["id"] != uint64(7) {
			t.Fatal(read)
		}
		if err := tx.SetBlock(t.Context(), "foundation", 0, 0, Record{"id": 9}); err != nil {
			return err
		}
		read, err = tx.GetBlockWithMetadata(t.Context(), "foundation", 0, 0, "id")
		if err != nil {
			return err
		}
		if read.Version != nil || read.Record["id"] != uint64(9) {
			t.Fatal(read)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	area, err := c.GetAreaWithSchema(t.Context(), "foundation", 0, 0, 0, 0, "id")
	if err != nil || area.SchemaVersion != schema.Version || len(area.Chunks) != 1 || area.Chunks[0].Chunk.Block(0, 0)["id"] != uint64(9) {
		t.Fatal(area, err)
	}
	if err := c.EnableFeature(t.Context(), "feature_activation"); err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(t.Context())
	if err != nil || status.ActivationPending || !slices.Contains(status.Directory.Enabled, "feature_activation") {
		t.Fatal(status, err)
	}
	if len(status.Tables) != 1 || status.Tables[0].Name != "foundation" {
		t.Fatal(status.Tables)
	}
	if err := c.CreateSlotWithOptions(t.Context(), "foundation", "consumer", SlotOptions{Columns: []string{"id"}, NoBefore: true, Chunks: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.RenameColumn(t.Context(), "foundation", "id", "renamed"); err != nil {
		t.Fatal(err)
	}
	slots, err := c.Slots(t.Context(), "foundation")
	if err != nil || len(slots) != 1 || slots[0].Filters == nil {
		t.Fatal(slots, err)
	}
	if len(slots[0].Filters.ColumnIDs) != 1 || len(slots[0].Filters.Columns) != 1 || slots[0].Filters.Columns[0] == nil || slots[0].Filters.ColumnIDs[0] != uint64(schema.Columns[0].ID) || *slots[0].Filters.Columns[0] != "renamed" || !slots[0].Filters.Chunks {
		t.Fatal(slots, err)
	}
	if err := c.DropColumn(t.Context(), "foundation", "renamed"); err != nil {
		t.Fatal(err)
	}
	slots, err = c.Slots(t.Context(), "foundation")
	if err != nil || len(slots) != 1 || slots[0].Filters == nil || len(slots[0].Filters.Columns) != 1 {
		t.Fatal(slots, err)
	}
	if !slots[0].Lost || slots[0].LostReason == nil || *slots[0].LostReason != "schema_filter" || slots[0].Filters.Columns[0] != nil {
		t.Fatal(slots, err)
	}
}

func TestIntegrationFoundationFramingPipeline(t *testing.T) {
	server := startServer(t, serverConfig{authNone: true})
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(server.host, strconv.Itoa(server.port)), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	writeRaw(conn, "HELLO 3\r\n")
	hello, err := readReply(reader)
	if err != nil {
		t.Fatal(err)
	}
	info, err := parseServerInfo(hello)
	if err != nil || !info.Supports("protocol_foundation") {
		t.Fatal(info, err)
	}
	writeRaw(conn, "PING\r\nbad $1 $1\r\n$3\r\none\r\n$3\r\ntwo\r\nPING\r\n")
	for i := range 3 {
		r, err := readReply(reader)
		if err != nil {
			t.Fatalf("ordered reply %d: %v", i, err)
		}
		if i == 1 {
			if r.Kind != ReplyError || r.Code != CodeUnknownCommand {
				t.Fatal(r)
			}
			var typed *Error
			if !errors.As(replyError(PhaseResponse, "BAD", r), &typed) || typed.RetryClass != RetryFatal {
				t.Fatal(r)
			}
		} else if r.Kind != ReplySimple || r.Text != "PONG" {
			t.Fatal(i, r)
		}
	}
}
