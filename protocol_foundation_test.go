package chunkdb

import (
	"bufio"
	"errors"
	"math"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestFoundationBigNumbers(t *testing.T) {
	for _, text := range []string{"-170141183460469231731687303715884105728", "-9223372036854775809", "-9223372036854775808", "0", "9223372036854775807", "9223372036854775808", "18446744073709551615", "18446744073709551616", "170141183460469231731687303715884105727"} {
		r := readOne(t, "("+text+"\r\n")
		exact, _ := new(big.Int).SetString(text, 10)
		value, ok := r.BigInt()
		if !ok || value.Cmp(exact) != 0 {
			t.Fatal(text, r)
		}
		i, iok := r.Int64()
		u, uok := r.Uint64()
		if iok != exact.IsInt64() || (iok && i != exact.Int64()) || uok != exact.IsUint64() || (uok && u != exact.Uint64()) {
			t.Fatal(text, i, iok, u, uok)
		}
		value.SetInt64(9)
		again, _ := r.BigInt()
		if again.Cmp(exact) != 0 {
			t.Fatal("BigInt accessor leaked storage")
		}
	}
	for _, text := range []string{"", "+1", " 1", "1x", "--1", "0x10", "1.0"} {
		_, err := readReply(bufio.NewReader(strings.NewReader("(" + text + "\r\n")))
		if !errors.Is(err, ErrProtocol) {
			t.Fatal(text, err)
		}
	}
	if v, ok := readOne(t, ":-9223372036854775808\r\n").BigInt(); !ok || v.Int64() != math.MinInt64 {
		t.Fatal(v, ok)
	}
}

func TestFoundationParameterOccurrences(t *testing.T) {
	for statement, want := range map[string]int{
		"bad $1 $1": 2, "bad $3": 1, "bad $0 $999999999999999999999999999999": 2,
		`bad '$9' "$8" $2 $1`: 2, `bad 'a''$8' x'$7' b'$6' $1`: 1,
		`bad "a""$8" $1`: 1, `bad '$9 $1`: 0, `bad "unterminated $1`: 0,
		"bad! $1 nonsense $2": 2, "bad $ $x": 0,
	} {
		if got := parameterOccurrences(statement); got != want {
			t.Fatal(statement, got, want)
		}
		frames := make([][]byte, want)
		if _, err := encodeRequest("BAD", statement, frames); err != nil {
			t.Fatal(statement, err)
		}
		if _, err := encodeRequest("BAD", statement, append(frames, []byte("extra"))); !errors.Is(err, ErrProtocol) {
			t.Fatal(statement, err)
		}
	}
	wire, err := encodeRequest("BAD", "bad $2 $1", [][]byte{[]byte("one"), []byte("two")})
	if err != nil || string(wire) != "bad $2 $1\r\n$3\r\none\r\n$3\r\ntwo\r\n" {
		t.Fatal(string(wire), err)
	}
}

func TestFoundationErrorKeysAndClasses(t *testing.T) {
	r := readOne(t, "-ERR VERSION_MISMATCH current=18446744073709551615 extra=a%20b%3Dc percent=%Q0 reread first\r\n")
	e := replyError(PhaseResponse, "SET BLOCK", r)
	var base *Error
	var mismatch *VersionMismatchError
	if !errors.As(e, &base) || !errors.As(e, &mismatch) || mismatch.Current != math.MaxUint64 || base.RetryClass != RetryReread || base.ServerKeys["extra"] != "a b=c" || base.ServerKeys["percent"] != "%Q0" || base.ServerMessage != "reread first" {
		t.Fatal(e, base)
	}
	for code, want := range map[string]RetryClass{CodeConditionFailed: RetryReread, CodeSchemaMismatch: RetryReread, CodeConflict: RetryRerun, CodeBusy: RetryBackoff, CodeDiskFull: RetryBackoff, CodeRateLimited: RetryBackoff, CodeOutcomeUnknown: RetryUnknownOutcome, CodeMigrationMismatch: RetryFatal, CodeSlotBusy: RetryFatal, CodeCancelled: RetryFatal, CodeTimeout: RetryFatal, CodeKilled: RetryFatal, "FUTURE_CODE": RetryFatal} {
		base := serverError(PhaseResponse, "DO", code, "CONFLICT backoff unknown_outcome")
		if base.RetryClass != want {
			t.Fatal(code, base.RetryClass, want)
		}
	}
	r = readOne(t, "-ERR FUTURE_CODE revision=18446744073709551616 connection_id=18446744073709551615 reason=future hello\r\n")
	e = replyError(PhaseResponse, "DO", r)
	if !errors.As(e, &base) || base.Metadata.Revision != nil || base.Metadata.ConnectionID == nil || *base.Metadata.ConnectionID != math.MaxUint64 || base.RetryClass != RetryFatal {
		t.Fatal(e, base)
	}
}

func TestFoundationUnknownPushDoesNotConsumeReplies(t *testing.T) {
	firstSeen := make(chan struct{})
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, statement string) {
		switch statement {
		case "FIRST":
			close(firstSeen)
		case "SECOND":
			writeRaw(conn, "+FIRST\r\n"+push(respBulk("future"), respMap("nested", respArray(respBulk("bytes"), "(18446744073709551616\r\n")))+"+SECOND\r\n")
		}
	}))
	c := newTestClient(t, server, func(o *Options) { o.PipelineDepth = 2 })
	first := make(chan result, 1)
	go func() { r, e := c.Do(t.Context(), "FIRST"); first <- result{r, e} }()
	select {
	case <-firstSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("FIRST request never arrived")
	}
	second, e := c.Do(t.Context(), "SECOND")
	if e != nil || second.Text != "SECOND" {
		t.Fatal(second, e)
	}
	select {
	case got := <-first:
		if got.err != nil || got.reply.Text != "FIRST" {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FIRST response never returned")
	}
}

func TestFoundationUnknownColumnAndExtendedRows(t *testing.T) {
	schema, err := parseSchema(readOne(t, describeReply("world", 1, []string{
		fakeColumn(1, "id", "u8", false, false, respInt(7)), fakeColumn(2, "future", "decimal(20)", false, false, respBulk("123.40")), fakeColumn(3, "flag", "bool", false, false, respBool(true)),
	})))
	if err != nil {
		t.Fatal(err)
	}
	if schema.Columns[1].Type.String() != "decimal(20)" || string(schema.Columns[1].Default.([]byte)) != "123.40" {
		t.Fatal(schema)
	}
	row, err := decodeWatchRow(schema.Columns, readOne(t, respArray(respInt(8), respBulk("999.99"), respBool(false), respMap("extra", respNull))))
	if err != nil || row["id"] != uint64(8) || string(row["future"].([]byte)) != "999.99" || row["flag"] != false {
		t.Fatal(row, err)
	}
	if _, err := decodeWatchRow(schema.Columns, readOne(t, respArray(respInt(8), respInt(9), respBool(false)))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if _, err := DecodeChunk(schema, nil); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "unknown column type") {
		t.Fatal(err)
	}
	if _, err := EncodeChunk(schema, NewChunk(schema)); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func foundationHelloReply() string {
	return respMap("protocol", respInt(3), "server_version", respBulk("test"),
		"max_line_bytes", respInt(65536), "max_parameters", respInt(65535),
		"max_area_chunks", respInt(256), "max_response_bytes", respInt(64<<20), "max_scan_limit", respInt(1024), "server_signature", respNull,
		"connection_id", "(18446744073709551615\r\n", "features", respArray(respBulk("protocol_foundation"), respBulk("read_metadata")),
		"options", respMap("client_name", respBulk("worker"), "future", respArray(respBulk("opaque"))),
		"not_applied", respMap("statement_timeout_ms", respMap("reason", respBulk("implementation_pending"), "extra", respInt(1)), "default_tag", respMap("reason", respBulk("implementation_pending")), "future", respMap("reason", respBulk("unknown_option"))),
		"idle_timeout_ms", respInt(60000), "limits", respMap("max_area_chunks", respInt(256), "max_area_response_bytes", respInt(67108864), "txn_max_written_chunks", respInt(64), "txn_max_read_chunks", respInt(1024), "max_watches", respInt(64), "max_returning_rows", respInt(65536), "max_expression_nodes", respInt(256), "future", respNull))
}

func TestFoundationHelloOptions(t *testing.T) {
	tag := []byte{0, 255}
	opts := Options{SessionOptions: map[string]any{"CLIENT_NAME": "worker", "statement_timeout_ms": uint64(math.MaxUint64), "default_tag": tag, "future": true}}
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, statement string) {
		if statement != "HELLO 3 WITH client_name = $1, default_tag = $2, future = TRUE, statement_timeout_ms = 18446744073709551615" {
			t.Errorf("unexpected HELLO: %s", statement)
		}
		params := s.paramsOf(conn)
		if len(params) != 2 || string(params[0]) != "worker" || string(params[1]) != string([]byte{0, 255}) {
			t.Errorf("params: %v", params)
		}
		writeRaw(conn, foundationHelloReply())
	})
	opts.URI = server.uri("")
	c, err := NewClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tag[0] = 9
	opts.SessionOptions["CLIENT_NAME"] = "changed"
	if err := c.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	info := c.ServerInfo()
	if info.ConnectionID != math.MaxUint64 || !info.Supports("read_metadata") || info.Supports("returning") || info.NotApplied["default_tag"] != "implementation_pending" || info.Limits.MaxExpressionNodes != 256 {
		t.Fatal(info)
	}
	info.Features[0] = "changed"
	info.Options["future"].Array[0].Bulk[0] = 'x'
	info.NotApplied["default_tag"] = "changed"
	again := c.ServerInfo()
	if !again.Supports("protocol_foundation") || string(again.Options["future"].Array[0].Bulk) != "opaque" || again.NotApplied["default_tag"] != "implementation_pending" {
		t.Fatal("ServerInfo aliases internal state")
	}
	normalized, err := sessionOptions(map[string]any{"default_tag": []byte(nil)})
	if err != nil {
		t.Fatal(err)
	}
	_, params := helloOptions("HELLO 3 USER bot $1", [][]byte{[]byte("auth")}, normalized)
	if len(params) != 2 || params[1] == nil || len(params[1]) != 0 {
		t.Fatal("empty tag became NULL", params)
	}
	for _, invalid := range []map[string]any{{"CLIENT_NAME": "a", "client_name": "b"}, {"client_name": []byte("a")}, {"statement_timeout_ms": -1}, {"default_tag": make([]byte, 257)}, {"future": 1.5}} {
		if _, err := NewClient(Options{SessionOptions: invalid}); !errors.Is(err, ErrProtocol) {
			t.Fatal(invalid, err)
		}
	}
}

func TestFoundationHelloKnownFieldValidation(t *testing.T) {
	for _, key := range []string{"connection_id", "features", "options", "not_applied", "idle_timeout_ms", "limits"} {
		r := readOne(t, foundationHelloReply())
		for i := range r.Map {
			if string(r.Map[i].Key.Bulk) == key {
				r.Map[i].Value = Reply{Kind: ReplyNull}
			}
		}
		if _, err := parseServerInfo(r); !errors.Is(err, ErrProtocol) {
			t.Fatal(key, err)
		}
	}
	r := readOne(t, foundationHelloReply())
	for i := range r.Map {
		if string(r.Map[i].Key.Bulk) == "connection_id" {
			r.Map[i].Value = Reply{Kind: ReplyInteger}
		}
	}
	if _, err := parseServerInfo(r); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestFoundationReadMetadata(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, statement string) {
		switch {
		case strings.HasPrefix(statement, "DESCRIBE"):
			writeRaw(conn, describeReply("world", 1, []string{fakeColumn(1, "id", "u8", false, false, respInt(0))}))
		case strings.HasPrefix(statement, "GET BLOCK"):
			row := respArray(respInt(7), respMap("future", respNull))
			if strings.Contains(statement, "GET BLOCK 1 ") {
				row = respNull
			}
			v := "(18446744073709551615\r\n"
			if strings.Contains(statement, "GET BLOCK 2 ") {
				v = respNull
			}
			version, schema := strings.Contains(statement, "WITH VERSION"), strings.Contains(statement, "WITH SCHEMA")
			switch {
			case version && schema:
				writeRaw(conn, respArray(v, respInt(1), row, respNull))
			case version:
				writeRaw(conn, respArray(v, row, respNull))
			case schema:
				writeRaw(conn, respArray(respInt(1), row, respNull))
			default:
				writeRaw(conn, row)
			}
		case strings.HasPrefix(statement, "GET AREA"):
			writeRaw(conn, respArray(respInt(1), respArray(), respMap("future", respNull)))
		}
	}))
	c := newTestClient(t, server, nil)
	for _, opts := range []ReadOptions{{}, {WithVersion: true}, {WithSchema: true}, {WithVersion: true, WithSchema: true}} {
		for x := int64(0); x < 3; x++ {
			got, err := c.GetBlockWithOptions(t.Context(), "world", x, 0, opts)
			if err != nil {
				t.Fatal(opts, x, err)
			}
			if x == 1 {
				if got.Record != nil {
					t.Fatal(got)
				}
			} else if got.Record["id"] != uint64(7) {
				t.Fatal(got)
			}
			if opts.WithVersion && x != 2 {
				if got.Version == nil || *got.Version != math.MaxUint64 {
					t.Fatal(got)
				}
			} else if got.Version != nil {
				t.Fatal(got)
			}
			if opts.WithSchema && got.SchemaVersion != 1 {
				t.Fatal(got)
			}
		}
	}
	area, err := c.GetAreaWithSchema(t.Context(), "world", 0, 0, 1, 1)
	if err != nil || area.SchemaVersion != 1 || area.Chunks == nil || len(area.Chunks) != 0 {
		t.Fatal(area, err)
	}
	for _, wire := range []string{respNull, respArray(respNull), respArray(respBulk("x"), respInt(1), respNull), respArray(respInt(1), respBulk("x"), respNull)} {
		if _, _, err := parseBlockEnvelope(readOne(t, wire), ReadOptions{true, true}); !errors.Is(err, ErrProtocol) {
			t.Fatal(wire, err)
		}
	}
}

func TestFoundationDescribeMetadata(t *testing.T) {
	r := readOne(t, describeReply("world", 1, []string{fakeColumn(1, "id", "u8", false, false, respInt(0))}))
	r.Map = append(r.Map, MapEntry{readOne(t, respBulk("epoch")), readOne(t, respBulk(watchEpoch))}, MapEntry{readOne(t, respBulk("revision")), readOne(t, "(18446744073709551615\r\n")}, MapEntry{readOne(t, respBulk("dims")), readOne(t, respInt(2))}, MapEntry{readOne(t, respBulk("chunk_columns")), readOne(t, respArray(fakeColumn(2, "meta", "future", false, false, respBulk("value"))))})
	schema, err := parseSchema(r)
	if err != nil || schema.Epoch != watchEpoch || (schema.Revision == nil || *schema.Revision != math.MaxUint64) || schema.Dims != 2 || string(schema.ChunkColumns[0].Default.([]byte)) != "value" {
		t.Fatal(schema, err)
	}
	clone := schema.clone()
	clone.ChunkColumns[0].Default.([]byte)[0] = 'x'
	if string(schema.ChunkColumns[0].Default.([]byte)) != "value" {
		t.Fatal("chunk default aliases schema")
	}
	for _, key := range []string{"epoch", "dims", "chunk_columns"} {
		bad := cloneReply(r)
		for i := range bad.Map {
			if string(bad.Map[i].Key.Bulk) == key {
				bad.Map[i].Value = Reply{Kind: ReplyNull}
			}
		}
		if _, err := parseSchema(bad); !errors.Is(err, ErrProtocol) {
			t.Fatal(key, err)
		}
	}
}

func foundationSchemaPush(dims int64) string {
	return push(respBulk("schema"), respMap("epoch", respBulk(watchEpoch), "revision", respInt(11), "schema_version", respInt(2), "dims", respInt(dims), "columns", respArray(fakeColumn(1, "id", "u8", false, false, respInt(0)), fakeColumn(2, "future", "new_type", false, false, respBulk("raw"))), "chunk_columns", respArray(fakeColumn(3, "meta", "bool", false, false, respBool(false))), "extra", respArray()), respNull)
}

func TestFoundationFeedPrefixes(t *testing.T) {
	for _, dims := range []int64{2, 3} {
		w := &Watch{schemas: make(map[uint64][]Column)}
		schemaEvent, err := w.decode(t.Context(), readOne(t, foundationSchemaPush(dims)))
		if err != nil {
			t.Fatal(err)
		}
		se := schemaEvent.(*SchemaEvent)
		se.Columns[0].Name = "changed"
		se.ChunkColumns[0].Name = "changed"
		axes := []string{respInt(-1), respArray(respInt(math.MaxInt64), respInt(3), respNull)}
		chunkAxes := []string{respInt(-2), respInt(4)}
		if dims == 3 {
			axes = append(axes, respInt(5))
			chunkAxes = append(chunkAxes, respInt(6))
		}
		block := append(append([]string{}, axes...), respNull, respArray(respInt(9), respBulk("new"), respNull), respNull)
		chunk := append(append([]string{}, chunkAxes...), respNull, respArray(respBool(true), respNull), respNull)
		a := []string{respBulk("change"), respBulk(watchEpoch), respInt(12), respInt(123), respNull, respInt(2), respArray(respArray(block...)), respBulk(""), "(18446744073709551615\r\n", respArray(respArray(chunk...)), respNull}
		event, err := w.decode(t.Context(), readOne(t, push(a...)))
		if err != nil {
			t.Fatal(err)
		}
		e := event.(*ChangeEvent)
		if e.Tag == nil || len(e.Tag) != 0 || e.Txn == nil || *e.Txn != math.MaxUint64 || e.Blocks[0].After["id"] != uint64(9) || string(e.Blocks[0].After["future"].([]byte)) != "new" || e.ChunkChanges[0].After["meta"] != true {
			t.Fatal(e)
		}
		if dims == 3 && (e.Blocks[0].Z != int64(5) || e.ChunkChanges[0].CZ == nil || *e.ChunkChanges[0].CZ != 6) {
			t.Fatal(e)
		}
		frame := append(append([]string{}, chunkAxes...), "(18446744073709551615\r\n", respBulk("rawform"), respNull)
		chunks, err := w.decode(t.Context(), readOne(t, push(respBulk("chunks"), respBulk(watchEpoch), respInt(13), respInt(124), respBulk("writer"), respInt(2), respArray(respArray(frame...)), respNull, respNull, respNull)))
		if err != nil || string(chunks.(*ChunksEvent).Chunks[0].Form) != "rawform" || chunks.(*ChunksEvent).Chunks[0].Version != math.MaxUint64 {
			t.Fatal(chunks, err)
		}
		for index, wire := range map[int]string{3: respBulk("bad"), 4: respInt(1), 5: respNull, 6: respNull, 7: respInt(1), 8: respBulk("id"), 9: respNull} {
			invalid := append([]string{}, a...)
			invalid[index] = wire
			if _, err := w.decode(t.Context(), readOne(t, push(invalid...))); !errors.Is(err, ErrProtocol) {
				t.Fatal(index, err)
			}
		}
	}
	w := &Watch{schemas: make(map[uint64][]Column)}
	catalog, err := w.decode(t.Context(), readOne(t, push(respBulk("catalog"), respBulk(watchEpoch), respInt(14), respInt(125), respNull, respArray(respMap("kind", respBulk("table"), "action", respBulk("create"), "name", respBulk("world"), "table", respNull, "data", respMap("future", respNull), "extra", respNull)), "(18446744073709551615\r\n", respNull)))
	if err != nil || *catalog.(*CatalogEvent).Txn != math.MaxUint64 {
		t.Fatal(catalog, err)
	}
	for _, wire := range []string{respInt(-1), "(18446744073709551616\r\n", respBulk("1")} {
		if _, err := nullableUint64(readOne(t, wire)); !errors.Is(err, ErrProtocol) {
			t.Fatal(wire, err)
		}
	}
	resync, err := w.decode(t.Context(), readOne(t, push(respBulk("resync"), respBulk(watchEpoch), respInt(15), respBulk(watchEpoch), "(18446744073709551615\r\n", respNull)))
	if err != nil || *resync.(*ResyncEvent).ForkRevision != math.MaxUint64 {
		t.Fatal(resync, err)
	}
}

func TestFoundationProgressDeliveryAckAndFilters(t *testing.T) {
	acked := make(chan string, 4)
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, statement string) {
		switch verbOf(statement) {
		case "HELLO":
			writeRaw(conn, foundationHelloReply())
		case "DESCRIBE":
			writeRaw(conn, describeReply("world", 1, []string{fakeColumn(1, "id", "u8", false, false, respInt(0))}))
		case "CREATE":
			if statement != "CREATE SLOT IF NOT EXISTS 'consumer' ON world AREA -1 -2 TO 3 4 COLUMNS (id) NO BEFORE CHUNKS" {
				t.Errorf("slot: %s", statement)
			}
			writeSimple(conn, "OK")
		case "WATCH":
			if statement != "WATCH world SLOT 'consumer' COLUMNS (id) NO BEFORE CHUNKS TAKEOVER ACK TIMEOUT 1000" {
				t.Errorf("watch: %s", statement)
			}
			writeRaw(conn, respMap("epoch", respBulk(watchEpoch), "revision", respInt(10), "extra", respNull)+push(respBulk("future"), respArray("(18446744073709551616\r\n"))+foundationSchemaPush(2)+push(respBulk("progress"), respBulk(watchEpoch), respInt(12), respNull)+push(respBulk("resync"), respBulk(watchEpoch), respInt(13), respNull, respNull, respNull))
		case "ACK":
			acked <- statement
		case "UNWATCH":
			writeSimple(conn, "OK")
		}
	})
	c := newTestClient(t, server, nil)
	if err := c.CreateSlotWithOptions(t.Context(), "world", "consumer", SlotOptions{Area: &Area{-1, -2, 3, 4}, Columns: []string{"id"}, NoBefore: true, Chunks: true, IfNotExists: true}); err != nil {
		t.Fatal(err)
	}
	w, err := c.Watch(t.Context(), "world", WatchOptions{Slot: "consumer", Columns: []string{"id"}, NoBefore: true, Chunks: true, Takeover: true, AckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Ack(t.Context(), 12); !errors.Is(err, ErrProtocol) {
		t.Fatal("undelivered progress advanced ACK", err)
	}
	if _, ok := nextWatch(t, w).(*SchemaEvent); !ok {
		t.Fatal("schema")
	}
	if err := w.Ack(t.Context(), 11); !errors.Is(err, ErrProtocol) {
		t.Fatal("schema advanced ACK", err)
	}
	if nextWatch(t, w).(*ProgressEvent).Position.Revision != 12 {
		t.Fatal("progress")
	}
	if err := w.Ack(t.Context(), 12); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-acked:
		if got != "ACK 12" {
			t.Fatal(got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ACK did not reach server")
	}
	if _, ok := nextWatch(t, w).(*ResyncEvent); !ok {
		t.Fatal("resync")
	}
	if err := w.Ack(t.Context(), 13); !errors.Is(err, ErrProtocol) {
		t.Fatal("resync advanced ACK", err)
	}
	for _, opts := range []WatchOptions{{Takeover: true}, {AckTimeout: time.Second}, {Slot: "consumer", AckTimeout: -1}, {Slot: "consumer", AckTimeout: time.Nanosecond}, {Columns: []string{}}, {Columns: []string{"id", "id"}}} {
		if _, err := c.Watch(t.Context(), "world", opts); !errors.Is(err, ErrProtocol) {
			t.Fatal(opts, err)
		}
	}
}

func TestFoundationRejectedParametersKeepConnection(t *testing.T) {
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, statement string) {
		switch verbOf(statement) {
		case "HELLO":
			writeRaw(conn, foundationHelloReply())
		case "PING":
			writeSimple(conn, "PONG")
		default:
			if len(s.paramsOf(conn)) != 2 {
				t.Errorf("incorrect lexical framing: %v", s.paramsOf(conn))
			}
			writeRaw(conn, "-ERR SYNTAX reason=parameter_refs Future-Key=retained diagnostic\r\n")
		}
	})
	c := newTestClient(t, server, nil)
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err := c.Do(t.Context(), "bad $1 $1", []byte("one"), []byte("two"))
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != CodeSyntax || typed.ServerKeys["Future-Key"] != "retained" {
		t.Fatal(err)
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	hellos := 0
	for _, statement := range server.commands() {
		if verbOf(statement) == "HELLO" {
			hellos++
		}
	}
	if hellos != 1 {
		t.Fatal("rejection caused reconnect", server.commands())
	}
}

func TestFoundationAuthenticatedOptionFrames(t *testing.T) {
	login := fakeLogin{user: "bot", password: "secret"}
	handler := login.handler(genericHandler)
	server := newFakeServer(t, func(s *fakeServer, conn net.Conn, statement string) {
		if strings.HasPrefix(statement, "HELLO") {
			if statement != "HELLO 3 USER bot $1 WITH client_name = $2, default_tag = $3" {
				t.Errorf("HELLO: %s", statement)
			}
			params := s.paramsOf(conn)
			if len(params) != 3 || !strings.HasPrefix(string(params[0]), "n,,") || string(params[1]) != "worker" || string(params[2]) != "tag" {
				t.Errorf("HELLO frames: %v", params)
			}
		}
		handler(s, conn, statement)
	})
	c, err := Connect(t.Context(), Options{URI: server.uri("bot:secret"), SessionOptions: map[string]any{"client_name": "worker", "default_tag": []byte("tag")}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFoundationExplicitFeatureActivation(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, statement string) {
		switch statement {
		case "ENABLE FEATURE feature_activation":
			writeSimple(conn, "OK")
		case "ENABLE FEATURE future_feature":
			writeRaw(conn, "-ERR INVALID_ARGUMENT reason=unknown_feature feature=future_feature\r\n")
		default:
			t.Errorf("unexpected command: %s", statement)
		}
	}))
	c := newTestClient(t, server, nil)
	if err := c.EnableFeature(t.Context(), "feature_activation"); err != nil {
		t.Fatal(err)
	}
	var typed *Error
	if err := c.EnableFeature(t.Context(), "future_feature"); !errors.As(err, &typed) || typed.ServerKeys["feature"] != "future_feature" || typed.Metadata.Reason != "unknown_feature" {
		t.Fatal(err)
	}
	if err := c.EnableFeature(t.Context(), "unsafe name"); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestFoundationTransactionPrivateReadMetadata(t *testing.T) {
	server, state := newTxServer(t)
	state.replies["GET BLOCK"] = []string{respArray(respInt(5), respInt(1), respArray(respInt(7))), respArray(respNull, respInt(1), respArray(respInt(9)))}
	state.replies["GET AREA"] = []string{respArray(respInt(1), respArray())}
	c := newTestClient(t, server, nil)
	_, err := c.Transaction(t.Context(), func(tx *Tx) error {
		snapshot, err := tx.GetBlockWithMetadata(t.Context(), "world", 0, 0, "id")
		if err != nil {
			return err
		}
		if snapshot.Version == nil || *snapshot.Version != 5 || snapshot.Record["id"] != uint64(7) {
			t.Fatal(snapshot)
		}
		if err := tx.SetBlock(t.Context(), "world", 0, 0, Record{"id": 9}); err != nil {
			return err
		}
		private, err := tx.GetBlockWithMetadata(t.Context(), "world", 0, 0, "id")
		if err != nil {
			return err
		}
		if private.Version != nil || private.SchemaVersion != 1 || private.Record["id"] != uint64(9) {
			t.Fatal(private)
		}
		area, err := tx.GetAreaWithSchema(t.Context(), "world", 0, 0, 1, 1)
		if err != nil {
			return err
		}
		if area.SchemaVersion != 1 || area.Chunks == nil || len(area.Chunks) != 0 {
			t.Fatal(area)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFoundationUnknownWidthKnownProjection(t *testing.T) {
	schema, err := parseSchema(readOne(t, describeReply("world", 1, []string{fakeColumn(1, "id", "u8", false, false, respInt(0)), fakeColumn(2, "future", "new_type", false, false, respBulk("raw")), fakeColumn(3, "flag", "bool", false, false, respBool(false))})))
	if err != nil {
		t.Fatal(err)
	}
	projected := schema.clone()
	projected.Columns = []Column{schema.Columns[0], schema.Columns[2]}
	chunk := NewChunk(projected)
	chunk.SetBlock(0, 0, Record{"id": 7, "flag": true})
	form, err := EncodeChunk(projected, chunk)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeChunk(schema, form, "id", "flag")
	if err != nil || decoded.Block(0, 0)["id"] != uint64(7) || decoded.Block(0, 0)["flag"] != true {
		t.Fatal(decoded, err)
	}
}
