package chunkdb

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func areaPageHello(maxBytes int64) string {
	return strings.Replace(strings.Replace(helloReply, "%8\r\n", "%9\r\n", 1), respInt(64<<20), respInt(maxBytes), 1) + respBulk("features") + respArray(respBulk("area_paging"))
}

func areaPageWire(rows []string, more bool, after []int64) string {
	cursor := respNull
	if after != nil {
		axes := make([]string, len(after))
		for i, value := range after {
			axes[i] = respInt(value)
		}
		cursor = respArray(axes...)
	}
	return respMap("chunks", respArray(rows...), "more", respBool(more), "after", cursor)
}

func TestAreaPageCursorValidation(t *testing.T) {
	request := areaPage2D(math.MinInt64, 0, math.MinInt64+2, 0, nil, AreaPageOptions{Limit: 2})
	good := areaPageWire(nil, true, []int64{math.MinInt64 + 1, 0})
	_, more, after, err := validateAreaPage(readOne(t, good), request, 2)
	if err != nil || !more || after == nil || *after != ([3]int64{math.MinInt64 + 1, 0}) {
		t.Fatal(more, after, err)
	}
	for _, wire := range []string{
		areaPageWire(nil, true, nil), areaPageWire(nil, false, nil), areaPageWire(nil, false, []int64{math.MinInt64 + 1, 0}),
		areaPageWire(nil, true, []int64{math.MinInt64 + 2, 0}), areaPageWire(nil, true, []int64{math.MinInt64, 0, 0}),
		areaPageWire([]string{respArray(respInt(math.MinInt64+2), respInt(0), respBulk(""))}, true, []int64{math.MinInt64 + 1, 0}),
	} {
		if _, _, _, err := validateAreaPage(readOne(t, wire), request, 2); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid page accepted", wire, err)
		}
	}
	request.after = &[3]int64{math.MinInt64 + 2, 0}
	if _, _, after, err := validateAreaPage(readOne(t, areaPageWire(nil, false, nil)), request, 2); err != nil || after != nil {
		t.Fatal(after, err)
	}
	radius := int64(1)
	ball := areaPage3D(0, 0, 0, 0, 0, 0, &radius, AreaPageOptions3D{Limit: 1})
	if ball.contains([3]int64{1, 0, 1}) || ball.contains([3]int64{0, 1, 1}) || !ball.contains([3]int64{0, 0, 1}) {
		t.Fatal("ball ignored an active axis")
	}
	if _, _, _, err := validateAreaPage(readOne(t, areaPageWire(nil, true, []int64{1, 0, 1})), ball, 2); err == nil {
		t.Fatal("outside cursor accepted")
	}
	radius = math.MaxInt64
	wide := areaPage3D(math.MinInt64, 0, 0, math.MinInt64, 0, 0, &radius, AreaPageOptions3D{Limit: 1})
	if _, _, after, err := validateAreaPage(readOne(t, areaPageWire(nil, true, []int64{math.MinInt64, -math.MaxInt64, 0})), wide, 2); err != nil || after == nil {
		t.Fatal(after, err)
	}
}

func TestAreaPagesClientPoolAndTransaction(t *testing.T) {
	for _, dims := range []int{2, 3} {
		t.Run(fmt.Sprint(dims), func(t *testing.T) {
			description := modelDescription(dims, false)
			schema, err := parseSchema(readOne(t, description))
			if err != nil {
				t.Fatal(err)
			}
			chunk := NewChunk(schema)
			chunk.Version = 42
			chunk.CommitTimeMS = 11
			chunk.Metadata = Record{"title": "seed"}
			if dims == 2 {
				chunk.SetBlock(0, 0, Record{"n": 9})
			} else {
				chunk.SetBlock3D(0, 0, 0, Record{"n": 9})
			}
			form, err := encodeSectionedChunk(schema, chunk, "n")
			if err != nil {
				t.Fatal(err)
			}
			point := func(x int64) []int64 {
				if dims == 3 {
					return []int64{x, 0, 0}
				}
				return []int64{x, 0}
			}
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
				switch commandOf(line) {
				case "HELLO":
					writeRaw(conn, areaPageHello(64<<20))
				case "DESCRIBE":
					writeRaw(conn, description)
				case "GET AREA":
					last, more := int64(1), true
					row := int64(0)
					if strings.Contains(line, " AFTER ") {
						last, more, row = 3, false, 3
					}
					fields := make([]string, 0, dims+1)
					for _, value := range point(row) {
						fields = append(fields, respInt(value))
					}
					fields = append(fields, respBulk(string(form)))
					cursor := point(last)
					if !more {
						cursor = nil
					}
					writeRaw(conn, respArray(respInt(7), areaPageWire([]string{respArray(fields...)}, more, cursor)))
				case "COMMIT":
					writeRaw(conn, respNull)
				default:
					writeSimple(conn, "OK")
				}
			})
			client := newTestClient(t, server, nil)
			pool, err := ConnectPool(t.Context(), PoolOptions{Options: Options{URI: server.uri("")}, MaxConnections: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			if dims == 2 {
				page, err := client.GetAreaPage(t.Context(), "space", 0, 0, 3, 0, AreaPageOptions{Limit: 2})
				if err != nil || !page.More || page.After == nil || *page.After != (ChunkCoord{1, 0}) || len(page.Chunks) != 1 || page.Chunks[0].Chunk.Metadata["title"] != "seed" {
					t.Fatal(page, err)
				}
				last, err := client.GetAreaPage(t.Context(), "space", 0, 0, 3, 0, AreaPageOptions{Limit: 2, After: page.After}, "n")
				if err != nil || last.More || last.After != nil {
					t.Fatal(last, err)
				}
				if got := server.lastCommand(t); got != "GET AREA 0 0 TO 3 0 FROM space COLUMNS n LIMIT 2 AFTER 1 0 WITH SCHEMA" {
					t.Fatal(got)
				}
				if _, err := pool.GetAreaPage(t.Context(), "space", 0, 0, 3, 0, AreaPageOptions{Limit: 2}); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Transaction(t.Context(), func(tx *Tx) error {
					_, err := tx.GetAreaPage(t.Context(), "space", 0, 0, 3, 0, AreaPageOptions{Limit: 2})
					return err
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				page, err := client.GetAreaPage3D(t.Context(), "space", 0, 0, 0, 3, 0, 0, AreaPageOptions3D{Limit: 2})
				if err != nil || page.After == nil || *page.After != (ChunkCoord3D{1, 0, 0}) || page.Chunks[0].CZ != 0 {
					t.Fatal(page, err)
				}
				last, err := client.GetAreaPage3D(t.Context(), "space", 0, 0, 0, 3, 0, 0, AreaPageOptions3D{Limit: 2, After: page.After}, "n")
				if err != nil || last.More || last.After != nil {
					t.Fatal(last, err)
				}
				if got := server.lastCommand(t); got != "GET AREA 0 0 0 TO 3 0 0 FROM space COLUMNS n LIMIT 2 AFTER 1 0 0 WITH SCHEMA" {
					t.Fatal(got)
				}
				if _, err := pool.GetAreaPage3D(t.Context(), "space", 0, 0, 0, 3, 0, 0, AreaPageOptions3D{Limit: 2}); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Transaction(t.Context(), func(tx *Tx) error {
					_, err := tx.GetAreaPage3D(t.Context(), "space", 0, 0, 0, 3, 0, 0, AreaPageOptions3D{Limit: 2})
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := server.commands()
			if _, err := client.GetAreaAroundPage(t.Context(), "space", 0, 0, -1, AreaPageOptions{}); err == nil {
				t.Fatal("negative radius accepted")
			}
			if _, err := client.GetAreaPage(t.Context(), "space", 0, 0, 1, 0, AreaPageOptions{Limit: 257}); err == nil {
				t.Fatal("over-limit page accepted")
			}
			if !reflect.DeepEqual(before, server.commands()) {
				t.Fatal("invalid request was sent")
			}
		})
	}
}

func TestAreaPageMissingFeatureAndReadBudget(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, _ string) { writeSimple(conn, "PONG") }))
	client := newTestClient(t, server, nil)
	if _, err := client.GetAreaPage(t.Context(), "space", 0, 0, 0, 0, AreaPageOptions{}); err == nil || !strings.Contains(err.Error(), "area_paging") {
		t.Fatal(err)
	}
	if got := server.commands(); !reflect.DeepEqual(got, []string{"HELLO 3"}) {
		t.Fatal(got)
	}
	limited := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
		switch commandOf(line) {
		case "HELLO":
			writeRaw(conn, areaPageHello(80))
		case "DESCRIBE":
			writeRaw(conn, modelDescription(2, false))
		case "GET AREA":
			writeRaw(conn, "*1\r\n*3\r\n:0\r\n:0\r\n$33554432\r\n")
		default:
			writeSimple(conn, "PONG")
		}
	})
	c := newTestClient(t, limited, nil)
	if _, err := c.GetArea(t.Context(), "space", 0, 0, 0, 0); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "negotiated") {
		t.Fatal("did not refuse before body", err)
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if limited.acceptedConns() != 2 {
		t.Fatal("poisoned stream was reused", limited.acceptedConns())
	}
}

func TestRawAreaCardinalityAdmission(t *testing.T) {
	for _, test := range []struct{ statement, header string }{
		{"GET AREA 0 0 TO 0 0 FROM after COLUMNS limit", "*2\r\n"},
		{"GET AREA-1-2 TO 0 0 FROM after COLUMNS limit", "*2\r\n"},
		{"GET AREA-1-2 TO 0 0 FROM t LIMIT 1", "%3\r\n+chunks\r\n*2\r\n"},
		{"GET AREA 0 0 TO 0 0 FROM t COLUMNS n, after WITH SCHEMA", "*2\r\n:1\r\n*2\r\n"},
		{"GET AREA 0 0 TO 0 0 FROM t LIMIT 1", "%3\r\n+chunks\r\n*2\r\n"},
		{"GET AREA 0 0 TO 0 0 FROM t COLUMNS \"limit\",\"after\" AFTER 0 0 WITH SCHEMA", "*2\r\n:1\r\n%3\r\n+chunks\r\n*2\r\n"},
		{"GET AREA -1 0TO 1 0FROM\"after\"COLUMNS\"limit\",after LIMIT 1AFTER-1 0WITH SCHEMA", "*2\r\n:1\r\n%3\r\n+chunks\r\n*2\r\n"},
		{"GET AREA-1-2 TO 0 0FROM after COLUMNS limit LIMIT 1AFTER-1-2 WITH SCHEMA", "*2\r\n:1\r\n%3\r\n+chunks\r\n*2\r\n"},
	} {
		t.Run(test.statement, func(t *testing.T) {
			server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
				if commandOf(line) == "HELLO" {
					writeRaw(conn, strings.Replace(areaPageHello(64<<20), respBulk("max_area_chunks")+respInt(256), respBulk("max_area_chunks")+respInt(1), 1))
					return
				}
				if line == "BEGIN" {
					writeSimple(conn, "OK")
					return
				}
				writeRaw(conn, test.header)
			})
			client := newTestClient(t, server, nil)
			if _, err := client.Do(t.Context(), test.statement); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "cardinality") {
				t.Fatal("raw area waited for oversized array", err)
			}
			before := server.acceptedConns()
			_, err := client.Transaction(t.Context(), func(tx *Tx) error {
				tx.mu.Lock()
				defer tx.mu.Unlock()
				_, err := tx.call(t.Context(), test.statement, nil)
				return err
			})
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "cardinality") {
				t.Fatal("transaction raw reader bypassed cardinality", err)
			}
			if server.acceptedConns() != before+1 {
				t.Fatal("transaction reconnected after budget failure", server.acceptedConns(), before)
			}
		})
	}
}

func TestAreaAggregateDecodedCells(t *testing.T) {
	columns := make([]string, 1024)
	for i := range columns {
		columns[i] = fakeColumn(int64(i+1), fmt.Sprintf("v%d", i), "text(1)", true, false, respNull)
	}
	description := strings.Replace(describeReply("space", 1, columns), respArray(respInt(2), respInt(2)), respArray(respInt(1024), respInt(1)), 1)
	form := sectionedForm{header: chunkFormHeader{schema: 1, version: 1}, sections: []chunkFormSection{{sectionPresence, make([]byte, 128)}, {sectionBlockSpans, make([]byte, 4)}}}
	full, err := serializeSectionedForm(form, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	form.header.flags = chunkFormProjected
	projected, err := serializeSectionedForm(form, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
		switch commandOf(line) {
		case "HELLO":
			writeRaw(conn, areaPageHello(64<<20))
		case "DESCRIBE":
			writeRaw(conn, description)
		case "BEGIN", "ROLLBACK":
			writeSimple(conn, "OK")
		case "COMMIT":
			writeRaw(conn, respNull)
		case "PING":
			writeSimple(conn, "PONG")
		default:
			data := full
			if strings.Contains(line, "COLUMNS v0 LIMIT") || strings.HasSuffix(line, "COLUMNS v0") || strings.Contains(line, "COLUMNS v0 WITH") {
				data = projected
			}
			rows := make([]string, 5)
			for i := range rows {
				rows[i] = respArray(respInt(int64(i)), respInt(0), respBulk(string(data)))
			}
			reply := respArray(rows...)
			if strings.Contains(line, " LIMIT ") {
				reply = areaPageWire(rows, false, nil)
			}
			if strings.Contains(line, " WITH SCHEMA") {
				reply = respArray(respInt(1), reply)
			}
			writeRaw(conn, reply)
		}
	})
	client := newTestClient(t, server, nil)
	if _, err := client.GetArea(t.Context(), "space", 0, 0, 4, 0); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "decoded area") {
		t.Fatal("unpaged area was not admitted as a whole", err)
	}
	if _, err := client.GetAreaWithSchema(t.Context(), "space", 0, 0, 4, 0); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "decoded area") {
		t.Fatal("metadata area was not admitted as a whole", err)
	}
	if _, err := client.GetAreaPage(t.Context(), "space", 0, 0, 4, 0, AreaPageOptions{Limit: 5}); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "decoded area") {
		t.Fatal("page was not admitted as a whole", err)
	}
	page, err := client.GetAreaPage(t.Context(), "space", 0, 0, 4, 0, AreaPageOptions{Limit: 5}, "v0")
	if err != nil || len(page.Chunks) != 5 || len(page.Chunks[0].Chunk.Columns) != 1 {
		t.Fatal(page, err)
	}
	if _, err := client.Do(t.Context(), "GET AREA 0 0 TO 4 0 FROM space LIMIT 5 WITH SCHEMA"); err != nil {
		t.Fatal("raw wire-bounded page", err)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Transaction(t.Context(), func(tx *Tx) error {
		if _, err := tx.GetAreaPage(t.Context(), "space", 0, 0, 4, 0, AreaPageOptions{Limit: 5}); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "decoded area") {
			return fmt.Errorf("missing local limit: %w", err)
		}
		_, err := tx.GetAreaPage(t.Context(), "space", 0, 0, 4, 0, AreaPageOptions{Limit: 5}, "v0")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if server.acceptedConns() != 1 {
		t.Fatal("local decode admission closed its synchronized connection", server.acceptedConns())
	}
}

func TestAreaSemanticFailureClosesConnection(t *testing.T) {
	for _, mode := range []string{"client", "pool", "transaction"} {
		for _, defect := range []string{"cursor", "envelope", "form"} {
			t.Run(mode+"/"+defect, func(t *testing.T) {
				closed := make(chan error, 1)
				var staged, applied atomic.Int64
				server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
					switch commandOf(line) {
					case "HELLO":
						writeRaw(conn, areaPageHello(64<<20))
					case "DESCRIBE":
						writeRaw(conn, modelDescription(2, false))
					case "BEGIN", "ROLLBACK":
						writeSimple(conn, "OK")
					case "SET BLOCK":
						staged.Add(1)
						writeRaw(conn, respNull)
					case "COMMIT":
						applied.Add(staged.Load())
						writeRaw(conn, respNull)
					case "GET BLOCK":
						writeRaw(conn, respArray(respInt(1)))
					case "GET AREA":
						reply := respArray(respInt(7), areaPageWire(nil, true, nil))
						if defect == "envelope" {
							reply = respArray(respInt(7))
						}
						if defect == "form" {
							reply = respArray(respInt(7), areaPageWire([]string{respArray(respInt(0), respInt(0), respBulk("invalid form"))}, false, nil))
						}
						writeRaw(conn, reply)
						_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
						var one [1]byte
						_, err := conn.Read(one[:])
						if errors.Is(err, io.EOF) {
							err = nil
						}
						closed <- err
					default:
						writeSimple(conn, "PONG")
					}
				})
				client := newTestClient(t, server, nil)
				var err error
				switch mode {
				case "pool":
					pool, poolErr := ConnectPool(t.Context(), PoolOptions{Options: Options{URI: server.uri("")}, MaxConnections: 1})
					if poolErr != nil {
						t.Fatal(poolErr)
					}
					defer pool.Close()
					_, err = pool.GetAreaPage(t.Context(), "space", 0, 0, 1, 0, AreaPageOptions{Limit: 2})
					if !errors.Is(err, ErrProtocol) {
						t.Fatal("semantic page was accepted", err)
					}
					if peerErr := <-closed; peerErr != nil {
						t.Fatal("malformed pooled socket was reused", peerErr)
					}
					before := server.acceptedConns()
					if err := pool.Ping(t.Context()); err != nil {
						t.Fatal(err)
					}
					if server.acceptedConns() != before+1 {
						t.Fatal("pool reused malformed socket")
					}
					return
				case "transaction":
					_, err = client.Transaction(t.Context(), func(tx *Tx) error {
						if err := tx.SetBlock(t.Context(), "space", 0, 0, Record{"n": 1}); err != nil {
							return err
						}
						if _, err := tx.GetAreaPage(t.Context(), "space", 0, 0, 1, 0, AreaPageOptions{Limit: 2}); !errors.Is(err, ErrProtocol) {
							return fmt.Errorf("semantic failure missing: %w", err)
						}
						if _, err := tx.GetBlock(t.Context(), "space", 0, 0); !errors.Is(err, ErrProtocol) {
							return fmt.Errorf("transaction continued after malformed response: %w", err)
						}
						return nil
					})
					if staged.Load() != 1 || applied.Load() != 0 {
						t.Fatal("malformed read allowed commit", staged.Load(), applied.Load())
					}
					for _, line := range server.commands() {
						if line == "COMMIT" || strings.HasPrefix(line, "GET BLOCK") {
							t.Fatal("transaction reused malformed socket", line)
						}
					}
				default:
					_, err = client.GetAreaPage(t.Context(), "space", 0, 0, 1, 0, AreaPageOptions{Limit: 2})
				}
				if !errors.Is(err, ErrProtocol) {
					t.Fatal("semantic page was accepted", err)
				}
				client.mu.Lock()
				active := client.active
				client.mu.Unlock()
				if active != nil {
					t.Fatal("malformed response connection remained published")
				}
				if peerErr := <-closed; peerErr != nil {
					t.Fatal("malformed socket was reused", peerErr)
				}
				before := server.acceptedConns()
				if err := client.Ping(t.Context()); err != nil {
					t.Fatal(err)
				}
				if server.acceptedConns() != before+1 {
					t.Fatal("client reused malformed socket")
				}
			})
		}
	}
}

func TestAreaSchemaRefreshKeepsConnection(t *testing.T) {
	var revision atomic.Int64
	revision.Store(7)
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, line string) {
		switch commandOf(line) {
		case "HELLO":
			writeRaw(conn, areaPageHello(64<<20))
		case "DESCRIBE":
			writeRaw(conn, strings.Replace(modelDescription(2, false), respBulk("version")+respInt(7), respBulk("version")+respInt(revision.Load()), 1))
		case "GET AREA":
			revision.Store(8)
			writeRaw(conn, respArray(respInt(8), areaPageWire(nil, false, nil)))
		default:
			writeSimple(conn, "PONG")
		}
	})
	client := newTestClient(t, server, nil)
	page, err := client.GetAreaPage(t.Context(), "space", 0, 0, 0, 0, AreaPageOptions{})
	if err != nil || page.SchemaVersion != 8 || server.acceptedConns() != 1 {
		t.Fatal(page, err, server.acceptedConns())
	}
	describes := 0
	for _, line := range server.commands() {
		if strings.HasPrefix(line, "DESCRIBE") {
			describes++
		}
	}
	if describes != 2 {
		t.Fatal("schema refresh did not run", describes)
	}
}

func TestAreaPageCompletedNullCursor(t *testing.T) {
	for _, dims := range []int{2, 3} {
		for _, around := range []bool{false, true} {
			t.Run(fmt.Sprintf("dims=%d/around=%v", dims, around), func(t *testing.T) {
				request := areaPageRequest{dims: dims, first: [3]int64{}, last: [3]int64{1, 0, 0}, limit: 2}
				if around {
					radius := uint64(1)
					request.radius = &radius
					request.limit = uint64(2*dims + 1)
				}
				point := []int64{0, 0}
				last := []int64{1, 0}
				if dims == 3 {
					point = append(point, 0)
					last = append(last, 0)
				}
				fields := make([]string, 0, dims+1)
				for _, value := range point {
					fields = append(fields, respInt(value))
				}
				fields = append(fields, respBulk(""))
				row := respArray(fields...)
				for _, rows := range [][]string{nil, {row}} {
					if _, more, after, err := validateAreaPage(readOne(t, areaPageWire(rows, false, nil)), request, request.limit); err != nil || more || after != nil {
						t.Fatal("valid completed page", more, after, err)
					}
				}
				if _, _, _, err := validateAreaPage(readOne(t, areaPageWire([]string{row}, false, last)), request, request.limit); err == nil {
					t.Fatal("completed nonnull cursor accepted")
				}
				if _, _, _, err := validateAreaPage(readOne(t, areaPageWire([]string{row}, true, last)), request, request.limit); err == nil {
					t.Fatal("unfinished endpoint accepted")
				}
			})
		}
	}
}
