package chunkdb

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func TestRawChunkWriteAdmission(t *testing.T) {
	for _, mode := range []string{"client", "pool", "transaction"} {
		for _, scenario := range []string{"unknown", "oversize", "known"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				columns := []Column{
					{ID: 1, Name: "id", Type: TypeUint(9), Null: true},
					{ID: 2, Name: "label", Type: TypeText(16), Null: true},
					{ID: 3, Name: "blob", Type: TypeBytes(16)},
				}
				schema := &Schema{Table: "world", Dims: 2, Version: 1, ChunkWidth: 2, ChunkHeight: 2,
					Columns: columns, Options: TableOptions{VarMaxChunkBytes: 64}}
				chunk := NewChunk(schema)
				chunk.SetBlock(0, 0, Record{"id": 7, "label": "river", "blob": []byte{0, '\r', '\n', 255}})
				form, err := EncodeChunk(schema, chunk)
				if err != nil {
					t.Fatal(err)
				}
				wireColumns := []string{
					fakeColumn(1, "id", "u9", true, false, respNull),
					fakeColumn(2, "label", "text(16)", true, false, respNull),
					fakeColumn(3, "blob", "bytes(16)", false, false, respNull),
				}
				if scenario == "unknown" {
					wireColumns[1] = fakeColumn(2, "future", "future_fixed(19)", false, false, respNull)
				}
				server := newFakeServer(t, withHello(func(s *fakeServer, cn net.Conn, command string) {
					switch commandOf(command) {
					case "DESCRIBE":
						description := describeReply("world", 1, wireColumns)
						description = strings.Replace(description, respBulk("var_max_chunk_bytes")+respInt(1<<20),
							respBulk("var_max_chunk_bytes")+respInt(64), 1)
						writeRaw(cn, description)
					case "GET CHUNK":
						writeRaw(cn, respBulk(string(form)))
					case "GET BLOCK":
						label := "river"
						if scenario == "unknown" {
							label = "opaque\x00bytes"
						}
						writeRaw(cn, respArray(respInt(7), respBulk(label), respBulk("")))
					case "SET CHUNK":
						if scenario != "known" {
							t.Errorf("%s: refused SET CHUNK and its parameter reached the server", scenario)
						}
						params := s.paramsOf(cn)
						if len(params) != 1 || !bytes.Equal(params[0], form) {
							t.Errorf("raw SET changed its bytes: %x", params)
						}
						if mode == "transaction" {
							writeRaw(cn, respNull)
						} else {
							writeRaw(cn, respInt(7))
						}
					case "BEGIN", "ROLLBACK":
						writeSimple(cn, "OK")
					case "COMMIT":
						writeRaw(cn, respInt(42))
					default:
						t.Errorf("unexpected request %s", command)
						writeServerError(cn, "INVALID_ARGUMENT reason=unexpected_request")
					}
				}))
				exercise := func(read func() ([]byte, error), write func([]byte) error, probe func() (Record, error)) error {
					raw, err := read()
					if err != nil || !bytes.Equal(raw, form) {
						t.Fatalf("raw GET retention: %x, %v", raw, err)
					}
					if scenario == "oversize" {
						// 16 header + 1 presence + ceil(4*9/8) + 1 null bitmap + 64 vars.
						raw = make([]byte, 88)
					}
					err = write(raw)
					if scenario == "known" {
						if err != nil {
							t.Fatalf("known variable-width raw write: %v", err)
						}
					} else {
						if !errors.Is(err, ErrProtocol) {
							t.Fatalf("local %s refusal: %v", scenario, err)
						}
						if scenario == "unknown" && (!strings.Contains(err.Error(), "future") || !strings.Contains(err.Error(), "future_fixed(19)")) {
							t.Fatalf("refusal must name column and retained type: %v", err)
						}
						if scenario == "oversize" && !strings.Contains(err.Error(), "at most 87 bytes") {
							t.Fatalf("wrong full-form bound: %v", err)
						}
					}
					// The completed row is a wire barrier after admission/refusal and
					// proves the same client/lease/transaction can still make progress.
					row, err := probe()
					if err != nil || row["id"] != uint64(7) {
						t.Fatalf("known neighbor/connection continuation: %v, %v", row, err)
					}
					if scenario == "unknown" && !bytes.Equal(row["future"].([]byte), []byte("opaque\x00bytes")) {
						t.Fatalf("unknown row bytes changed: %v", row)
					}
					return nil
				}
				switch mode {
				case "pool":
					pool := newTestPool(t, server, func(o *PoolOptions) { o.MaxConnections = 1 })
					err = exercise(func() ([]byte, error) { return pool.GetChunkRaw(ctx, "world", 0, 0) },
						func(raw []byte) error { _, err := pool.SetChunkRaw(ctx, "world", 0, 0, raw); return err },
						func() (Record, error) { return pool.GetBlock(ctx, "world", 0, 0) })
				case "transaction":
					client := newTestClient(t, server, nil)
					var version uint64
					version, err = client.Transaction(ctx, func(tx *Tx) error {
						return exercise(func() ([]byte, error) { return tx.GetChunkRaw(ctx, "world", 0, 0) },
							func(raw []byte) error { return tx.SetChunkRaw(ctx, "world", 0, 0, raw) },
							func() (Record, error) { return tx.GetBlock(ctx, "world", 0, 0) })
					})
					if err == nil && version != 42 {
						t.Fatalf("transaction continuation COMMIT: got %d", version)
					}
				default:
					client := newTestClient(t, server, nil)
					err = exercise(func() ([]byte, error) { return client.GetChunkRaw(ctx, "world", 0, 0) },
						func(raw []byte) error { _, err := client.SetChunkRaw(ctx, "world", 0, 0, raw); return err },
						func() (Record, error) { return client.GetBlock(ctx, "world", 0, 0) })
				}
				if err != nil {
					t.Fatal(err)
				}
				sets, describes := 0, 0
				for _, command := range server.commands() {
					switch commandOf(command) {
					case "SET CHUNK":
						sets++
					case "DESCRIBE":
						describes++
					}
				}
				wantSets := 0
				if scenario == "known" {
					wantSets = 1
				}
				if sets != wantSets || describes != 1 || server.acceptedConns() != 1 {
					t.Fatalf("got SET=%d DESCRIBE=%d connections=%d; want %d, 1, 1", sets, describes, server.acceptedConns(), wantSets)
				}
			})
		}
	}
}

func TestRawChunkFormLimit(t *testing.T) {
	schema := &Schema{Table: "world", Dims: 2, ChunkWidth: 3, ChunkHeight: 3,
		Columns: []Column{{Name: "id", Type: TypeUint(9), Null: true}, {Name: "solid", Type: TypeBool()},
			{Name: "label", Type: TypeText(16), Null: true}, {Name: "blob", Type: TypeBytes(16)}},
		Options: TableOptions{VarMaxChunkBytes: 64}}
	if limit, err := chunkFormLimit(schema); err != nil || limit != 97 {
		t.Fatalf("rounded sections and variable budget: %d, %v; want 97", limit, err)
	}
	for _, scenario := range []string{"dimensions", "geometry", "unknown", "fixed overflow", "budget overflow"} {
		t.Run(scenario, func(t *testing.T) {
			copy := schema.clone()
			switch scenario {
			case "dimensions":
				copy.Dims = 3
			case "geometry":
				copy.ChunkWidth = 0
			case "unknown":
				copy.Columns[0].Type = ColumnType{Kind: ColumnUnknown, RawType: "future9"}
			case "fixed overflow":
				copy.ChunkWidth, copy.ChunkHeight = 1<<30, 1<<30
				copy.Columns[0].Type = TypeUint(64)
			case "budget overflow":
				copy.Options.VarMaxChunkBytes = math.MaxUint64
			}
			if _, err := chunkFormLimit(copy); !errors.Is(err, ErrProtocol) {
				t.Fatalf("%s: want local refusal, got %v", scenario, err)
			}
		})
	}
}
