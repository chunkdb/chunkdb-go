package chunkdb

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchProjectionFallbackAfterRename(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var describes atomic.Int32
	server := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
		switch verbOf(command) {
		case "DESCRIBE":
			version, name := int64(1), "old"
			if describes.Add(1) > 1 {
				version, name = 2, "renamed"
			}
			columns := []string{fakeColumn(1, name, "u8", false, false, respNull),
				fakeColumn(2, "ignored", "u8", false, false, respNull), fakeColumn(3, "text", "text(16)", false, false, respNull)}
			description := strings.Replace(describeReply("world", version, columns), "%6\r\n", "%10\r\n", 1) +
				respBulk("epoch") + respBulk(watchEpoch) + respBulk("revision") + respInt(10) +
				respBulk("dims") + respInt(2) + respBulk("chunk_columns") + respArray(fakeColumn(4, "owner", "u8", false, false, respNull))
			writeRaw(cn, description)
		case "WATCH":
			// The new schema is available only through DESCRIBE. Its column
			// name changed while the projection's stable IDs and order did not.
			writeRaw(cn, respMap("epoch", respBulk(watchEpoch), "revision", respInt(10))+
				push(respBulk("change"), respBulk(watchEpoch), respInt(11), respInt(123), respNull, respInt(2),
					respArray(respArray(respInt(0), respInt(0), respNull, respArray(respBulk("selected"), respInt(7)))),
					respNull, respNull, respArray(respArray(respInt(0), respInt(0), respNull, respArray(respInt(9))))))
		case "UNWATCH":
			writeSimple(cn, "OK")
		}
	}))
	c := newTestClient(t, server, nil)
	w, err := c.Watch(ctx, "world", WatchOptions{Columns: []string{"text", "old"}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	event, err := w.Next(ctx)
	change, ok := event.(*ChangeEvent)
	if err != nil || !ok || len(change.Blocks) != 1 || len(change.ChunkChanges) != 1 ||
		!reflect.DeepEqual(change.Blocks[0].After, Record{"text": "selected", "renamed": uint64(7)}) ||
		!reflect.DeepEqual(change.ChunkChanges[0].After, Record{"owner": uint64(9)}) || describes.Load() != 2 {
		t.Fatal("renamed projection fallback", event, err, describes.Load())
	}
}

func TestWatchProjectionEpochBinding(t *testing.T) {
	for _, mode := range []string{"start_race", "resync", "schema_then_rename", "legacy_schema_then_rename", "unprojected_schema_add"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			announced := strings.Contains(mode, "schema")
			projected := mode != "unprojected_schema_add"
			var describes atomic.Int32
			newEpoch := replacementWatchEpoch
			if !projected {
				newEpoch = watchEpoch
			}
			version := int64(1)
			if announced {
				version = 2
			}
			server := newFakeServer(t, withHello(func(_ *fakeServer, cn net.Conn, command string) {
				switch verbOf(command) {
				case "DESCRIBE":
					epoch, schemaVersion := watchEpoch, int64(1)
					columns := []string{fakeColumn(1, "id", "u8", false, false, respNull)}
					if describes.Add(1) > 1 {
						epoch, schemaVersion = newEpoch, version
						name := "id"
						if announced {
							name = "renamed"
						}
						columns = []string{fakeColumn(1, "unused", "u8", false, false, respNull), fakeColumn(2, name, "u8", false, false, respNull)}
						if !projected {
							columns = []string{fakeColumn(1, "id", "u8", false, false, respNull), fakeColumn(2, "added", "u8", false, false, respNull)}
						}
					}
					description := strings.Replace(describeReply("world", schemaVersion, columns), "%6\r\n", "%10\r\n", 1) +
						respBulk("epoch") + respBulk(epoch) + respBulk("revision") + respInt(10) +
						respBulk("dims") + respInt(2) + respBulk("chunk_columns") + respArray()
					writeRaw(cn, description)
				case "WATCH":
					startEpoch := newEpoch
					if mode == "resync" {
						startEpoch = watchEpoch
					}
					frames := respMap("epoch", respBulk(startEpoch), "revision", respInt(10))
					if mode == "resync" {
						frames += push(respBulk("resync"), respBulk(newEpoch), respInt(10), respNull, respNull)
					}
					if announced {
						id := int64(2)
						if !projected {
							id = 1
						}
						columns := respArray(fakeColumn(id, "id", "u8", false, false, respNull))
						if mode == "legacy_schema_then_rename" {
							frames += push(respBulk("schema"), respBulk(newEpoch), respInt(10), respInt(1), columns)
						} else {
							frames += push(respBulk("schema"), respMap("epoch", respBulk(newEpoch), "revision", respInt(10), "schema_version", respInt(1), "dims", respInt(2), "columns", columns, "chunk_columns", respArray()))
						}
					}
					row := respArray(respInt(7))
					if !projected {
						row = respArray(respInt(7), respInt(9))
					}
					frames += push(respBulk("change"), respBulk(newEpoch), respInt(11), respInt(123), respNull, respInt(version),
						respArray(respArray(respInt(0), respInt(0), respNull, row)), respNull, respNull, respArray())
					writeRaw(cn, frames)
				case "UNWATCH":
					writeSimple(cn, "OK")
				}
			}))
			c := newTestClient(t, server, nil)
			var names []string
			if projected {
				names = []string{"id"}
			}
			w, err := c.Watch(ctx, "world", WatchOptions{Columns: names})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if projected {
				names[0] = "unused"
			}
			if mode == "resync" || announced {
				event, err := w.Next(ctx)
				if err != nil || event.GetPosition().Epoch != newEpoch {
					t.Fatal("new epoch announcement", event, err)
				}
				if announced {
					schema, ok := event.(*SchemaEvent)
					if !ok || len(schema.Columns) != 1 {
						t.Fatal("projected schema", event)
					}
					schema.Columns[0].ID = 99
					schema.Columns[0].Name = "caller mutation"
				} else if _, ok := event.(*ResyncEvent); !ok {
					t.Fatal("resync", event)
				}
			}
			event, err := w.Next(ctx)
			change, ok := event.(*ChangeEvent)
			want := Record{"id": uint64(7)}
			if announced && projected {
				want = Record{"renamed": uint64(7)}
			} else if !projected {
				want["added"] = uint64(9)
			}
			if err != nil || !ok || len(change.Blocks) != 1 || !reflect.DeepEqual(change.Blocks[0].After, want) || describes.Load() != 2 {
				t.Fatal("epoch-owned projection", event, err, describes.Load())
			}
		})
	}
}
