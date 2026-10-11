package chunkdb

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"
)

type chunkReader interface {
	GetChunk(context.Context, string, int64, int64, ...string) (*Chunk, error)
	GetChunkRaw(context.Context, string, int64, int64, ...string) ([]byte, error)
}

func TestNeverWrittenChunkNull(t *testing.T) {
	for _, mode := range []string{"client", "pool", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			server := newFakeServer(t, withHello(func(s *fakeServer, c net.Conn, command string) {
				switch {
				case strings.HasPrefix(command, "GET CHUNK 8 8 "):
					writeRaw(c, respNull)
				case command == "BEGIN" || command == "ROLLBACK":
					writeSimple(c, "OK")
				case command == "COMMIT":
					writeRaw(c, respNull)
				default:
					genericHandler(s, c, command)
				}
			}))
			check := func(r chunkReader) error {
				chunk, err := r.GetChunk(t.Context(), "world", 8, 8)
				if err != nil || chunk != nil {
					t.Fatalf("never-written typed chunk: %+v, %v", chunk, err)
				}
				raw, err := r.GetChunkRaw(t.Context(), "world", 8, 8)
				if err != nil || raw != nil {
					t.Fatalf("never-written raw chunk: %v, %v", raw, err)
				}
				chunk, err = r.GetChunk(t.Context(), "world", 9, 9)
				if err != nil || chunk == nil || chunk.Version != 7 || slices.Contains(chunk.Present, true) {
					t.Fatalf("versioned empty chunk: %+v, %v", chunk, err)
				}
				raw, err = r.GetChunkRaw(t.Context(), "world", 9, 9)
				if err != nil || string(raw) != emptyWorldForm(worldFormBytes) {
					t.Fatalf("versioned empty raw chunk: %v", err)
				}
				return nil
			}
			if mode == "pool" {
				_ = check(newTestPool(t, server, nil))
				return
			}
			client := newTestClient(t, server, nil)
			if mode == "client" {
				_ = check(client)
				return
			}
			if _, err := client.Transaction(t.Context(), func(tx *Tx) error { return check(tx) }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
