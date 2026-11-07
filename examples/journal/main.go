// Run with CHUNKDB_URI=chunk://admin:password@127.0.0.1:4242/ go run ./examples/journal.
//
// The collector follows a durable slot and appends each change to a JSON Lines file.
// It syncs the file before it acknowledges the change, so an acknowledged change is always stored.
// On start it resumes after the last stored position and skips changes the file already holds.
// Interrupt it to stop; the slot keeps its acknowledged position.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	chunkdb "github.com/chunkdb/chunkdb-go/v2"
)

// entry is one committed change as stored in the journal, one JSON object per line.
type entry struct {
	Epoch    string  `json:"epoch"`
	Revision uint64  `json:"revision"`
	TimeMs   uint64  `json:"time_ms"`
	User     *string `json:"user"`
	Txn      *uint64 `json:"txn,omitempty"`
	Blocks   []block `json:"blocks"`
}

// block holds the values before and after the change; null means the block is absent.
type block struct {
	X      chunkdb.Coordinate `json:"x"`
	Y      chunkdb.Coordinate `json:"y"`
	Z      chunkdb.Coordinate `json:"z,omitempty"`
	Before map[string]any     `json:"before"`
	After  map[string]any     `json:"after"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "journal:", err)
		os.Exit(1)
	}
}

func run() error {
	table := flag.String("table", "world_go", "table to follow")
	slot := flag.String("slot", "journal", "durable slot to follow; created when the journal file is empty")
	path := flag.String("file", "journal.jsonl", "append-only journal file")
	takeover := flag.Bool("takeover", false, "replace a watcher of the slot left by an earlier run")
	flag.Parse()
	uri := os.Getenv("CHUNKDB_URI")
	if uri == "" {
		return fmt.Errorf("set CHUNKDB_URI to your server URI, for example chunk://admin:password@127.0.0.1:4242/")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	journal, last, err := openJournal(*path)
	if err != nil {
		return err
	}
	defer journal.Close()

	setup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := chunkdb.ConnectURI(setup, uri)
	if err != nil {
		return err
	}
	defer client.Close()
	opts := chunkdb.WatchOptions{Slot: *slot, Takeover: *takeover}
	if last == nil {
		// The slot holds only changes committed after it exists; earlier state is in a base snapshot.
		if err := client.CreateSlotWithOptions(setup, *table, *slot, chunkdb.SlotOptions{IfNotExists: true}); err != nil {
			return explain(err, *table, *slot)
		}
	} else {
		opts.After = last
	}
	watch, err := client.Watch(setup, *table, opts)
	if err != nil {
		return explain(err, *table, *slot)
	}
	defer watch.Close()
	fmt.Printf("watching %s slot %s from %s:%d\n", *table, *slot, watch.Start().Epoch, watch.Start().Revision)

	for {
		event, err := watch.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return shutdown(watch)
			}
			return explain(err, *table, *slot)
		}
		switch e := event.(type) {
		case *chunkdb.ChangeEvent:
			position := e.Position
			if last != nil && position.Epoch != last.Epoch {
				return fmt.Errorf("change in epoch %s after journal epoch %s: start a new journal from a base snapshot", position.Epoch, last.Epoch)
			}
			if last != nil && position.Revision <= last.Revision {
				// A crash before the ACK reached the server can deliver a stored change again.
				fmt.Printf("skipped %s:%d\n", position.Epoch, position.Revision)
			} else {
				if err := appendEntry(journal, e); err != nil {
					return err
				}
				last = &position
				fmt.Printf("stored %s:%d\n", position.Epoch, position.Revision)
			}
			if err := watch.Ack(ctx, position.Revision); err != nil {
				if ctx.Err() != nil {
					return shutdown(watch)
				}
				return explain(err, *table, *slot)
			}
		case *chunkdb.ResyncEvent:
			return fmt.Errorf("resync at %s:%d: the slot no longer holds the changes after this journal; rebuild from a base snapshot and start a new journal", e.Position.Epoch, e.Position.Revision)
		case *chunkdb.ProgressEvent, *chunkdb.SchemaEvent:
			// Nothing to store. A slot with AREA must also store and acknowledge progress.
		default:
			return fmt.Errorf("unexpected %T: this example stores block changes from a slot without CHUNKS", event)
		}
	}
}

// shutdown closes the watch, which persists the last accepted ACK.
func shutdown(watch *chunkdb.Watch) error {
	if err := watch.Close(); err != nil {
		return err
	}
	fmt.Println("stopped")
	return nil
}

// explain turns errors that end the slot watch into the action they need.
func explain(err error, table, slot string) error {
	var server *chunkdb.Error
	if !errors.As(err, &server) {
		return err
	}
	switch server.ServerCode {
	case chunkdb.CodeSlotLost:
		return fmt.Errorf("slot %s on %s is lost, so this journal has a gap: take a base snapshot, drop and recreate the slot, and start a new journal: %w", slot, table, err)
	case "SLOT_BUSY":
		return fmt.Errorf("slot %s on %s has another watcher: stop it, or run with -takeover to replace a watcher left by an earlier run: %w", slot, table, err)
	case chunkdb.CodeNoTable:
		return fmt.Errorf("table %s is missing or this user can no longer read it: %w", table, err)
	case "KILLED":
		return fmt.Errorf("another watcher took over slot %s on %s: %w", slot, table, err)
	}
	return err
}

// openJournal opens the journal for appending and returns its last stored position, or nil when it is empty.
// A last line without its newline never finished its sync, so it was never acknowledged; it is removed and delivered again.
func openJournal(path string) (*os.File, *chunkdb.Position, error) {
	_, statErr := os.Stat(path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if errors.Is(statErr, os.ErrNotExist) {
		if err := syncDir(filepath.Dir(path)); err != nil {
			file.Close()
			return nil, nil, err
		}
	}
	var last *chunkdb.Position
	var complete int64
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			file.Close()
			return nil, nil, err
		}
		var stored entry
		if err := json.Unmarshal(line, &stored); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("%s: entry at byte %d: %w", path, complete, err)
		}
		complete += int64(len(line))
		last = &chunkdb.Position{Epoch: stored.Epoch, Revision: stored.Revision}
	}
	info, err := file.Stat()
	if err == nil && info.Size() > complete {
		if err = file.Truncate(complete); err == nil {
			err = file.Sync()
		}
	}
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, last, nil
}

// appendEntry writes the change as one line and waits until the file is synced.
func appendEntry(file *os.File, change *chunkdb.ChangeEvent) error {
	stored := entry{
		Epoch: change.Position.Epoch, Revision: change.Position.Revision,
		TimeMs: change.CommitTimeMs, User: change.User, Txn: change.Txn,
	}
	for _, b := range change.Blocks {
		stored.Blocks = append(stored.Blocks, block{X: b.X, Y: b.Y, Z: b.Z, Before: values(b.Before), After: values(b.After)})
	}
	line, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

// values keeps a row JSON-encodable: bits become digit strings, and non-finite floats become strings.
func values(row chunkdb.Record) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for name, value := range row {
		switch v := value.(type) {
		case chunkdb.Bits:
			out[name] = v.String()
		case float32:
			out[name] = finite(float64(v), value)
		case float64:
			out[name] = finite(v, value)
		default:
			out[name] = value
		}
	}
	return out
}

func finite(v float64, value any) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprint(v)
	}
	return value
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
