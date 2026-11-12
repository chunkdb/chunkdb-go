// Run with CHUNKDB_URI=chunk://admin:password@127.0.0.1:4242/ go run ./examples/journal.
//
// The collector follows a durable slot and appends each change and schema description to a JSON Lines file.
// It syncs the file before it acknowledges the change, so an acknowledged change is always stored.
// On start it resumes after the last stored change and skips changes and schema versions the file already holds.
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

// header starts every journal line; kind is "schema" or "change".
type header struct {
	Kind          string `json:"kind"`
	Epoch         string `json:"epoch"`
	Revision      uint64 `json:"revision"`
	SchemaVersion uint64 `json:"schema_version"`
}

// schemaEntry holds the columns of every later change; blocks stored before it keep the earlier columns.
type schemaEntry struct {
	header
	Columns []column `json:"columns"`
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Null bool   `json:"null,omitempty"`
}

// changeEntry is one committed change.
type changeEntry struct {
	header
	TimeMs uint64  `json:"time_ms"`
	User   *string `json:"user"`
	Txn    *uint64 `json:"txn,omitempty"`
	Blocks []block `json:"blocks"`
}

// block holds the values before and after the change; null means the block is absent.
type block struct {
	X      chunkdb.Coordinate `json:"x"`
	Y      chunkdb.Coordinate `json:"y"`
	Z      chunkdb.Coordinate `json:"z,omitempty"`
	Before map[string]any     `json:"before"`
	After  map[string]any     `json:"after"`
}

// state is what the journal already holds.
type state struct {
	epoch  string            // epoch of the last line, or "" for an empty journal
	change *chunkdb.Position // last stored change, or nil
	schema uint64            // last stored schema version
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

	journal, held, err := openJournal(*path)
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
	opts := chunkdb.WatchOptions{Slot: *slot, Takeover: *takeover, After: held.change}
	if held.epoch == "" {
		// The slot holds only changes committed after it exists; earlier state is in a base snapshot.
		if err := client.CreateSlotWithOptions(setup, *table, *slot, chunkdb.SlotOptions{IfNotExists: true}); err != nil {
			return explain(err, *table, *slot)
		}
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
			if held.epoch != "" && position.Epoch != held.epoch {
				return fmt.Errorf("change in epoch %s after journal epoch %s: start a new journal from a base snapshot", position.Epoch, held.epoch)
			}
			if len(e.Tag) > 0 || len(e.ChunkChanges) > 0 {
				return fmt.Errorf("change %s:%d carries a tag or chunk changes, which this journal does not store", position.Epoch, position.Revision)
			}
			if held.change != nil && position.Revision <= held.change.Revision {
				// A crash before the ACK reached the server can deliver a stored change again.
				fmt.Printf("skipped %s:%d\n", position.Epoch, position.Revision)
			} else {
				if err := appendLine(journal, changeLine(e)); err != nil {
					return err
				}
				held.epoch, held.change = position.Epoch, &position
				fmt.Printf("stored %s:%d\n", position.Epoch, position.Revision)
			}
			if err := watch.Ack(ctx, position.Revision); err != nil {
				if ctx.Err() != nil {
					return shutdown(watch)
				}
				return explain(err, *table, *slot)
			}
		case *chunkdb.SchemaEvent:
			if held.epoch != "" && e.Position.Epoch != held.epoch {
				return fmt.Errorf("schema in epoch %s after journal epoch %s: start a new journal from a base snapshot", e.Position.Epoch, held.epoch)
			}
			// A watch describes the schema before its first change, so a resumed watch repeats a stored version.
			if held.epoch != "" && e.Version <= held.schema {
				fmt.Printf("skipped schema version %d\n", e.Version)
				continue
			}
			if err := appendLine(journal, schemaLine(e)); err != nil {
				return err
			}
			held.epoch, held.schema = e.Position.Epoch, e.Version
			fmt.Printf("stored schema version %d at %s:%d\n", e.Version, e.Position.Epoch, e.Position.Revision)
		case *chunkdb.ResyncEvent:
			return fmt.Errorf("resync at %s:%d: the slot no longer holds the changes after this journal; rebuild from a base snapshot and start a new journal", e.Position.Epoch, e.Position.Revision)
		case *chunkdb.ProgressEvent:
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
		return fmt.Errorf("slot %s on %s is lost, so this journal has a gap: drop and recreate the slot, then take a base snapshot and start a new journal: %w", slot, table, err)
	case chunkdb.CodeSlotBusy:
		return fmt.Errorf("slot %s on %s has another watcher: stop it, or run with -takeover to replace a watcher left by an earlier run: %w", slot, table, err)
	case chunkdb.CodeNoTable:
		return fmt.Errorf("table %s is missing or this user can no longer read it: %w", table, err)
	case chunkdb.CodeKilled:
		return fmt.Errorf("another watcher took over slot %s on %s: %w", slot, table, err)
	}
	return err
}

// openJournal opens the journal for appending and returns what it already holds.
// A last line without its newline never finished its sync, so it was never acknowledged; it is removed and delivered again.
func openJournal(path string) (*os.File, state, error) {
	var held state
	_, statErr := os.Stat(path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, held, err
	}
	if errors.Is(statErr, os.ErrNotExist) {
		if err := syncDir(filepath.Dir(path)); err != nil {
			file.Close()
			return nil, held, err
		}
	}
	var complete int64
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			file.Close()
			return nil, held, err
		}
		var stored header
		if err := json.Unmarshal(line, &stored); err != nil {
			file.Close()
			return nil, held, fmt.Errorf("%s: entry at byte %d: %w", path, complete, err)
		}
		complete += int64(len(line))
		held.epoch = stored.Epoch
		switch stored.Kind {
		case "schema":
			held.schema = stored.SchemaVersion
		case "change":
			held.change = &chunkdb.Position{Epoch: stored.Epoch, Revision: stored.Revision}
		default:
			file.Close()
			return nil, held, fmt.Errorf("%s: entry at byte %d has kind %q", path, complete-int64(len(line)), stored.Kind)
		}
	}
	info, err := file.Stat()
	if err == nil && info.Size() > complete {
		if err = file.Truncate(complete); err == nil {
			err = file.Sync()
		}
	}
	if err != nil {
		file.Close()
		return nil, held, err
	}
	return file, held, nil
}

func schemaLine(e *chunkdb.SchemaEvent) schemaEntry {
	stored := schemaEntry{header: header{Kind: "schema", Epoch: e.Position.Epoch, Revision: e.Position.Revision, SchemaVersion: e.Version}}
	for _, c := range e.Columns {
		stored.Columns = append(stored.Columns, column{Name: c.Name, Type: c.Type.String(), Null: c.Null})
	}
	return stored
}

func changeLine(e *chunkdb.ChangeEvent) changeEntry {
	stored := changeEntry{
		header: header{Kind: "change", Epoch: e.Position.Epoch, Revision: e.Position.Revision, SchemaVersion: e.SchemaVersion},
		TimeMs: e.CommitTimeMs, User: e.User, Txn: e.Txn,
	}
	for _, b := range e.Blocks {
		stored.Blocks = append(stored.Blocks, block{X: b.X, Y: b.Y, Z: b.Z, Before: values(b.Before), After: values(b.After)})
	}
	return stored
}

// appendLine writes the entry as one line and waits until the file is synced.
func appendLine(file *os.File, stored any) error {
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
