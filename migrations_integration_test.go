package chunkdb

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestIntegrationMigrationsConcurrentAndConflict(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "tls"}[secure], func(t *testing.T) {
			s := startServer(t, serverConfig{tls: secure, workers: 4})
			configure := func(o *Options) {
				if secure {
					o.CA = s.caPEM
				}
			}
			c := connectIntegration(t, s, configure)
			opts := Options{URI: s.uri, CommandTimeout: 10 * time.Second}
			configure(&opts)
			p, err := ConnectPool(t.Context(), PoolOptions{Options: opts, MinConnections: 1, MaxConnections: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			steps := []Migration{{"create_world", "CREATE TABLE world (v u8) CHUNK 2 x 2"}, {"add_label", "ALTER TABLE world ADD COLUMN label text(16) NULL"}}
			type outcome struct {
				results []MigrationResult
				err     error
			}
			out := make(chan outcome, 2)
			start := make(chan struct{})
			var ready sync.WaitGroup
			ready.Add(2)
			for _, migrate := range []func(context.Context, []Migration) ([]MigrationResult, error){c.Migrate, p.Migrate} {
				go func() {
					ready.Done()
					<-start
					results, err := migrate(t.Context(), steps)
					out <- outcome{results, err}
				}()
			}
			ready.Wait()
			close(start)
			applied := make([]int, len(steps))
			skipped := make([]int, len(steps))
			for range 2 {
				result := <-out
				if result.err != nil || len(result.results) != len(steps) {
					t.Fatal(result)
				}
				for i, step := range result.results {
					if step.Name != steps[i].Name {
						t.Fatal(result.results)
					}
					switch step.Status {
					case "applied":
						applied[i]++
					case "skipped":
						skipped[i]++
					default:
						t.Fatal(step)
					}
				}
			}
			if !reflect.DeepEqual(applied, []int{1, 1}) || !reflect.DeepEqual(skipped, []int{1, 1}) {
				t.Fatal(applied, skipped)
			}
			schema, err := c.Schema(t.Context(), "world")
			if err != nil || len(schema.Columns) != 2 || schema.Columns[1].Name != "label" {
				t.Fatal(schema, err)
			}
			records, err := c.Do(t.Context(), "SHOW MIGRATIONS")
			if err != nil || records.Kind != ReplyArray || len(records.Array) != 2 {
				t.Fatal(records, err)
			}
			for i, record := range records.Array {
				name, _ := record.Lookup("name")
				statement, _ := record.Lookup("statement")
				user, _ := record.Lookup("user")
				if name.Kind != ReplyBulk || string(name.Bulk) != steps[i].Name || statement.Kind != ReplyBulk || string(statement.Bulk) != steps[i].Statement || user.Kind != ReplyBulk || string(user.Bulk) != testAdmin {
					t.Fatal(record)
				}
			}
			conflicting := []Migration{steps[0], {"add_label", "ALTER  TABLE world ADD COLUMN label text(16) NULL"}, {"later", "CREATE TABLE later (v u8) CHUNK 2 x 2"}}
			got, err := p.Migrate(t.Context(), conflicting)
			var failed *MigrationError
			if !reflect.DeepEqual(got, []MigrationResult{{"create_world", "skipped"}}) || !errors.Is(err, ErrConflict) || !errors.As(err, &failed) || failed.Name != "add_label" || failed.Index != 1 {
				t.Fatal(got, err)
			}
			if _, err := c.Describe(t.Context(), "later"); !isServerCode(err, CodeNoTable) {
				t.Fatal("step after conflict was run", err)
			}
			if got, err := c.Migrate(t.Context(), steps); err != nil || !reflect.DeepEqual(got, []MigrationResult{{"create_world", "skipped"}, {"add_label", "skipped"}}) {
				t.Fatal(got, err)
			}
		})
	}
}
