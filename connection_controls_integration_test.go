package chunkdb

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestIntegrationConnectionControls(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plain"
		if tlsEnabled {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			server := startServer(t, serverConfig{workers: 4, tls: tlsEnabled})
			c := connectIntegration(t, server, func(o *Options) {
				o.TLSInsecure = tlsEnabled
				o.SessionOptions = map[string]any{"statement_timeout_ms": uint64(0)}
			})
			info := c.ServerInfo()
			if !info.Supports("connection_controls") || !info.Supports("statement_timeout") {
				t.Fatal("matching server lacks connection controls", info)
			}
			timeout, ok := info.Options["statement_timeout_ms"].Uint64()
			if !ok || timeout != 0 {
				t.Fatal(info.Options)
			}
			rows, err := c.ShowConnections(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.ID == info.ConnectionID {
					found = true
					if row.Peer == "" || row.State == "" {
						t.Fatal(row)
					}
				}
			}
			if !found {
				t.Fatal("calling connection missing", rows, info.ConnectionID)
			}
			if yes, err := c.Cancel(t.Context(), info.ConnectionID); err != nil || yes {
				t.Fatal(yes, err)
			}
			if yes, err := c.Cancel(t.Context(), math.MaxUint64); err != nil || yes {
				t.Fatal(yes, err)
			}
			if yes, err := c.KillConnection(t.Context(), info.ConnectionID); err != nil || !yes {
				t.Fatal(yes, err)
			}
		})
	}
}

// requireNotApplied checks a statement stopped by the server: a server error,
// not a client deadline, that reports applied=false and keeps the connection.
func requireNotApplied(t *testing.T, err error, code string) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.ServerCode != code || typed.ServerKeys["applied"] != "false" {
		t.Fatalf("want %s applied=false, got %v", code, err)
	}
	if !errors.Is(err, ErrServer) || errors.Is(err, ErrTimeout) || typed.RetryClass != RetryFatal {
		t.Fatalf("%s must be a fatal server error, got %v", code, err)
	}
}

// TestIntegrationStatementStops runs a large chunk read under a statement
// deadline and under CANCEL from a second connection.
func TestIntegrationStatementStops(t *testing.T) {
	server := startServer(t, serverConfig{workers: 4})
	c := connectIntegration(t, server, func(o *Options) { o.Table = "stops" })
	createTable(t, c, "stops", TableSpec{Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}, {Name: "s", Type: TypeText(64), Null: true}}, ChunkWidth: 64, ChunkHeight: 64})
	schema, err := c.Describe(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	full := NewChunk(schema)
	for i := range 64 {
		for j := range 64 {
			full.SetBlock(i, j, Record{"n": 1, "s": strings.Repeat("x", 64)})
		}
	}
	const side = 8
	for x := range int64(side) {
		for y := range int64(side) {
			if _, err := c.SetChunk(t.Context(), "", x, y, full); err != nil {
				t.Fatal(err)
			}
		}
	}
	if area, err := c.GetArea(t.Context(), "", 0, 0, side-1, side-1); err != nil || len(area) != side*side {
		t.Fatal(len(area), err)
	}

	deadline := connectIntegration(t, server, func(o *Options) {
		o.Table = "stops"
		o.SessionOptions = map[string]any{"statement_timeout_ms": uint64(1)}
	})
	if value, ok := deadline.ServerInfo().Options["statement_timeout_ms"].Uint64(); !ok || value != 1 {
		t.Fatal(deadline.ServerInfo().Options)
	}
	_, err = deadline.GetArea(t.Context(), "", 0, 0, side-1, side-1)
	requireNotApplied(t, err, CodeTimeout)
	before, err := c.GetChunk(t.Context(), "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sparse := NewChunk(schema)
	sparse.SetBlock(0, 0, Record{"n": 2})
	if version, err := deadline.SetChunk(t.Context(), "", 0, 0, sparse); err != nil {
		requireNotApplied(t, err, CodeTimeout)
		after, err := c.GetChunk(t.Context(), "", 0, 0)
		if err != nil || after.Version != before.Version {
			t.Fatal("a timed-out write changed the chunk", after, err)
		}
	} else if version <= before.Version {
		t.Fatal(version, before.Version)
	}
	if _, err := deadline.Do(t.Context(), "PING"); err != nil {
		t.Fatal(err)
	}

	reader := connectIntegration(t, server, func(o *Options) { o.Table = "stops" })
	id := reader.ServerInfo().ConnectionID
	// Cancel only once SHOW CONNECTIONS shows the read running: a CANCEL that
	// reaches the server first finds nothing to cancel.
	running := func() bool {
		rows, err := c.ShowConnections(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.ID == id {
				return row.Statement != nil
			}
		}
		t.Fatal("reader connection missing", rows)
		return false
	}
	accepted := false
	for attempt := 0; attempt < 20 && !accepted; attempt++ {
		done := make(chan error, 1)
		go func() {
			_, err := reader.GetArea(t.Context(), "", 0, 0, side-1, side-1)
			done <- err
		}()
		var readErr error
		for finished := false; !finished; {
			select {
			case readErr = <-done:
				finished = true
				continue
			default:
			}
			if !running() {
				continue
			}
			cancelled, err := c.Cancel(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			readErr, finished = <-done, true
			if cancelled {
				requireNotApplied(t, readErr, CodeCancelled)
				accepted = true
			}
		}
		if !accepted && readErr != nil {
			t.Fatal("a read without an accepted cancellation failed", readErr)
		}
	}
	if !accepted {
		t.Fatal("no cancellation of a running area read was accepted")
	}
	if _, err := reader.Do(t.Context(), "PING"); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationUserLimits logs in with channel binding and a statement
// deadline, then reaches both per-user limits.
func TestIntegrationUserLimits(t *testing.T) {
	server := startServer(t, serverConfig{workers: 4, tls: true})
	admin := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = true })
	const password = "limited-password"
	if err := admin.CreateUser(t.Context(), "bot", password, CreateUserOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Do(t.Context(), "ALTER USER bot LIMIT max_connections = 1, statements_per_sec = 1"); err != nil {
		t.Fatal(err)
	}
	bot := connectIntegration(t, server, func(o *Options) {
		o.URI = server.uriAs("bot", password)
		o.TLSInsecure = true
		o.ChannelBinding = ChannelBindingRequire
		o.SessionOptions = map[string]any{"statement_timeout_ms": uint64(5000)}
	})
	if value, ok := bot.ServerInfo().Options["statement_timeout_ms"].Uint64(); !ok || value != 5000 {
		t.Fatal(bot.ServerInfo().Options)
	}
	requireRateLimited := func(err error, reason string) {
		t.Helper()
		var typed *Error
		if !errors.As(err, &typed) || typed.ServerCode != CodeRateLimited || typed.Metadata.Reason != reason || typed.Metadata.RetryAfterMS == nil {
			t.Fatalf("want RATE_LIMITED reason=%s with retry_after_ms, got %v", reason, err)
		}
		if typed.RetryClass != RetryBackoff || errors.Is(err, ErrAuth) {
			t.Fatal(typed.RetryClass, err)
		}
	}
	second, err := Connect(t.Context(), Options{URI: server.uriAs("bot", password), TLSInsecure: true, ConnectTimeout: 5 * time.Second})
	if err == nil {
		_ = second.Close()
	}
	requireRateLimited(err, "max_connections")
	if _, err := bot.Do(t.Context(), "PING"); err != nil {
		t.Fatal(err)
	}
	_, err = bot.Do(t.Context(), "PING")
	requireRateLimited(err, "statements_per_sec")
	if cancelled, err := bot.Cancel(t.Context(), bot.ServerInfo().ConnectionID); err != nil || cancelled {
		t.Fatal("CANCEL must stay available at the statement limit", cancelled, err)
	}
}
