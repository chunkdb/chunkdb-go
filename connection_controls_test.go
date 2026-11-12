package chunkdb

import (
	"errors"
	"math"
	"net"
	"testing"
)

func connectionRow(id string) string {
	return respMap("id", id, "user", respNull, "peer", respBulk("127.0.0.1:42"), "state", respBulk("idle"), "statement", respNull, "future", respArray())
}

func TestConnectionControlsWire(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch command {
		case "SHOW CONNECTIONS":
			writeRaw(conn, respArray(connectionRow("(18446744073709551615\r\n")))
		case "CANCEL 18446744073709551615":
			writeRaw(conn, respMap("cancelled", respBool(false)))
		case "KILL CONNECTION 18446744073709551615":
			writeRaw(conn, respMap("killed", respBool(true)))
		case "CANCEL 1":
			writeRaw(conn, respMap("cancelled", respBool(true)))
		case "KILL CONNECTION 1":
			writeRaw(conn, respMap("killed", respBool(false)))
		default:
			t.Errorf("unexpected request %q", command)
			writeSimple(conn, "unexpected")
		}
	}))
	c := newTestClient(t, server, nil)
	rows, err := c.ShowConnections(t.Context())
	if err != nil || len(rows) != 1 || rows[0].ID != math.MaxUint64 || rows[0].User != nil || rows[0].Statement != nil || rows[0].State != "idle" {
		t.Fatal(rows, err)
	}
	if yes, err := c.Cancel(t.Context(), math.MaxUint64); err != nil || yes {
		t.Fatal(yes, err)
	}
	if yes, err := c.KillConnection(t.Context(), math.MaxUint64); err != nil || !yes {
		t.Fatal(yes, err)
	}
	if yes, err := c.Cancel(t.Context(), 1); err != nil || !yes {
		t.Fatal(yes, err)
	}
	if yes, err := c.KillConnection(t.Context(), 1); err != nil || yes {
		t.Fatal(yes, err)
	}
	if _, err := c.Cancel(t.Context(), 0); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	pool, err := ConnectPool(t.Context(), PoolOptions{Options: Options{URI: server.uri("")}, MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if rows, err := pool.ShowConnections(t.Context()); err != nil || len(rows) != 1 || rows[0].ID != math.MaxUint64 {
		t.Fatal(rows, err)
	}
	if yes, err := pool.Cancel(t.Context(), math.MaxUint64); err != nil || yes {
		t.Fatal(yes, err)
	}
	if yes, err := pool.KillConnection(t.Context(), math.MaxUint64); err != nil || !yes {
		t.Fatal(yes, err)
	}
}

func TestConnectionControlReplyValidation(t *testing.T) {
	valid := connectionRow(respInt(1))
	for _, wire := range []string{respMap(), respArray(respNull), respArray(connectionRow(respInt(0))), respArray(connectionRow("(18446744073709551616\r\n")), respArray(valid, valid), respArray(respMap("id", respInt(1), "user", respNull, "peer", respBulk("peer"), "state", respInt(1), "statement", respNull))} {
		reply := readOne(t, wire)
		if _, err := parseConnections(reply); !errors.Is(err, ErrProtocol) {
			t.Fatal(wire, err)
		}
	}
	for _, field := range []string{"cancelled", "killed"} {
		server := newFakeServer(t, respondWith(respMap(field, respInt(1))))
		c := newTestClient(t, server, nil)
		var err error
		if field == "cancelled" {
			_, err = c.Cancel(t.Context(), 1)
		} else {
			_, err = c.KillConnection(t.Context(), 1)
		}
		if !errors.Is(err, ErrProtocol) {
			t.Fatal(field, err)
		}
	}
}

func TestStatementTimeoutBounds(t *testing.T) {
	for _, value := range []any{uint64(0), uint64(86400000)} {
		server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, statement string) {
			expected := "HELLO 3 WITH statement_timeout_ms = 0"
			if value == uint64(86400000) {
				expected = "HELLO 3 WITH statement_timeout_ms = 86400000"
			}
			if statement != expected {
				t.Errorf("%s", statement)
			}
			writeRaw(conn, helloReply)
		})
		c := newTestClient(t, server, func(o *Options) { o.SessionOptions = map[string]any{"statement_timeout_ms": value} })
		_ = c.Close()
	}
	for _, value := range []any{uint64(86400001), uint64(math.MaxUint64), -1, 1.5, "0", nil} {
		if _, err := NewClient(Options{SessionOptions: map[string]any{"statement_timeout_ms": value}}); !errors.Is(err, ErrProtocol) {
			t.Fatal(value, err)
		}
	}
	for _, value := range []string{respInt(86400001), respBulk("0"), respBool(false)} {
		reply := readOne(t, respMap("options", respMap("statement_timeout_ms", value)))
		if err := parseHelloExtensions(reply, &ServerInfo{}); !errors.Is(err, ErrProtocol) {
			t.Fatal(value, err)
		}
	}
}
