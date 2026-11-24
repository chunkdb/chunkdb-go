package chunkdb

import (
	"errors"
	"testing"
)

func TestClientSetUserLimits(t *testing.T) {
	server := newFakeServer(t, withHello(usersHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()
	connections, statements, off := uint64(4), uint64(200), uint64(0)
	for _, step := range []struct {
		limits UserLimits
		want   string
	}{
		{UserLimits{MaxConnections: &connections, StatementsPerSec: &statements}, "ALTER USER bot LIMIT max_connections = 4, statements_per_sec = 200"},
		{UserLimits{StatementsPerSec: &off}, "ALTER USER bot LIMIT statements_per_sec = 0"},
		{UserLimits{MaxConnections: &off}, "ALTER USER bot LIMIT max_connections = 0"},
	} {
		if err := client.SetUserLimits(ctx, "bot", step.limits); err != nil {
			t.Fatal(err)
		}
		if got := server.lastCommand(t); got != step.want {
			t.Fatalf("got %q, want %q", got, step.want)
		}
	}
	sent := len(server.commands())
	if err := client.SetUserLimits(ctx, "bot", UserLimits{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v for no limits", err)
	}
	if err := client.SetUserLimits(ctx, "Bad", UserLimits{MaxConnections: &connections}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v for a bad name", err)
	}
	if len(server.commands()) != sent {
		t.Fatal("a refused call sent a statement")
	}
}

func TestClientRotatePasswordUsesTheLoginSalt(t *testing.T) {
	login := &fakeLogin{user: "bot", password: "old-password"}
	server := newFakeServer(t, login.handler(usersHandler))
	client := newTestClient(t, server, func(o *Options) { o.URI = server.uri("bot:old-password") })
	if err := client.RotatePassword(t.Context(), "new-password"); err != nil {
		t.Fatal(err)
	}
	if got := server.lastCommand(t); got != "ALTER USER bot ADD VERIFIER $1" {
		t.Fatalf("got %q", got)
	}
	// The fake login sends this salt and 4096 iterations in its server-first.
	want, err := ComputeVerifierWithSalt("new-password", []byte("fake-salt-16byte"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(server.lastParams(t)[0]); got != want {
		t.Fatalf("got verifier %q, want %q", got, want)
	}
	if client.URI().Password != "new-password" {
		t.Fatal("the client keeps the old password")
	}
	if err := client.RotatePassword(t.Context(), ""); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v for an empty password", err)
	}

	anonymous := newTestClient(t, newFakeServer(t, withHello(usersHandler)), nil)
	if err := anonymous.RotatePassword(t.Context(), "x"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v for a client without a user", err)
	}
}
