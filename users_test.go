package chunkdb

import (
	"errors"
	"maps"
	"net"
	"slices"
	"strings"
	"testing"
)

func usersHandler(s *fakeServer, conn net.Conn, command string) {
	switch commandOf(command) {
	case "CREATE USER", "ALTER USER", "DROP USER", "GRANT", "REVOKE":
		writeSimple(conn, "OK")
	case "SHOW USERS":
		writeRaw(conn, respArray(
			respMap("name", respBulk("admin"), "manages_users", respBool(true),
				"grants", respMap("*", respBulk("ADMIN"))),
			respMap("name", respBulk("bot"), "manages_users", respBool(false),
				"grants", respMap("world", respBulk("WRITE"), "terrain", respBulk("READ"))),
		))
	default:
		genericHandler(s, conn, command)
	}
}

func TestClientUserStatements(t *testing.T) {
	server := newFakeServer(t, withHello(usersHandler))
	client := newTestClient(t, server, func(o *Options) { o.Table = "world"; o.VerifierIterations = 5000 })
	ctx := t.Context()

	verifierParam := func(t *testing.T) {
		t.Helper()
		params := server.lastParams(t)
		if len(params) != 1 {
			t.Fatalf("got %d parameters, want the verifier", len(params))
		}
		if iterations, salt, _, _ := parseVerifier(t, string(params[0])); iterations != 5000 || len(salt) != 16 {
			t.Fatalf("got %d iterations and a %d-byte salt", iterations, len(salt))
		}
		if strings.Contains(string(params[0]), "hunter2") {
			t.Fatal("the password was sent")
		}
	}

	steps := []struct {
		run  func() error
		want string
	}{
		{func() error { return client.CreateUser(ctx, "bot", "hunter2", CreateUserOptions{}) }, "CREATE USER bot VERIFIER $1"},
		{func() error { return client.CreateUser(ctx, "ops", "hunter2", CreateUserOptions{ManagesUsers: true}) },
			"CREATE USER ops VERIFIER $1 MANAGES USERS"},
		{func() error { return client.SetPassword(ctx, "bot", "hunter2") }, "ALTER USER bot VERIFIER $1"},
		{func() error { return client.SetManagesUsers(ctx, "bot", true) }, "ALTER USER bot MANAGES USERS"},
		{func() error { return client.SetManagesUsers(ctx, "bot", false) }, "ALTER USER bot NO MANAGES USERS"},
		{func() error { return client.Grant(ctx, RightRead, AllTables, "bot") }, "GRANT READ ON * TO bot"},
		{func() error { return client.Grant(ctx, RightWrite, "", "bot") }, "GRANT WRITE ON world TO bot"},
		{func() error { return client.Revoke(ctx, RightAdmin, "terrain", "bot") }, "REVOKE ADMIN ON terrain FROM bot"},
		{func() error { return client.DropUser(ctx, "bot") }, "DROP USER bot"},
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.want, err)
		}
		if got := server.lastCommand(t); got != step.want {
			t.Fatalf("got %q, want %q", got, step.want)
		}
		if strings.Contains(step.want, "VERIFIER") {
			verifierParam(t)
		} else if len(server.lastParams(t)) != 0 {
			t.Fatalf("%s: got parameters", step.want)
		}
	}

	users, err := client.Users(ctx)
	if err != nil {
		t.Fatalf("Users: %v", err)
	}
	want := []User{
		{Name: "admin", ManagesUsers: true, Grants: map[string]Right{"*": RightAdmin}},
		{Name: "bot", Grants: map[string]Right{"world": RightWrite, "terrain": RightRead}},
	}
	if !slices.EqualFunc(users, want, func(a, b User) bool {
		return a.Name == b.Name && a.ManagesUsers == b.ManagesUsers && maps.Equal(a.Grants, b.Grants)
	}) {
		t.Fatalf("got %+v, want %+v", users, want)
	}
}

func TestClientUserStatementsValidateBeforeSending(t *testing.T) {
	server := newFakeServer(t, withHello(usersHandler))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	for name, run := range map[string]func() error{
		"user name":       func() error { return client.CreateUser(ctx, "Bot", "pw", CreateUserOptions{}) },
		"password user":   func() error { return client.SetPassword(ctx, "a b", "pw") },
		"manages user":    func() error { return client.SetManagesUsers(ctx, "", true) },
		"drop user":       func() error { return client.DropUser(ctx, "bot;") },
		"right":           func() error { return client.Grant(ctx, Right("ALL"), "world", "bot") },
		"table":           func() error { return client.Grant(ctx, RightRead, "World", "bot") },
		"grantee":         func() error { return client.Revoke(ctx, RightRead, "world", "TO") },
		"lowercase right": func() error { return client.Revoke(ctx, Right("read"), "world", "bot") },
	} {
		if err := run(); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: got %v, want a request error", name, err)
		}
	}
	if got := server.commands(); len(got) != 1 {
		t.Fatalf("got %q, want HELLO alone", got)
	}
}

func TestClientPermissionDeniedIsTyped(t *testing.T) {
	server := newFakeServer(t, withHello(func(_ *fakeServer, conn net.Conn, command string) {
		switch commandOf(command) {
		case "SHOW USERS":
			writeServerError(conn, "ERR PERMISSION_DENIED MANAGES USERS")
		case "CREATE TABLE":
			writeServerError(conn, "ERR PERMISSION_DENIED ADMIN on *")
		default:
			writeServerError(conn, "ERR PERMISSION_DENIED WRITE on world")
		}
	}))
	client := newTestClient(t, server, nil)
	ctx := t.Context()

	cases := []struct {
		err     error
		right   string
		table   string
		command string
	}{
		{func() error { _, err := client.Do(ctx, "DELETE BLOCK 0 0 FROM world"); return err }(), "WRITE", "world", "DELETE BLOCK"},
		{func() error { _, err := client.Users(ctx); return err }(), "MANAGES USERS", "", "SHOW USERS"},
		{client.CreateTable(ctx, "t", TableSpec{Columns: []ColumnDef{{Name: "id", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2}),
			"ADMIN", "*", "CREATE TABLE"},
	}
	for _, testCase := range cases {
		err := testCase.err
		if !errors.Is(err, ErrPermissionDenied) || !errors.Is(err, ErrServer) || errors.Is(err, ErrAuth) {
			t.Fatalf("got %v, want ErrPermissionDenied and ErrServer", err)
		}
		var denied *PermissionDeniedError
		if !errors.As(err, &denied) || denied.Right != testCase.right || denied.Table != testCase.table {
			t.Fatalf("got %+v, want %s on %q", denied, testCase.right, testCase.table)
		}
		var typed *Error
		if !errors.As(err, &typed) || typed.ServerCode != CodePermissionDenied || typed.Command != testCase.command {
			t.Fatalf("got %+v", typed)
		}
		if !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("got %q", err)
		}
	}

	// Other server errors are not permission errors.
	if err := (&Error{Kind: KindServer, ServerCode: CodeNoTable}); errors.Is(err, ErrPermissionDenied) {
		t.Fatal("NO_TABLE matches ErrPermissionDenied")
	}
}
