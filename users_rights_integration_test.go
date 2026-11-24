package chunkdb

import (
	"errors"
	"reflect"
	"slices"
	"testing"
)

func TestIntegrationVerifierRotationAndServerRights(t *testing.T) {
	server := startServer(t, serverConfig{})
	ctx := t.Context()
	admin := connectIntegration(t, server, nil)
	old, err := ComputeVerifier("old-password", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Do(ctx, "CREATE USER rot VERIFIER $1", []byte(old)); err != nil {
		t.Fatal(err)
	}
	salt, iterations, err := ParseVerifier(old)
	if err != nil || len(salt) != 16 || iterations != DefaultVerifierIterations {
		t.Fatalf("%v %d %v", salt, iterations, err)
	}
	next, err := ComputeVerifierWithSalt("new-password", salt, iterations)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.AddVerifier(ctx, "rot", next); err != nil {
		t.Fatal(err)
	}
	for _, password := range []string{"old-password", "new-password"} {
		_ = connectIntegration(t, server, func(o *Options) { o.URI = server.uriAs("rot", password) }).Close()
	}
	verifiers := func() int {
		users, err := admin.Users(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, user := range users {
			if user.Name == "rot" {
				return user.Verifiers
			}
		}
		t.Fatal("user rot missing")
		return 0
	}
	if n := verifiers(); n != 2 {
		t.Fatalf("verifiers = %d", n)
	}
	other, _ := ComputeVerifier("other-password", 0)
	if err := admin.AddVerifier(ctx, "rot", other); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("a third verifier with another salt: %v", err)
	}
	if err := admin.DropOldVerifier(ctx, "rot"); err != nil {
		t.Fatal(err)
	}
	if n := verifiers(); n != 1 {
		t.Fatalf("verifiers = %d", n)
	}
	if c, err := ConnectURI(ctx, server.uriAs("rot", "old-password")); err == nil {
		_ = c.Close()
		t.Fatal("the old password still logs in")
	}
	if _, err := ComputeVerifierWithSalt("p", salt[:8], iterations); err == nil {
		t.Fatal("accepted a short salt")
	}
	if _, _, err := ParseVerifier("SCRAM-SHA-256$4096:bad$x:y"); err == nil {
		t.Fatal("parsed a broken verifier")
	}

	rot := connectIntegration(t, server, func(o *Options) { o.URI = server.uriAs("rot", "new-password") })
	_, err = rot.Metrics(ctx)
	var denied *PermissionDeniedError
	if !errors.As(err, &denied) || !reflect.DeepEqual(denied.Rights, []string{"MONITOR", "ADMIN"}) || denied.Table != "*" {
		t.Fatalf("%#v %v", denied, err)
	}
	if err := admin.Grant(ctx, RightMonitor, AllTables, "rot"); err != nil {
		t.Fatal(err)
	}
	if _, err := rot.Metrics(ctx); err != nil {
		t.Fatalf("MONITOR does not read metrics: %v", err)
	}
	if err := admin.Grant(ctx, RightBackup, "world", "rot"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a server right on a table: %v", err)
	}
	if err := admin.Grant(ctx, RightRead, "tenant1_*", "rot"); err != nil {
		t.Fatal(err)
	}
	createTable(t, admin, "tenant1_map", TableSpec{ChunkWidth: 2, ChunkHeight: 2, Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}}})
	if _, err := rot.GetBlock(ctx, "tenant1_map", 0, 0); err != nil {
		t.Fatalf("a prefix grant does not cover a new table: %v", err)
	}
	users, err := admin.Users(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		if user.Name == "rot" && (!slices.Equal(user.ServerRights, []Right{RightMonitor}) || user.Grants["tenant1_*"] != RightRead) {
			t.Fatalf("%+v", user)
		}
	}
	if err := admin.Revoke(ctx, RightMonitor, AllTables, "rot"); err != nil {
		t.Fatal(err)
	}
}
