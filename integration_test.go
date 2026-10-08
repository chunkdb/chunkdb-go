package chunkdb

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests run against a real chunkdb server binary. They are skipped when
// no binary is available, so `go test ./...` still works in a bare checkout.
//
// The binary is looked up as:
//  1. $CHUNKDB_SERVER_BIN
//  2. $CHUNKDB_REPO_ROOT/build-js-tests/chunkdb_server
//  3. ../chunkdb/build-js-tests/chunkdb_server
//
// Build one with, from the chunkdb repository:
//
//	cmake -S . -B build-js-tests -DCHUNKDB_WITH_TLS=ON && cmake --build build-js-tests --target chunkdb_server

const testToken = "chunk-token"

func serverBinary(t *testing.T) string {
	t.Helper()

	if fromEnv := os.Getenv("CHUNKDB_SERVER_BIN"); fromEnv != "" {
		return fromEnv
	}

	name := "chunkdb_server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}

	root := os.Getenv("CHUNKDB_REPO_ROOT")
	if root == "" {
		root = filepath.Join("..", "chunkdb")
	}

	candidate := filepath.Join(root, "build-js-tests", name)
	if _, err := os.Stat(candidate); err != nil {
		t.Skipf("chunkdb server binary not found at %s; set CHUNKDB_SERVER_BIN to run integration tests", candidate)
	}
	return candidate
}

type testServer struct {
	uri   string
	host  string
	port  int
	caPEM []byte
}

type serverConfig struct {
	tls   bool
	token string
	// workers sizes the server's worker pool. A worker is occupied for as long
	// as a client connection is open, so a test that keeps N connections alive
	// needs at least N workers.
	workers int
}

func startServer(t *testing.T, config serverConfig) *testServer {
	t.Helper()

	binary := serverBinary(t)
	port := freePort(t)
	token := config.token
	if token == "" {
		token = testToken
	}

	scheme := "chunk"
	if config.tls {
		scheme = "chunks"
	}
	uri := scheme + "://" + token + "@127.0.0.1:" + strconv.Itoa(port) + "/"

	workers := config.workers
	if workers == 0 {
		workers = 2
	}

	args := []string{
		"--listen-uri", uri,
		"--data-dir", t.TempDir(),
		"--durability", "relaxed",
		"--workers", strconv.Itoa(workers),
		"--log-level", "warn",
	}

	server := &testServer{uri: uri, host: "127.0.0.1", port: port}
	if config.tls {
		certPath, keyPath, caPEM := writeTLSFixture(t)
		args = append(args, "--tls-cert", certPath, "--tls-key", keyPath)
		server.caPEM = caPEM
	}

	command := exec.Command(binary, args...)
	var output strings.Builder
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", binary, err)
	}

	t.Cleanup(func() {
		_ = command.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = command.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
	})

	waitForListener(t, server.address(), &output)
	return server
}

func (s *testServer) address() string { return net.JoinHostPort(s.host, strconv.Itoa(s.port)) }

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForListener(t *testing.T, address string, output *strings.Builder) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not start on %s; output:\n%s", address, output.String())
}

// writeTLSFixture generates a self-signed certificate for 127.0.0.1 and returns
// the certificate path, key path, and the PEM bytes to trust it.
func writeTLSFixture(t *testing.T) (certPath, keyPath string, caPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "chunkdb-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, caPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath, caPEM
}

func connectIntegration(t *testing.T, server *testServer, configure func(*Options)) *Client {
	t.Helper()

	opts := Options{URI: server.uri, ConnectTimeout: 5 * time.Second, CommandTimeout: 5 * time.Second}
	if configure != nil {
		configure(&opts)
	}

	client, err := Connect(t.Context(), opts)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// typesSpec is a table with a column of every type, chunks of 4x4 blocks.
var typesSpec = TableSpec{
	Columns: []ColumnDef{
		{Name: "id", Type: TypeUint(64), Required: true},
		{Name: "small", Type: TypeUint(3), Default: 5},
		{Name: "temp", Type: TypeInt(64), Null: true},
		{Name: "solid", Type: TypeBool()},
		{Name: "h", Type: TypeF32(), Default: float32(1.5)},
		{Name: "d", Type: TypeF64(), Null: true},
		{Name: "mask", Type: TypeBits(10), Null: true},
		{Name: "name", Type: TypeText(32), Null: true},
		{Name: "blob", Type: TypeBytes(8)},
	},
	ChunkWidth:  4,
	ChunkHeight: 4,
}

func createTable(t *testing.T, client *Client, name string, spec TableSpec) {
	t.Helper()
	if err := client.CreateTable(t.Context(), name, spec); err != nil {
		t.Fatalf("CreateTable(%s): %v", name, err)
	}
}

func mustBits(t *testing.T, digits string) Bits {
	t.Helper()
	b, err := ParseBits(digits)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sameRecord compares records by value, []byte and Bits included.
func sameRecord(got, want Record) bool {
	if len(got) != len(want) {
		return false
	}
	for name, value := range want {
		other, ok := got[name]
		if !ok {
			return false
		}
		switch v := value.(type) {
		case []byte:
			o, ok := other.([]byte)
			if !ok || !bytes.Equal(o, v) {
				return false
			}
		case Bits:
			o, ok := other.(Bits)
			if !ok || !o.Equal(v) {
				return false
			}
		case float64:
			o, ok := other.(float64)
			if !ok || (o != v && !(math.IsNaN(o) && math.IsNaN(v))) {
				return false
			}
		default:
			if other != value {
				return false
			}
		}
	}
	return true
}

func TestIntegrationServerInfo(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)

	info := client.ServerInfo()
	if info == nil || info.Protocol != 3 || info.ServerVersion == "" {
		t.Fatalf("got %+v", info)
	}
	if info.MaxLineBytes != 65536 || info.MaxParameters != 65535 || info.MaxAreaChunks != 256 ||
		info.MaxResponseBytes != 64<<20 || info.MaxScanLimit != 1024 {
		t.Fatalf("got limits %+v", info)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if client.DefaultTable() != "default" {
		t.Fatalf("got default table %q", client.DefaultTable())
	}
	// The server's default table has one bits column.
	schema, err := client.Describe(t.Context(), "")
	if err != nil || schema.Table != "default" || len(schema.Columns) == 0 || schema.Columns[0].ID == 0 {
		t.Fatalf("Describe: %+v, %v", schema, err)
	}
}

func TestIntegrationTypedBlocks(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, func(o *Options) { o.Table = "things" })
	ctx := t.Context()
	createTable(t, client, "things", typesSpec)

	full := Record{
		"id": uint64(math.MaxUint64), "small": uint64(7), "temp": int64(math.MinInt64), "solid": true,
		"h": float32(-0.1), "d": math.Inf(-1), "mask": mustBits(t, "1000000001"),
		"name": "it's\r\nünïcode", "blob": []byte{0, '\r', '\n', 0, 0xff},
	}
	version, err := client.SetBlock(ctx, "", -5, 9, full)
	if err != nil || version == 0 {
		t.Fatalf("SetBlock: %d, %v", version, err)
	}
	got, err := client.GetBlock(ctx, "", -5, 9)
	if err != nil || !sameRecord(got, full) {
		t.Fatalf("GetBlock: got %v, %v; want %v", got, err, full)
	}

	// A new block takes defaults, NULL, or zero for the columns not given.
	if _, err := client.SetBlock(ctx, "", 0, 0, Record{"id": 1, "d": math.NaN()}); err != nil {
		t.Fatalf("SetBlock: %v", err)
	}
	got, err = client.GetBlock(ctx, "", 0, 0)
	want := Record{
		"id": uint64(1), "small": uint64(5), "temp": nil, "solid": false, "h": float32(1.5), "d": math.NaN(),
		"mask": nil, "name": nil, "blob": []byte{},
	}
	if err != nil || !sameRecord(got, want) {
		t.Fatalf("GetBlock: got %v, %v; want %v", got, err, want)
	}

	// NULL and empty values travel as parameters.
	if _, err := client.SetBlock(ctx, "", 0, 0, Record{"temp": nil, "name": "", "blob": []byte{}, "mask": nil}); err != nil {
		t.Fatalf("SetBlock: %v", err)
	}
	got, err = client.GetBlock(ctx, "", 0, 0, "name", "temp", "blob")
	if err != nil || !sameRecord(got, Record{"name": "", "temp": nil, "blob": []byte{}}) {
		t.Fatalf("GetBlock: got %v, %v", got, err)
	}

	// A REQUIRED column must be given for a new block.
	if _, err := client.SetBlock(ctx, "", 1, 0, Record{"small": 1}); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT", err)
	}
	// A value the column cannot take fails before it is sent.
	if _, err := client.SetBlock(ctx, "", 1, 0, Record{"id": 1, "small": 8}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a request error", err)
	}

	deleted, err := client.DeleteBlock(ctx, "", 0, 0)
	if err != nil || deleted == version {
		t.Fatalf("DeleteBlock: %d, %v", deleted, err)
	}
	if got, err := client.GetBlock(ctx, "", 0, 0); err != nil || got != nil {
		t.Fatalf("got %v, %v; want an absent block", got, err)
	}
	if got, err := client.GetBlock(ctx, "", 1000, -1000); err != nil || got != nil {
		t.Fatalf("got %v, %v; want an absent block", got, err)
	}
}

func TestIntegrationIfVersion(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, client, "things", typesSpec)

	first, err := client.SetBlock(ctx, "things", 1, 1, Record{"id": 1})
	if err != nil {
		t.Fatalf("SetBlock: %v", err)
	}
	// Another block of the same chunk changes the chunk's version.
	second, err := client.SetBlock(ctx, "things", 2, 2, Record{"id": 2})
	if err != nil || second == first {
		t.Fatalf("SetBlock: %d, %v", second, err)
	}

	_, err = client.SetBlock(ctx, "things", 1, 1, Record{"id": 3}, IfVersion(first))
	var mismatch *VersionMismatchError
	if !errors.As(err, &mismatch) || mismatch.Current != second || !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want a mismatch at version %d", err, second)
	}
	if got, _ := client.GetBlock(ctx, "things", 1, 1, "id"); got["id"] != uint64(1) {
		t.Fatalf("a refused write changed the block: %v", got)
	}
	third, err := client.SetBlock(ctx, "things", 1, 1, Record{"id": 3}, IfVersion(mismatch.Current))
	if err != nil {
		t.Fatalf("SetBlock IF VERSION: %v", err)
	}

	if _, err := client.DeleteBlock(ctx, "things", 1, 1, IfVersion(first)); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want a mismatch", err)
	}
	if _, err := client.DeleteBlock(ctx, "things", 1, 1, IfVersion(third)); err != nil {
		t.Fatalf("DeleteBlock IF VERSION: %v", err)
	}

	chunk, err := client.GetChunk(ctx, "things", 0, 0)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	chunk.SetBlock(3, 3, Record{"id": 9})
	written, err := client.SetChunk(ctx, "things", 0, 0, chunk, IfVersion(chunk.Version))
	if err != nil {
		t.Fatalf("SetChunk IF VERSION: %v", err)
	}
	_, err = client.SetChunk(ctx, "things", 0, 0, chunk, IfVersion(chunk.Version))
	if !errors.As(err, &mismatch) || mismatch.Current != written {
		t.Fatalf("got %v, want a mismatch at version %d", err, written)
	}
	// An absent chunk has a version too, so a write can create it only while
	// it is still absent.
	empty, err := client.GetChunk(ctx, "things", 7, 7)
	if err != nil || slices.Contains(empty.Present, true) {
		t.Fatalf("GetChunk of an absent chunk: %+v, %v", empty, err)
	}
	empty.SetBlock(0, 0, Record{"id": 1})
	if _, err := client.SetChunk(ctx, "things", 7, 7, empty, IfVersion(empty.Version)); err != nil {
		t.Fatalf("SetChunk creating a chunk: %v", err)
	}
	if _, err := client.SetChunk(ctx, "things", 7, 7, empty, IfVersion(empty.Version)); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want a mismatch", err)
	}
}

func TestIntegrationChunks(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()
	createTable(t, client, "things", typesSpec)

	blocks := map[[2]int64]Record{
		{0, 0}: {"id": uint64(1), "name": "a\r\nb", "blob": []byte{0, '\n'}, "mask": mustBits(t, "1100000001")},
		{3, 0}: {"id": uint64(2), "temp": int64(-7), "d": 2.5},
		{1, 2}: {"id": uint64(math.MaxUint64), "solid": true, "name": ""},
	}
	var version uint64
	for at, record := range blocks {
		var err error
		if version, err = client.SetBlock(ctx, "things", at[0], at[1], record); err != nil {
			t.Fatalf("SetBlock: %v", err)
		}
	}

	chunk, err := client.GetChunk(ctx, "things", 0, 0)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if chunk.Version != version || chunk.Width != 4 || chunk.Height != 4 {
		t.Fatalf("got version %d size %dx%d", chunk.Version, chunk.Width, chunk.Height)
	}
	for at := range blocks {
		block, err := client.GetBlock(ctx, "things", at[0], at[1])
		if err != nil {
			t.Fatalf("GetBlock: %v", err)
		}
		if got := chunk.Block(int(at[0]), int(at[1])); !sameRecord(got, block) {
			t.Fatalf("block %v: chunk has %v, GetBlock %v", at, got, block)
		}
	}
	present := 0
	for _, p := range chunk.Present {
		if p {
			present++
		}
	}
	if present != len(blocks) {
		t.Fatalf("got %d present blocks, want %d", present, len(blocks))
	}

	// Named columns only.
	part, err := client.GetChunk(ctx, "things", 0, 0, "name", "temp")
	if err != nil || len(part.Columns) != 2 || part.Columns["name"][0] != "a\r\nb" || part.Columns["temp"][3] != int64(-7) {
		t.Fatalf("GetChunk COLUMNS: %+v, %v", part, err)
	}

	// A typed chunk written to another chunk reads back the same.
	chunk.SetBlock(2, 3, Record{"id": 5, "blob": []byte("\r\n\x00")})
	chunk.DeleteBlock(3, 0)
	if _, err := client.SetChunk(ctx, "things", -1, 4, chunk); err != nil {
		t.Fatalf("SetChunk: %v", err)
	}
	copied, err := client.GetChunk(ctx, "things", -1, 4)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	// The new block (2, 3) took its defaults: what the client encodes is
	// what the server stores.
	schema, _ := client.Schema(ctx, "things")
	form, _ := EncodeChunk(schema, chunk)
	want, _ := DecodeChunk(schema, form)
	for i := range chunk.Present {
		lx, ly := i%4, i/4
		got, wantBlock := copied.Block(lx, ly), want.Block(lx, ly)
		if (got == nil) != (wantBlock == nil) || (got != nil && !sameRecord(got, wantBlock)) {
			t.Fatalf("block %d: got %v, want %v", i, got, wantBlock)
		}
	}
	if copied.Block(3, 0) != nil || copied.Block(2, 3)["h"] != float32(1.5) {
		t.Fatalf("got %v and %v", copied.Block(3, 0), copied.Block(2, 3))
	}
	if got, _ := client.GetBlock(ctx, "things", -2, 19, "blob", "small"); !sameRecord(got, Record{"blob": []byte("\r\n\x00"), "small": uint64(5)}) {
		t.Fatalf("got %v", got)
	}

	// The raw form copies a chunk without decoding it.
	raw, err := client.GetChunkRaw(ctx, "things", 0, 0)
	if err != nil {
		t.Fatalf("GetChunkRaw: %v", err)
	}
	if _, err := client.SetChunkRaw(ctx, "things", 10, 10, raw); err != nil {
		t.Fatalf("SetChunkRaw: %v", err)
	}
	again, err := client.GetChunkRaw(ctx, "things", 10, 10)
	if err != nil || !bytes.Equal(again[8:], raw[8:]) {
		t.Fatalf("GetChunkRaw: %v; the copy differs", err)
	}
	if _, err := client.SetChunkRaw(ctx, "things", 10, 10, raw[:12]); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT for a short form", err)
	}
}

func TestIntegrationAreasAndScans(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, func(o *Options) { o.Table = "things" })
	ctx := t.Context()
	createTable(t, client, "things", typesSpec)

	// One block in each of chunks (-1, 0), (0, 0), (0, 1), (2, -1), (5, 5).
	chunks := []ChunkCoord{{-1, 0}, {0, 0}, {0, 1}, {2, -1}, {5, 5}}
	for i, c := range chunks {
		if _, err := client.SetBlock(ctx, "", c.CX*4+1, c.CY*4+2, Record{"id": i, "name": "n"}); err != nil {
			t.Fatalf("SetBlock: %v", err)
		}
	}

	area, err := client.GetArea(ctx, "", -1, -1, 2, 1)
	if err != nil {
		t.Fatalf("GetArea: %v", err)
	}
	var got []ChunkCoord
	for _, entry := range area {
		got = append(got, ChunkCoord{entry.CX, entry.CY})
		if block := entry.Chunk.Block(1, 2); block == nil || block["name"] != "n" {
			t.Fatalf("chunk %d %d: got %v", entry.CX, entry.CY, block)
		}
	}
	if !slices.Equal(got, chunks[:4]) {
		t.Fatalf("got %v, want %v", got, chunks[:4])
	}

	around, err := client.GetAreaAround(ctx, "", 0, 0, 1, "id")
	if err != nil || len(around) != 3 || len(around[0].Chunk.Columns) != 1 {
		t.Fatalf("GetAreaAround: %+v, %v", around, err)
	}
	if _, err := client.GetArea(ctx, "", 0, 0, 300, 0); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT for a too large area", err)
	}

	page, err := client.ScanChunks(ctx, "", nil, 2)
	if err != nil || !page.More || !slices.Equal(page.Chunks, chunks[:2]) {
		t.Fatalf("ScanChunks: %+v, %v", page, err)
	}
	page, err = client.ScanChunks(ctx, "", &page.Chunks[1], 0)
	if err != nil || page.More || !slices.Equal(page.Chunks, chunks[2:]) {
		t.Fatalf("ScanChunks after: %+v, %v", page, err)
	}
	var all []ChunkCoord
	for coord, err := range client.AllChunks(ctx, "", 2) {
		if err != nil {
			t.Fatalf("AllChunks: %v", err)
		}
		all = append(all, coord)
	}
	if !slices.Equal(all, chunks) {
		t.Fatalf("got %v, want %v", all, chunks)
	}
	for _, err := range client.AllChunks(ctx, "nowhere", 0) {
		if !isServerCode(err, CodeNoTable) {
			t.Fatalf("got %v, want NO_TABLE", err)
		}
	}
}

func TestIntegrationTables(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()

	spec := typesSpec
	spec.LargeWidth, spec.LargeHeight = 2, 2
	spec.Options = TableOptions{DurabilityMode: "fsync-wal", VarMaxChunkBytes: 4096}
	createTable(t, client, "land", spec)
	if err := client.CreateTable(ctx, "land", spec); !isServerCode(err, CodeTableExists) {
		t.Fatalf("got %v, want TABLE_EXISTS", err)
	}

	tables, err := client.Tables(ctx)
	if err != nil || !slices.Contains(tables, "land") || !slices.Contains(tables, "default") {
		t.Fatalf("Tables: %v, %v", tables, err)
	}

	schema, err := client.Describe(ctx, "land")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if schema.Table != "land" || schema.Version != 1 || len(schema.Columns) != len(spec.Columns) ||
		schema.ChunkWidth != 4 || schema.ChunkHeight != 4 || schema.LargeWidth != 2 || schema.LargeHeight != 2 ||
		schema.Options.DurabilityMode != "fsync-wal" || schema.Options.VarMaxChunkBytes != 4096 {
		t.Fatalf("got %+v", schema)
	}
	for i, column := range schema.Columns {
		def := spec.Columns[i]
		if column.Name != def.Name || column.Type != def.Type || column.Null != def.Null || column.Required != def.Required {
			t.Fatalf("column %d: got %+v, want %+v", i, column, def)
		}
	}
	if small, _ := schema.Column("small"); small.Default != uint64(5) {
		t.Fatalf("got default %v", small.Default)
	}
	if h, _ := schema.Column("h"); h.Default != float32(1.5) {
		t.Fatalf("got default %v", h.Default)
	}

	if err := client.SetTableOption(ctx, "land", "durability_mode", "relaxed"); err != nil {
		t.Fatalf("SetTableOption: %v", err)
	}
	if err := client.SetTableOption(ctx, "land", "checkpoint_updates", 64); err != nil {
		t.Fatalf("SetTableOption: %v", err)
	}
	if err := client.SetTableOption(ctx, "land", "nope", 1); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT", err)
	}
	schema, err = client.Schema(ctx, "land")
	if err != nil || schema.Options.DurabilityMode != "relaxed" || schema.Options.CheckpointUpdates != 64 {
		t.Fatalf("got options %+v, %v", schema.Options, err)
	}

	if err := client.RenameColumn(ctx, "land", "name", "label"); err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}
	if err := client.AlterColumnType(ctx, "land", "small", TypeUint(8), ConvertNone); err != nil {
		t.Fatalf("AlterColumnType: %v", err)
	}
	if _, err := client.SetBlock(ctx, "land", 0, 0, Record{"id": 1, "small": 200, "label": "x"}); err != nil {
		t.Fatalf("SetBlock after ALTER: %v", err)
	}
	if err := client.AlterColumnType(ctx, "land", "small", TypeUint(4), ConvertNone); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT for a value that does not fit", err)
	}
	if err := client.AlterColumnType(ctx, "land", "small", TypeUint(4), ConvertClamp); err != nil {
		t.Fatalf("AlterColumnType USING CLAMP: %v", err)
	}
	if got, err := client.GetBlock(ctx, "land", 0, 0, "small", "label"); err != nil || !sameRecord(got, Record{"small": uint64(15), "label": "x"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if err := client.AlterColumnType(ctx, "land", "label", TypeText(4), ConvertTruncate); err != nil {
		t.Fatalf("AlterColumnType USING TRUNCATE: %v", err)
	}
	schema, _ = client.Schema(ctx, "land")
	if schema.Version != 5 {
		t.Fatalf("got schema version %d, want 5", schema.Version)
	}

	if err := client.DropTable(ctx, "land"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := client.GetBlock(ctx, "land", 0, 0); !isServerCode(err, CodeNoTable) {
		t.Fatalf("got %v, want NO_TABLE", err)
	}
	if err := client.DropTable(ctx, "land"); !isServerCode(err, CodeNoTable) {
		t.Fatalf("got %v, want NO_TABLE", err)
	}
}

// Another client changes the table's columns; this client's cached schema
// catches up.
func TestIntegrationSchemaChanges(t *testing.T) {
	server := startServer(t, serverConfig{workers: 4})
	client := connectIntegration(t, server, func(o *Options) { o.Table = "things" })
	other := connectIntegration(t, server, func(o *Options) { o.Table = "things" })
	ctx := t.Context()
	createTable(t, client, "things", TableSpec{
		Columns: []ColumnDef{
			{Name: "id", Type: TypeUint(16)},
			{Name: "h", Type: TypeF32()},
			{Name: "first", Type: TypeText(8), Null: true},
			{Name: "old", Type: TypeBytes(8), Null: true},
		},
		ChunkWidth: 4, ChunkHeight: 4,
	})
	if _, err := client.SetBlock(ctx, "", 0, 0, Record{"id": 1, "h": 0.5, "first": "a", "old": []byte("o")}); err != nil {
		t.Fatalf("SetBlock: %v", err)
	}

	// A column added by another client: unknown to the cached schema, which
	// is fetched again.
	if err := other.AddColumn(ctx, "", ColumnDef{Name: "note", Type: TypeText(16), Null: true}); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if _, err := client.SetBlock(ctx, "", 1, 0, Record{"id": 2, "note": "added"}); err != nil {
		t.Fatalf("SetBlock of an added column: %v", err)
	}

	// A column dropped by another client: the cached schema still names it.
	if err := other.DropColumn(ctx, "", "old"); err != nil {
		t.Fatalf("DropColumn: %v", err)
	}
	record, err := client.GetBlock(ctx, "", 0, 0)
	if err != nil || !sameRecord(record, Record{"id": uint64(1), "h": float32(0.5), "first": "a", "note": nil}) {
		t.Fatalf("GetBlock after DROP COLUMN: %v, %v", record, err)
	}

	// The text columns now have ids 3 and 5: chunk forms map them by id.
	chunk, err := client.GetChunk(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if chunk.Columns["first"][0] != "a" || chunk.Columns["note"][1] != "added" || chunk.Columns["note"][0] != nil {
		t.Fatalf("got %v", chunk.Columns)
	}
	schema, _ := client.Schema(ctx, "")
	if first, _ := schema.Column("first"); first.ID != 3 {
		t.Fatalf("got id %d for first", first.ID)
	}
	if note, _ := schema.Column("note"); note.ID != 5 {
		t.Fatalf("got id %d for note", note.ID)
	}
	chunk.SetBlock(2, 2, Record{"id": 3, "note": "copied"})
	if _, err := client.SetChunk(ctx, "", 1, 1, chunk); err != nil {
		t.Fatalf("SetChunk: %v", err)
	}
	if got, err := client.GetBlock(ctx, "", 6, 6, "note"); err != nil || got["note"] != "copied" {
		t.Fatalf("got %v, %v", got, err)
	}

	// A type changed by another client: the f32 parameter has the wrong size
	// for the f64 column, so the client refreshes and writes once more.
	if err := other.AlterColumnType(ctx, "", "h", TypeF64(), ConvertNone); err != nil {
		t.Fatalf("AlterColumnType: %v", err)
	}
	if _, err := client.SetBlock(ctx, "", 0, 0, Record{"h": 0.25}); err != nil {
		t.Fatalf("SetBlock after a type change: %v", err)
	}
	if got, err := other.GetBlock(ctx, "", 0, 0, "h"); err != nil || got["h"] != 0.25 {
		t.Fatalf("got %v, %v", got, err)
	}

	// Chunk forms carry the schema version: after another client's change,
	// a raw form of the old version is refused, and SetChunk encodes the
	// chunk again with the new schema.
	chunk, err = client.GetChunk(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	raw, err := client.GetChunkRaw(ctx, "", 0, 0)
	if err != nil {
		t.Fatalf("GetChunkRaw: %v", err)
	}
	if err := other.AlterColumnType(ctx, "", "id", TypeUint(32), ConvertNone); err != nil {
		t.Fatalf("AlterColumnType: %v", err)
	}
	newSchema, _ := other.Describe(ctx, "")
	_, err = client.SetChunkRaw(ctx, "", 3, 3, raw)
	var mismatch *SchemaMismatchError
	if !errors.As(err, &mismatch) || mismatch.Current != newSchema.Version || chunk.SchemaVersion == newSchema.Version {
		t.Fatalf("got %v, want a SchemaMismatchError at version %d", err, newSchema.Version)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping after SCHEMA_MISMATCH: %v", err)
	}
	chunk.SetBlock(1, 1, Record{"id": 7})
	if _, err := client.SetChunk(ctx, "", 3, 3, chunk); err != nil {
		t.Fatalf("SetChunk after a schema change: %v", err)
	}
	if got, err := other.GetBlock(ctx, "", 13, 13, "id"); err != nil || got["id"] != uint64(7) {
		t.Fatalf("got %v, %v", got, err)
	}

	// A statement with parameters naming a column the table does not have
	// makes the server close the connection; the client reconnects.
	if _, err := client.Do(ctx, "SET BLOCK 0 0 IN things nope = $1", []byte{1}); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping after the server closed the connection: %v", err)
	}
}

func TestIntegrationDo(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()

	if _, err := client.Do(ctx, "CREATE TABLE raw (a u8, b bytes(4) NULL) CHUNK 2 x 2"); err != nil {
		t.Fatalf("Do CREATE: %v", err)
	}
	reply, err := client.Do(ctx, "SET BLOCK 0 0 IN raw a = $1, b = $2", []byte{7, 0, 0, 0, 0, 0, 0, 0}, nil)
	if err != nil || reply.Kind != ReplyInteger {
		t.Fatalf("Do SET: %+v, %v", reply, err)
	}
	reply, err = client.Do(ctx, "GET BLOCK 0 0 FROM raw")
	if err != nil || reply.Kind != ReplyArray || len(reply.Array) != 2 || reply.Array[1].Kind != ReplyNull {
		t.Fatalf("Do GET: %+v, %v", reply, err)
	}
	if a, _ := reply.Array[0].Uint64(); a != 7 {
		t.Fatalf("got %+v", reply.Array[0])
	}
	// A wrong-size parameter is an ordinary error and leaves the connection
	// usable.
	if _, err := client.Do(ctx, "SET BLOCK 0 0 IN raw a = $1", []byte{1}); !isServerCode(err, CodeInvalidArgument) {
		t.Fatalf("got %v, want INVALID_ARGUMENT", err)
	}
	if _, err := client.Do(ctx, "GET NOTHING"); !isServerCode(err, CodeSyntax) {
		t.Fatalf("got %v, want SYNTAX", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestIntegrationDurabilityAndMetrics(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()

	if err := client.FlushWAL(ctx); err != nil {
		t.Fatalf("FlushWAL: %v", err)
	}
	metrics, err := client.Metrics(ctx)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if !strings.Contains(metrics, "chunkdb_") {
		t.Fatalf("got metrics without any chunkdb_ series:\n%s", metrics)
	}
}

func TestIntegrationAuthFailure(t *testing.T) {
	server := startServer(t, serverConfig{})

	cases := map[string]struct {
		uri  string
		code string
	}{
		"wrong token":   {strings.Replace(server.uri, testToken, "wrong-token", 1), CodeAuthFailed},
		"missing token": {strings.Replace(server.uri, testToken+"@", "", 1), CodeAuthRequired},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			client, err := NewClient(Options{
				URI:            testCase.uri,
				ConnectTimeout: 5 * time.Second,
				CommandTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer func() { _ = client.Close() }()

			err = client.Connect(t.Context())
			if !errors.Is(err, ErrAuth) {
				t.Fatalf("got %v, want ErrAuth", err)
			}
			var typed *Error
			if !errors.As(err, &typed) {
				t.Fatalf("got %T, want *chunkdb.Error", err)
			}
			if typed.ServerCode != testCase.code || typed.Phase != PhaseAuth {
				t.Fatalf("got code %q phase %q, want %s in phase %s", typed.ServerCode, typed.Phase, testCase.code, PhaseAuth)
			}
			if client.ServerInfo() != nil {
				t.Fatal("a failed handshake left ServerInfo set")
			}
		})
	}
}

func TestIntegrationPipelining(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, func(o *Options) { o.PipelineDepth = 8; o.Table = "things" })
	ctx := t.Context()
	createTable(t, client, "things", typesSpec)

	const writes = 64
	var group sync.WaitGroup
	for i := range writes {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := client.SetBlock(ctx, "", int64(i), 50, Record{"id": i, "name": strconv.Itoa(i)}); err != nil {
				t.Errorf("SetBlock(%d): %v", i, err)
			}
		}()
	}
	group.Wait()

	// Reads in flight together each get their own reply.
	for i := range writes {
		group.Add(1)
		go func() {
			defer group.Done()
			got, err := client.GetBlock(ctx, "", int64(i), 50, "id", "name")
			if err != nil || !sameRecord(got, Record{"id": uint64(i), "name": strconv.Itoa(i)}) {
				t.Errorf("block %d: got %v, %v", i, got, err)
			}
		}()
	}
	group.Wait()
}

func TestIntegrationPool(t *testing.T) {
	// One worker stays occupied per open connection, so the server needs at
	// least as many workers as the pool's connection ceiling.
	server := startServer(t, serverConfig{workers: 8})

	pool, err := ConnectPool(t.Context(), PoolOptions{
		Options:        Options{URI: server.uri + "things", ConnectTimeout: 5 * time.Second, CommandTimeout: 5 * time.Second},
		MaxConnections: 4,
		MinConnections: 2,
		AcquireTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ConnectPool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ctx := t.Context()
	if err := pool.WithClient(ctx, func(ctx context.Context, client *Client) error {
		return client.CreateTable(ctx, "", typesSpec)
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	var group sync.WaitGroup
	for i := range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := pool.SetBlock(ctx, "", int64(i), 60, Record{"id": i}); err != nil {
				t.Errorf("SetBlock(%d): %v", i, err)
			}
		}()
	}
	group.Wait()

	for i := range 32 {
		got, err := pool.GetBlock(ctx, "", int64(i), 60, "id")
		if err != nil || got["id"] != uint64(i) {
			t.Fatalf("block %d: got %v, %v", i, got, err)
		}
	}
	raw, err := pool.GetChunkRaw(ctx, "", 0, 15)
	if err != nil {
		t.Fatalf("GetChunkRaw: %v", err)
	}
	if _, err := pool.SetChunkRaw(ctx, "", 9, 9, raw); err != nil {
		t.Fatalf("SetChunkRaw: %v", err)
	}
	if got, err := pool.GetBlock(ctx, "", 36, 36, "id"); err != nil || got["id"] != uint64(0) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestIntegrationTLS(t *testing.T) {
	server := startServer(t, serverConfig{tls: true})
	ctx := t.Context()

	t.Run("trusted certificate", func(t *testing.T) {
		client, err := Connect(ctx, Options{
			URI:            server.uri,
			CA:             server.caPEM,
			ConnectTimeout: 5 * time.Second,
			CommandTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer func() { _ = client.Close() }()

		if info := client.ServerInfo(); info == nil || info.Protocol != 3 {
			t.Fatalf("got ServerInfo %+v", info)
		}
		createTable(t, client, "things", typesSpec)
		record := Record{"id": uint64(3), "blob": []byte("\r\n\x00tls")}
		if _, err := client.SetBlock(ctx, "things", 1, 1, record); err != nil {
			t.Fatalf("SetBlock: %v", err)
		}
		if got, err := client.GetBlock(ctx, "things", 1, 1, "id", "blob"); err != nil || !sameRecord(got, record) {
			t.Fatalf("GetBlock: %v, %v", got, err)
		}
		if chunk, err := client.GetChunk(ctx, "things", 0, 0); err != nil || chunk.Block(1, 1)["id"] != uint64(3) {
			t.Fatalf("GetChunk: %v", err)
		}
	})

	t.Run("insecure skips verification", func(t *testing.T) {
		client, err := Connect(ctx, Options{
			URI:            server.uri,
			TLSInsecure:    true,
			ConnectTimeout: 5 * time.Second,
			CommandTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer func() { _ = client.Close() }()

		if err := client.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
	})

	t.Run("untrusted certificate is rejected", func(t *testing.T) {
		_, err := Connect(ctx, Options{
			URI:            server.uri,
			ConnectTimeout: 5 * time.Second,
		})
		if !errors.Is(err, ErrTLS) {
			t.Fatalf("got %v, want ErrTLS", err)
		}
	})
}
