package chunkdb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
//	cmake -S . -B build-js-tests -DCHUNKDB_ENABLE_TLS=ON && cmake --build build-js-tests

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

// serverGeometry is the chunk shape reported by INFO, used to build full-chunk
// payloads of the right size.
type serverGeometry struct {
	blockBits   int
	blockCount  int
	payloadBits int
	width       int
	height      int
}

func readGeometry(t *testing.T, client *Client) serverGeometry {
	t.Helper()

	info, err := client.Info(t.Context())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	read := func(key string) int {
		value, err := strconv.Atoi(info.Values[key])
		if err != nil || value <= 0 {
			t.Fatalf("INFO %s: got %q", key, info.Values[key])
		}
		return value
	}

	geo := serverGeometry{
		blockBits: read("block_bits"),
		width:     read("chunk_width_blocks"),
		height:    read("chunk_height_blocks"),
	}
	geo.blockCount = geo.width * geo.height
	geo.payloadBits = geo.blockCount * geo.blockBits
	return geo
}

func (g serverGeometry) blockPattern(seed byte) string {
	bits := make([]byte, g.blockBits)
	for i := range bits {
		if (i+int(seed))%2 == 0 {
			bits[i] = '1'
			continue
		}
		bits[i] = '0'
	}
	return string(bits)
}

func (g serverGeometry) zeroBlock() string { return strings.Repeat("0", g.blockBits) }

// blockOrigin returns the coordinate of the first block inside chunk (cx, cy).
// Chunk batches may only touch blocks belonging to their own chunk.
func (g serverGeometry) blockOrigin(cx, cy int64) (x, y int64) {
	return cx * int64(g.width), cy * int64(g.height)
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

func TestIntegrationBlockOperations(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	t.Run("unset block reads as absent", func(t *testing.T) {
		state, err := client.ReadBlock(ctx, 0, 0)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if state.Exists {
			t.Fatalf("got %+v, want an absent block", state)
		}

		// The low-level read still returns zero bits for an absent block.
		bits, err := client.Get(ctx, 0, 0)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if bits != geo.zeroBlock() {
			t.Fatalf("got %q, want zero bits", bits)
		}
	})

	t.Run("set and read back", func(t *testing.T) {
		pattern := geo.blockPattern(0)
		if err := client.Set(ctx, 1, 1, pattern); err != nil {
			t.Fatalf("Set: %v", err)
		}

		state, err := client.ReadBlock(ctx, 1, 1)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if !state.Exists || state.Bits != pattern {
			t.Fatalf("got %+v, want %q", state, pattern)
		}

		present, err := client.Exists(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !present {
			t.Fatal("got Exists false, want true")
		}
	})

	t.Run("explicit zero block stays present", func(t *testing.T) {
		if err := client.Set(ctx, 2, 1, geo.zeroBlock()); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.ReadBlock(ctx, 2, 1)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if !state.Exists || state.Bits != geo.zeroBlock() {
			t.Fatalf("got %+v, want an explicitly present zero block", state)
		}
	})

	t.Run("unset clears presence", func(t *testing.T) {
		if err := client.Set(ctx, 3, 1, geo.blockPattern(1)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := client.Unset(ctx, 3, 1); err != nil {
			t.Fatalf("Unset: %v", err)
		}
		state, err := client.ReadBlock(ctx, 3, 1)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if state.Exists {
			t.Fatalf("got %+v, want an absent block after Unset", state)
		}
	})

	t.Run("negative coordinates", func(t *testing.T) {
		pattern := geo.blockPattern(1)
		if err := client.Set(ctx, -5, -7, pattern); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.ReadBlock(ctx, -5, -7)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if !state.Exists || state.Bits != pattern {
			t.Fatalf("got %+v, want %q", state, pattern)
		}
	})

	t.Run("batch read and write", func(t *testing.T) {
		first, second := geo.blockPattern(0), geo.blockPattern(1)
		if err := client.MSet(ctx, []Block{
			{X: 10, Y: 2, Bits: first},
			{X: 11, Y: 2, Bits: second},
		}); err != nil {
			t.Fatalf("MSet: %v", err)
		}

		values, err := client.MGet(ctx, []BlockRef{{X: 10, Y: 2}, {X: 11, Y: 2}, {X: 12, Y: 2}})
		if err != nil {
			t.Fatalf("MGet: %v", err)
		}
		if len(values) != 3 {
			t.Fatalf("got %d values, want 3", len(values))
		}
		if values[0] != first || values[1] != second {
			t.Fatalf("got %q, want %q and %q", values, first, second)
		}
		if values[2] != geo.zeroBlock() {
			t.Fatalf("got %q for an unset block, want zero bits", values[2])
		}
	})

	t.Run("invalid payload is rejected by the server", func(t *testing.T) {
		err := client.Set(ctx, 0, 0, "101")
		var typed *Error
		if !errors.As(err, &typed) || !errors.Is(err, ErrServer) {
			t.Fatalf("got %v, want a server error", err)
		}
		if typed.ServerCode == "" {
			t.Fatalf("got %+v, want a server error code", typed)
		}
	})
}

func TestIntegrationChunkOperations(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	payloadBytes := (geo.payloadBits + 7) / 8
	presenceBytes := (geo.blockCount + 7) / 8

	t.Run("absent chunk", func(t *testing.T) {
		present, err := client.ChunkExists(ctx, 100, 100)
		if err != nil {
			t.Fatalf("ChunkExists: %v", err)
		}
		if present {
			t.Fatal("got true, want an absent chunk")
		}

		state, err := client.ReadChunk(ctx, 100, 100)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if state.Exists {
			t.Fatalf("got %+v, want an absent chunk", state)
		}
		if state.Bits != strings.Repeat("0", geo.payloadBits) {
			t.Fatalf("got %d payload bits, want %d zero bits", len(state.Bits), geo.payloadBits)
		}
		if state.Presence != strings.Repeat("0", geo.blockCount) {
			t.Fatalf("got presence %q, want all absent", state.Presence)
		}
	})

	t.Run("full chunk replace", func(t *testing.T) {
		bits := strings.Repeat("1", geo.payloadBits)
		if err := client.SetChunk(ctx, 5, 5, bits); err != nil {
			t.Fatalf("SetChunk: %v", err)
		}

		state, err := client.ReadChunk(ctx, 5, 5)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if !state.Exists || state.Bits != bits {
			t.Fatalf("got Exists=%v with %d bits, want the written payload", state.Exists, len(state.Bits))
		}
		if state.Presence != strings.Repeat("1", geo.blockCount) {
			t.Fatalf("got presence %q, want every block present", state.Presence)
		}

		raw, err := client.Chunk(ctx, 5, 5)
		if err != nil {
			t.Fatalf("Chunk: %v", err)
		}
		if raw != bits {
			t.Fatalf("got %d bits from Chunk, want the written payload", len(raw))
		}
	})

	t.Run("explicit zero chunk stays present", func(t *testing.T) {
		if err := client.SetChunk(ctx, 6, 5, strings.Repeat("0", geo.payloadBits)); err != nil {
			t.Fatalf("SetChunk: %v", err)
		}
		state, err := client.ReadChunk(ctx, 6, 5)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if !state.Exists {
			t.Fatal("got Exists false, want an explicitly present all-zero chunk")
		}
	})

	t.Run("mixed presence state", func(t *testing.T) {
		bits := []byte(strings.Repeat("0", geo.payloadBits))
		for i := range geo.blockBits {
			bits[i] = '1'
		}
		presence := []byte(strings.Repeat("0", geo.blockCount))
		presence[0] = '1'

		if err := client.SetChunkState(ctx, 7, 5, ChunkStateInput{
			Bits:     string(bits),
			Presence: string(presence),
		}); err != nil {
			t.Fatalf("SetChunkState: %v", err)
		}

		state, err := client.ReadChunk(ctx, 7, 5)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if state.Presence != string(presence) {
			t.Fatalf("got presence %q, want %q", state.Presence, presence)
		}
		if state.Bits != string(bits) {
			t.Fatalf("got payload %q, want %q", state.Bits, bits)
		}
	})

	t.Run("binary transfer", func(t *testing.T) {
		bits := strings.Repeat("10", geo.payloadBits/2)
		if err := client.SetChunk(ctx, 8, 5, bits); err != nil {
			t.Fatalf("SetChunk: %v", err)
		}

		packed, err := client.ChunkBin(ctx, 8, 5)
		if err != nil {
			t.Fatalf("ChunkBin: %v", err)
		}
		if len(packed) != payloadBytes {
			t.Fatalf("got %d bytes, want %d", len(packed), payloadBytes)
		}

		state, err := client.ChunkBinState(ctx, 8, 5)
		if err != nil {
			t.Fatalf("ChunkBinState: %v", err)
		}
		if len(state) != payloadBytes+presenceBytes {
			t.Fatalf("got %d bytes, want %d", len(state), payloadBytes+presenceBytes)
		}
		if string(state[:payloadBytes]) != string(packed) {
			t.Fatal("the state payload prefix does not match ChunkBin")
		}
	})

	t.Run("compressed transfer matches uncompressed", func(t *testing.T) {
		// A mostly-zero chunk exercises the codec's zero runs.
		bits := []byte(strings.Repeat("0", geo.payloadBits))
		for i := range geo.blockBits {
			bits[i] = '1'
		}
		if err := client.SetChunk(ctx, 9, 5, string(bits)); err != nil {
			t.Fatalf("SetChunk: %v", err)
		}

		packed, err := client.ChunkBin(ctx, 9, 5)
		if err != nil {
			t.Fatalf("ChunkBin: %v", err)
		}
		compressed, err := client.ChunkBinCompressed(ctx, 9, 5)
		if err != nil {
			t.Fatalf("ChunkBinCompressed: %v", err)
		}
		if string(compressed) != string(packed) {
			t.Fatal("ChunkBinCompressed did not decode to the ChunkBin payload")
		}

		state, err := client.ChunkBinState(ctx, 9, 5)
		if err != nil {
			t.Fatalf("ChunkBinState: %v", err)
		}
		compressedState, err := client.ChunkBinStateCompressed(ctx, 9, 5)
		if err != nil {
			t.Fatalf("ChunkBinStateCompressed: %v", err)
		}
		if string(compressedState) != string(state) {
			t.Fatal("ChunkBinStateCompressed did not decode to the ChunkBinState payload")
		}
	})
}

func TestIntegrationWorldReads(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	populated := []CoordPair{{CX: 0, CY: 0}, {CX: 0, CY: 1}, {CX: 1, CY: 0}, {CX: 2, CY: 2}}
	bits := strings.Repeat("1", geo.payloadBits)
	for _, coordinate := range populated {
		if err := client.SetChunk(ctx, coordinate.CX, coordinate.CY, bits); err != nil {
			t.Fatalf("SetChunk(%d,%d): %v", coordinate.CX, coordinate.CY, err)
		}
	}

	t.Run("scan paginates in order", func(t *testing.T) {
		var (
			seen   []CoordPair
			cursor *CoordPair
		)
		for range 10 {
			page, err := client.ChunkScan(ctx, 2, cursor)
			if err != nil {
				t.Fatalf("ChunkScan: %v", err)
			}
			seen = append(seen, page.Coords...)
			if page.NextCursor == nil {
				break
			}
			cursor = page.NextCursor
		}

		if len(seen) != len(populated) {
			t.Fatalf("got %+v, want %+v", seen, populated)
		}
		for i, coordinate := range populated {
			if seen[i] != coordinate {
				t.Fatalf("entry %d: got %+v, want %+v", i, seen[i], coordinate)
			}
		}
	})

	t.Run("range returns populated chunks only", func(t *testing.T) {
		entries, err := client.ChunkRange(ctx, 0, 0, 1, 1)
		if err != nil {
			t.Fatalf("ChunkRange: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("got %d entries, want 3", len(entries))
		}
		for _, entry := range entries {
			if entry.Bits != bits {
				t.Fatalf("chunk (%d,%d): got %d payload bits, want %d", entry.CX, entry.CY, len(entry.Bits), geo.payloadBits)
			}
			if entry.Presence != strings.Repeat("1", geo.blockCount) {
				t.Fatalf("chunk (%d,%d): got presence %q", entry.CX, entry.CY, entry.Presence)
			}
		}
	})

	t.Run("radius returns the disc", func(t *testing.T) {
		entries, err := client.ChunkRadius(ctx, 0, 0, 1)
		if err != nil {
			t.Fatalf("ChunkRadius: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("got %d entries, want the three chunks within radius 1", len(entries))
		}
		for _, entry := range entries {
			if entry.CX*entry.CX+entry.CY*entry.CY > 1 {
				t.Fatalf("chunk (%d,%d) is outside radius 1", entry.CX, entry.CY)
			}
		}
	})

	t.Run("empty range", func(t *testing.T) {
		entries, err := client.ChunkRange(ctx, 500, 500, 501, 501)
		if err != nil {
			t.Fatalf("ChunkRange: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("got %d entries, want none", len(entries))
		}
	})
}

func TestIntegrationVersionedMutations(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	pattern := geo.blockPattern(0)

	t.Run("compare and set", func(t *testing.T) {
		version, err := client.ChunkVersion(ctx, 20, 20)
		if err != nil {
			t.Fatalf("ChunkVersion: %v", err)
		}

		state := ChunkStateInput{
			Bits:     strings.Repeat("1", geo.payloadBits),
			Presence: strings.Repeat("1", geo.blockCount),
		}
		result, err := client.ChunkCompareAndSet(ctx, 20, 20, version, state)
		if err != nil {
			t.Fatalf("ChunkCompareAndSet: %v", err)
		}
		if !result.OK {
			t.Fatalf("got %+v, want the mutation to apply", result)
		}
		if result.Version == version {
			t.Fatal("the version did not change after a content mutation")
		}

		// The stale version must now be rejected without changing state.
		stale, err := client.ChunkCompareAndSet(ctx, 20, 20, version, state)
		if err != nil {
			t.Fatalf("ChunkCompareAndSet: %v", err)
		}
		if stale.OK {
			t.Fatal("a stale version was accepted")
		}
		if stale.Version != result.Version {
			t.Fatalf("got current version %d, want %d", stale.Version, result.Version)
		}
	})

	t.Run("atomic batch", func(t *testing.T) {
		x, y := geo.blockOrigin(21, 21)
		result, err := client.ChunkBatch(ctx, 21, 21, []BatchOperation{
			SetOp(x, y, pattern),
			SetOp(x+1, y, pattern),
		})
		if err != nil {
			t.Fatalf("ChunkBatch: %v", err)
		}
		if !result.OK {
			t.Fatalf("got %+v, want the batch to apply", result)
		}

		state, err := client.ReadBlock(ctx, x, y)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if !state.Exists || state.Bits != pattern {
			t.Fatalf("got %+v, want the batched write", state)
		}
	})

	t.Run("batch with stale version is rejected", func(t *testing.T) {
		x, y := geo.blockOrigin(22, 22)
		version, err := client.ChunkVersion(ctx, 22, 22)
		if err != nil {
			t.Fatalf("ChunkVersion: %v", err)
		}
		if _, err := client.ChunkBatch(ctx, 22, 22, []BatchOperation{SetOp(x, y, pattern)}); err != nil {
			t.Fatalf("ChunkBatch: %v", err)
		}

		result, err := client.ChunkBatchIfVersion(ctx, 22, 22, version, []BatchOperation{UnsetOp(x, y)})
		if err != nil {
			t.Fatalf("ChunkBatchIfVersion: %v", err)
		}
		if result.OK {
			t.Fatal("a stale version was accepted")
		}

		// The rejected batch must have left the block untouched.
		state, err := client.ReadBlock(ctx, x, y)
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		if !state.Exists {
			t.Fatal("the rejected batch changed the chunk")
		}
	})

	t.Run("batch outside the chunk is rejected", func(t *testing.T) {
		_, err := client.ChunkBatch(ctx, 23, 23, []BatchOperation{SetOp(0, 0, pattern)})
		if !errors.Is(err, ErrServer) {
			t.Fatalf("got %v, want a server error", err)
		}
	})
}

func TestIntegrationDurabilityAndMetrics(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	if err := client.Set(ctx, 0, 0, geo.blockPattern(0)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.WALFlush(ctx); err != nil {
		t.Fatalf("WALFlush: %v", err)
	}

	metrics, err := client.Metrics(ctx)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if !strings.Contains(metrics, "chunkdb_") {
		t.Fatalf("got metrics without any chunkdb_ series:\n%s", metrics)
	}

	info, err := client.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Values["chunkdb_version"] == "" {
		t.Fatalf("got INFO without chunkdb_version: %q", info.Raw)
	}
}

func TestIntegrationAuthFailure(t *testing.T) {
	server := startServer(t, serverConfig{})

	_, err := Connect(t.Context(), Options{
		URI:            strings.Replace(server.uri, testToken, "wrong-token", 1),
		ConnectTimeout: 5 * time.Second,
		CommandTimeout: 5 * time.Second,
	})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("got %v, want ErrAuth", err)
	}

	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("got %T, want *chunkdb.Error", err)
	}
	if typed.ServerCode != "AUTH_FAILED" {
		t.Fatalf("got server code %q, want AUTH_FAILED", typed.ServerCode)
	}
}

func TestIntegrationPipelining(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, func(o *Options) { o.PipelineDepth = 8 })
	geo := readGeometry(t, client)
	ctx := t.Context()

	const writes = 64
	pattern := geo.blockPattern(0)

	var group sync.WaitGroup
	for i := range writes {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := client.Set(ctx, int64(i), 50, pattern); err != nil {
				t.Errorf("Set(%d): %v", i, err)
			}
		}()
	}
	group.Wait()

	refs := make([]BlockRef, 0, writes)
	for i := range writes {
		refs = append(refs, BlockRef{X: int64(i), Y: 50})
	}
	values, err := client.MGet(ctx, refs)
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(values) != writes {
		t.Fatalf("got %d values, want %d", len(values), writes)
	}
	for i, value := range values {
		if value != pattern {
			t.Fatalf("block %d: got %q, want %q", i, value, pattern)
		}
	}
}

func TestIntegrationPool(t *testing.T) {
	// One worker stays occupied per open connection, so the server needs at
	// least as many workers as the pool's connection ceiling.
	server := startServer(t, serverConfig{workers: 8})

	pool, err := ConnectPool(t.Context(), PoolOptions{
		Options:        Options{URI: server.uri, ConnectTimeout: 5 * time.Second, CommandTimeout: 5 * time.Second},
		MaxConnections: 4,
		MinConnections: 2,
		AcquireTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("ConnectPool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ctx := t.Context()
	info, err := pool.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	blockBits, err := strconv.Atoi(info.Values["block_bits"])
	if err != nil {
		t.Fatalf("INFO block_bits: %v", err)
	}
	pattern := strings.Repeat("10", blockBits/2)

	var group sync.WaitGroup
	for i := range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := pool.Set(ctx, int64(i), 60, pattern); err != nil {
				t.Errorf("Set(%d): %v", i, err)
			}
		}()
	}
	group.Wait()

	for i := range 32 {
		state, err := pool.ReadBlock(ctx, int64(i), 60)
		if err != nil {
			t.Fatalf("ReadBlock(%d): %v", i, err)
		}
		if !state.Exists || state.Bits != pattern {
			t.Fatalf("block %d: got %+v, want %q", i, state, pattern)
		}
	}

	if err := pool.WithClient(ctx, func(ctx context.Context, client *Client) error {
		return client.Ping(ctx)
	}); err != nil {
		t.Fatalf("WithClient: %v", err)
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

		if err := client.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
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

		if _, err := client.Info(ctx); err != nil {
			t.Fatalf("Info: %v", err)
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
