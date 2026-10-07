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

// serverGeometry is the chunk shape of the connection's table, from HELLO.
type serverGeometry struct {
	blockBits     int
	width         int
	height        int
	blockCount    int
	payloadBytes  int
	presenceBytes int
}

func readGeometry(t *testing.T, client *Client) serverGeometry {
	t.Helper()

	info := client.ServerInfo()
	if info == nil || info.Table == nil {
		t.Fatalf("got ServerInfo %+v, want a table", info)
	}
	geo := serverGeometry{
		blockBits: info.Table.BlockBits,
		width:     info.Table.ChunkWidthBlocks,
		height:    info.Table.ChunkHeightBlocks,
	}
	geo.blockCount = geo.width * geo.height
	geo.payloadBytes = (geo.blockCount*geo.blockBits + 7) / 8
	geo.presenceBytes = (geo.blockCount + 7) / 8
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

// denseBytes returns n bytes without zero runs, which zrle cannot shrink.
func denseBytes(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*37+int(seed)) | 0x01
	}
	return out
}

// sparsePayload returns a payload that is zero except for a few bytes.
func (g serverGeometry) sparsePayload(seed byte) []byte {
	out := make([]byte, g.payloadBytes)
	out[0] = seed | 0x01
	out[len(out)/2] = 0x5a
	out[len(out)-1] = 0x80
	return out
}

func allOnes(n int) []byte { return bytes.Repeat([]byte{0xff}, n) }

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

func TestIntegrationServerInfo(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)

	info := client.ServerInfo()
	if info == nil {
		t.Fatal("got nil ServerInfo")
	}
	if info.Protocol != 2 || info.ServerVersion == "" || !slices.Contains(info.Capabilities, "zrle") {
		t.Fatalf("got %+v", info)
	}
	if info.MaxLineBytes != 65536 || info.MaxAreaChunks != 256 || info.MaxResponseBytes != 64<<20 ||
		info.MaxScanLimit != 1024 || info.MaxBatchOps != 1024 {
		t.Fatalf("got limits %+v", info)
	}
	table := info.Table
	if table == nil || table.Name != "default" || len(table.StoreID) != 32 || table.BlockBits <= 0 ||
		table.ChunkWidthBlocks <= 0 || table.ChunkHeightBlocks <= 0 || table.Options.DurabilityMode != "relaxed" {
		t.Fatalf("got table %+v", table)
	}
	if info.Values["protocol"] != "2" || info.Values["table"] != "default" {
		t.Fatalf("got values %v", info.Values)
	}

	// INFO reports runtime statistics; geometry comes from HELLO.
	stats, err := client.Info(t.Context())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if stats.Values["table"] != "default" {
		t.Fatalf("got INFO %q", stats.Raw)
	}
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
		state, err := client.Get(ctx, 0, 0)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state != (BlockState{}) {
			t.Fatalf("got %+v, want an unset block", state)
		}
	})

	t.Run("set and read back", func(t *testing.T) {
		pattern := geo.blockPattern(0)
		if err := client.Set(ctx, 1, 1, pattern); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.Get(ctx, 1, 1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state != (BlockState{Exists: true, Bits: pattern}) {
			t.Fatalf("got %+v, want %q", state, pattern)
		}
	})

	t.Run("explicit zero block stays present", func(t *testing.T) {
		if err := client.Set(ctx, 2, 1, geo.zeroBlock()); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.Get(ctx, 2, 1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state != (BlockState{Exists: true, Bits: geo.zeroBlock()}) {
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
		state, err := client.Get(ctx, 3, 1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state.Exists {
			t.Fatalf("got %+v, want an unset block after Unset", state)
		}
	})

	t.Run("negative coordinates", func(t *testing.T) {
		pattern := geo.blockPattern(1)
		if err := client.Set(ctx, -5, -7, pattern); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.Get(ctx, -5, -7)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state != (BlockState{Exists: true, Bits: pattern}) {
			t.Fatalf("got %+v, want %q", state, pattern)
		}
	})

	t.Run("batch read and write", func(t *testing.T) {
		first := geo.blockPattern(0)
		if err := client.MSet(ctx, []Block{
			{X: 10, Y: 2, Bits: first},
			{X: 11, Y: 2, Bits: geo.zeroBlock()},
		}); err != nil {
			t.Fatalf("MSet: %v", err)
		}

		values, err := client.MGet(ctx, []BlockRef{{X: 10, Y: 2}, {X: 11, Y: 2}, {X: 12, Y: 2}})
		if err != nil {
			t.Fatalf("MGet: %v", err)
		}
		want := []BlockState{{Exists: true, Bits: first}, {Exists: true, Bits: geo.zeroBlock()}, {}}
		if !slices.Equal(values, want) {
			t.Fatalf("got %+v, want %+v", values, want)
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

	t.Run("absent chunk reads as zeros", func(t *testing.T) {
		present, err := client.ChunkExists(ctx, 100, 100)
		if err != nil || present {
			t.Fatalf("ChunkExists: %v, %v; want an absent chunk", present, err)
		}
		payload, err := client.GetChunk(ctx, 100, 100, GetOptions{})
		if err != nil || !bytes.Equal(payload, make([]byte, geo.payloadBytes)) {
			t.Fatalf("GetChunk: %d bytes, %v; want %d zero bytes", len(payload), err, geo.payloadBytes)
		}
		state, err := client.GetChunkState(ctx, 100, 100, GetOptions{})
		if err != nil {
			t.Fatalf("GetChunkState: %v", err)
		}
		if state.Exists || !bytes.Equal(state.Payload, make([]byte, geo.payloadBytes)) ||
			!bytes.Equal(state.Presence, make([]byte, geo.presenceBytes)) {
			t.Fatalf("got %+v, want an absent chunk of zeros", state)
		}
	})

	t.Run("bit layout matches block reads", func(t *testing.T) {
		// Bit i of the chunk is payload[i/8] >> (i%8) & 1; block b holds bits
		// b*block_bits .. (b+1)*block_bits-1, blocks in row-major order, and
		// character j of the block's bit text is bit b*block_bits+j.
		x, y := geo.blockOrigin(50, 50)
		bits := []byte(geo.zeroBlock())
		bits[0] = '1'
		if err := client.Set(ctx, x+1, y+1, string(bits)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		state, err := client.GetChunkState(ctx, 50, 50, GetOptions{})
		if err != nil {
			t.Fatalf("GetChunkState: %v", err)
		}
		block := geo.width + 1
		bit := block * geo.blockBits
		if state.Payload[bit/8]>>(bit%8)&1 != 1 || state.Presence[block/8]>>(block%8)&1 != 1 {
			t.Fatalf("block %d bit not where documented: payload %x presence %x", block, state.Payload, state.Presence)
		}
	})

	t.Run("payload round trip", func(t *testing.T) {
		payload := denseBytes(geo.payloadBytes, 11)
		result, err := client.PutChunk(ctx, 5, 5, payload, PutOptions{})
		if err != nil || !result.OK {
			t.Fatalf("PutChunk: %+v, %v", result, err)
		}
		got, err := client.GetChunk(ctx, 5, 5, GetOptions{})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("GetChunk: %x, %v; want %x", got, err, payload)
		}
		state, err := client.GetChunkState(ctx, 5, 5, GetOptions{})
		if err != nil {
			t.Fatalf("GetChunkState: %v", err)
		}
		if !state.Exists || !bytes.Equal(state.Payload, payload) || !bytes.Equal(state.Presence, allOnes(geo.presenceBytes)) {
			t.Fatalf("got %+v, want the payload with every block present", state)
		}
		version, err := client.ChunkVersion(ctx, 5, 5)
		if err != nil || version != result.Version {
			t.Fatalf("ChunkVersion: %d, %v; want %d", version, err, result.Version)
		}
	})

	t.Run("explicit zero chunk stays present", func(t *testing.T) {
		if _, err := client.PutChunk(ctx, 6, 5, make([]byte, geo.payloadBytes), PutOptions{}); err != nil {
			t.Fatalf("PutChunk: %v", err)
		}
		state, err := client.GetChunkState(ctx, 6, 5, GetOptions{})
		if err != nil || !state.Exists {
			t.Fatalf("GetChunkState: %+v, %v; want an explicitly present all-zero chunk", state, err)
		}
	})

	t.Run("state round trip stores absent blocks as zero", func(t *testing.T) {
		// Blocks 0 and 1 carry bits; only block 0 is present.
		payload := make([]byte, geo.payloadBytes)
		for bit := range 2 * geo.blockBits {
			payload[bit/8] |= 1 << (bit % 8)
		}
		presence := make([]byte, geo.presenceBytes)
		presence[0] = 0x01
		if _, err := client.PutChunkState(ctx, 7, 5, ChunkStateInput{Payload: payload, Presence: presence}, PutOptions{}); err != nil {
			t.Fatalf("PutChunkState: %v", err)
		}

		want := make([]byte, geo.payloadBytes)
		for bit := range geo.blockBits {
			want[bit/8] |= 1 << (bit % 8)
		}
		state, err := client.GetChunkState(ctx, 7, 5, GetOptions{})
		if err != nil {
			t.Fatalf("GetChunkState: %v", err)
		}
		if !state.Exists || !bytes.Equal(state.Payload, want) || !bytes.Equal(state.Presence, presence) {
			t.Fatalf("got payload %x presence %x, want %x and %x", state.Payload, state.Presence, want, presence)
		}

		x, y := geo.blockOrigin(7, 5)
		values, err := client.MGet(ctx, []BlockRef{{X: x, Y: y}, {X: x + 1, Y: y}})
		if err != nil {
			t.Fatalf("MGet: %v", err)
		}
		if values[0] != (BlockState{Exists: true, Bits: strings.Repeat("1", geo.blockBits)}) || values[1].Exists {
			t.Fatalf("got %+v", values)
		}
	})

	t.Run("empty presence leaves the chunk absent", func(t *testing.T) {
		state := ChunkStateInput{Payload: denseBytes(geo.payloadBytes, 3), Presence: make([]byte, geo.presenceBytes)}
		if _, err := client.PutChunkState(ctx, 8, 5, state, PutOptions{}); err != nil {
			t.Fatalf("PutChunkState: %v", err)
		}
		exists, err := client.ChunkExists(ctx, 8, 5)
		if err != nil || exists {
			t.Fatalf("ChunkExists: %v, %v; want absent", exists, err)
		}
		read, err := client.GetChunkState(ctx, 8, 5, GetOptions{})
		if err != nil || read.Exists || !bytes.Equal(read.Payload, make([]byte, geo.payloadBytes)) {
			t.Fatalf("GetChunkState: %+v, %v; want an absent chunk of zeros", read, err)
		}
	})

	t.Run("client-side size errors keep the connection usable", func(t *testing.T) {
		payload := denseBytes(geo.payloadBytes, 5)
		if _, err := client.PutChunk(ctx, 9, 5, payload[:1], PutOptions{}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("short payload: got %v, want ErrProtocol", err)
		}
		if _, err := client.PutChunk(ctx, 9, 5, append(payload, 0), PutOptions{ZRLE: true}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("long payload: got %v, want ErrProtocol", err)
		}
		if _, err := client.PutChunkState(ctx, 9, 5, ChunkStateInput{Payload: payload, Presence: []byte{1}}, PutOptions{}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("short presence: got %v, want ErrProtocol", err)
		}
		if _, err := client.PutChunk(ctx, 9, 5, payload, PutOptions{}); err != nil {
			t.Fatalf("PutChunk after the rejected writes: %v", err)
		}
		if got, err := client.GetChunk(ctx, 9, 5, GetOptions{}); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("GetChunk: %v", err)
		}
	})

	t.Run("zrle reads and writes", func(t *testing.T) {
		cases := map[string]ChunkStateInput{
			"sparse": {Payload: geo.sparsePayload(7), Presence: append([]byte{0x0f}, make([]byte, geo.presenceBytes-1)...)},
			"dense":  {Payload: denseBytes(geo.payloadBytes, 9), Presence: allOnes(geo.presenceBytes)},
		}
		cx := int64(20)
		for name, state := range cases {
			t.Run(name, func(t *testing.T) {
				cx++
				if _, err := client.PutChunkState(ctx, cx, 5, state, PutOptions{ZRLE: true}); err != nil {
					t.Fatalf("PutChunkState: %v", err)
				}
				raw, err := client.GetChunkState(ctx, cx, 5, GetOptions{})
				if err != nil {
					t.Fatalf("GetChunkState: %v", err)
				}
				compressed, err := client.GetChunkState(ctx, cx, 5, GetOptions{ZRLE: true})
				if err != nil {
					t.Fatalf("GetChunkState ZRLE: %v", err)
				}
				if !bytes.Equal(compressed.Payload, raw.Payload) || !bytes.Equal(compressed.Presence, raw.Presence) {
					t.Fatal("the ZRLE read differs from the raw read")
				}
				// Only present blocks keep their payload bits; the sparse
				// payload's bits all lie in blocks 0..3 or are absent.
				if !bytes.Equal(raw.Presence, state.Presence) {
					t.Fatalf("got presence %x, want %x", raw.Presence, state.Presence)
				}

				cx++
				if _, err := client.PutChunk(ctx, cx, 5, state.Payload, PutOptions{ZRLE: true}); err != nil {
					t.Fatalf("PutChunk: %v", err)
				}
				for _, zrle := range []bool{false, true} {
					got, err := client.GetChunk(ctx, cx, 5, GetOptions{ZRLE: zrle})
					if err != nil || !bytes.Equal(got, state.Payload) {
						t.Fatalf("GetChunk zrle=%v: %v; payload differs", zrle, err)
					}
				}
			})
		}
	})

	t.Run("conditional writes", func(t *testing.T) {
		version, err := client.ChunkVersion(ctx, 30, 5)
		if err != nil {
			t.Fatalf("ChunkVersion: %v", err)
		}

		state := ChunkStateInput{Payload: denseBytes(geo.payloadBytes, 1), Presence: allOnes(geo.presenceBytes)}
		result, err := client.PutChunkState(ctx, 30, 5, state, PutOptions{IfVersion: &version})
		if err != nil {
			t.Fatalf("PutChunkState: %v", err)
		}
		if !result.OK || result.Version == version {
			t.Fatalf("got %+v, want the write to apply with a new version (was %d)", result, version)
		}

		// The stale version is rejected and the chunk is unchanged.
		other := ChunkStateInput{Payload: denseBytes(geo.payloadBytes, 2), Presence: allOnes(geo.presenceBytes)}
		for _, zrle := range []bool{false, true} {
			stale, err := client.PutChunkState(ctx, 30, 5, other, PutOptions{IfVersion: &version, ZRLE: zrle})
			if err != nil {
				t.Fatalf("PutChunkState: %v", err)
			}
			if stale.OK || stale.Version != result.Version {
				t.Fatalf("got %+v, want a mismatch reporting version %d", stale, result.Version)
			}
		}
		if _, err := client.PutChunk(ctx, 30, 5, other.Payload, PutOptions{IfVersion: &version}); err != nil {
			t.Fatalf("PutChunk: %v", err)
		}
		read, err := client.GetChunkState(ctx, 30, 5, GetOptions{})
		if err != nil || !bytes.Equal(read.Payload, state.Payload) {
			t.Fatalf("GetChunkState: %v; a rejected write changed the chunk", err)
		}

		// A write that does not change the chunk keeps its version.
		same, err := client.PutChunkState(ctx, 30, 5, state, PutOptions{IfVersion: &result.Version})
		if err != nil || !same.OK || same.Version != result.Version {
			t.Fatalf("got %+v, %v; want the version %d unchanged", same, err, result.Version)
		}
	})
}

func TestIntegrationWorldReads(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	populated := []CoordPair{{CX: 0, CY: 0}, {CX: 0, CY: 1}, {CX: 1, CY: 0}, {CX: 2, CY: 2}}
	for i, coordinate := range populated {
		state := ChunkStateInput{Payload: geo.sparsePayload(byte(i)), Presence: make([]byte, geo.presenceBytes)}
		state.Presence[i] = 0xff
		if i%2 == 1 {
			state = ChunkStateInput{Payload: denseBytes(geo.payloadBytes, byte(i)), Presence: allOnes(geo.presenceBytes)}
		}
		if _, err := client.PutChunkState(ctx, coordinate.CX, coordinate.CY, state, PutOptions{}); err != nil {
			t.Fatalf("PutChunkState(%d,%d): %v", coordinate.CX, coordinate.CY, err)
		}
	}

	checkEntries := func(t *testing.T, entries []RangeEntry, want []CoordPair) {
		t.Helper()
		if len(entries) != len(want) {
			t.Fatalf("got %d entries, want %d", len(entries), len(want))
		}
		for i, entry := range entries {
			if (CoordPair{CX: entry.CX, CY: entry.CY}) != want[i] {
				t.Fatalf("entry %d: got (%d,%d), want %+v", i, entry.CX, entry.CY, want[i])
			}
			state, err := client.GetChunkState(ctx, entry.CX, entry.CY, GetOptions{})
			if err != nil {
				t.Fatalf("GetChunkState: %v", err)
			}
			if !bytes.Equal(entry.Payload, state.Payload) || !bytes.Equal(entry.Presence, state.Presence) {
				t.Fatalf("chunk (%d,%d): the range entry differs from GetChunkState", entry.CX, entry.CY)
			}
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
		if !slices.Equal(seen, populated) {
			t.Fatalf("got %+v, want %+v", seen, populated)
		}
	})

	for _, zrle := range []bool{false, true} {
		opts := GetOptions{ZRLE: zrle}
		name := "raw"
		if zrle {
			name = "zrle"
		}

		t.Run("range returns populated chunks only/"+name, func(t *testing.T) {
			entries, err := client.ChunkRange(ctx, 0, 0, 1, 1, opts)
			if err != nil {
				t.Fatalf("ChunkRange: %v", err)
			}
			checkEntries(t, entries, populated[:3])
		})

		t.Run("radius returns the disc/"+name, func(t *testing.T) {
			entries, err := client.ChunkRadius(ctx, 0, 0, 1, opts)
			if err != nil {
				t.Fatalf("ChunkRadius: %v", err)
			}
			checkEntries(t, entries, populated[:3])

			entries, err = client.ChunkRadius(ctx, 1, 1, 2, opts)
			if err != nil {
				t.Fatalf("ChunkRadius: %v", err)
			}
			checkEntries(t, entries, populated)
		})

		t.Run("empty range/"+name, func(t *testing.T) {
			entries, err := client.ChunkRange(ctx, 500, 500, 501, 501, opts)
			if err != nil {
				t.Fatalf("ChunkRange: %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("got %d entries, want none", len(entries))
			}
		})
	}
}

func TestIntegrationVersionedMutations(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, nil)
	geo := readGeometry(t, client)
	ctx := t.Context()

	pattern := geo.blockPattern(0)

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

		state, err := client.Get(ctx, x, y)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if state != (BlockState{Exists: true, Bits: pattern}) {
			t.Fatalf("got %+v, want the batched write", state)
		}
	})

	t.Run("batch with the current version applies", func(t *testing.T) {
		x, y := geo.blockOrigin(24, 24)
		version, err := client.ChunkVersion(ctx, 24, 24)
		if err != nil {
			t.Fatalf("ChunkVersion: %v", err)
		}
		result, err := client.ChunkBatchIfVersion(ctx, 24, 24, version, []BatchOperation{SetOp(x, y, pattern)})
		if err != nil || !result.OK || result.Version == version {
			t.Fatalf("ChunkBatchIfVersion: %+v, %v", result, err)
		}
	})

	t.Run("batch with stale version is rejected", func(t *testing.T) {
		x, y := geo.blockOrigin(22, 22)
		version, err := client.ChunkVersion(ctx, 22, 22)
		if err != nil {
			t.Fatalf("ChunkVersion: %v", err)
		}
		applied, err := client.ChunkBatch(ctx, 22, 22, []BatchOperation{SetOp(x, y, pattern)})
		if err != nil {
			t.Fatalf("ChunkBatch: %v", err)
		}

		result, err := client.ChunkBatchIfVersion(ctx, 22, 22, version, []BatchOperation{UnsetOp(x, y)})
		if err != nil {
			t.Fatalf("ChunkBatchIfVersion: %v", err)
		}
		if result.OK || result.Version != applied.Version {
			t.Fatalf("got %+v, want a mismatch reporting version %d", result, applied.Version)
		}

		// The rejected batch must have left the block untouched.
		state, err := client.Get(ctx, x, y)
		if err != nil {
			t.Fatalf("Get: %v", err)
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
}

func TestIntegrationAuthFailure(t *testing.T) {
	server := startServer(t, serverConfig{})

	cases := map[string]struct {
		uri  string
		code string
	}{
		"wrong token":   {strings.Replace(server.uri, testToken, "wrong-token", 1), "AUTH_FAILED"},
		"missing token": {strings.Replace(server.uri, testToken+"@", "", 1), "AUTH_REQUIRED"},
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
		if value != (BlockState{Exists: true, Bits: pattern}) {
			t.Fatalf("block %d: got %+v, want %q", i, value, pattern)
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
	var geo serverGeometry
	if err := pool.WithClient(ctx, func(ctx context.Context, client *Client) error {
		geo = readGeometry(t, client)
		return client.Ping(ctx)
	}); err != nil {
		t.Fatalf("WithClient: %v", err)
	}
	pattern := geo.blockPattern(0)

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
		state, err := pool.Get(ctx, int64(i), 60)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if state != (BlockState{Exists: true, Bits: pattern}) {
			t.Fatalf("block %d: got %+v, want %q", i, state, pattern)
		}
	}

	payload := denseBytes(geo.payloadBytes, 4)
	if _, err := pool.PutChunk(ctx, 3, 3, payload, PutOptions{ZRLE: true}); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if got, err := pool.GetChunk(ctx, 3, 3, GetOptions{ZRLE: true}); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("GetChunk: %v", err)
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
		if info := client.ServerInfo(); info == nil || info.Protocol != 2 {
			t.Fatalf("got ServerInfo %+v", info)
		}

		geo := readGeometry(t, client)
		state := ChunkStateInput{Payload: geo.sparsePayload(1), Presence: allOnes(geo.presenceBytes)}
		if _, err := client.PutChunkState(ctx, 1, 1, state, PutOptions{ZRLE: true}); err != nil {
			t.Fatalf("PutChunkState: %v", err)
		}
		read, err := client.GetChunkState(ctx, 1, 1, GetOptions{ZRLE: true})
		if err != nil || !bytes.Equal(read.Payload, state.Payload) || !bytes.Equal(read.Presence, state.Presence) {
			t.Fatalf("GetChunkState: %v; the state did not round-trip", err)
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

func TestIntegrationTables(t *testing.T) {
	// Up to four connections are open at once.
	server := startServer(t, serverConfig{workers: 6})
	client := connectIntegration(t, server, nil)
	ctx := t.Context()

	if client.CurrentTable() != "default" {
		t.Fatalf("got %q, want default", client.CurrentTable())
	}
	defaultGeo := readGeometry(t, client)
	if err := client.CreateTable(ctx, "terrain", TableSpec{
		BlockBits: 4, ChunkWidthBlocks: 8, ChunkHeightBlocks: 2,
		Options: TableOptions{DurabilityMode: "fsync-wal"},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	var serverErr *Error
	if err := client.CreateTable(ctx, "terrain", TableSpec{BlockBits: 4}); !errors.As(err, &serverErr) ||
		serverErr.ServerCode != CodeTableExists {
		t.Fatalf("got %v, want %s", err, CodeTableExists)
	}
	names, err := client.Tables(ctx)
	if err != nil || !slices.Equal(names, []string{"default", "terrain"}) {
		t.Fatalf("Tables: %q, %v", names, err)
	}
	info, err := client.TableInfo(ctx, "terrain")
	if err != nil {
		t.Fatalf("TableInfo: %v", err)
	}
	if info.BlockBits != 4 || info.ChunkWidthBlocks != 8 || info.ChunkHeightBlocks != 2 ||
		info.LargeChunkWidthChunks != 8 || info.Options.DurabilityMode != "fsync-wal" ||
		len(info.StoreID) != 32 {
		t.Fatalf("got %+v", info)
	}

	// A handle is its own connection on the table, with the table's geometry
	// from HELLO: 8x2 blocks of 4 bits are 8 payload and 2 presence bytes.
	terrain, err := client.Table(ctx, "terrain")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	defer terrain.Close()
	if hello := terrain.ServerInfo(); hello == nil || hello.Table == nil || hello.Table.Name != "terrain" ||
		hello.Table.StoreID != info.StoreID {
		t.Fatalf("got ServerInfo %+v", hello)
	}
	if geo := readGeometry(t, terrain); geo.payloadBytes != 8 || geo.presenceBytes != 2 {
		t.Fatalf("got geometry %+v", geo)
	}
	if err := terrain.Set(ctx, 1, 1, "1010"); err != nil {
		t.Fatalf("Set on terrain: %v", err)
	}
	if err := client.Set(ctx, 1, 1, defaultGeo.blockPattern(0)); err != nil {
		t.Fatalf("Set on default: %v", err)
	}
	if block, err := terrain.Get(ctx, 1, 1); err != nil || block != (BlockState{Exists: true, Bits: "1010"}) {
		t.Fatalf("Get on terrain: %+v, %v", block, err)
	}

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if _, err := terrain.PutChunk(ctx, 3, 3, payload, PutOptions{}); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if got, err := terrain.GetChunk(ctx, 3, 3, GetOptions{}); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("GetChunk: %v, %v", got, err)
	}
	// The default table's sizes do not fit terrain.
	if _, err := terrain.PutChunk(ctx, 3, 3, make([]byte, defaultGeo.payloadBytes), PutOptions{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a size error", err)
	}

	// The URI path selects the table too.
	sky := strings.TrimSuffix(server.uri, "/") + "/terrain"
	byPath, err := Connect(ctx, Options{URI: sky})
	if err != nil {
		t.Fatalf("Connect by path: %v", err)
	}
	if hello := byPath.ServerInfo(); hello.Table == nil || hello.Table.Name != "terrain" {
		t.Fatalf("got ServerInfo %+v", hello)
	}
	if block, err := byPath.Get(ctx, 1, 1); err != nil || block.Bits != "1010" {
		t.Fatalf("Get by path: %+v, %v", block, err)
	}
	_ = byPath.Close()

	// An unknown table fails the handshake.
	if _, err := Connect(ctx, Options{URI: server.uri, Table: "nope"}); !errors.As(err, &serverErr) ||
		serverErr.ServerCode != CodeNoTable {
		t.Fatalf("got %v, want %s for an unknown table", err, CodeNoTable)
	}

	// Use switches the table and its geometry.
	if _, err := client.Use(ctx, "terrain"); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if block, err := client.Get(ctx, 1, 1); err != nil || block.Bits != "1010" {
		t.Fatalf("Get after Use: %+v, %v", block, err)
	}
	if got, err := client.GetChunk(ctx, 3, 3, GetOptions{ZRLE: true}); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("GetChunk after Use: %v, %v", got, err)
	}
	if _, err := client.PutChunkState(ctx, 4, 4, ChunkStateInput{Payload: payload, Presence: []byte{0xff, 0xff}}, PutOptions{}); err != nil {
		t.Fatalf("PutChunkState after Use: %v", err)
	}
	if err := client.SetTableOptions(ctx, "terrain", TableOptions{CheckpointUpdates: 3}); err != nil {
		t.Fatalf("SetTableOptions: %v", err)
	}
	if info, err := client.TableInfo(ctx, "terrain"); err != nil || info.Options.CheckpointUpdates != 3 {
		t.Fatalf("TableInfo after SetTableOptions: %+v, %v", info, err)
	}

	// A drop reaches every connection on the table.
	if _, err := client.Use(ctx, "default"); err != nil {
		t.Fatalf("Use default: %v", err)
	}
	if err := client.DropTable(ctx, "terrain"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := terrain.Get(ctx, 1, 1); !errors.As(err, &serverErr) || serverErr.ServerCode != CodeNoTable {
		t.Fatalf("got %v, want %s", err, CodeNoTable)
	}
	if _, err := Connect(ctx, Options{URI: sky}); !errors.As(err, &serverErr) ||
		serverErr.ServerCode != CodeNoTable {
		t.Fatalf("got %v, want %s for a dropped table in the URI", err, CodeNoTable)
	}

	// Without a default table, a connection that names none has no table
	// until Use selects one.
	if err := client.CreateTable(ctx, "sea", TableSpec{BlockBits: 8}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := client.DropTable(ctx, "default"); err != nil {
		t.Fatalf("DropTable default: %v", err)
	}
	bare := connectIntegration(t, server, nil)
	if hello := bare.ServerInfo(); hello == nil || hello.Table != nil {
		t.Fatalf("got ServerInfo %+v, want no table", hello)
	}
	if _, err := bare.GetChunk(ctx, 0, 0, GetOptions{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a request error without a table", err)
	}
	if _, err := bare.Use(ctx, "sea"); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if got, err := bare.GetChunk(ctx, 0, 0, GetOptions{}); err != nil || len(got) != 16*16 {
		t.Fatalf("GetChunk after Use: %d bytes, %v", len(got), err)
	}
}

// Use while chunk writes are pipelined: each write runs entirely on the old
// or the new table, so it either succeeds or fails the client-side size
// check; none reaches the server framed for the wrong table, which would
// make it close the connection under every request in flight.
func TestIntegrationUseIsExclusiveWithPipelinedChunkWrites(t *testing.T) {
	server := startServer(t, serverConfig{})
	client := connectIntegration(t, server, func(o *Options) { o.PipelineDepth = 8 })
	ctx := t.Context()
	if err := client.CreateTable(ctx, "small", TableSpec{BlockBits: 1, ChunkWidthBlocks: 2, ChunkHeightBlocks: 2}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	payload := make([]byte, readGeometry(t, client).payloadBytes)
	for i := range payload {
		payload[i] = byte(i)
	}

	const writers = 8
	var group sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 1024)
	for w := range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := client.PutChunk(ctx, int64(w), i%16, payload, PutOptions{}); err != nil {
					errs <- err
				}
			}
		}()
	}
	for round := range 20 {
		table := "small"
		if round%2 == 1 {
			table = "default"
		}
		if _, err := client.Use(ctx, table); err != nil {
			t.Fatalf("Use(%s): %v", table, err)
		}
	}
	close(stop)
	group.Wait()
	close(errs)

	for err := range errs {
		var typed *Error
		if !errors.As(err, &typed) || typed.Phase != PhaseRequest {
			t.Fatalf("a pipelined write failed with %v, want only client-side size errors", err)
		}
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping after the switches: %v", err)
	}
}
