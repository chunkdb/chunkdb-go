package chunkdb

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// testCertificate is a self-signed server certificate for 127.0.0.1 with key,
// and the PEM bytes that trust it.
func testCertificate(t *testing.T, key crypto.Signer) (tls.Certificate, []byte) {
	t.Helper()
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
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, key.Public(), key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func ecdsaCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return testCertificate(t, key)
}

// boundServerFinal plays the server side of a login bound with binding: it
// checks the client-final message and returns the server signature.
func boundServerFinal(t *testing.T, login scramLogin, binding scramBinding, final string) string {
	t.Helper()
	fake := &fakeLogin{user: rfcUser, password: rfcPassword}
	serverFirst := "r=" + rfcClientNonce + fakeServerNonce + ",s=" + base64.StdEncoding.EncodeToString([]byte("fake-salt-16byte")) + ",i=4096"
	attribute := "c=" + base64.StdEncoding.EncodeToString(append([]byte(binding.header()), binding.data...))
	signature, ok := fake.check(login.firstBare, serverFirst, attribute, final)
	if !ok {
		t.Fatalf("the server refused client-final %q with %s", final, attribute)
	}
	return signature
}

func TestScramChannelBinding(t *testing.T) {
	serverFirst := "r=" + rfcClientNonce + fakeServerNonce + ",s=" + base64.StdEncoding.EncodeToString([]byte("fake-salt-16byte")) + ",i=4096"
	data := bytes.Repeat([]byte{0xa5}, 32)
	cases := []struct {
		binding scramBinding
		first   string
		c       string
	}{
		{scramBinding{}, "n,,", "c=biws"},
		{scramBinding{kind: "tls-exporter", data: data}, "p=tls-exporter,,",
			"c=" + base64.StdEncoding.EncodeToString(append([]byte("p=tls-exporter,,"), data...))},
		{scramBinding{kind: "tls-server-end-point", data: data}, "p=tls-server-end-point,,",
			"c=" + base64.StdEncoding.EncodeToString(append([]byte("p=tls-server-end-point,,"), data...))},
	}
	finals := make(map[string]bool)
	signatures := make(map[string]bool)
	for _, testCase := range cases {
		login := newScramLogin(rfcUser, rfcClientNonce, testCase.binding)
		if got, want := login.clientFirst(), testCase.first+"n=user,r="+rfcClientNonce; got != want {
			t.Fatalf("got client-first %q, want %q", got, want)
		}
		final, signature, err := login.clientFinal(rfcPassword, serverFirst)
		if err != nil {
			t.Fatalf("clientFinal: %v", err)
		}
		if !strings.HasPrefix(final, testCase.c+",r="+rfcClientNonce+fakeServerNonce+",p=") {
			t.Fatalf("got client-final %q, want it to start with %s", final, testCase.c)
		}
		// The proof and the server signature cover the c= attribute.
		if want := boundServerFinal(t, login, testCase.binding, final); signature != want {
			t.Fatalf("got server signature %q, want %q", signature, want)
		}
		finals[final], signatures[signature] = true, true
	}
	if len(finals) != len(cases) || len(signatures) != len(cases) {
		t.Fatalf("got %d proofs and %d signatures for %d bindings", len(finals), len(signatures), len(cases))
	}

	// Other binding data gives another proof.
	login := newScramLogin(rfcUser, rfcClientNonce, scramBinding{kind: "tls-exporter", data: data})
	final, _, _ := login.clientFinal(rfcPassword, serverFirst)
	other := scramBinding{kind: "tls-exporter", data: bytes.Repeat([]byte{0x5a}, 32)}
	attribute := "c=" + base64.StdEncoding.EncodeToString(append([]byte(other.header()), other.data...))
	if _, ok := (&fakeLogin{user: rfcUser, password: rfcPassword}).check(login.firstBare, serverFirst, attribute, final); ok {
		t.Fatal("a login bound to other data was accepted")
	}
}

func TestChannelBindingOptions(t *testing.T) {
	cases := []struct {
		opts Options
		want ChannelBinding
		err  string
	}{
		{Options{URI: "chunks://bot:pw@127.0.0.1/"}, ChannelBindingAuto, ""},
		{Options{URI: "chunk://127.0.0.1/", ChannelBinding: ChannelBindingAuto}, ChannelBindingAuto, ""},
		{Options{URI: "chunks://bot:pw@127.0.0.1/", ChannelBinding: ChannelBindingOff}, ChannelBindingOff, ""},
		{Options{URI: "chunks://bot:pw@127.0.0.1/", ChannelBinding: ChannelBindingRequire}, ChannelBindingRequire, ""},
		{Options{URI: "chunk://bot:pw@127.0.0.1/", TLS: true, ChannelBinding: ChannelBindingRequire}, ChannelBindingRequire, ""},
		{Options{URI: "chunk://bot:pw@127.0.0.1/", ChannelBinding: ChannelBindingRequire}, "", "needs TLS"},
		{Options{URI: "chunks://127.0.0.1/", ChannelBinding: ChannelBindingRequire}, "", "needs a user"},
		{Options{URI: "chunks://bot:pw@127.0.0.1/", ChannelBinding: "AUTO"}, "", `invalid ChannelBinding "AUTO"`},
		{Options{URI: "chunks://bot:pw@127.0.0.1/", ChannelBinding: "y"}, "", `invalid ChannelBinding "y"`},
	}
	for _, testCase := range cases {
		resolved, err := resolveOptions(testCase.opts)
		if testCase.err != "" {
			if !errors.Is(err, ErrConnection) || !strings.Contains(err.Error(), testCase.err) {
				t.Fatalf("%+v: got %v, want an ErrConnection saying %q", testCase.opts, err, testCase.err)
			}
			continue
		}
		if err != nil || resolved.channelBinding != testCase.want {
			t.Fatalf("%+v: got %q, %v, want %q", testCase.opts, resolved.channelBinding, err, testCase.want)
		}
	}

	// require over plain TCP fails before the client connects.
	server := newFakeServer(t, withHello(genericHandler))
	_, err := Connect(t.Context(), Options{URI: server.uri("bot:secret"), ChannelBinding: ChannelBindingRequire})
	if !errors.Is(err, ErrConnection) || server.acceptedConns() != 0 {
		t.Fatalf("got %v and %d connections, want an option error before connecting", err, server.acceptedConns())
	}
}

func TestServerEndPointHash(t *testing.T) {
	for algorithm, want := range map[x509.SignatureAlgorithm]int{
		x509.MD5WithRSA: sha256.Size, x509.SHA1WithRSA: sha256.Size, x509.ECDSAWithSHA1: sha256.Size, x509.DSAWithSHA1: sha256.Size,
		x509.SHA256WithRSA: sha256.Size, x509.SHA256WithRSAPSS: sha256.Size, x509.ECDSAWithSHA256: sha256.Size,
		x509.SHA384WithRSA: sha512.Size384, x509.SHA384WithRSAPSS: sha512.Size384, x509.ECDSAWithSHA384: sha512.Size384,
		x509.SHA512WithRSA: sha512.Size, x509.SHA512WithRSAPSS: sha512.Size, x509.ECDSAWithSHA512: sha512.Size,
		x509.PureEd25519: 0, x509.MD2WithRSA: 0, x509.UnknownSignatureAlgorithm: 0,
	} {
		newHash := serverEndPointHash(algorithm)
		if got := 0; newHash != nil {
			got = newHash().Size()
			if got != want {
				t.Fatalf("%s: got a %d-byte hash, want %d", algorithm, got, want)
			}
		} else if want != 0 {
			t.Fatalf("%s: got no hash, want %d bytes", algorithm, want)
		}
	}
}

// tlsPair runs a TLS handshake between a client trusting caPEM and a server
// with certificate, capped at maxVersion, and returns both ends.
func tlsPair(t *testing.T, certificate tls.Certificate, caPEM []byte, maxVersion uint16) (client, server *tls.Conn) {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MaxVersion: maxVersion})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	accepted := make(chan *tls.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		serverConn := conn.(*tls.Conn)
		_ = serverConn.Handshake()
		accepted <- serverConn
	}()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	client, err = tls.Dial("tcp", listener.Addr().String(), &tls.Config{RootCAs: roots})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	server = <-accepted
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

func TestChannelBindingOf(t *testing.T) {
	certificate, caPEM := ecdsaCertificate(t)

	t.Run("TLS 1.3 uses tls-exporter", func(t *testing.T) {
		client, server := tlsPair(t, certificate, caPEM, tls.VersionTLS13)
		binding, err := channelBindingOf(ChannelBindingAuto, client)
		if err != nil {
			t.Fatalf("channelBindingOf: %v", err)
		}
		state := server.ConnectionState()
		want, _ := state.ExportKeyingMaterial("EXPORTER-Channel-Binding", nil, 32)
		if binding.kind != "tls-exporter" || binding.header() != "p=tls-exporter,," || !bytes.Equal(binding.data, want) {
			t.Fatalf("got %s %x, want tls-exporter %x", binding.kind, binding.data, want)
		}
		if required, err := channelBindingOf(ChannelBindingRequire, client); err != nil || !bytes.Equal(required.data, want) {
			t.Fatalf("require: got %x, %v", required.data, err)
		}
	})

	t.Run("TLS 1.2 uses tls-server-end-point", func(t *testing.T) {
		client, _ := tlsPair(t, certificate, caPEM, tls.VersionTLS12)
		binding, err := channelBindingOf(ChannelBindingAuto, client)
		if err != nil {
			t.Fatalf("channelBindingOf: %v", err)
		}
		want := sha256.Sum256(certificate.Certificate[0])
		if binding.kind != "tls-server-end-point" || binding.header() != "p=tls-server-end-point,," || !bytes.Equal(binding.data, want[:]) {
			t.Fatalf("got %s %x, want tls-server-end-point %x", binding.kind, binding.data, want)
		}
	})

	t.Run("off and plain TCP are not bound", func(t *testing.T) {
		client, _ := tlsPair(t, certificate, caPEM, tls.VersionTLS13)
		plain, other := net.Pipe()
		t.Cleanup(func() { _ = plain.Close(); _ = other.Close() })
		for _, check := range []struct {
			mode ChannelBinding
			conn net.Conn
		}{{ChannelBindingOff, client}, {ChannelBindingAuto, plain}, {ChannelBindingOff, plain}} {
			if binding, err := channelBindingOf(check.mode, check.conn); err != nil || binding.kind != "" || binding.header() != "n,," {
				t.Fatalf("%s: got %+v, %v, want n,,", check.mode, binding, err)
			}
		}
		if _, err := channelBindingOf(ChannelBindingRequire, plain); !errors.Is(err, ErrTLS) {
			t.Fatalf("require over plain TCP: got %v, want ErrTLS", err)
		}
	})

	t.Run("Ed25519 on TLS 1.2 has no tls-server-end-point", func(t *testing.T) {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		edCertificate, edPEM := testCertificate(t, key)
		client, _ := tlsPair(t, edCertificate, edPEM, tls.VersionTLS12)
		_, err = channelBindingOf(ChannelBindingAuto, client)
		if !errors.Is(err, ErrTLS) || !strings.Contains(err.Error(), "TLS 1.3") || !strings.Contains(err.Error(), "ChannelBinding to off") {
			t.Fatalf("got %v, want an ErrTLS naming TLS 1.3 and turning channel binding off", err)
		}
		if binding, err := channelBindingOf(ChannelBindingOff, client); err != nil || binding.kind != "" {
			t.Fatalf("off: got %+v, %v", binding, err)
		}
	})
}

func TestClientBoundLogin(t *testing.T) {
	certificate, caPEM := ecdsaCertificate(t)
	cases := []struct {
		name       string
		maxVersion uint16
		mode       ChannelBinding
		header     string
	}{
		{"TLS 1.3", tls.VersionTLS13, "", "p=tls-exporter,,"},
		{"TLS 1.2", tls.VersionTLS12, ChannelBindingRequire, "p=tls-server-end-point,,"},
		{"off", tls.VersionTLS13, ChannelBindingOff, "n,,"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			login := &fakeLogin{user: "bot", password: "secret", certificate: certificate.Certificate[0]}
			server := newTLSFakeServer(t, &tls.Config{Certificates: []tls.Certificate{certificate}, MaxVersion: testCase.maxVersion},
				login.handler(genericHandler))
			client := newTestClient(t, server, func(o *Options) {
				o.URI, o.TLS, o.CA, o.ChannelBinding = server.uri("bot:secret"), true, caPEM, testCase.mode
			})
			if err := client.Ping(t.Context()); err != nil {
				t.Fatalf("Ping: %v", err)
			}
			server.mu.Lock()
			first, final := string(server.params[0][0]), string(server.params[1][0])
			server.mu.Unlock()
			if !strings.HasPrefix(first, testCase.header+"n=bot,r=") {
				t.Fatalf("got client-first %q, want %s", first, testCase.header)
			}
			if testCase.header == "n,," && !strings.HasPrefix(final, "c=biws,") {
				t.Fatalf("got client-final %q, want c=biws", final)
			}
		})
	}

	// No tls-server-end-point for an Ed25519 certificate on TLS 1.2: the
	// client sends nothing and does not fall back to an unbound login.
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	edCertificate, edPEM := testCertificate(t, key)
	server := newTLSFakeServer(t, &tls.Config{Certificates: []tls.Certificate{edCertificate}, MaxVersion: tls.VersionTLS12},
		(&fakeLogin{user: "bot", password: "secret"}).handler(genericHandler))
	_, err = Connect(t.Context(), Options{URI: server.uri("bot:secret"), TLS: true, CA: edPEM, ConnectTimeout: 2 * time.Second})
	if !errors.Is(err, ErrTLS) || !strings.Contains(err.Error(), "TLS 1.3") {
		t.Fatalf("got %v, want ErrTLS naming TLS 1.3", err)
	}
	if got := server.commands(); len(got) != 0 {
		t.Fatalf("got statements %q, want none", got)
	}
	waitForFinishedConns(t, server, 1)
}

func TestClientHelloKeepsChannelBindingAuthRequired(t *testing.T) {
	server := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) {
		writeServerError(conn, "ERR AUTH_REQUIRED reason=channel_binding this server takes only logins bound to their TLS connection")
	})
	_, err := Connect(t.Context(), Options{URI: server.uri("bot:secret")})
	var typed *Error
	if !errors.Is(err, ErrAuth) || errors.Is(err, ErrProtocol) || !errors.As(err, &typed) ||
		typed.ServerCode != CodeAuthRequired || typed.Metadata.Reason != "channel_binding" {
		t.Fatalf("got %v, want the server's AUTH_REQUIRED reason=channel_binding", err)
	}
}
