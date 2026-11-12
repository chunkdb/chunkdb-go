package chunkdb

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// loginCapped logs in with opts over a TLS connection capped at maxVersion,
// as dial does after its handshake, and runs PING. It returns the plaintext
// the client sent, which holds the SCRAM client-first message. The server
// offers TLS 1.3 otherwise, and Options has no TLS version cap.
func loginCapped(t *testing.T, opts Options, maxVersion uint16) (string, error) {
	t.Helper()
	opts.ConnectTimeout, opts.CommandTimeout = 5*time.Second, 5*time.Second
	client, err := NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	config, err := client.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	config.MaxVersion = maxVersion
	netConn, err := tls.Dial("tcp", client.opts.uri.Address(), config)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if got := netConn.ConnectionState().Version; got != maxVersion {
		t.Fatalf("got TLS version %#x, want %#x", got, maxVersion)
	}
	var sent bytes.Buffer
	established := &conn{client: client, netConn: netConn, reader: bufio.NewReader(netConn),
		writer: bufio.NewWriter(io.MultiWriter(netConn, &sent))}
	go established.readLoop()
	defer func() { _ = established.shutdown(ErrClosed) }()
	if err := client.helloOn(t.Context(), established); err != nil {
		return sent.String(), err
	}
	if _, err := client.execOn(t.Context(), established, "PING", "PING", nil); err != nil {
		t.Fatalf("PING: %v", err)
	}
	return sent.String(), nil
}

func TestIntegrationChannelBinding(t *testing.T) {
	server := startServer(t, serverConfig{tls: true, workers: 4})
	ctx := t.Context()
	admin := connectIntegration(t, server, func(o *Options) { o.CA = server.caPEM })
	if info := admin.ServerInfo(); !info.Supports("channel_binding") {
		t.Fatalf("got features %q, want channel_binding", info.Features)
	}
	as := func(user, password string, mode ChannelBinding) Options {
		return Options{URI: server.uriAs(user, password), CA: server.caPEM, ChannelBinding: mode}
	}

	t.Run("TLS 1.3 binds with tls-exporter", func(t *testing.T) {
		for _, mode := range []ChannelBinding{"", ChannelBindingAuto, ChannelBindingRequire} {
			sent, err := loginCapped(t, as(testAdmin, testAdminPassword, mode), tls.VersionTLS13)
			if err != nil || !strings.Contains(sent, "\r\np=tls-exporter,,n=admin,r=") {
				t.Fatalf("%q: got %v, sent %q, want a login bound with tls-exporter", mode, err, sent)
			}
		}
	})

	t.Run("TLS 1.2 binds with tls-server-end-point", func(t *testing.T) {
		sent, err := loginCapped(t, as(testAdmin, testAdminPassword, ""), tls.VersionTLS12)
		if err != nil || !strings.Contains(sent, "\r\np=tls-server-end-point,,n=admin,r=") {
			t.Fatalf("got %v, sent %q, want a login bound with tls-server-end-point", err, sent)
		}
	})

	t.Run("off logs in unbound", func(t *testing.T) {
		sent, err := loginCapped(t, as(testAdmin, testAdminPassword, ChannelBindingOff), tls.VersionTLS13)
		if err != nil || !strings.Contains(sent, "\r\nn,,n=admin,r=") || !strings.Contains(sent, "\r\nc=biws,r=") {
			t.Fatalf("got %v, sent %q, want an unbound login", err, sent)
		}
	})

	t.Run("a user who requires channel binding", func(t *testing.T) {
		if err := admin.CreateUser(ctx, "bound", "bound-password", CreateUserOptions{RequiresChannelBinding: true}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
		_ = connectIntegration(t, server, func(o *Options) { *o = as("bound", "bound-password", "") }).Close()
		if _, err := loginCapped(t, as("bound", "bound-password", ""), tls.VersionTLS12); err != nil {
			t.Fatalf("bound login on TLS 1.2: %v", err)
		}
		_, err := Connect(ctx, as("bound", "bound-password", ChannelBindingOff))
		if !errors.Is(err, ErrAuth) || !isServerCode(err, CodeAuthFailed) {
			t.Fatalf("got %v, want AUTH_FAILED for an unbound login", err)
		}

		users, err := admin.Users(ctx)
		if err != nil {
			t.Fatalf("Users: %v", err)
		}
		requires := make(map[string]bool)
		for _, user := range users {
			requires[user.Name] = user.RequiresChannelBinding
		}
		if !requires["bound"] || requires[testAdmin] {
			t.Fatalf("got %v, want only bound to require channel binding", requires)
		}

		if err := admin.SetRequiresChannelBinding(ctx, "bound", false); err != nil {
			t.Fatalf("SetRequiresChannelBinding: %v", err)
		}
		_ = connectIntegration(t, server, func(o *Options) { *o = as("bound", "bound-password", ChannelBindingOff) }).Close()
	})

	t.Run("require over plain TCP fails before connecting", func(t *testing.T) {
		plain := startServer(t, serverConfig{})
		_, err := Connect(ctx, Options{URI: plain.uri, ChannelBinding: ChannelBindingRequire, ConnectTimeout: 5 * time.Second})
		if !errors.Is(err, ErrConnection) || !strings.Contains(err.Error(), "needs TLS") {
			t.Fatalf("got %v, want an option error", err)
		}
	})
}

func TestIntegrationChannelBindingRequiredByServer(t *testing.T) {
	server := startServer(t, serverConfig{tls: true, args: []string{"--auth-channel-binding", "require"}})

	_, err := Connect(t.Context(), Options{URI: server.uri, CA: server.caPEM, ChannelBinding: ChannelBindingOff, ConnectTimeout: 5 * time.Second})
	var typed *Error
	if !errors.Is(err, ErrAuth) || errors.Is(err, ErrProtocol) || !errors.As(err, &typed) ||
		typed.ServerCode != CodeAuthRequired || typed.Metadata.Reason != "channel_binding" {
		t.Fatalf("got %v, want the server's AUTH_REQUIRED reason=channel_binding", err)
	}

	client := connectIntegration(t, server, func(o *Options) { o.CA = server.caPEM })
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if _, err := loginCapped(t, Options{URI: server.uri, CA: server.caPEM}, tls.VersionTLS12); err != nil {
		t.Fatalf("bound login on TLS 1.2: %v", err)
	}
}

// startTLSRelay ends TLS with its own certificate and relays each connection
// over a new TLS connection to server. It returns the relay's address and the
// PEM bytes that trust its certificate.
func startTLSRelay(t *testing.T, server *testServer) (string, []byte) {
	t.Helper()
	certificate, caPEM := ecdsaCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(server.caPEM)
	var relays sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		relays.Wait()
	})
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			relays.Add(1)
			go func() {
				defer relays.Done()
				defer client.Close()
				upstream, err := tls.Dial("tcp", server.address(), &tls.Config{RootCAs: roots})
				if err != nil {
					return
				}
				defer upstream.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
				go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return listener.Addr().String(), caPEM
}

func TestIntegrationChannelBindingRelay(t *testing.T) {
	server := startServer(t, serverConfig{tls: true})
	address, relayPEM := startTLSRelay(t, server)
	relay, _ := ParseURI(server.uri)
	relayAddress, _ := net.ResolveTCPAddr("tcp", address)
	relay.Port = relayAddress.Port
	relayURI := relay.String()

	// The client accepts the relay's certificate; the bound login still fails.
	_, err := Connect(t.Context(), Options{URI: relayURI, CA: relayPEM, ConnectTimeout: 5 * time.Second, CommandTimeout: 5 * time.Second})
	var typed *Error
	if !errors.Is(err, ErrAuth) || !errors.As(err, &typed) || typed.ServerCode != CodeAuthFailed || typed.Metadata.Reason != "channel_binding" {
		t.Fatalf("got %v, want AUTH_FAILED reason=channel_binding", err)
	}

	client, err := Connect(t.Context(), Options{URI: relayURI, CA: relayPEM, ChannelBinding: ChannelBindingOff,
		ConnectTimeout: 5 * time.Second, CommandTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Connect with channel binding off: %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
