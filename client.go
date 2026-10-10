package chunkdb

import (
	"bufio"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
)

// protocolVersion is the chunkdb protocol this client speaks.
const protocolVersion = 3

type resolvedOptions struct {
	uri            URI
	connectTimeout time.Duration
	commandTimeout time.Duration
	tls            bool
	tlsInsecure    bool
	tlsServerName  string
	ca             []byte
	cert           []byte
	key            []byte
	pipelineDepth  int
	table          string
	// verifierIterations is the PBKDF2 iteration count of new verifiers.
	verifierIterations int
}

// resolveTimeout maps the [Options] convention onto an internal duration where
// zero means "no client-side deadline".
func resolveTimeout(d time.Duration) time.Duration {
	switch {
	case d == 0:
		return DefaultTimeout
	case d < 0:
		return 0
	default:
		return d
	}
}

func resolveOptions(opts Options) (resolvedOptions, error) {
	var parsed URI
	if opts.URI != "" {
		var err error
		if parsed, err = ParseURI(opts.URI); err != nil {
			return resolvedOptions{}, err
		}
	}

	port := cmp.Or(opts.Port, parsed.Port, DefaultPort)
	if port <= 0 || port > 65535 {
		return resolvedOptions{}, connectionErrorf("", nil, "invalid port: %d", port)
	}

	user := cmp.Or(opts.User, parsed.User)
	password := cmp.Or(opts.Password, parsed.Password)
	secure := opts.TLS || parsed.Secure

	scheme := "chunk"
	if secure {
		scheme = "chunks"
	}

	if user == "" && password != "" {
		return resolvedOptions{}, connectionErrorf("", nil, "a password needs a user")
	}
	if user != "" && !isName(user) {
		return resolvedOptions{}, connectionErrorf("", nil, "invalid user name %q: names are [a-z_][a-z0-9_]*", user)
	}

	iterations := cmp.Or(opts.VerifierIterations, DefaultVerifierIterations)
	if iterations < DefaultVerifierIterations {
		return resolvedOptions{}, connectionErrorf("", nil, "VerifierIterations must be at least %d, got %d",
			DefaultVerifierIterations, iterations)
	}

	table := opts.Table
	if table == "" {
		var err error
		if table, err = TableFromPath(parsed.Path); err != nil {
			return resolvedOptions{}, err
		}
	}
	if table != "" && !isName(table) {
		return resolvedOptions{}, connectionErrorf("", nil, "invalid table name %q: names are [a-z_][a-z0-9_]*", table)
	}

	return resolvedOptions{
		uri: URI{
			Scheme:   scheme,
			Secure:   secure,
			Host:     cmp.Or(opts.Host, parsed.Host, defaultHost),
			Port:     port,
			User:     user,
			Password: password,
			Path:     "/" + table,
		},
		connectTimeout:     resolveTimeout(opts.ConnectTimeout),
		commandTimeout:     resolveTimeout(opts.CommandTimeout),
		tls:                secure,
		tlsInsecure:        opts.TLSInsecure,
		tlsServerName:      opts.TLSServerName,
		ca:                 opts.CA,
		cert:               opts.Cert,
		key:                opts.Key,
		pipelineDepth:      max(1, opts.PipelineDepth),
		table:              table,
		verifierIterations: iterations,
	}, nil
}

// Client is one long-lived connection to a chunkdb server.
//
// A Client is safe for concurrent use. By default it sends one request at a
// time; raise [Options.PipelineDepth] to keep several requests in flight on the
// same socket, whose replies come back in request order. The connection is
// established lazily and re-established on the next request after a transport
// failure. A request is never resent after a transport failure. Typed writes may
// refresh stale schema and retry as documented by [Client.SetBlock] and
// [Client.SetChunk].
//
// A client keeps the schemas of the tables it uses, to encode and decode
// values; see [Client.Schema].
type Client struct {
	opts  resolvedOptions
	slots chan struct{}
	// dialGate serializes connection attempts so concurrent callers share one
	// dial instead of opening redundant sockets.
	dialGate chan struct{}
	// txGate lets one transaction at a time take every pipeline slot; see
	// [Client.holdConnection].
	txGate chan struct{}

	mu     sync.Mutex
	active *conn
	closed bool

	// hello is the reply to the most recent successful handshake.
	helloMu sync.Mutex
	hello   *ServerInfo

	// schemas caches DESCRIBE replies by table name.
	schemaMu sync.Mutex
	schemas  map[string]*Schema
}

// NewClient builds a client without connecting. The connection is opened on
// the first request, or explicitly by [Client.Connect].
func NewClient(opts Options) (*Client, error) {
	resolved, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Client{
		opts:     resolved,
		slots:    make(chan struct{}, resolved.pipelineDepth),
		dialGate: make(chan struct{}, 1),
		txGate:   make(chan struct{}, 1),
		schemas:  make(map[string]*Schema),
	}, nil
}

// Connect builds a client and opens its connection. Every connection starts
// with the HELLO 3 handshake, which logs in the user with SCRAM-SHA-256. A
// wrong user or password, or a missing user, fails with [ErrAuth]; a server
// that cannot prove it holds the user's verifier with [ErrConnection]; and a
// server of an older chunkdb protocol with [ErrProtocol].
func Connect(ctx context.Context, opts Options) (*Client, error) {
	client, err := NewClient(opts)
	if err != nil {
		return nil, err
	}
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}
	return client, nil
}

// ConnectURI is shorthand for [Connect] with only [Options.URI] set. Use
// [Connect] directly to combine a URI with other options.
func ConnectURI(ctx context.Context, uri string) (*Client, error) {
	return Connect(ctx, Options{URI: uri})
}

// URI reports the resolved endpoint, with the default table as its path when
// one was configured. Note that [URI.String] renders the user and password
// into the userinfo component.
func (c *Client) URI() URI {
	return c.opts.uri
}

// DefaultTable reports the table a method uses when its table argument is "":
// [Options.Table], else the URI path, else [DefaultTableName].
func (c *Client) DefaultTable() string {
	return cmp.Or(c.opts.table, DefaultTableName)
}

// ServerInfo returns the server's HELLO reply for the most recent connection,
// or nil before the first successful connection. The result is a copy.
func (c *Client) ServerInfo() *ServerInfo {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	if c.hello == nil {
		return nil
	}
	info := *c.hello
	return &info
}

// Connect opens the connection if it is not already established.
func (c *Client) Connect(ctx context.Context) error {
	_, err := c.connection(ctx)
	return err
}

// Close releases the connection. In-flight requests fail. A closed client
// cannot be reused; further calls report [ErrClosed].
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	active := c.active
	c.active = nil
	c.mu.Unlock()

	if active == nil {
		return nil
	}
	return active.shutdown(closedError(""))
}

// acquireSlot reserves one pipeline slot for the duration of a public call.
func (c *Client) acquireSlot(ctx context.Context, command string) (func(), error) {
	select {
	case c.slots <- struct{}{}:
		return func() { <-c.slots }, nil
	case <-ctx.Done():
		return nil, timeoutErrorf(command, ctx.Err(), "%s", ctx.Err())
	}
}

// holdConnection gives the caller the connection to itself: it takes every
// pipeline slot, so no other request of the client is sent until release.
// Requests already in flight finish first; later ones wait. txGate admits one
// holder at a time, so two transactions never each take part of the slots
// and wait on each other.
func (c *Client) holdConnection(ctx context.Context, command string) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, timeoutErrorf(command, err, "%s", err)
	}
	select {
	case c.txGate <- struct{}{}:
	case <-ctx.Done():
		return nil, timeoutErrorf(command, ctx.Err(), "%s", ctx.Err())
	}
	taken := 0
	release = func() {
		for range taken {
			<-c.slots
		}
		<-c.txGate
	}
	for taken < cap(c.slots) {
		select {
		case c.slots <- struct{}{}:
			taken++
		case <-ctx.Done():
			release()
			return nil, timeoutErrorf(command, ctx.Err(), "%s", ctx.Err())
		}
	}
	return release, nil
}

// connection returns the live connection, dialing when necessary.
func (c *Client) connection(ctx context.Context) (*conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, closedError("")
	}
	if active := c.active; active != nil && active.terminalError() == nil {
		c.mu.Unlock()
		return active, nil
	}
	c.active = nil
	c.mu.Unlock()

	select {
	case c.dialGate <- struct{}{}:
	case <-ctx.Done():
		return nil, timeoutErrorf("CONNECT", ctx.Err(), "%s", ctx.Err())
	}
	defer func() { <-c.dialGate }()

	// Another caller may have connected while this one waited for the gate.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, closedError("")
	}
	if active := c.active; active != nil && active.terminalError() == nil {
		c.mu.Unlock()
		return active, nil
	}
	c.active = nil
	c.mu.Unlock()

	established, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = established.shutdown(closedError(""))
		return nil, closedError("")
	}
	// The connection can die between dialing and publishing it. shutdown marks
	// the connection failed before it detaches under this same lock, so
	// checking here is what keeps a dead connection from being published with
	// nothing left to clear it.
	if err := established.terminalError(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.active = established
	c.mu.Unlock()
	return established, nil
}

func (c *Client) dial(ctx context.Context) (*conn, error) {
	dialCtx := ctx
	if c.opts.connectTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, c.opts.connectTimeout)
		defer cancel()
	}

	address := c.opts.uri.Address()
	dialer := &net.Dialer{}

	var (
		netConn net.Conn
		err     error
	)
	if c.opts.tls {
		var tlsConfig *tls.Config
		if tlsConfig, err = c.tlsConfig(); err != nil {
			return nil, err
		}
		netConn, err = (&tls.Dialer{NetDialer: dialer, Config: tlsConfig}).DialContext(dialCtx, "tcp", address)
	} else {
		netConn, err = dialer.DialContext(dialCtx, "tcp", address)
	}
	if err != nil {
		return nil, c.wrapDialError(ctx, dialCtx, address, err)
	}

	established := &conn{
		client:  c,
		netConn: netConn,
		reader:  bufio.NewReader(netConn),
		writer:  bufio.NewWriter(netConn),
	}
	go established.readLoop()

	// A failed handshake closes the socket: the caller never gets the
	// connection, so nothing else would.
	if err := c.helloOn(ctx, established); err != nil {
		_ = established.shutdown(err)
		var typed *Error
		if !c.opts.tls && errors.Is(err, ErrConnection) && errors.As(err, &typed) && typed.Phase != PhaseAuth {
			hinted := *typed
			hinted.Message += "; check the URI scheme: chunk:// needs a plain listener, chunks:// needs TLS"
			hinted.Err = err
			return nil, &hinted
		}
		return nil, err
	}
	return established, nil
}

// helloOn performs the HELLO 3 handshake on a connection that has not been
// published yet, so it bypasses the pipeline slots held by the request that
// triggered the dial.
func (c *Client) helloOn(ctx context.Context, established *conn) error {
	var (
		reply     Reply
		signature string
		err       error
	)
	if c.opts.uri.User == "" {
		reply, err = c.execOn(ctx, established, "HELLO", "HELLO 3", nil)
		err = c.helloError(err)
	} else {
		reply, signature, err = c.login(ctx, established)
	}
	if err != nil {
		return err
	}
	info, err := parseServerInfo(reply)
	if err != nil {
		return err
	}
	if c.opts.uri.User != "" && !hmac.Equal([]byte(info.ServerSignature), []byte(signature)) {
		return newError(KindConnection, PhaseAuth, "HELLO",
			"the server could not prove it knows the password: its SCRAM signature does not match", nil)
	}
	established.info = info
	c.helloMu.Lock()
	c.hello = info
	c.helloMu.Unlock()
	return nil
}

// login runs the SCRAM-SHA-256 exchange: HELLO 3 USER with the client-first
// message, then AUTH with the client-final message. It returns the HELLO
// reply and the server signature that reply must carry.
func (c *Client) login(ctx context.Context, established *conn) (Reply, string, error) {
	nonce, err := newScramNonce()
	if err != nil {
		return Reply{}, "", err
	}
	exchange := newScramLogin(c.opts.uri.User, nonce)
	reply, err := c.execOn(ctx, established, "HELLO", "HELLO 3 USER "+c.opts.uri.User+" $1",
		[][]byte{[]byte(exchange.clientFirst())})
	if err != nil {
		return Reply{}, "", c.helloError(err)
	}
	serverFirst, ok := strings.CutPrefix(reply.Text, "SCRAM ")
	if reply.Kind != ReplySimple || !ok {
		return Reply{}, "", protocolErrorf("HELLO", "expected +SCRAM <server-first message>, got a reply of kind %d %q",
			reply.Kind, reply.Text)
	}
	final, signature, err := exchange.clientFinal(c.opts.uri.Password, serverFirst)
	if err != nil {
		return Reply{}, "", err
	}
	reply, err = c.execOn(ctx, established, "AUTH", "AUTH $1", [][]byte{[]byte(final)})
	if err != nil {
		return Reply{}, "", err
	}
	return reply, signature, nil
}

// helloError reports a server of an older protocol as such. A server of
// protocol 2 answers "-ERR PROTOCOL expected HELLO 2"; a 1.x server does not
// know HELLO, or, when it requires a token, answers AUTH_REQUIRED although
// HELLO named a user, which a protocol 3 server never does. The server error
// is not wrapped, so the result matches ErrProtocol and not ErrServer.
func (c *Client) helloError(err error) error {
	var typed *Error
	if errors.As(err, &typed) && ((typed.ServerCode == CodeProtocol && strings.Contains(typed.ServerMessage, "HELLO 2")) ||
		typed.ServerCode == "UNKNOWN_COMMAND" || (typed.ServerCode == CodeAuthRequired && c.opts.uri.User != "")) {
		return protocolErrorf("HELLO",
			"the server speaks an older chunkdb protocol (it replied %q); this client needs a server of protocol 3",
			typed.ServerCode+" "+typed.ServerMessage)
	}
	return err
}

func (c *Client) tlsConfig() (*tls.Config, error) {
	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.opts.tlsInsecure,
		// An empty ServerName makes crypto/tls verify against the dial host,
		// which is what an unset TLSServerName should mean.
		ServerName: c.opts.tlsServerName,
	}

	if len(c.opts.ca) > 0 {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(c.opts.ca) {
			return nil, newError(KindTLS, PhaseTLS, "", "CA option contains no valid PEM certificate", nil)
		}
		config.RootCAs = roots
	}

	if len(c.opts.cert) > 0 || len(c.opts.key) > 0 {
		pair, err := tls.X509KeyPair(c.opts.cert, c.opts.key)
		if err != nil {
			return nil, newError(KindTLS, PhaseTLS, "", "invalid client certificate or key: "+err.Error(), err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config, nil
}

// wrapDialError classifies a failed dial. A deadline that came from
// ConnectTimeout is reported as a timeout, TLS handshake and certificate
// problems as [ErrTLS], and everything else as [ErrConnection].
func (c *Client) wrapDialError(parent, dialCtx context.Context, address string, err error) *Error {
	if dialCtx.Err() != nil && parent.Err() == nil {
		return timeoutErrorf("CONNECT", err, "connection timeout to %s after %s; check the server address, port and network, or increase ConnectTimeout", address, c.opts.connectTimeout)
	}
	if parent.Err() != nil {
		return timeoutErrorf("CONNECT", parent.Err(), "%s", parent.Err())
	}
	if isTLSError(err) {
		return newError(KindTLS, PhaseTLS, "CONNECT", "TLS connection to "+address+": "+err.Error()+"; use chunks:// only with a TLS listener and check the trusted CA and server hostname", err)
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return connectionErrorf("CONNECT", err, "connection refused at %s: %s; start the server and check its listen address and port", address, err)
	}
	return connectionErrorf("CONNECT", err, "connect %s: %s; check the server address, port and network", address, err)
}

func isTLSError(err error) bool {
	var (
		recordErr  tls.RecordHeaderError
		alertErr   tls.AlertError
		verifyErr  *tls.CertificateVerificationError
		hostErr    x509.HostnameError
		authErr    x509.UnknownAuthorityError
		invalidErr x509.CertificateInvalidError
	)
	return errors.As(err, &recordErr) ||
		errors.As(err, &alertErr) ||
		errors.As(err, &verifyErr) ||
		errors.As(err, &hostErr) ||
		errors.As(err, &authErr) ||
		errors.As(err, &invalidErr)
}

// detach clears the client's reference to a dead connection so the next
// request dials a fresh one.
func (c *Client) detach(dead *conn) {
	c.mu.Lock()
	if c.active == dead {
		c.active = nil
	}
	c.mu.Unlock()
}

// callDeadline carries both the caller's context and the derived
// command-timeout context so an expired deadline can be attributed correctly.
type callDeadline struct {
	parent  context.Context
	ctx     context.Context
	command string
	timeout time.Duration
	cancel  context.CancelFunc
}

func (c *Client) commandDeadline(ctx context.Context, command string) callDeadline {
	deadline := callDeadline{parent: ctx, ctx: ctx, command: command, timeout: c.opts.commandTimeout, cancel: func() {}}
	if c.opts.commandTimeout > 0 {
		deadline.ctx, deadline.cancel = context.WithTimeout(ctx, c.opts.commandTimeout)
	}
	return deadline
}

func (d callDeadline) err() *Error {
	if d.parent.Err() != nil {
		return timeoutErrorf(d.command, d.parent.Err(), "%s", d.parent.Err())
	}
	return timeoutErrorf(d.command, d.ctx.Err(), "command timeout after %s; check server availability or increase CommandTimeout; a sent write may have an unknown outcome", d.timeout)
}

// exec runs one statement on the live connection. The caller must already
// hold a pipeline slot.
func (c *Client) exec(ctx context.Context, command, statement string, params [][]byte) (Reply, error) {
	established, err := c.connection(ctx)
	if err != nil {
		return Reply{}, err
	}
	if err := checkLimits(established.info, command, statement, params); err != nil {
		return Reply{}, err
	}
	return c.execOn(ctx, established, command, statement, params)
}

// checkLimits refuses a statement the server's HELLO limits do not admit.
func checkLimits(info *ServerInfo, command, statement string, params [][]byte) error {
	if info == nil {
		return nil
	}
	if len(statement)+2 > info.MaxLineBytes {
		return requestErrorf(command, "the statement takes %d bytes; the server takes lines of at most %d",
			len(statement)+2, info.MaxLineBytes)
	}
	if len(params) > info.MaxParameters {
		return requestErrorf(command, "%d parameters; the server takes at most %d", len(params), info.MaxParameters)
	}
	return nil
}

func (c *Client) execOn(ctx context.Context, established *conn, command, statement string, params [][]byte) (Reply, error) {
	wire, err := encodeRequest(command, statement, params)
	if err != nil {
		return Reply{}, err
	}

	deadline := c.commandDeadline(ctx, command)
	defer deadline.cancel()

	reply, err := established.roundTrip(deadline, wire)
	if err != nil {
		return Reply{}, err
	}

	if reply.Kind == ReplyError {
		phase := PhaseResponse
		if (command == "HELLO" || command == "AUTH") && (reply.Code == CodeAuthFailed || reply.Code == CodeAuthRequired) {
			phase = PhaseAuth
		}
		if closesConnection(reply, len(params) > 0) {
			// The server closes the connection after this reply; requests
			// pipelined behind it were not executed.
			_ = established.shutdown(connectionErrorf(command, nil,
				"the server closed the connection after %s to an earlier request", reply.Code))
		}
		return Reply{}, replyError(phase, command, reply)
	}
	return reply, nil
}

// pending is one request waiting for its reply.
type pending struct {
	command string
	ch      chan result
}

type result struct {
	reply Reply
	err   error
}

// conn is one established socket plus the goroutine reading its responses.
//
// The protocol has no request identifiers, so responses are matched to requests
// strictly in order: the queue is appended to under writeMu, together with the
// write itself, and drained in the same order by readLoop.
type conn struct {
	client  *Client
	netConn net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer

	writeMu sync.Mutex

	mu           sync.Mutex
	queue        []*pending
	failed       bool
	termErr      error
	watch        *Watch
	watchStarted bool

	// info is the HELLO reply; set before the connection is published.
	info *ServerInfo
}

func (cn *conn) roundTrip(deadline callDeadline, wire []byte) (Reply, error) {
	waiter := &pending{command: deadline.command, ch: make(chan result, 1)}

	cn.writeMu.Lock()
	cn.mu.Lock()
	if cn.failed {
		err := cn.termErr
		cn.mu.Unlock()
		cn.writeMu.Unlock()
		// Detach defensively: a caller holding a reference to a connection that
		// died must not keep the client pinned to it.
		cn.client.detach(cn)
		return Reply{}, err
	}
	cn.queue = append(cn.queue, waiter)
	cn.mu.Unlock()

	_, err := cn.writer.Write(wire)
	if err == nil {
		err = cn.writer.Flush()
	}
	cn.writeMu.Unlock()

	if err != nil {
		wrapped := newError(KindConnection, PhaseRequest, deadline.command, "write request: "+err.Error(), err)
		_ = cn.shutdown(wrapped)
		return Reply{}, wrapped
	}

	select {
	case res := <-waiter.ch:
		if res.err != nil {
			return Reply{}, res.err
		}
		return res.reply, nil
	case <-deadline.ctx.Done():
		// The response for this request may still arrive, and without request
		// identifiers it cannot be skipped without desynchronizing every later
		// response. Dropping the connection is the only safe recovery.
		err := deadline.err()
		_ = cn.shutdown(err)
		return Reply{}, err
	}
}

func (cn *conn) readLoop() {
	for {
		reply, err := readReply(cn.reader)
		if err != nil {
			_ = cn.shutdown(cn.readError(err))
			return
		}

		cn.mu.Lock()
		watch := cn.watch
		streaming := cn.watchStarted
		cn.mu.Unlock()
		if watch == nil && reply.Kind == ReplyPush {
			_ = cn.shutdown(protocolErrorf("", "push outside a watch"))
			return
		}
		if watch != nil && (reply.Kind == ReplyPush || reply.Kind == ReplyError) {
			if reply.Kind == ReplyError && !(streaming && watch.slot != "" && reply.Code == CodeInvalidArgument) {
				_ = cn.shutdown(replyError(PhaseResponse, "WATCH", reply))
				return
			}
			select {
			case watch.pushes <- reply:
			case <-watch.closing:
			case <-watch.done:
				return
			}
			continue
		}
		waiter := cn.popPending()
		if waiter == nil {
			_ = cn.shutdown(protocolErrorf("", "unsolicited response from server"))
			return
		}
		if waiter.command == "WATCH" && reply.Kind == ReplySimple {
			cn.mu.Lock()
			cn.watchStarted = true
			cn.mu.Unlock()
		}
		waiter.ch <- result{reply: reply}
	}
}

// readError converts a transport-level read failure into a client error.
func (cn *conn) readError(err error) error {
	var typed *Error
	if errors.As(err, &typed) {
		return typed
	}
	return connectionErrorf("", err, "connection closed: %s", err)
}

// terminalError reports the failure that tore the connection down, or nil while
// it is still usable.
func (cn *conn) terminalError() error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if !cn.failed {
		return nil
	}
	return cn.termErr
}

func (cn *conn) popPending() *pending {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if len(cn.queue) == 0 {
		return nil
	}
	waiter := cn.queue[0]
	cn.queue = cn.queue[1:]
	return waiter
}

// shutdown tears the connection down exactly once: it records the cause,
// closes the socket, fails every queued request, and detaches the connection so
// the next request dials a fresh one.
//
// The teardown is synchronous on purpose. A request abandoned mid-flight must
// not leave a dead connection published for the next caller to write into,
// which it would if detaching were left to readLoop.
func (cn *conn) shutdown(cause error) error {
	cn.mu.Lock()
	if cn.failed {
		cn.mu.Unlock()
		// Another teardown may still be closing the socket. Complete detaching
		// before returning a closing error reply to the caller.
		cn.client.detach(cn)
		return nil
	}
	cn.failed = true
	if cn.termErr == nil {
		cn.termErr = cause
	}
	err := cn.termErr
	queued := cn.queue
	cn.queue = nil
	if cn.watch != nil {
		close(cn.watch.done)
	}
	cn.mu.Unlock()

	closeErr := cn.netConn.Close()
	for _, waiter := range queued {
		waiter.ch <- result{err: err}
	}
	cn.client.detach(cn)

	if errors.Is(closeErr, net.ErrClosed) {
		return nil
	}
	return closeErr
}

// parseServerInfo parses and checks a HELLO reply.
func parseServerInfo(reply Reply) (*ServerInfo, error) {
	if reply.Kind != ReplyMap {
		return nil, protocolErrorf("HELLO", "HELLO reply is not a map")
	}
	protocol, _ := reply.Lookup("protocol")
	if value, ok := protocol.Int64(); !ok || value != protocolVersion {
		return nil, protocolErrorf("HELLO", "the server replied with another protocol, expected %d", protocolVersion)
	}
	version, ok := reply.Lookup("server_version")
	if !ok || version.Kind != ReplyBulk {
		return nil, protocolErrorf("HELLO", "HELLO reply has no server_version")
	}
	info := &ServerInfo{Protocol: protocolVersion, ServerVersion: string(version.Bulk)}
	for _, field := range []struct {
		key    string
		target *int
	}{
		{"max_line_bytes", &info.MaxLineBytes},
		{"max_parameters", &info.MaxParameters},
		{"max_area_chunks", &info.MaxAreaChunks},
		{"max_response_bytes", &info.MaxResponseBytes},
		{"max_scan_limit", &info.MaxScanLimit},
	} {
		value, _ := reply.Lookup(field.key)
		number, ok := value.Int64()
		if !ok || number <= 0 || number > 1<<40 {
			return nil, protocolErrorf("HELLO", "HELLO reply has no valid %s", field.key)
		}
		*field.target = int(number)
	}
	// A bulk string after a login with a user, null without one.
	if signature, ok := reply.Lookup("server_signature"); ok && signature.Kind == ReplyBulk {
		info.ServerSignature = string(signature.Bulk)
	}
	return info, nil
}
