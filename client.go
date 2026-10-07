package chunkdb

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync"
	"time"
)

type resolvedOptions struct {
	uri            URI
	autoAuth       bool
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

	token := cmp.Or(opts.Token, parsed.Token)
	secure := opts.TLS || parsed.Secure

	scheme := "chunk"
	if secure {
		scheme = "chunks"
	}

	table := opts.Table
	if table == "" {
		var err error
		if table, err = TableFromPath(parsed.Path); err != nil {
			return resolvedOptions{}, err
		}
	}

	return resolvedOptions{
		uri: URI{
			Scheme: scheme,
			Secure: secure,
			Host:   cmp.Or(opts.Host, parsed.Host, defaultHost),
			Port:   port,
			Token:  token,
			Path:   "/" + table,
		},
		autoAuth:       !opts.DisableAutoAuth && token != "",
		connectTimeout: resolveTimeout(opts.ConnectTimeout),
		commandTimeout: resolveTimeout(opts.CommandTimeout),
		tls:            secure,
		tlsInsecure:    opts.TLSInsecure,
		tlsServerName:  opts.TLSServerName,
		ca:             opts.CA,
		cert:           opts.Cert,
		key:            opts.Key,
		pipelineDepth:  max(1, opts.PipelineDepth),
		table:          table,
	}, nil
}

// Client is one long-lived connection to a chunkdb server.
//
// A Client is safe for concurrent use. By default it sends one request at a
// time; raise [Options.PipelineDepth] to keep several requests in flight on the
// same socket. The connection is established lazily and re-established on the
// next request after a transport failure, but no request is ever retried
// automatically.
type Client struct {
	// options is what the client was built with; [Client.Table] reuses it.
	options Options
	opts    resolvedOptions
	slots   chan struct{}
	// dialGate serializes connection attempts so concurrent callers share one
	// dial instead of opening redundant sockets.
	dialGate chan struct{}

	mu     sync.Mutex
	active *conn
	closed bool

	geoMu sync.Mutex
	geo   *geometry

	// table is the selected table, empty for the server's default. Every
	// new connection selects it.
	tableMu sync.Mutex
	table   string
}

// NewClient builds a client without connecting. The connection is opened on
// the first request, or explicitly by [Client.Connect].
func NewClient(opts Options) (*Client, error) {
	resolved, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Client{
		options:  opts,
		opts:     resolved,
		slots:    make(chan struct{}, resolved.pipelineDepth),
		dialGate: make(chan struct{}, 1),
		table:    resolved.table,
	}, nil
}

// Connect builds a client and opens its connection, authenticating when a
// token is configured.
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

// URI reports the resolved endpoint, with the selected table as its path. Note
// that [URI.String] renders the token into the userinfo component.
func (c *Client) URI() URI {
	uri := c.opts.uri
	uri.Path = "/" + c.selectedTable()
	return uri
}

func (c *Client) selectedTable() string {
	c.tableMu.Lock()
	defer c.tableMu.Unlock()
	return c.table
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

// connection returns the live connection, dialing when necessary.
func (c *Client) connection(ctx context.Context) (*conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, closedError("")
	}
	if active := c.active; active != nil {
		c.mu.Unlock()
		return active, nil
	}
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
	if active := c.active; active != nil {
		c.mu.Unlock()
		return active, nil
	}
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

	if c.opts.autoAuth {
		if err := c.authOn(ctx, established, c.opts.token()); err != nil {
			_ = established.shutdown(err)
			return nil, err
		}
	}
	if table := c.selectedTable(); table != "" {
		if _, err := c.useOn(ctx, established, table); err != nil {
			_ = established.shutdown(err)
			return nil, err
		}
	}
	return established, nil
}

func (o resolvedOptions) token() string { return o.uri.Token }

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
		return timeoutErrorf("CONNECT", err, "connection timeout after %s", c.opts.connectTimeout)
	}
	if parent.Err() != nil {
		return timeoutErrorf("CONNECT", parent.Err(), "%s", parent.Err())
	}
	if isTLSError(err) {
		return newError(KindTLS, PhaseTLS, "CONNECT", "connect "+address+": "+err.Error(), err)
	}
	return connectionErrorf("CONNECT", err, "connect %s: %s", address, err)
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
	return timeoutErrorf(d.command, d.ctx.Err(), "command timeout after %s", d.timeout)
}

// exec runs one command on the live connection. The caller must already hold a
// pipeline slot.
func (c *Client) exec(ctx context.Context, command string, args ...string) (Frame, error) {
	established, err := c.connection(ctx)
	if err != nil {
		return Frame{}, err
	}
	return c.execOn(ctx, established, command, args...)
}

func (c *Client) execOn(ctx context.Context, established *conn, command string, args ...string) (Frame, error) {
	return c.execPayloadOn(ctx, established, nil, command, args...)
}

// execPayload is exec for commands that carry raw bytes after the request
// line (CHUNKSETBIN): the payload is written right after the line, followed by
// an empty line, as one write so pipelined peers never see a partial request.
func (c *Client) execPayload(ctx context.Context, payload []byte, command string, args ...string) (Frame, error) {
	established, err := c.connection(ctx)
	if err != nil {
		return Frame{}, err
	}
	return c.execPayloadOn(ctx, established, payload, command, args...)
}

func (c *Client) execPayloadOn(ctx context.Context, established *conn, payload []byte, command string, args ...string) (Frame, error) {
	line, err := SerializeCommand(append([]string{command}, args...)...)
	if err != nil {
		return Frame{}, err
	}
	if payload != nil {
		wire := make([]byte, 0, len(line)+len(payload)+2)
		wire = append(wire, line...)
		wire = append(wire, payload...)
		wire = append(wire, '\r', '\n')
		line = wire
	}

	deadline := c.commandDeadline(ctx, command)
	defer deadline.cancel()

	frame, err := established.roundTrip(deadline, line)
	if err != nil {
		return Frame{}, err
	}

	if frame.Kind == FrameError {
		phase := PhaseResponse
		if command == "AUTH" {
			phase = PhaseAuth
		}
		return Frame{}, serverError(phase, command, frame.Code, frame.Message)
	}
	return frame, nil
}

// pending is one request waiting for its response frame.
type pending struct {
	command string
	ch      chan result
}

type result struct {
	frame Frame
	err   error
}

// conn is one established socket plus the goroutine reading its responses.
//
// Protocol v1 has no request identifiers, so responses are matched to requests
// strictly in order: the queue is appended to under writeMu, together with the
// write itself, and drained in the same order by readLoop.
type conn struct {
	client  *Client
	netConn net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer

	writeMu sync.Mutex

	mu      sync.Mutex
	queue   []*pending
	failed  bool
	termErr error
}

func (cn *conn) roundTrip(deadline callDeadline, line []byte) (Frame, error) {
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
		return Frame{}, err
	}
	cn.queue = append(cn.queue, waiter)
	cn.mu.Unlock()

	_, err := cn.writer.Write(line)
	if err == nil {
		err = cn.writer.Flush()
	}
	cn.writeMu.Unlock()

	if err != nil {
		wrapped := newError(KindConnection, PhaseRequest, deadline.command, "write command: "+err.Error(), err)
		_ = cn.shutdown(wrapped)
		return Frame{}, wrapped
	}

	select {
	case res := <-waiter.ch:
		if res.err != nil {
			return Frame{}, res.err
		}
		return res.frame, nil
	case <-deadline.ctx.Done():
		// The response for this request may still arrive, and without request
		// identifiers it cannot be skipped without desynchronizing every later
		// response. Dropping the connection is the only safe recovery.
		err := deadline.err()
		_ = cn.shutdown(err)
		return Frame{}, err
	}
}

func (cn *conn) readLoop() {
	for {
		frame, err := ReadFrame(cn.reader)
		if err != nil {
			_ = cn.shutdown(cn.readError(err))
			return
		}

		waiter := cn.popPending()
		if waiter == nil {
			_ = cn.shutdown(protocolErrorf("", "unsolicited response from server"))
			return
		}
		waiter.ch <- result{frame: frame}
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
		return nil
	}
	cn.failed = true
	if cn.termErr == nil {
		cn.termErr = cause
	}
	err := cn.termErr
	queued := cn.queue
	cn.queue = nil
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

// geometry holds the chunk dimensions derived from INFO. Server geometry is
// static, so it is fetched once per client.
type geometry struct {
	chunkPayloadBits  int
	chunkBlockCount   int
	chunkPayloadBytes int
	presenceBytes     int
}

// chunkGeometry returns the cached geometry, fetching it when needed. The
// caller must already hold a pipeline slot.
func (c *Client) chunkGeometry(ctx context.Context) (geometry, error) {
	c.geoMu.Lock()
	cached := c.geo
	c.geoMu.Unlock()
	if cached != nil {
		return *cached, nil
	}

	frame, err := c.exec(ctx, "INFO")
	if err != nil {
		return geometry{}, err
	}
	payload, err := expectBulk(frame, "INFO")
	if err != nil {
		return geometry{}, err
	}
	values := ParseInfo(payload)

	blockBits, err := positiveInfoInt(values, "block_bits")
	if err != nil {
		return geometry{}, err
	}
	width, err := positiveInfoInt(values, "chunk_width_blocks")
	if err != nil {
		return geometry{}, err
	}
	height, err := positiveInfoInt(values, "chunk_height_blocks")
	if err != nil {
		return geometry{}, err
	}

	blockCount := width * height
	resolved := geometry{
		chunkPayloadBits:  blockCount * blockBits,
		chunkBlockCount:   blockCount,
		chunkPayloadBytes: (blockCount*blockBits + 7) / 8,
		presenceBytes:     (blockCount + 7) / 8,
	}

	c.geoMu.Lock()
	c.geo = &resolved
	c.geoMu.Unlock()
	return resolved, nil
}
