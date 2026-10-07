package chunkdb

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// protocolVersion is the chunkdb protocol this client speaks.
const protocolVersion = 2

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
	// exclusiveMu lets one caller at a time collect every slot, so two
	// exclusive callers cannot each hold part of them.
	exclusiveMu sync.Mutex
	// dialGate serializes connection attempts so concurrent callers share one
	// dial instead of opening redundant sockets.
	dialGate chan struct{}

	mu     sync.Mutex
	active *conn
	closed bool

	// hello is the reply to the most recent successful handshake.
	helloMu sync.Mutex
	hello   *HelloInfo

	// table is the selected table, empty for the server's default. Every
	// new connection names it in HELLO.
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

// Connect builds a client and opens its connection. Every connection starts
// with the HELLO 2 handshake, which carries the token and the table; a wrong
// or missing token fails with [ErrAuth], an unknown table with [CodeNoTable],
// and a server that does not speak protocol 2 with [ErrProtocol].
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

// ServerInfo returns the server's HELLO reply for the most recent connection,
// or nil before the first successful connection. The result is a copy.
func (c *Client) ServerInfo() *HelloInfo {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	return c.hello.clone()
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

// acquireAllSlots reserves every pipeline slot, so no other request is in
// flight while the caller runs. USE needs it: it changes the table, and with
// it the sizes that chunk requests are checked and framed with, so a chunk
// request must not be sent between USE and its reply.
func (c *Client) acquireAllSlots(ctx context.Context, command string) (func(), error) {
	c.exclusiveMu.Lock()
	held := 0
	release := func() {
		for ; held > 0; held-- {
			<-c.slots
		}
		c.exclusiveMu.Unlock()
	}
	for held < cap(c.slots) {
		select {
		case c.slots <- struct{}{}:
			held++
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

	// A failed handshake closes the socket: the caller never gets the
	// connection, so nothing else would.
	if err := c.helloOn(ctx, established); err != nil {
		_ = established.shutdown(err)
		return nil, err
	}
	return established, nil
}

// helloOn performs the HELLO 2 handshake on a connection that has not been
// published yet, so it bypasses the pipeline slots held by the request that
// triggered the dial.
func (c *Client) helloOn(ctx context.Context, established *conn) error {
	args := []string{strconv.Itoa(protocolVersion)}
	token := c.opts.uri.Token
	if token != "" {
		args = append(args, "AUTH", token)
	}
	if table := c.selectedTable(); table != "" {
		args = append(args, "TABLE", table)
	}

	frame, err := c.execOn(ctx, established, "HELLO", args...)
	if err != nil {
		// A 1.x server does not know HELLO; one that requires a token
		// answers AUTH_REQUIRED although HELLO carried it, which a protocol 2
		// server never does. The server error is not wrapped, so the result
		// matches ErrProtocol and not ErrServer.
		var typed *Error
		if errors.As(err, &typed) && (typed.ServerCode == codeUnknownCommand ||
			(typed.ServerCode == codeAuthRequired && token != "")) {
			return protocolErrorf("HELLO",
				"server does not speak protocol 2 (chunkdb 1.x; it replied %s); this client needs chunkdb 2.0 or later",
				typed.Message)
		}
		return err
	}
	payload, err := expectBulk(frame, "HELLO")
	if err != nil {
		return err
	}
	info, err := parseHelloInfo(payload)
	if err != nil {
		return err
	}

	if info.Table != nil {
		established.setGeometry(geometryOf(*info.Table))
	}
	c.helloMu.Lock()
	c.hello = info
	c.helloMu.Unlock()
	return nil
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

// execPayloadOn is execOn for commands that carry raw bytes after the request
// line (CHUNKPUT): the payload is written right after the line, followed by an
// empty line, as one write so pipelined peers never see a partial request.
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
		if command == "HELLO" && (frame.Code == codeAuthFailed || frame.Code == codeAuthRequired) {
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
// The protocol has no request identifiers, so responses are matched to requests
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

	// geo is the chunk geometry of the connection's table, nil while it has
	// none. HELLO sets it and USE replaces it.
	geoMu sync.Mutex
	geo   *geometry
}

func (cn *conn) setGeometry(geo geometry) {
	cn.geoMu.Lock()
	cn.geo = &geo
	cn.geoMu.Unlock()
}

func (cn *conn) geometry() (geometry, bool) {
	cn.geoMu.Lock()
	defer cn.geoMu.Unlock()
	if cn.geo == nil {
		return geometry{}, false
	}
	return *cn.geo, true
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

// geometry holds the chunk sizes of a table.
type geometry struct {
	payloadBytes  int
	presenceBytes int
}

func geometryOf(info TableInfo) geometry {
	blockCount := info.ChunkWidthBlocks * info.ChunkHeightBlocks
	return geometry{
		payloadBytes:  (blockCount*info.BlockBits + 7) / 8,
		presenceBytes: (blockCount + 7) / 8,
	}
}

func (g geometry) stateBytes() int { return g.payloadBytes + g.presenceBytes }

// chunkConnection returns the live connection and its table's geometry. The
// caller must already hold a pipeline slot.
func (c *Client) chunkConnection(ctx context.Context, command string) (*conn, geometry, error) {
	established, err := c.connection(ctx)
	if err != nil {
		return nil, geometry{}, err
	}
	geo, ok := established.geometry()
	if !ok {
		return nil, geometry{}, requestErrorf(command,
			"%s needs a table: the connection has none (the server has no default table); select one with Use", command)
	}
	return established, geo, nil
}

// parseHelloInfo parses and checks a HELLO reply.
func parseHelloInfo(payload []byte) (*HelloInfo, error) {
	values := ParseInfo(payload)
	if values["protocol"] != strconv.Itoa(protocolVersion) {
		protocol := values["protocol"]
		if protocol == "" {
			protocol = "(none)"
		}
		return nil, protocolErrorf("HELLO", "server replied with protocol %s, expected %d", protocol, protocolVersion)
	}

	info := &HelloInfo{
		Protocol:      protocolVersion,
		ServerVersion: values["server_version"],
		Capabilities:  []string{},
		Values:        values,
	}
	for _, capability := range strings.Split(values["capabilities"], ",") {
		if capability != "" {
			info.Capabilities = append(info.Capabilities, capability)
		}
	}
	for _, field := range []struct {
		key    string
		target *int
	}{
		{"max_line_bytes", &info.MaxLineBytes},
		{"max_area_chunks", &info.MaxAreaChunks},
		{"max_response_bytes", &info.MaxResponseBytes},
		{"max_scan_limit", &info.MaxScanLimit},
		{"max_batch_ops", &info.MaxBatchOps},
	} {
		value, err := strconv.Atoi(values[field.key])
		if err != nil || value <= 0 {
			return nil, protocolErrorf("HELLO", "HELLO missing valid %s", field.key)
		}
		*field.target = value
	}

	// Without a default table and without TABLE, the connection has none.
	if _, ok := values["table"]; ok {
		table, err := parseTableValues(ParseInfo(payload), "HELLO")
		if err != nil {
			return nil, err
		}
		info.Table = &table
	}
	return info, nil
}

// clone returns a deep copy, so callers cannot change the client's state.
func (h *HelloInfo) clone() *HelloInfo {
	if h == nil {
		return nil
	}
	out := *h
	out.Capabilities = slices.Clone(h.Capabilities)
	out.Values = maps.Clone(h.Values)
	if h.Table != nil {
		table := *h.Table
		table.Values = maps.Clone(h.Table.Values)
		out.Table = &table
	}
	return &out
}
