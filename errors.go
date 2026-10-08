package chunkdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Phase identifies the stage of an operation that produced an [Error].
type Phase string

const (
	PhaseConnect  Phase = "connect"
	PhaseAuth     Phase = "auth"
	PhaseRequest  Phase = "request"
	PhaseResponse Phase = "response"
	PhaseTimeout  Phase = "timeout"
	PhaseProtocol Phase = "protocol"
	PhaseTLS      Phase = "tls"
)

// Kind classifies an [Error]. Match it with [errors.Is] against the package
// sentinels rather than comparing the field directly.
type Kind int

const (
	// KindConnection covers dial, socket, and connection-lifecycle failures.
	KindConnection Kind = iota + 1
	// KindTimeout covers command and connect deadlines as well as caller
	// context cancellation.
	KindTimeout
	// KindProtocol covers malformed or unexpected server replies and
	// client-side validation of statements and values.
	KindProtocol
	// KindServer covers "-ERR <code> <message>" replies.
	KindServer
	// KindAuth covers AUTH_FAILED (wrong user or password) and AUTH_REQUIRED
	// (no user) replies. It is a specialization of KindServer and matches
	// both [ErrServer] and [ErrAuth].
	KindAuth
	// KindTLS covers TLS configuration and handshake failures.
	KindTLS
)

// Sentinel errors for use with [errors.Is].
var (
	ErrConnection = errors.New("chunkdb: connection error")
	ErrTimeout    = errors.New("chunkdb: timeout")
	ErrProtocol   = errors.New("chunkdb: protocol error")
	ErrServer     = errors.New("chunkdb: server error")
	ErrAuth       = errors.New("chunkdb: authentication failed")
	ErrTLS        = errors.New("chunkdb: tls error")
	// ErrClosed is reported when a closed [Client] or [Pool] is used.
	ErrClosed = errors.New("chunkdb: client is closed")
	// ErrVersionMismatch matches a write refused by IF VERSION. The error is a
	// [*VersionMismatchError], which carries the chunk's current version.
	ErrVersionMismatch = errors.New("chunkdb: version mismatch")
	// ErrSchemaMismatch matches a chunk form refused because it was encoded
	// for another schema version than the table's. The error is a
	// [*SchemaMismatchError].
	ErrSchemaMismatch = errors.New("chunkdb: schema mismatch")
	// ErrPermissionDenied matches a statement refused because the user lacks
	// the right it needs. The error is a [*PermissionDeniedError].
	ErrPermissionDenied = errors.New("chunkdb: permission denied")
)

// Server error codes, as reported in [Error.ServerCode].
const (
	// CodeProtocol: no HELLO 3 yet, another protocol version, or a second
	// HELLO.
	CodeProtocol = "PROTOCOL"
	// CodeAuthRequired: the server has users and the client logged in
	// without one.
	CodeAuthRequired = "AUTH_REQUIRED"
	// CodeAuthFailed: the user or the password is wrong.
	CodeAuthFailed = "AUTH_FAILED"
	// CodePermissionDenied: the user lacks the right the statement needs.
	// See [PermissionDeniedError].
	CodePermissionDenied = "PERMISSION_DENIED"
	// CodeSyntax: the statement does not parse.
	CodeSyntax = "SYNTAX"
	// CodeInvalidArgument: a value, column, option or size the statement
	// cannot take.
	CodeInvalidArgument = "INVALID_ARGUMENT"
	// CodeOutOfRange: a reply would exceed the server's max_response_bytes.
	CodeOutOfRange = "OUT_OF_RANGE"
	// CodeVersionMismatch: IF VERSION did not match; nothing changed. See
	// [VersionMismatchError].
	CodeVersionMismatch = "VERSION_MISMATCH"
	// CodeSchemaMismatch: SET CHUNK of a form encoded for another schema
	// version than the table's; nothing changed. See [SchemaMismatchError].
	CodeSchemaMismatch = "SCHEMA_MISMATCH"
	// CodeNoTable: the table does not exist.
	CodeNoTable = "NO_TABLE"
	// CodeTableExists: CREATE TABLE of a name that is taken.
	CodeTableExists = "TABLE_EXISTS"
	// CodeBadRequest: the request could not be framed; the server closes the
	// connection, and so does the client.
	CodeBadRequest = "BAD_REQUEST"
	// CodeBusy: the server has no room for the connection.
	CodeBusy = "BUSY"
	// CodeInternal: a server failure. After a write, a message starting with
	// "write outcome unknown" means the write may or may not be applied.
	CodeInternal = "INTERNAL"
)

// Error is the error type returned by every operation in this package.
// Use [errors.As] to read its fields and [errors.Is] to test it against the
// package sentinels.
type Error struct {
	Kind  Kind
	Phase Phase
	// Command names the statement, for example "SET BLOCK" or "HELLO".
	Command string
	Message string
	// ServerCode and ServerMessage carry the parts of a "-ERR <code>
	// <message>" reply. They are empty unless Kind is [KindServer] or
	// [KindAuth].
	ServerCode    string
	ServerMessage string
	Err           error
}

func (e *Error) Error() string {
	if e.Command == "" {
		return "chunkdb: " + e.Message
	}
	return "chunkdb: " + e.Command + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	switch target {
	case ErrConnection:
		return e.Kind == KindConnection
	case ErrTimeout:
		return e.Kind == KindTimeout
	case ErrProtocol:
		return e.Kind == KindProtocol
	case ErrServer:
		return e.Kind == KindServer || e.Kind == KindAuth
	case ErrAuth:
		return e.Kind == KindAuth
	case ErrTLS:
		return e.Kind == KindTLS
	case ErrVersionMismatch:
		return e.ServerCode == CodeVersionMismatch
	case ErrSchemaMismatch:
		return e.ServerCode == CodeSchemaMismatch
	case ErrPermissionDenied:
		return e.ServerCode == CodePermissionDenied
	}
	return false
}

// VersionMismatchError is returned by a write with [IfVersion] when the chunk
// is no longer at that version. Nothing was changed. It wraps the server's
// [*Error] (code [CodeVersionMismatch]) and matches [ErrVersionMismatch] and
// [ErrServer].
type VersionMismatchError struct {
	// Current is the chunk's version now.
	Current uint64
	Err     *Error
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("chunkdb: %s: version mismatch, the chunk is at version %d", e.Err.Command, e.Current)
}

func (e *VersionMismatchError) Unwrap() error { return e.Err }

// SchemaMismatchError is returned when SET CHUNK refuses a chunk form encoded
// for another schema version than the table's. Nothing was changed.
// [Client.SetChunk] fetches the schema and encodes the chunk again once before
// it returns this error. It wraps the server's [*Error] (code
// [CodeSchemaMismatch]) and matches [ErrSchemaMismatch] and [ErrServer].
type SchemaMismatchError struct {
	// Current is the table's schema version now.
	Current uint64
	Err     *Error
}

func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("chunkdb: %s: schema mismatch, the table is at schema version %d", e.Err.Command, e.Current)
}

func (e *SchemaMismatchError) Unwrap() error { return e.Err }

// PermissionDeniedError is returned when the user lacks the right a
// statement needs. Nothing was changed. It wraps the server's [*Error] (code
// [CodePermissionDenied]) and matches [ErrPermissionDenied] and [ErrServer].
//
// A table the user has no right on at all is reported as one that does not
// exist ([CodeNoTable]), not as this error.
type PermissionDeniedError struct {
	// Right is the right the statement needs: "READ", "WRITE", "ADMIN" or
	// "MANAGES USERS".
	Right string
	// Table is the table the right is needed on, "*" for every table, and
	// empty for MANAGES USERS.
	Table string
	Err   *Error
}

func (e *PermissionDeniedError) Error() string {
	if e.Table == "" {
		return fmt.Sprintf("chunkdb: %s: permission denied, %s needed", e.Err.Command, e.Right)
	}
	return fmt.Sprintf("chunkdb: %s: permission denied, %s on %s needed", e.Err.Command, e.Right, e.Table)
}

func (e *PermissionDeniedError) Unwrap() error { return e.Err }

func newError(kind Kind, phase Phase, command, message string, cause error) *Error {
	return &Error{Kind: kind, Phase: phase, Command: command, Message: message, Err: cause}
}

func connectionErrorf(command string, cause error, format string, args ...any) *Error {
	return newError(KindConnection, PhaseConnect, command, fmt.Sprintf(format, args...), cause)
}

func protocolErrorf(command, format string, args ...any) *Error {
	return newError(KindProtocol, PhaseProtocol, command, fmt.Sprintf(format, args...), nil)
}

// requestErrorf reports client-side validation failures, before anything is
// written to the socket.
func requestErrorf(command, format string, args ...any) *Error {
	return newError(KindProtocol, PhaseRequest, command, fmt.Sprintf(format, args...), nil)
}

func timeoutErrorf(command string, cause error, format string, args ...any) *Error {
	return newError(KindTimeout, PhaseTimeout, command, fmt.Sprintf(format, args...), cause)
}

func serverError(phase Phase, command, code, message string) *Error {
	kind := KindServer
	if code == CodeAuthFailed || code == CodeAuthRequired {
		kind = KindAuth
	}
	text := "server error " + code
	if message != "" {
		text += ": " + message
	}
	return &Error{
		Kind:          kind,
		Phase:         phase,
		Command:       command,
		Message:       text,
		ServerCode:    code,
		ServerMessage: message,
	}
}

// replyError converts an error reply into the error a call returns: a
// [*VersionMismatchError] for VERSION_MISMATCH, a [*SchemaMismatchError] for
// SCHEMA_MISMATCH, a [*PermissionDeniedError] for PERMISSION_DENIED, an
// [*Error] otherwise.
func replyError(phase Phase, command string, reply Reply) error {
	base := serverError(phase, command, reply.Code, reply.Message)
	if reply.Code == CodePermissionDenied {
		// "<right> on <table>", or "MANAGES USERS".
		right, table, _ := strings.Cut(reply.Message, " on ")
		return &PermissionDeniedError{Right: right, Table: table, Err: base}
	}
	if reply.Code != CodeVersionMismatch && reply.Code != CodeSchemaMismatch {
		return base
	}
	// "current=<v>", then for SCHEMA_MISMATCH an explanation.
	first, _, _ := strings.Cut(reply.Message, " ")
	text, found := strings.CutPrefix(first, "current=")
	current, err := strconv.ParseUint(text, 10, 64)
	if !found || err != nil {
		return protocolErrorf(command, "malformed %s reply: %q", reply.Code, reply.Message)
	}
	if reply.Code == CodeSchemaMismatch {
		return &SchemaMismatchError{Current: current, Err: base}
	}
	return &VersionMismatchError{Current: current, Err: base}
}

// closesConnection reports an error reply after which the server closes the
// connection: BAD_REQUEST always, and for a statement with parameters, whose
// frames the server then does not read, SYNTAX, NO_TABLE, and
// INVALID_ARGUMENT for an unknown column.
func closesConnection(reply Reply, withParams bool) bool {
	switch reply.Code {
	case CodeBadRequest:
		return true
	case CodeSyntax, CodeNoTable:
		return withParams
	case CodeInvalidArgument:
		return withParams && strings.HasPrefix(reply.Message, "the table has no column ")
	}
	return false
}

func closedError(command string) *Error {
	return newError(KindConnection, PhaseConnect, command, "client is closed", ErrClosed)
}

// isServerCode reports whether err is a server error with the given code.
func isServerCode(err error, code string) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.ServerCode == code
}
