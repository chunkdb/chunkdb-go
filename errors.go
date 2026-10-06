package chunkdb

import (
	"errors"
	"fmt"
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
	// KindProtocol covers malformed or unexpected server responses and
	// client-side argument validation.
	KindProtocol
	// KindServer covers "-ERR <code> <message>" responses.
	KindServer
	// KindAuth covers AUTH_FAILED responses. It is a specialization of
	// KindServer and matches both [ErrServer] and [ErrAuth].
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
)

// Error is the error type returned by every operation in this package.
// Use [errors.As] to read its fields and [errors.Is] to test it against the
// package sentinels.
type Error struct {
	Kind    Kind
	Phase   Phase
	Command string
	Message string
	// ServerCode and ServerMessage carry the parts of a "-ERR <code>
	// <message>" response. They are empty unless Kind is [KindServer] or
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
	}
	return false
}

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
	if code == codeAuthFailed {
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

func closedError(command string) *Error {
	return newError(KindConnection, PhaseConnect, command, "client is closed", ErrClosed)
}

const (
	codeAuthFailed      = "AUTH_FAILED"
	codeVersionMismatch = "VERSION_MISMATCH"

	// CodeNoTable is the [Error.ServerCode] for an unknown table, and for any
	// command on a connection whose table was dropped.
	CodeNoTable = "NO_TABLE"
	// CodeTableExists is the [Error.ServerCode] for creating a table whose
	// name is taken.
	CodeTableExists = "TABLE_EXISTS"
)
