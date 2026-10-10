package chunkdb

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestConnectionGuidancePreservesCauses(t *testing.T) {
	c, err := NewClient(Options{ConnectTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cause := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connect: %w", syscall.ECONNREFUSED)}
	refused := c.wrapDialError(t.Context(), t.Context(), "localhost:4242", cause)
	var network *net.OpError
	if !errors.Is(refused, ErrConnection) || !errors.Is(refused, syscall.ECONNREFUSED) || !errors.As(refused, &network) || network != cause || !strings.Contains(refused.Message, "start the server") || !strings.Contains(refused.Message, "localhost:4242") {
		t.Fatal(refused)
	}
	deadline, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	timedOut := c.wrapDialError(t.Context(), deadline, "localhost:4242", context.DeadlineExceeded)
	if !errors.Is(timedOut, ErrTimeout) || !errors.Is(timedOut, context.DeadlineExceeded) || !strings.Contains(timedOut.Message, "ConnectTimeout") || !strings.Contains(timedOut.Message, "localhost:4242") {
		t.Fatal(timedOut)
	}
	commandTimeout := (callDeadline{parent: t.Context(), ctx: deadline, command: "SET BLOCK", timeout: time.Second}).err()
	if !errors.Is(commandTimeout, ErrTimeout) || !errors.Is(commandTimeout, context.DeadlineExceeded) || commandTimeout.Command != "SET BLOCK" || !strings.Contains(commandTimeout.Message, "CommandTimeout") || !strings.Contains(commandTimeout.Message, "unknown outcome") {
		t.Fatal(commandTimeout)
	}
}

func TestConnectionGuidanceTLSMismatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		// Read the client's TLS record before closing, so unread input cannot
		// turn the intended plaintext response into a TCP reset.
		var header [5]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, make([]byte, binary.BigEndian.Uint16(header[3:]))); err != nil {
			return
		}
		conn.Write([]byte("plain listener\r\n"))
	}()
	_, err = Connect(t.Context(), Options{URI: "chunks://" + listener.Addr().String() + "/", ConnectTimeout: 2 * time.Second})
	<-done
	var record tls.RecordHeaderError
	if !errors.Is(err, ErrTLS) || !errors.As(err, &record) || !strings.Contains(err.Error(), "TLS listener") || !strings.Contains(err.Error(), "trusted CA") {
		t.Fatal(err)
	}
}

func TestConnectionGuidancePlainHelloClosed(t *testing.T) {
	s := newFakeServer(t, func(_ *fakeServer, conn net.Conn, _ string) { conn.Close() })
	_, err := Connect(t.Context(), Options{URI: s.uri("")})
	if !errors.Is(err, ErrConnection) || !errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "chunk:// needs a plain listener") || !strings.Contains(err.Error(), "chunks:// needs TLS") {
		t.Fatal(err)
	}
}

func TestTypedServerErrorsPreserveGuidance(t *testing.T) {
	for _, message := range []string{"WRITE on world; ask an administrator to grant this right", "MANAGES USERS; ask an administrator to grant this right"} {
		err := replyError(PhaseResponse, "SET BLOCK", Reply{Kind: ReplyError, Code: CodePermissionDenied, Message: message})
		var permission *PermissionDeniedError
		var underlying *Error
		wantRight, wantTable := "WRITE", "world"
		if strings.HasPrefix(message, "MANAGES USERS") {
			wantRight, wantTable = "MANAGES USERS", ""
		}
		if !errors.As(err, &permission) || !errors.As(err, &underlying) || !errors.Is(err, ErrPermissionDenied) || permission.Right != wantRight || permission.Table != wantTable || underlying.ServerMessage != message || !strings.Contains(err.Error(), "ask an administrator") {
			t.Fatal(err)
		}
	}
	err := replyError(PhaseResponse, "SET CHUNK", Reply{Kind: ReplyError, Code: CodeSchemaMismatch, Message: "current=7 schema changed; DESCRIBE the table and re-encode the chunk"})
	var schema *SchemaMismatchError
	if !errors.As(err, &schema) || schema.Current != 7 || !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), "re-encode the chunk") {
		t.Fatal(err)
	}
}
