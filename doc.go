// Package chunkdb is the official Go client for chunkdb.
//
// It speaks chunkdb protocol 3: one CQL statement per request, values sent as
// binary parameters, typed RESP3 replies. It does not connect to servers of an
// earlier protocol.
//
// The package is intentionally small:
//
//   - [Client] is one long-lived socket
//   - requests are sequential per client by default, with opt-in pipelining
//     ([Options.PipelineDepth])
//   - opt-in pooling via [Pool]
//   - no background reconnect loops
//
// Every connection starts with the HELLO 3 handshake, which logs in the user
// ([Options.User] and [Options.Password], or chunk://user:password@host/)
// with SCRAM-SHA-256: the password never crosses the network, and the server
// proves it holds the user's verifier. [Client.ServerInfo] returns the
// server's limits. [Client.CreateUser], [Client.Grant] and the other user
// methods manage users and their rights. Every statement names its
// table; the methods take the table as their first argument after the
// context, and "" means the client's default table ([Options.Table], the URI
// path, else "default").
//
// Values are typed by the table's columns. A client caches each table's
// schema ([Client.Schema]) to encode parameters and decode chunks;
// [EncodeValue] lists the Go types of each column type. [Client.Do] sends any
// statement with raw parameters. [Client.Transaction] reads one snapshot of
// a table and writes several chunks together, running again on a conflict.
//
// Every request method takes a [context.Context]. Cancelling it aborts the
// call; because the protocol has no request identifiers, an aborted in-flight
// request also drops the connection, since the client cannot resynchronize
// with the reply stream. The next request transparently reconnects.
//
// Basic use:
//
//	client, err := chunkdb.ConnectURI(ctx, "chunk://bot:secret@127.0.0.1:4242/world")
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	version, err := client.SetBlock(ctx, "", 10, 4, chunkdb.Record{"id": 23, "light": 7})
//	if err != nil {
//		return err
//	}
//	block, err := client.GetBlock(ctx, "", 10, 4)
package chunkdb
