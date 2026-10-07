// Package chunkdb is the official Go client for chunkdb.
//
// It targets the stable chunkdb 1.x protocol.
//
// The package is intentionally small:
//
//   - [Client] is one long-lived socket
//   - requests are sequential per client by default, with opt-in pipelining
//     ([Options.PipelineDepth])
//   - opt-in pooling via [Pool]
//   - no automatic retries or background reconnect loops
//
// A chunkdb 2.0 server holds named tables; a client works on one of them,
// chosen by [Options.Table], the URI path, or [Client.Use], and the server's
// default table otherwise.
//
// Every request method takes a [context.Context]. Cancelling it aborts the
// call; because protocol v1 has no request identifiers, an aborted in-flight
// request also drops the connection, since the client cannot resynchronize
// with the response stream. The next request transparently reconnects.
//
// Basic use:
//
//	client, err := chunkdb.ConnectURI(ctx, "chunk://chunk-token@127.0.0.1:4242/")
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	if err := client.Set(ctx, 0, 0, "1011001110110011"); err != nil {
//		return err
//	}
//	state, err := client.ReadBlock(ctx, 0, 0)
package chunkdb
