// Package chunkdb is the official Go client for chunkdb.
//
// It speaks chunkdb protocol 2, which chunkdb 2.0 servers serve; it does not
// connect to chunkdb 1.x servers.
//
// The package is intentionally small:
//
//   - [Client] is one long-lived socket
//   - requests are sequential per client by default, with opt-in pipelining
//     ([Options.PipelineDepth])
//   - opt-in pooling via [Pool]
//   - no automatic retries or background reconnect loops
//
// Every connection starts with the HELLO 2 handshake, which carries the token
// and the table; [Client.ServerInfo] returns the reply. A chunkdb server holds
// named tables; a client works on one of them, chosen by [Options.Table], the
// URI path, or [Client.Use], and the server's default table otherwise.
//
// Chunk data is binary: [Client.GetChunk], [Client.GetChunkState],
// [Client.PutChunk] and [Client.PutChunkState] transfer the packed block
// payload (bit i of the chunk is payload[i/8] >> (i%8) & 1) and the presence
// bitmap, sized by the table's geometry.
//
// Every request method takes a [context.Context]. Cancelling it aborts the
// call; because the protocol has no request identifiers, an aborted in-flight
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
//	block, err := client.Get(ctx, 0, 0)
package chunkdb
