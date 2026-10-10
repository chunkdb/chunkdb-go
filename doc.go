// Package chunkdb is the Go client for chunkdb 2.0, using CQL and protocol 3.
// Tables contain chunks of typed blocks; methods encode values as binary
// parameters and decode typed replies.
//
// # First connection
//
// Start a server, then connect with its administrator URI:
//
//	client, err := chunkdb.ConnectURI(ctx, uri)
//	if err != nil { return err }
//	defer client.Close()
//	_, err = client.Migrate(ctx, []chunkdb.Migration{{
//	    Name: "world_go_schema",
//	    Statement: "CREATE TABLE world_go (tile u8, label text(16)) CHUNK 2 x 2",
//	}})
//	if err != nil { return err }
//	_, err = client.SetBlock(ctx, "world_go", 0, 0, chunkdb.Record{"tile": 1, "label": "grass"})
//	if err != nil { return err }
//	block, err := client.GetBlock(ctx, "world_go", 0, 0)
//	if err != nil { return err }
//
// The runnable world example is at examples/world in the repository.
// Every statement names a table; "" uses [Options.Table], then the URI path,
// then [DefaultTableName]. [EncodeValue] lists each column type's Go values.
// A nil record means an absent block; a nil field means NULL.
//
// # Connections and data
//
// [Client] is safe for concurrent use and owns one connection; [Pool] leases
// multiple connections. Credentials use SCRAM-SHA-256; chunks:// selects TLS.
// Every method takes a context. An interrupted in-flight ordinary request
// closes its connection; the next request reconnects without replaying the
// failed request. A failed write's outcome may be unknown.
//
// [Client.GetChunk], [Client.SetChunk], [Client.GetArea] and [Client.AllChunks]
// handle chunk data and scans. [Client.Transaction] reads one table snapshot
// and atomically publishes its writes; conflict retries may repeat its callback.
// [Client.CreateUser], [Client.Grant] and related methods administer users.
// [Client.Watch] owns a separate connection; [Client.CreateSlot] and [Watch.Ack]
// support durable consumers. Watches must be closed separately.
// [Client.Migrate] runs independently durable named schema steps and skips
// completed names with unchanged text. [Client.Do] sends other CQL statements,
// including BACKUP; backup paths belong to the server's filesystem.
// [Client.FlushWAL] makes previously acknowledged writes durable.
// Errors support errors.Is and errors.As; see [Error] and [MigrationError].
package chunkdb
