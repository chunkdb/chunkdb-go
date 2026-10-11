// Package chunkdb connects Go applications to chunkdb, a storage engine for typed grid data.
// A block holds column values at a grid coordinate.
// A chunk groups a fixed rectangle of blocks; an area selects a rectangle of chunks.
// A schema defines each table's columns and chunk dimensions.
//
// # First connection
//
// Start a server, then connect with its administrator URI:
//
//	client, err := chunkdb.ConnectURI(ctx, uri)
//	if err != nil { return err }
//	defer client.Close()
//
// A fresh server has no tables.
// Run examples/world in the repository to create a table, write blocks, read an area, and watch a change.
// [Client.CreateTable] creates tables directly; [Client.Migrate] provides repeatable named schema steps.
// Table methods use [Options.Table], the URI path, then [DefaultTableName] when their table argument is empty.
//
// # Values and versions
//
// [EncodeValue] lists the Go values for each column type.
// A nil record means an absent block; a nil field means NULL.
// A chunk version identifies state for conditional writes; use [IfVersion] to compare it for equality.
// A revision orders committed changes within a table's change feed.
//
// # Connections and transactions
//
// [Client] supports concurrent use on one connection; [Pool] leases several connections.
// Credentials use SCRAM-SHA-256; chunks:// enables TLS.
// Network methods take a context; an interrupted in-flight request closes its connection.
// The next ordinary request reconnects without replaying the failed request; a failed write's outcome can be unknown.
//
// [Client.Transaction] reads one table snapshot and publishes writes together.
// Conflict retries can repeat its callback; keep external effects outside it.
// [Client.FlushWAL] makes previously acknowledged writes durable.
// [Client.CreateUser] and [Client.Grant] manage users and table access.
//
// # Watches and server operations
//
// [Client.Watch] uses a separate connection; close each watch yourself.
// [Client.CreateSlot] and [Watch.Ack] support durable consumers.
// [Client.Migrate] applies independently durable named steps and skips unchanged completed steps.
// [Client.Do] sends other CQL statements; backup paths refer to the server's filesystem.
//
// Errors support errors.Is and errors.As; see [Error] and [MigrationError].
package chunkdb
