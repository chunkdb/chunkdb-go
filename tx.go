package chunkdb

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// DefaultTxRetries is how many times [Client.Transaction] runs a transaction
// again after a conflict, unless [TxRetries] says otherwise.
const DefaultTxRetries = 5

// The pause before running a transaction again grows from txBackoffMin and
// stays below txBackoffMax; each pause is drawn at random from its upper half.
const (
	txBackoffMin = time.Millisecond
	txBackoffMax = 10 * time.Millisecond
)

// TxOption configures [Client.Transaction].
type TxOption func(*txOptions)

type txOptions struct {
	retries int
}

// TxRetries sets how many times [Client.Transaction] runs the transaction
// again after a conflict. Zero runs it once.
func TxRetries(n int) TxOption {
	return func(o *txOptions) { o.retries = max(0, n) }
}

// Transaction runs fn atomically and returns the shared version of written chunks, or zero when nothing changes.
// Reads use one table snapshot and include the transaction's own writes.
// All writes apply together when fn returns nil and commit succeeds.
//
// A conflict applies no writes and retries fn up to [DefaultTxRetries] times after the first attempt.
// [TxRetries] changes this limit; exhausted attempts return [ConflictError].
// Keep external effects outside fn because it can run again, and return statement errors.
// Other callback errors roll back without retrying; transport failures can leave the outcome unknown.
//
// Transaction holds the connection until it ends; other requests on that Client wait.
// Use only [Tx] methods inside fn; calling the originating Client can wait until its context expires.
// Use [Pool] to keep other work running.
func (c *Client) Transaction(ctx context.Context, fn func(tx *Tx) error, opts ...TxOption) (uint64, error) {
	options := txOptions{retries: DefaultTxRetries}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	release, err := c.holdConnection(ctx, "BEGIN")
	if err != nil {
		return 0, err
	}
	defer release()

	for attempt := 0; ; attempt++ {
		version, err := c.runTransaction(ctx, fn)
		var conflict *ConflictError
		if !errors.As(err, &conflict) || attempt >= options.retries {
			return version, err
		}
		if err := txBackoff(ctx, attempt); err != nil {
			return 0, err
		}
	}
}

// txBackoff pauses before attempt+1 runs.
func txBackoff(ctx context.Context, attempt int) error {
	ceiling := min(txBackoffMin<<min(attempt, 8), txBackoffMax)
	pause := ceiling/2 + rand.N(ceiling/2+1)
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return timeoutErrorf("BEGIN", ctx.Err(), "%s", ctx.Err())
	}
}

// runTransaction runs fn once, from BEGIN to COMMIT or ROLLBACK. A returned
// [*ConflictError] means the transaction ended with CONFLICT and may run again.
func (c *Client) runTransaction(ctx context.Context, fn func(tx *Tx) error) (uint64, error) {
	established, err := c.connection(ctx)
	if err != nil {
		return 0, err
	}
	reply, err := c.execOn(ctx, established, "BEGIN", "BEGIN", nil)
	if err == nil {
		err = expectSimple(reply, "BEGIN", "OK")
	}
	if err != nil {
		// The server may or may not have a transaction open now; dropping the
		// connection makes sure no later request of the client runs in one.
		_ = established.shutdown(connectionErrorf("BEGIN", err, "BEGIN failed: %s", err))
		return 0, err
	}

	tx := &Tx{client: c, conn: established, tables: make(map[string]bool)}
	err = tx.run(fn)
	var conflict *ConflictError
	switch {
	case tx.conflict != nil && (err == nil || errors.As(err, &conflict)):
		// A statement's CONFLICT ended the transaction, which stays on the
		// connection until ROLLBACK.
		tx.rollback(ctx)
		return 0, tx.conflict
	case err != nil:
		tx.rollback(ctx)
		return 0, err
	}
	return tx.commit(ctx)
}

// Tx is a transaction passed to [Client.Transaction] or [Pool.Transaction].
// Its methods take the same table and data arguments as Client methods.
// An empty table argument selects the client's default table.
// The first data statement selects one table; another table is refused without ending the transaction.
//
// Reads see the first statement's snapshot plus the transaction's own writes.
// [Chunk.Version] identifies the chunk state in that snapshot.
// Writes return no version until commit; [Client.Transaction] returns their shared version.
// [IfVersion] is unavailable because commit checks every chunk read or written.
//
// A Tx supports concurrent use; its statements run one at a time.
// Stop using it when fn returns.
type Tx struct {
	client *Client
	// conn is the connection BEGIN was sent on. Every statement goes to it
	// and never to a reconnected one, where it would run outside the
	// transaction; after the connection fails, they fail with its error.
	conn *conn

	// mu is held for the whole of each method, so a statement and the
	// transaction state it changes are never interleaved with another.
	mu sync.Mutex
	// done is set once fn returned.
	done bool
	// tables are the tables the statements named, whose cached schemas a
	// CONFLICT table_changed drops.
	tables map[string]bool
	// conflict is the CONFLICT that ended the transaction. No statement but
	// ROLLBACK is sent after it: the server answers each with the same
	// CONFLICT until ROLLBACK or COMMIT closes the transaction.
	conflict error
	// closed is set once COMMIT was sent: the server has closed the
	// transaction, and ROLLBACK is not needed.
	closed bool
}

// run calls fn and then closes the Tx to further statements. A panic in fn
// drops the connection, which rolls the transaction back, before it goes on.
func (tx *Tx) run(fn func(tx *Tx) error) (err error) {
	defer func() {
		tx.mu.Lock()
		tx.done = true
		tx.mu.Unlock()
		if r := recover(); r != nil {
			_ = tx.conn.shutdown(connectionErrorf("", nil, "the transaction function panicked"))
			panic(r)
		}
	}()
	return fn(tx)
}

// call sends one statement of the transaction on its connection. It refuses
// statements after fn returned, and after a CONFLICT, which the server would
// answer with the same CONFLICT.
func (tx *Tx) call(ctx context.Context, statement string, params [][]byte, options ...areaReplyOptions) (Reply, error) {
	command := commandOf(statement)
	if tx.done {
		return Reply{}, requestErrorf(command, "the transaction has ended")
	}
	if tx.conflict != nil {
		return Reply{}, tx.conflict
	}
	if err := checkLimits(tx.conn.info, command, statement, params); err != nil {
		return Reply{}, err
	}
	reply, err := tx.client.execOn(ctx, tx.conn, command, statement, params, options...)
	tx.ended(err)
	return reply, err
}

// ended records a CONFLICT, which ends the transaction.
func (tx *Tx) ended(err error) {
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		return
	}
	tx.conflict = err
	if conflict.Reason == ConflictTableChanged {
		for table := range tx.tables {
			tx.client.forgetSchema(table)
		}
	}
}

func (tx *Tx) tableName(command, table string) (string, error) {
	name, err := tx.client.tableName(command, table)
	if err == nil {
		tx.tables[name] = true
	}
	return name, err
}

// schemaOf returns the cached schema of table, or fetches it with DESCRIBE on
// the transaction's connection.
func (tx *Tx) schemaOf(ctx context.Context, table string) (*Schema, bool, error) {
	if schema := tx.client.cachedSchema(table); schema != nil {
		return schema, false, nil
	}
	schema, err := tx.fetchSchema(ctx, table)
	return schema, true, err
}

func (tx *Tx) refetchSchema(ctx context.Context, table string, _ error) (*Schema, error) {
	return tx.fetchSchema(ctx, table)
}

func (tx *Tx) fetchSchema(ctx context.Context, table string) (*Schema, error) {
	reply, err := tx.call(ctx, "DESCRIBE "+table, nil)
	return tx.client.keepSchema(table, reply, err)
}

func (tx *Tx) forgetSchema(table string) { tx.client.forgetSchema(table) }

// commit sends COMMIT, which closes the transaction in any case.
func (tx *Tx) commit(ctx context.Context) (uint64, error) {
	tx.closed = true
	reply, err := tx.client.execOn(ctx, tx.conn, "COMMIT", "COMMIT", nil)
	if err != nil {
		tx.ended(err)
		return 0, err
	}
	if reply.Kind == ReplyNull {
		return 0, nil
	}
	return versionOf(reply, "COMMIT")
}

// rollback closes a transaction that is still on the connection, open or
// ended by a statement's CONFLICT. When ROLLBACK does not answer +OK, it drops
// the connection, which rolls the transaction back, so no later request of
// the client runs inside it.
func (tx *Tx) rollback(ctx context.Context) {
	if tx.closed || tx.conn.terminalError() != nil {
		return
	}
	tx.closed = true
	reply, err := tx.client.execOn(ctx, tx.conn, "ROLLBACK", "ROLLBACK", nil)
	if err == nil {
		err = expectSimple(reply, "ROLLBACK", "OK")
	}
	if err != nil {
		_ = tx.conn.shutdown(connectionErrorf("ROLLBACK", err, "ROLLBACK failed: %s", err))
	}
}

// expectNoVersion checks the reply of a write inside a transaction: null, as
// the write applies at COMMIT.
func expectNoVersion(reply Reply, err error, command string) error {
	if err != nil {
		return err
	}
	if reply.Kind != ReplyNull {
		return protocolErrorf(command, "expected _ for a write inside a transaction, got a reply of kind %d", reply.Kind)
	}
	return nil
}

// GetBlock reads block (x, y) like [Client.GetBlock], as of the transaction's
// snapshot and its own writes.
func (tx *Tx) GetBlock(ctx context.Context, table string, x, y int64, columns ...string) (Record, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getBlock(ctx, tx, table, x, y, columns)
}

// SetBlock writes block (x, y) like [Client.SetBlock]; the write applies at
// COMMIT.
func (tx *Tx) SetBlock(ctx context.Context, table string, x, y int64, values Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setBlock(ctx, tx, table, x, y, values, "")
	return expectNoVersion(reply, err, "SET BLOCK")
}

// DeleteBlock deletes block (x, y) like [Client.DeleteBlock]; the delete
// applies at COMMIT.
func (tx *Tx) DeleteBlock(ctx context.Context, table string, x, y int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := deleteBlock(ctx, tx, table, x, y, "")
	return expectNoVersion(reply, err, "DELETE BLOCK")
}

// GetChunk reads chunk (cx, cy) like [Client.GetChunk], as of the
// transaction's snapshot and its own writes.
func (tx *Tx) GetChunk(ctx context.Context, table string, cx, cy int64, columns ...string) (*Chunk, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getChunk(ctx, tx, table, cx, cy, columns)
}

// GetChunkRaw reads chunk (cx, cy) like [Client.GetChunkRaw], as of the
// transaction's snapshot and its own writes.
func (tx *Tx) GetChunkRaw(ctx context.Context, table string, cx, cy int64, columns ...string) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getChunkRaw(ctx, tx, table, cx, cy, columns)
}

// SetChunk replaces chunk (cx, cy) like [Client.SetChunk]; the write applies
// at COMMIT.
func (tx *Tx) SetChunk(ctx context.Context, table string, cx, cy int64, chunk *Chunk) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunk(ctx, tx, table, cx, cy, chunk, "")
	return expectNoVersion(reply, err, "SET CHUNK")
}

// SetChunkRaw replaces chunk (cx, cy) with a chunk form like
// [Client.SetChunkRaw]; the write applies at COMMIT.
func (tx *Tx) SetChunkRaw(ctx context.Context, table string, cx, cy int64, form []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	reply, err := setChunkRaw(ctx, tx, table, cx, cy, form, "")
	return expectNoVersion(reply, err, "SET CHUNK")
}

// GetArea reads an area like [Client.GetArea], as of the transaction's
// snapshot and its own writes.
func (tx *Tx) GetArea(ctx context.Context, table string, cx0, cy0, cx1, cy1 int64, columns ...string) ([]AreaChunk, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getArea(ctx, tx, table, areaRange(cx0, cy0, cx1, cy1), columns)
}

// GetAreaAround reads an area like [Client.GetAreaAround], as of the
// transaction's snapshot and its own writes.
func (tx *Tx) GetAreaAround(ctx context.Context, table string, cx, cy, radius int64, columns ...string) ([]AreaChunk, error) {
	area, err := areaAround(cx, cy, radius)
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return getArea(ctx, tx, table, area, columns)
}
