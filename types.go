package chunkdb

import "time"

// DefaultTimeout is used for connect and command deadlines when [Options]
// leaves them at zero.
const DefaultTimeout = 5 * time.Second

const (
	defaultHost = "127.0.0.1"
	// DefaultPort is the chunkdb server port used when a URI omits one.
	DefaultPort = 4242
	// DefaultTableName is the table a client works on when neither
	// [Options.Table] nor the URI path names one.
	DefaultTableName = "default"
)

// Options configure a [Client].
//
// Explicit fields win over values taken from URI. A zero Host, Port, User or
// Password falls back to URI and then to the package default.
type Options struct {
	// URI is a chunk:// or chunks:// endpoint, optionally carrying the user
	// and password as its userinfo component
	// (chunk://user:password@host:4242/) and the default table as its path.
	URI string

	Host string
	Port int
	// User and Password log in with SCRAM-SHA-256 when a connection opens;
	// the password never crosses the network. Without a user the client
	// sends HELLO 3 alone, which only a server started with --auth none
	// accepts.
	User     string
	Password string
	// VerifierIterations is the PBKDF2 iteration count of the verifiers
	// [Client.CreateUser] and [Client.SetPassword] compute. Zero means
	// [DefaultVerifierIterations]; fewer are refused.
	VerifierIterations int

	// ConnectTimeout bounds establishing the socket, the TLS handshake and
	// HELLO. Zero means [DefaultTimeout]; a negative value disables the
	// client-side deadline and leaves cancellation to the caller's context.
	ConnectTimeout time.Duration
	// CommandTimeout bounds waiting for one reply. Zero means
	// [DefaultTimeout]; a negative value disables the client-side deadline.
	CommandTimeout time.Duration

	// TLS forces a TLS transport. A chunks:// URI enables it as well.
	TLS bool
	// TLSInsecure skips certificate verification. Local testing only.
	TLSInsecure bool
	// TLSServerName overrides the SNI and hostname-verification target.
	TLSServerName string
	// CA, Cert, and Key hold PEM material for custom trust roots and client
	// certificates.
	CA   []byte
	Cert []byte
	Key  []byte

	// PipelineDepth caps concurrent in-flight requests on one connection.
	// Zero and one both mean sequential request/reply.
	PipelineDepth int

	// Table is the client's default table: the one a method uses when its
	// table argument is "". Empty means the URI path
	// (chunk://host:4242/terrain), then [DefaultTableName]. Every statement
	// names its table on the wire.
	Table string
}

// ServerInfo is the server's reply to the HELLO 3 handshake that opens every
// connection.
type ServerInfo struct {
	// Protocol is the protocol version, always 3.
	Protocol      int
	ServerVersion string
	// MaxLineBytes bounds one statement line, CRLF included.
	MaxLineBytes int
	// MaxParameters is the most parameters one statement takes.
	MaxParameters int
	// MaxAreaChunks is the most chunks one [Client.GetArea] or
	// [Client.GetAreaAround] covers.
	MaxAreaChunks int
	// MaxResponseBytes caps the reply of one area read.
	MaxResponseBytes int
	// MaxScanLimit is the largest [Client.ScanChunks] limit.
	MaxScanLimit int
	// ServerSignature is the SCRAM server-final message (v=...) the client
	// checked when it logged in with a user; empty without a user.
	ServerSignature string
}

// Record is one block: column name to value. See [EncodeValue] for the Go
// types each column type takes and returns.
type Record map[string]any

// ChunkCoord is a chunk coordinate; chunk coordinates count chunks, not
// blocks.
type ChunkCoord struct {
	CX int64
	CY int64
}

// AreaChunk is one chunk of an area read.
type AreaChunk struct {
	CX    int64
	CY    int64
	Chunk *Chunk
}

// ScanPage is one page of [Client.ScanChunks].
type ScanPage struct {
	// Chunks are the chunks with a present block, in ascending CX then CY.
	Chunks []ChunkCoord
	// More reports whether chunks follow the last one; pass the last one as
	// the next call's after.
	More bool
}

// WriteOption configures a write.
type WriteOption func(*writeOptions)

type writeOptions struct {
	ifVersion *uint64
}

// IfVersion makes a write conditional: it applies only while the chunk is
// still at version, and fails with a [*VersionMismatchError] otherwise. The
// version belongs to the whole chunk, so a block write also fails when
// another block of the chunk changed.
func IfVersion(version uint64) WriteOption {
	return func(o *writeOptions) { o.ifVersion = &version }
}

func resolveWriteOptions(opts []WriteOption) writeOptions {
	var resolved writeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}

// TableOptions are the options of a table. In a [TableSpec], a zero field
// takes the server's default.
type TableOptions struct {
	// DurabilityMode is "relaxed", "fsync-wal" or "fsync-checkpoint".
	DurabilityMode        string
	CheckpointUpdates     uint64
	CheckpointWalBytes    uint64
	WalGroupCommitUpdates uint64
	// CheckpointCompression is "none" or "zrle".
	CheckpointCompression string
	// VarMaxChunkBytes is the most bytes of text and bytes values one chunk
	// holds, counting 12 bytes per value.
	VarMaxChunkBytes uint64
}

// ColumnDef is a column of [TableSpec] or [Client.AddColumn].
type ColumnDef struct {
	Name string
	Type ColumnType
	// Null lets the column hold NULL.
	Null bool
	// Required makes a new block give the column a value.
	Required bool
	// Default is the value a new block takes when it does not give one; nil
	// means none. It takes the Go types [EncodeValue] lists.
	Default any
}

// TableSpec describes a new table for [Client.CreateTable].
type TableSpec struct {
	Columns []ColumnDef
	// ChunkWidth and ChunkHeight are the blocks of a chunk; both are
	// required.
	ChunkWidth  int
	ChunkHeight int
	// LargeWidth and LargeHeight are the chunks of a large chunk (one file
	// group on disk); zero takes the server's default.
	LargeWidth  int
	LargeHeight int
	Options     TableOptions
}

// Conversion is how [Client.AlterColumnType] converts stored values that the
// new type cannot hold.
type Conversion int

const (
	// ConvertNone widens, or narrows after the server checked every stored
	// value; the first value that does not fit fails the statement.
	ConvertNone Conversion = iota
	// ConvertClamp converts numbers to the nearest value.
	ConvertClamp
	// ConvertDefault converts to the column's default.
	ConvertDefault
	// ConvertTruncate cuts text, bytes and bits.
	ConvertTruncate
)
