package chunkdb

import "time"

// DefaultTimeout is used for connect and command deadlines when [Options]
// leaves them at zero.
const DefaultTimeout = 5 * time.Second

const (
	defaultHost = "127.0.0.1"
	// DefaultPort is the chunkdb server port used when a URI omits one.
	DefaultPort = 4242
)

// Options configure a [Client].
//
// Explicit fields win over values taken from URI. A zero Host, Port, or Token
// falls back to URI and then to the package default.
type Options struct {
	// URI is a chunk:// or chunks:// endpoint, optionally carrying the token
	// as its userinfo component.
	URI string

	Host  string
	Port  int
	Token string

	// DisableAutoAuth skips the implicit AUTH sent after connecting. By
	// default the client authenticates automatically whenever a token is
	// configured.
	DisableAutoAuth bool

	// ConnectTimeout bounds establishing the socket and completing the TLS
	// handshake. Zero means [DefaultTimeout]; a negative value disables the
	// client-side deadline and leaves cancellation to the caller's context.
	ConnectTimeout time.Duration
	// CommandTimeout bounds waiting for one command response. Zero means
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
	// Zero and one both mean sequential request/response.
	PipelineDepth int

	// Table is the table the connection works on (chunkdb 2.0+), selected
	// with USE after connecting and after every reconnect. Empty means the
	// URI path (chunk://host:4242/terrain), then the server's default table.
	Table string
}

// TableOptions are the options of a table. In [Client.SetTableOptions] and
// [TableSpec], a zero field leaves the option unchanged (or at the server's
// default for a new table).
type TableOptions struct {
	// DurabilityMode is "relaxed", "fsync-wal" or "fsync-checkpoint".
	DurabilityMode        string
	CheckpointUpdates     int
	CheckpointWalBytes    int
	WalGroupCommitUpdates int
	// CheckpointCompression is "none" or "zrle".
	CheckpointCompression string
}

// TableSpec describes a new table for [Client.CreateTable]. Its geometry is
// fixed once the table exists. Zero chunk and large-chunk sizes take the
// server defaults (16x16 blocks, 8x8 chunks).
type TableSpec struct {
	BlockBits              int
	ChunkWidthBlocks       int
	ChunkHeightBlocks      int
	LargeChunkWidthChunks  int
	LargeChunkHeightChunks int
	Options                TableOptions
}

// TableInfo is the geometry, options and identity of a table, as TABLEINFO
// and USE report them.
type TableInfo struct {
	Name string
	// StoreID changes when a table is dropped and created again under the
	// same name.
	StoreID                string
	BlockBits              int
	ChunkWidthBlocks       int
	ChunkHeightBlocks      int
	LargeChunkWidthChunks  int
	LargeChunkHeightChunks int
	Options                TableOptions
	// Values holds every key/value line of the reply.
	Values map[string]string
}

// Info is the parsed result of the INFO command.
type Info struct {
	// Raw is the server payload exactly as received.
	Raw string
	// Values holds the parsed key/value pairs.
	Values map[string]string
}

// BlockState is the result of [Client.ReadBlock].
//
// Bits is empty when Exists is false; an explicitly stored all-zero block
// reports Exists true with an all-zero Bits string.
type BlockState struct {
	Exists bool
	Bits   string
}

// ChunkState is the result of [Client.ReadChunk]. Exists reports whether any
// block in the chunk is explicitly present.
type ChunkState struct {
	Exists   bool
	Bits     string
	Presence string
}

// ChunkStateInput is the exact chunk state written by
// [Client.SetChunkState] and [Client.ChunkCompareAndSet].
type ChunkStateInput struct {
	Bits     string
	Presence string
}

// Block is one item of a batch write.
type Block struct {
	X    int64
	Y    int64
	Bits string
}

// BlockRef is one item of a batch read.
type BlockRef struct {
	X int64
	Y int64
}

// CoordPair is a chunk coordinate.
type CoordPair struct {
	CX int64
	CY int64
}

// ScanResult is the result of [Client.ChunkScan].
type ScanResult struct {
	Coords []CoordPair
	// NextCursor is nil once the scan is exhausted; otherwise pass it back to
	// [Client.ChunkScan] to continue.
	NextCursor *CoordPair
}

// RangeEntry is one populated chunk returned by [Client.ChunkRange] or
// [Client.ChunkRadius].
type RangeEntry struct {
	CX       int64
	CY       int64
	Bits     string
	Presence string
}

// BatchOpType selects the operation performed by a [BatchOperation].
type BatchOpType int

const (
	// BatchSet writes one block.
	BatchSet BatchOpType = iota + 1
	// BatchUnset clears explicit presence for one block.
	BatchUnset
)

// BatchOperation is one block operation inside a [Client.ChunkBatch] call.
type BatchOperation struct {
	Type BatchOpType
	X    int64
	Y    int64
	// Bits is used by [BatchSet] only.
	Bits string
}

// SetOp returns a [BatchSet] operation.
func SetOp(x, y int64, bits string) BatchOperation {
	return BatchOperation{Type: BatchSet, X: x, Y: y, Bits: bits}
}

// UnsetOp returns a [BatchUnset] operation.
func UnsetOp(x, y int64) BatchOperation {
	return BatchOperation{Type: BatchUnset, X: x, Y: y}
}

// MutationResult is the result of a conditional chunk mutation.
//
// Versions are opaque tokens: they change on every content mutation and
// whenever the server reloads the chunk (eviction or restart). On a version
// mismatch OK is false and Version is the chunk's current version; the stored
// state is unchanged.
type MutationResult struct {
	OK      bool
	Version uint64
}
