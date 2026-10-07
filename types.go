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

	Host string
	Port int
	// Token is sent as HELLO 2 AUTH <token> when a connection opens.
	Token string

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

	// Table is the table the connection works on, named in HELLO when
	// connecting and reconnecting. Empty means the URI path
	// (chunk://host:4242/terrain), then the server's default table.
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
	// ExtraMaxBlockBits is the longest per-block extra data value, in bits;
	// a non-zero value enables extra data on the table. Enabling is
	// permanent, and both extra data limits can only be raised. Zero in a
	// [TableInfo] means the table has no extra data.
	ExtraMaxBlockBits int
	// ExtraMaxChunkBytes is the most extra data one chunk can hold, 9 to
	// 16777216 (default 65536): each value costs 8 bytes plus ceil(bits/8).
	// It is refused on a table without extra data unless ExtraMaxBlockBits
	// enables it in the same call. Zero in a [TableInfo] means the table has
	// no extra data.
	ExtraMaxChunkBytes int
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

// TableInfo is the geometry, options and identity of a table, as HELLO,
// TABLEINFO and USE report them.
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

// HelloInfo is the server's reply to the HELLO handshake that opens every
// connection.
type HelloInfo struct {
	// Protocol is the protocol version, always 2.
	Protocol      int
	ServerVersion string
	// Capabilities lists optional features, for example "zrle" and
	// "extra-data".
	Capabilities []string
	// MaxLineBytes bounds one request line.
	MaxLineBytes int
	// MaxAreaChunks is the most chunks one [Client.ChunkRange] or
	// [Client.ChunkRadius] call may cover.
	MaxAreaChunks int
	// MaxResponseBytes caps a [Client.ChunkRange] or [Client.ChunkRadius]
	// response.
	MaxResponseBytes int
	// MaxScanLimit is the largest [Client.ChunkScan] limit.
	MaxScanLimit int
	// MaxBatchOps is the most operations one [Client.ChunkBatch] may carry.
	MaxBatchOps int
	// MaxExtraChunkBytes is the most extra data any chunk can hold on this
	// server, whatever its table options; it bounds [Client.XPut] values and
	// the extra data of [Client.PutChunkStateExtra]. Zero when the server
	// does not report it.
	MaxExtraChunkBytes int
	// Table is the connection's table at HELLO time, or nil when the
	// connection has none (the server has no default table and none was
	// named). [Client.Use] reports later selections.
	Table *TableInfo
	// Values holds every key/value line of the reply.
	Values map[string]string
}

// Info is the parsed result of the INFO command: runtime statistics of the
// selected table.
type Info struct {
	// Raw is the server payload exactly as received.
	Raw string
	// Values holds the parsed key/value pairs.
	Values map[string]string
}

// BlockState is one block as read by [Client.Get] and [Client.MGet].
//
// An unset block reports Exists false and an empty Bits; an explicitly stored
// all-zero block reports Exists true with an all-zero Bits string.
type BlockState struct {
	Exists bool
	Bits   string
}

// ChunkState is a chunk's binary state, as read by [Client.GetChunkState].
//
// Payload holds the packed block bits: bit i of the chunk is
// payload[i/8] >> (i%8) & 1. Presence holds one bit per block, laid out the
// same way, set when the block is explicitly present. Exists reports whether
// any presence bit is set.
type ChunkState struct {
	Exists   bool
	Payload  []byte
	Presence []byte
}

// ChunkStateInput is the chunk state written by [Client.PutChunkState], in
// the layout of [ChunkState].
type ChunkStateInput struct {
	Payload  []byte
	Presence []byte
}

// ExtraValue is the extra data of one block: BitLength bits (at least 1)
// held in Bytes, ceil(BitLength/8) of them. Bit n of the value is
// Bytes[n/8] >> (n%8) & 1. Padding bits past BitLength in the last byte are
// ignored when writing and zero when reading.
type ExtraValue struct {
	BitLength int
	Bytes     []byte
}

// ChunkStateExtra is a chunk's state and extra data, as read by
// [Client.GetChunkStateExtra]. Payload, Presence and Exists are as in
// [ChunkState].
type ChunkStateExtra struct {
	Exists   bool
	Payload  []byte
	Presence []byte
	// Extra holds the value of every block that has one, by block index:
	// local_y * ChunkWidthBlocks + local_x, where the local coordinates are
	// the block coordinates modulo the chunk size (never negative). It is
	// empty, not nil, when no block has a value.
	Extra map[int]ExtraValue
}

// ChunkStateExtraInput is the chunk state and extra data written by
// [Client.PutChunkStateExtra], in the layout of [ChunkStateExtra]. Extra
// replaces all of the chunk's values; each must belong to a block that
// Presence marks present.
type ChunkStateExtraInput struct {
	Payload  []byte
	Presence []byte
	Extra    map[int]ExtraValue
}

// GetOptions configure a chunk read.
type GetOptions struct {
	// ZRLE transfers the chunk zrle-compressed. The client decompresses it
	// and checks its size.
	ZRLE bool
}

// PutOptions configure a chunk write.
type PutOptions struct {
	// IfVersion, when set, applies the write only if the chunk's current
	// version equals it.
	IfVersion *uint64
	// ZRLE sends the chunk zrle-compressed when that is smaller than the raw
	// bytes, and raw otherwise.
	ZRLE bool
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
// [Client.ChunkRadius], with its state in the layout of [ChunkState].
type RangeEntry struct {
	CX       int64
	CY       int64
	Payload  []byte
	Presence []byte
}

// BatchOpType selects the operation performed by a [BatchOperation].
type BatchOpType int

const (
	// BatchSet writes one block.
	BatchSet BatchOpType = iota + 1
	// BatchUnset clears explicit presence for one block.
	BatchUnset
	// BatchXPut sets the extra data of one block, which must be present at
	// that point of the batch.
	BatchXPut
	// BatchXDel removes the extra data of one block, if it has any.
	BatchXDel
)

// BatchOperation is one block operation inside a [Client.ChunkBatch] call.
type BatchOperation struct {
	Type BatchOpType
	X    int64
	Y    int64
	// Bits is used by [BatchSet] and [BatchXPut].
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

// XPutOp returns a [BatchXPut] operation. bits is the value as 0/1 text:
// character n is bit n, and the value is len(bits) bits long.
func XPutOp(x, y int64, bits string) BatchOperation {
	return BatchOperation{Type: BatchXPut, X: x, Y: y, Bits: bits}
}

// XDelOp returns a [BatchXDel] operation.
func XDelOp(x, y int64) BatchOperation {
	return BatchOperation{Type: BatchXDel, X: x, Y: y}
}

// MutationResult is the result of a chunk write ([Client.PutChunk],
// [Client.PutChunkState], [Client.PutChunkStateExtra], [Client.ChunkBatch]).
//
// Version is the chunk's version after the write. Versions are opaque tokens:
// they change on every content mutation and survive eviction and restart; a
// write that does not change the chunk keeps its version. When a conditional
// write's version does not match, OK is false and Version is the chunk's
// current version; the chunk is unchanged.
type MutationResult struct {
	OK      bool
	Version uint64
}
