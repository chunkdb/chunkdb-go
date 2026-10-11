package chunkdb

import (
	"context"
)

// ProgressEvent reports the completed frontier after filtered-out changes.
// Returning it from Next makes that position eligible for acknowledgement.
type ProgressEvent struct{ Position Position }

func (e *ProgressEvent) GetPosition() Position { return e.Position }

// ChunkChange contains chunk-column rows before and after a commit.
type ChunkChange struct {
	CX, CY        int64
	CZ            *int64
	Before, After Record
}

// ChunkFrame contains a committed whole-chunk form, or nil for deletion.
// Form remains raw so future form formats do not require guessed decoding.
type ChunkFrame struct {
	CX, CY  int64
	CZ      *int64
	Version uint64
	Form    []byte
}

// ChunksEvent reports whole-chunk changes in one completed commit.
type ChunksEvent struct {
	Position      Position
	CommitTimeMs  uint64
	User          *string
	SchemaVersion uint64
	Chunks        []ChunkFrame
	Tag           []byte
	Txn           *uint64
}

func (e *ChunksEvent) GetPosition() Position { return e.Position }

// CatalogChange preserves catalog-specific data without assuming a schema.
type CatalogChange struct {
	Kind, Action, Name, Table *string
	Data                      Reply
}

// CatalogEvent is the reserved catalog envelope. Its decoding does not imply
// that the server implements WATCH CATALOG; check negotiated capabilities.
type CatalogEvent struct {
	Position     Position
	CommitTimeMs uint64
	User         *string
	Events       []CatalogChange
	Txn          *uint64
}

func (e *CatalogEvent) GetPosition() Position { return e.Position }

func parsePositionMap(r Reply) (Position, error) {
	epoch, _ := r.Lookup("epoch")
	revision, _ := r.Lookup("revision")
	n, ok := revision.Uint64()
	if r.Kind != ReplyMap || epoch.Kind != ReplyBulk || !validEpoch(string(epoch.Bulk)) || !ok {
		return Position{}, protocolErrorf("WATCH", "invalid position map")
	}
	return Position{string(epoch.Bulk), n}, nil
}

func watchColumns(r Reply, nonempty bool) ([]Column, error) {
	if r.Kind != ReplyArray || (nonempty && len(r.Array) == 0) {
		return nil, protocolErrorf("WATCH", "invalid schema columns")
	}
	columns := make([]Column, 0, len(r.Array))
	names := map[string]bool{}
	ids := map[uint32]bool{}
	for _, entry := range r.Array {
		col, err := parseColumn(entry)
		if err != nil {
			return nil, annotate("WATCH", err)
		}
		if names[col.Name] || ids[col.ID] {
			return nil, protocolErrorf("WATCH", "duplicate schema column")
		}
		names[col.Name] = true
		ids[col.ID] = true
		columns = append(columns, col)
	}
	return columns, nil
}

func nullableText(r Reply) (*string, error) {
	if r.Kind == ReplyNull {
		return nil, nil
	}
	if r.Kind != ReplyBulk {
		return nil, protocolErrorf("WATCH", "expected nullable text")
	}
	s := string(r.Bulk)
	return &s, nil
}

func nullableTag(r Reply) ([]byte, error) {
	if r.Kind == ReplyNull {
		return nil, nil
	}
	if r.Kind != ReplyBulk || len(r.Bulk) > 256 {
		return nil, protocolErrorf("WATCH", "invalid tag")
	}
	return append([]byte{}, r.Bulk...), nil
}

func (w *Watch) eventSchema(ctx context.Context, version uint64) (*Schema, error) {
	if schema := w.schemaDetails[version]; schema != nil {
		return schema, nil
	}
	if columns, ok := w.schemas[version]; ok {
		return &Schema{Version: version, Dims: 2, Columns: columns}, nil
	}
	schema, err := w.describe(ctx)
	if err != nil {
		return nil, err
	}
	if schema.Version != version {
		return nil, protocolErrorf("WATCH", "schema version %d unavailable (DESCRIBE returned %d); resume or rebuild state", version, schema.Version)
	}
	if w.schemaDetails == nil {
		w.schemaDetails = make(map[uint64]*Schema)
	}
	w.schemaDetails[version] = schema
	w.schemas[version] = schema.Columns
	return schema, nil
}

func (w *Watch) decode(ctx context.Context, r Reply) (Event, error) {
	bad := func() (Event, error) { return nil, protocolErrorf("WATCH", "malformed push") }
	if r.Kind != ReplyPush || len(r.Array) == 0 || r.Array[0].Kind != ReplyBulk {
		return bad()
	}
	a := r.Array
	kind := string(a[0].Bulk)
	if kind == "schema" && len(a) >= 2 && a[1].Kind == ReplyMap {
		payload := a[1]
		pos, err := parsePositionMap(payload)
		if err != nil {
			return nil, err
		}
		v, _ := payload.Lookup("schema_version")
		version, ok := v.Uint64()
		if !ok {
			return bad()
		}
		d, _ := payload.Lookup("dims")
		dims, ok := d.Int64()
		if !ok || (dims != 2 && dims != 3) {
			return bad()
		}
		cols, _ := payload.Lookup("columns")
		columns, err := watchColumns(cols, true)
		if err != nil {
			return nil, err
		}
		chunks, _ := payload.Lookup("chunk_columns")
		chunkColumns, err := watchColumns(chunks, false)
		if err != nil {
			return nil, err
		}
		if w.schemaDetails == nil {
			w.schemaDetails = make(map[uint64]*Schema)
		}
		w.schemas[version] = columns
		w.schemaDetails[version] = &Schema{Version: version, Dims: int(dims), Columns: columns, ChunkColumns: chunkColumns}
		return &SchemaEvent{Position: pos, Version: version, Dims: int(dims), Columns: cloneColumns(columns), ChunkColumns: cloneColumns(chunkColumns)}, nil
	}
	if kind == "schema" || (kind == "change" && len(a) == 7) || (kind == "resync" && len(a) == 3) {
		return w.decodeLegacy(ctx, r)
	}
	if len(a) < 3 || a[1].Kind != ReplyBulk || !validEpoch(string(a[1].Bulk)) {
		return bad()
	}
	revision, ok := a[2].Uint64()
	if !ok {
		return bad()
	}
	pos := Position{string(a[1].Bulk), revision}
	switch kind {
	case "progress":
		return &ProgressEvent{pos}, nil
	case "resync":
		if len(a) < 5 {
			return bad()
		}
		parent, err := nullableText(a[3])
		if err != nil || (parent != nil && !validEpoch(*parent)) {
			return bad()
		}
		var fork *uint64
		if a[4].Kind != ReplyNull {
			n, ok := a[4].Uint64()
			if !ok {
				return bad()
			}
			fork = &n
		}
		if (parent == nil) != (fork == nil) {
			return bad()
		}
		return &ResyncEvent{Position: pos, ParentEpoch: parent, ForkRevision: fork}, nil
	case "change", "chunks":
		minimum := 10
		if kind == "chunks" {
			minimum = 9
		}
		if len(a) < minimum {
			return bad()
		}
		ms, ok := a[3].Uint64()
		if !ok {
			return bad()
		}
		user, err := nullableText(a[4])
		if err != nil {
			return nil, err
		}
		version, ok := a[5].Uint64()
		if !ok || a[6].Kind != ReplyArray {
			return bad()
		}
		schema, err := w.eventSchema(ctx, version)
		if err != nil {
			return nil, err
		}
		tag, err := nullableTag(a[7])
		if err != nil {
			return nil, err
		}
		txn, err := nullableUint64(a[8])
		if err != nil {
			return nil, err
		}
		if kind == "chunks" {
			frames := make([]ChunkFrame, 0, len(a[6].Array))
			for _, entry := range a[6].Array {
				if entry.Kind != ReplyArray || len(entry.Array) < schema.Dims+2 {
					return bad()
				}
				b := entry.Array
				x, xok := b[0].Int64()
				y, yok := b[1].Int64()
				if !xok || !yok {
					return bad()
				}
				frame := ChunkFrame{CX: x, CY: y}
				if schema.Dims == 3 {
					z, ok := b[2].Int64()
					if !ok {
						return bad()
					}
					frame.CZ = &z
				}
				v, ok := b[schema.Dims].Uint64()
				if !ok {
					return bad()
				}
				frame.Version = v
				form := b[schema.Dims+1]
				if form.Kind == ReplyBulk {
					frame.Form = append([]byte{}, form.Bulk...)
				} else if form.Kind != ReplyNull {
					return bad()
				}
				frames = append(frames, frame)
			}
			return &ChunksEvent{Position: pos, CommitTimeMs: ms, User: user, SchemaVersion: version, Chunks: frames, Tag: tag, Txn: txn}, nil
		}
		blocks := make([]BlockChange, 0, len(a[6].Array))
		for _, entry := range a[6].Array {
			if entry.Kind != ReplyArray || len(entry.Array) < schema.Dims+2 {
				return bad()
			}
			b := entry.Array
			x, err := decodeCoordinate(b[0])
			if err != nil {
				return nil, err
			}
			y, err := decodeCoordinate(b[1])
			if err != nil {
				return nil, err
			}
			block := BlockChange{X: x, Y: y}
			if schema.Dims == 3 {
				block.Z, err = decodeCoordinate(b[2])
				if err != nil {
					return nil, err
				}
			}
			block.Before, err = decodeWatchRow(schema.Columns, b[schema.Dims])
			if err != nil {
				return nil, err
			}
			block.After, err = decodeWatchRow(schema.Columns, b[schema.Dims+1])
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, block)
		}
		if a[9].Kind != ReplyArray {
			return bad()
		}
		changes := make([]ChunkChange, 0, len(a[9].Array))
		for _, entry := range a[9].Array {
			if entry.Kind != ReplyArray || len(entry.Array) < schema.Dims+2 {
				return bad()
			}
			b := entry.Array
			x, xok := b[0].Int64()
			y, yok := b[1].Int64()
			if !xok || !yok {
				return bad()
			}
			change := ChunkChange{CX: x, CY: y}
			if schema.Dims == 3 {
				z, ok := b[2].Int64()
				if !ok {
					return bad()
				}
				change.CZ = &z
			}
			change.Before, err = decodeWatchRow(schema.ChunkColumns, b[schema.Dims])
			if err != nil {
				return nil, err
			}
			change.After, err = decodeWatchRow(schema.ChunkColumns, b[schema.Dims+1])
			if err != nil {
				return nil, err
			}
			changes = append(changes, change)
		}
		return &ChangeEvent{Position: pos, CommitTimeMs: ms, User: user, SchemaVersion: version, Blocks: blocks, Tag: tag, Txn: txn, ChunkChanges: changes}, nil
	case "catalog":
		if len(a) < 7 || a[5].Kind != ReplyArray {
			return bad()
		}
		ms, ok := a[3].Uint64()
		if !ok {
			return bad()
		}
		user, err := nullableText(a[4])
		if err != nil {
			return nil, err
		}
		events := make([]CatalogChange, 0, len(a[5].Array))
		for _, entry := range a[5].Array {
			if entry.Kind != ReplyMap {
				return bad()
			}
			change := CatalogChange{}
			for key, target := range map[string]**string{"kind": &change.Kind, "action": &change.Action, "name": &change.Name, "table": &change.Table} {
				value, exists := entry.Lookup(key)
				if !exists {
					return bad()
				}
				*target, err = nullableText(value)
				if err != nil {
					return nil, err
				}
			}
			data, exists := entry.Lookup("data")
			if !exists || (data.Kind != ReplyMap && data.Kind != ReplyNull) {
				return bad()
			}
			change.Data = cloneReply(data)
			events = append(events, change)
		}
		txn, err := nullableUint64(a[6])
		if err != nil {
			return nil, err
		}
		return &CatalogEvent{Position: pos, CommitTimeMs: ms, User: user, Events: events, Txn: txn}, nil
	}
	return bad()
}

func nullableUint64(r Reply) (*uint64, error) {
	if r.Kind == ReplyNull {
		return nil, nil
	}
	n, ok := r.Uint64()
	if !ok {
		return nil, protocolErrorf("WATCH", "expected nullable unsigned integer")
	}
	return &n, nil
}
