package chunkdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type modelReadAPI struct {
	c     *Client
	table string
	dims  int
}

func (a modelReadAPI) get(t *testing.T, p ChunkCoord3D, columns ...string) (*Chunk, error) {
	t.Helper()
	if a.dims == 3 {
		return a.c.GetChunk3D(t.Context(), a.table, p.CX, p.CY, p.CZ, columns...)
	}
	return a.c.GetChunk(t.Context(), a.table, p.CX, p.CY, columns...)
}

func (a modelReadAPI) raw(t *testing.T, p ChunkCoord3D) ([]byte, error) {
	t.Helper()
	if a.dims == 3 {
		return a.c.GetChunkRaw3D(t.Context(), a.table, p.CX, p.CY, p.CZ)
	}
	return a.c.GetChunkRaw(t.Context(), a.table, p.CX, p.CY)
}

func (a modelReadAPI) put(t *testing.T, p ChunkCoord3D, chunk *Chunk) uint64 {
	t.Helper()
	var version uint64
	var err error
	if a.dims == 3 {
		version, err = a.c.SetChunk3D(t.Context(), a.table, p.CX, p.CY, p.CZ, chunk)
	} else {
		version, err = a.c.SetChunk(t.Context(), a.table, p.CX, p.CY, chunk)
	}
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func (a modelReadAPI) area(t *testing.T, withSchema, around bool, columns ...string) (AreaRead3D, error) {
	t.Helper()
	if a.dims == 3 {
		if around {
			chunks, err := a.c.GetAreaAround3D(t.Context(), a.table, 0, 0, 0, 2, columns...)
			return AreaRead3D{Chunks: chunks}, err
		}
		if withSchema {
			return a.c.GetArea3DWithSchema(t.Context(), a.table, -1, 0, -1, 1, 0, 1, columns...)
		}
		chunks, err := a.c.GetArea3D(t.Context(), a.table, -1, 0, -1, 1, 0, 1, columns...)
		return AreaRead3D{Chunks: chunks}, err
	}
	var read AreaRead
	var err error
	if around {
		read.Chunks, err = a.c.GetAreaAround(t.Context(), a.table, 0, 0, 2, columns...)
	} else if withSchema {
		read, err = a.c.GetAreaWithSchema(t.Context(), a.table, -1, 0, 1, 0, columns...)
	} else {
		read.Chunks, err = a.c.GetArea(t.Context(), a.table, -1, 0, 1, 0, columns...)
	}
	result := AreaRead3D{SchemaVersion: read.SchemaVersion}
	for _, item := range read.Chunks {
		result.Chunks = append(result.Chunks, AreaChunk3D{CX: item.CX, CY: item.CY, Chunk: item.Chunk})
	}
	return result, err
}

func (a modelReadAPI) scan(t *testing.T, opts ScanOptions3D) (ChunkScanPage3D, error) {
	t.Helper()
	if a.dims == 3 {
		return a.c.ScanChunks3DWithOptions(t.Context(), a.table, opts)
	}
	options := ScanOptions{Limit: opts.Limit, Since: opts.Since, WithData: opts.WithData, Columns: opts.Columns, Epoch: opts.Epoch}
	if opts.After != nil {
		options.After = &ChunkCoord{CX: opts.After.CX, CY: opts.After.CY}
	}
	page, err := a.c.ScanChunksWithOptions(t.Context(), a.table, options)
	result := ChunkScanPage3D{More: page.More, Epoch: page.Epoch, Position: page.Position, RetainedDeletionFloor: page.RetainedDeletionFloor}
	for _, item := range page.Chunks {
		result.Chunks = append(result.Chunks, ScannedChunk3D{CX: item.CX, CY: item.CY, Version: item.Version, Chunk: item.Chunk, Deleted: item.Deleted})
	}
	return result, err
}

func modelReadSpec(dims int) TableSpec {
	spec := TableSpec{ChunkWidth: 2, ChunkHeight: 2,
		Columns: []ColumnDef{{Name: "n", Type: TypeUint(64)}, {Name: "maybe", Type: TypeInt(13), Null: true},
			{Name: "text", Type: TypeText(32), Null: true}, {Name: "raw", Type: TypeBytes(16), Null: true}},
		ChunkColumns: []ColumnDef{{Name: "unsigned", Type: TypeUint(64)}, {Name: "signed", Type: TypeInt(13)},
			{Name: "flag", Type: TypeBool()}, {Name: "single", Type: TypeF32()}, {Name: "double", Type: TypeF64()},
			{Name: "mask", Type: TypeBits(9)}, {Name: "label", Type: TypeText(32), Null: true},
			{Name: "blob", Type: TypeBytes(16)}, {Name: "nullable", Type: TypeUint(17), Null: true}}}
	if dims == 3 {
		spec.ChunkDepth = 2
	}
	return spec
}

func modelReadCases(t *testing.T, run func(*testing.T, bool, int)) {
	t.Helper()
	for _, tlsEnabled := range []bool{false, true} {
		transport := "plain"
		if tlsEnabled {
			transport = "tls"
		}
		for _, dims := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/%dd", transport, dims), func(t *testing.T) { run(t, tlsEnabled, dims) })
		}
	}
}

// Extend a captured server form independently of the client's serializer.
func appendFutureChunkSection(t *testing.T, raw []byte) []byte {
	t.Helper()
	if len(raw) < 36 || string(raw[:4]) != "CKCF" {
		t.Fatalf("not a chunk form: %x", raw)
	}
	count := int(binary.LittleEndian.Uint16(raw[32:]))
	directoryEnd := 36 + 12*count
	result := make([]byte, len(raw)+12+4)
	copy(result, raw[:directoryEnd])
	binary.LittleEndian.PutUint16(result[32:], uint16(count+1))
	for i := 0; i < count; i++ {
		offset := 36 + 12*i + 4
		binary.LittleEndian.PutUint32(result[offset:], binary.LittleEndian.Uint32(raw[offset:])+12)
	}
	binary.LittleEndian.PutUint16(result[directoryEnd:], 60000)
	binary.LittleEndian.PutUint32(result[directoryEnd+4:], uint32(len(raw)+12))
	binary.LittleEndian.PutUint32(result[directoryEnd+8:], 4)
	copy(result[directoryEnd+12:], raw[directoryEnd:])
	copy(result[len(raw)+12:], []byte{0, '\r', '\n', 255})
	return result
}

func TestIntegrationChunkModelReadForms(t *testing.T) {
	modelReadCases(t, func(t *testing.T, tlsEnabled bool, dims int) {
		server := startServer(t, serverConfig{tls: tlsEnabled})
		c := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled })
		createTable(t, c, "forms", modelReadSpec(dims))
		schema, err := c.Describe(t.Context(), "forms")
		if err != nil {
			t.Fatal(err)
		}
		a := modelReadAPI{c, "forms", dims}
		point := ChunkCoord3D{CX: -1}
		z := 0
		if dims == 3 {
			point.CZ, z = -1, 1
		}
		mask := NewBits(9)
		mask.SetBit(0, true)
		mask.SetBit(8, true)
		metadata := Record{"unsigned": uint64(math.MaxUint64), "signed": int64(-4096), "flag": true,
			"single": float32(-1.5), "double": float64(1.0 / 3.0), "mask": mask, "label": "雪", "blob": []byte{0, 255, 13, 10}, "nullable": nil}
		block := Record{"n": uint64(math.MaxUint64), "maybe": int64(-7), "text": "chunk", "raw": []byte{0, 255}}
		chunk := NewChunk(schema)
		chunk.Metadata = metadata
		chunk.SetBlock3D(1, 0, z, block)
		chunk.SetBlock3D(0, 1, 0, Record{"n": uint64(0), "maybe": nil, "text": "", "raw": []byte{}})
		chunk.Version, chunk.CommitTimeMS = math.MaxUint64, math.MaxUint64
		version := a.put(t, point, chunk)
		full, err := a.get(t, point)
		if err != nil || full == nil || full.Version != version || full.CommitTimeMS == 0 || full.CommitTimeMS == math.MaxUint64 || full.Private ||
			!reflect.DeepEqual(full.Metadata, metadata) || !reflect.DeepEqual(full.Block3D(1, 0, z), block) || full.Block3D(0, 1, 0)["maybe"] != nil {
			t.Fatalf("complete form: %+v, %v", full, err)
		}
		raw, err := a.raw(t, point)
		if err != nil || len(raw) < 36 || binary.LittleEndian.Uint64(raw[8:]) != version || binary.LittleEndian.Uint64(raw[24:]) != full.CommitTimeMS {
			t.Fatalf("raw identity: %x, %v", raw, err)
		}
		var kinds []uint16
		for i := 0; i < int(binary.LittleEndian.Uint16(raw[32:])); i++ {
			kinds = append(kinds, binary.LittleEndian.Uint16(raw[36+12*i:]))
		}
		if !slices.Equal(kinds, []uint16{1, 2, 3, 4, 5, 6}) {
			t.Fatalf("missing real-server sections: %v", kinds)
		}
		decoded, err := DecodeChunk(schema, raw)
		if err != nil || decoded.Projected || !reflect.DeepEqual(decoded.Metadata, full.Metadata) || !reflect.DeepEqual(decoded.Columns, full.Columns) {
			t.Fatalf("raw decode: %+v, %v", decoded, err)
		}
		future := appendFutureChunkSection(t, raw)
		decoded, err = DecodeChunk(schema, future)
		if err != nil || !bytes.Equal(decoded.UnknownSections[60000], []byte{0, 13, 10, 255}) ||
			!reflect.DeepEqual(decoded.Metadata, metadata) || !reflect.DeepEqual(decoded.Columns, full.Columns) || decoded.CommitTimeMS != full.CommitTimeMS {
			t.Fatalf("extended server form: %+v, %v", decoded, err)
		}
		future[len(future)-1] = 0
		if decoded.UnknownSections[60000][3] != 255 {
			t.Fatal("opaque section aliases response bytes")
		}
		metaOnly := NewChunk(schema)
		metaOnly.Metadata = metadata
		a.put(t, ChunkCoord3D{}, metaOnly)
		for _, mode := range []struct{ schema, around bool }{{false, false}, {true, false}, {false, true}} {
			area, err := a.area(t, mode.schema, mode.around, "text", "n")
			if err != nil || len(area.Chunks) != 2 || (mode.schema && area.SchemaVersion != schema.Version) {
				t.Fatalf("area envelope: %+v, %v", area, err)
			}
			if got := area.Chunks[0]; got.CX != point.CX || got.CY != point.CY || got.CZ != point.CZ || got.Chunk.Version != version ||
				got.Chunk.CommitTimeMS != full.CommitTimeMS || !got.Chunk.Projected || len(got.Chunk.Columns) != 2 ||
				!reflect.DeepEqual(got.Chunk.Metadata, metadata) || got.Chunk.Block3D(1, 0, z)["text"] != "chunk" {
				t.Fatalf("projected area chunk: %+v", got)
			}
			if got := area.Chunks[1]; got.CX != 0 || got.CY != 0 || got.CZ != 0 || slices.Contains(got.Chunk.Present, true) || !reflect.DeepEqual(got.Chunk.Metadata, metadata) {
				t.Fatalf("metadata-only area chunk: %+v", got)
			}
		}
		page, err := a.scan(t, ScanOptions3D{WithData: true, Columns: []string{"text", "n"}})
		if err != nil || len(page.Chunks) != 2 || page.Chunks[0].Chunk.CommitTimeMS != full.CommitTimeMS ||
			!reflect.DeepEqual(page.Chunks[0].Chunk.Metadata, metadata) || !page.Chunks[0].Chunk.Projected {
			t.Fatalf("projected scan: %+v, %v", page, err)
		}
		var emptySchema uint64
		var emptyCount int
		if dims == 3 {
			read, readErr := c.GetArea3DWithSchema(t.Context(), "forms", 99, 0, 0, 99, 0, 0)
			emptySchema, emptyCount, err = read.SchemaVersion, len(read.Chunks), readErr
		} else {
			read, readErr := c.GetAreaWithSchema(t.Context(), "forms", 99, 0, 99, 0)
			emptySchema, emptyCount, err = read.SchemaVersion, len(read.Chunks), readErr
		}
		if err != nil || emptyCount != 0 || emptySchema != schema.Version {
			t.Fatalf("empty area lost schema: version=%d chunks=%d err=%v", emptySchema, emptyCount, err)
		}
		rollback := errors.New("rollback private form")
		_, err = c.Transaction(t.Context(), func(tx *Tx) error {
			var committed, private *Chunk
			var privateRaw []byte
			var readErr error
			if dims == 3 {
				committed, readErr = tx.GetChunk3D(t.Context(), "forms", point.CX, point.CY, point.CZ)
			} else {
				committed, readErr = tx.GetChunk(t.Context(), "forms", point.CX, point.CY)
			}
			if readErr != nil || committed == nil || committed.Version != version || committed.CommitTimeMS != full.CommitTimeMS || committed.Private {
				t.Fatalf("committed snapshot form: %+v, %v", committed, readErr)
			}
			if dims == 3 {
				readErr = tx.SetChunkMetadata3D(t.Context(), "forms", point.CX, point.CY, point.CZ, Record{"label": nil})
			} else {
				readErr = tx.SetChunkMetadata(t.Context(), "forms", point.CX, point.CY, Record{"label": nil})
			}
			if readErr != nil {
				return readErr
			}
			if dims == 3 {
				private, readErr = tx.GetChunk3D(t.Context(), "forms", point.CX, point.CY, point.CZ, "text")
				if readErr == nil {
					privateRaw, readErr = tx.GetChunkRaw3D(t.Context(), "forms", point.CX, point.CY, point.CZ, "text")
				}
			} else {
				private, readErr = tx.GetChunk(t.Context(), "forms", point.CX, point.CY, "text")
				if readErr == nil {
					privateRaw, readErr = tx.GetChunkRaw(t.Context(), "forms", point.CX, point.CY, "text")
				}
			}
			if readErr != nil || private == nil || !private.Private || !private.Projected || private.Version != 0 || private.CommitTimeMS != 0 || private.Metadata["label"] != nil || private.Metadata["unsigned"] != uint64(math.MaxUint64) ||
				len(privateRaw) < 36 || binary.LittleEndian.Uint16(privateRaw[6:]) != 3 || binary.LittleEndian.Uint64(privateRaw[8:]) != 0 || binary.LittleEndian.Uint64(privateRaw[24:]) != 0 {
				t.Fatalf("private projected form: %+v, %x, %v", private, privateRaw, readErr)
			}
			var areaChunks []*Chunk
			var areaSchema uint64
			if dims == 3 {
				area, areaErr := tx.GetArea3DWithSchema(t.Context(), "forms", -1, 0, -1, 0, 0, 0, "text")
				areaSchema, readErr = area.SchemaVersion, areaErr
				for _, item := range area.Chunks {
					areaChunks = append(areaChunks, item.Chunk)
				}
			} else {
				area, areaErr := tx.GetAreaWithSchema(t.Context(), "forms", -1, 0, 0, 0, "text")
				areaSchema, readErr = area.SchemaVersion, areaErr
				for _, item := range area.Chunks {
					areaChunks = append(areaChunks, item.Chunk)
				}
			}
			if readErr != nil || areaSchema != schema.Version || len(areaChunks) != 2 || !areaChunks[0].Private || !areaChunks[0].Projected ||
				areaChunks[0].Version != 0 || areaChunks[0].CommitTimeMS != 0 || areaChunks[0].Metadata["label"] != nil ||
				areaChunks[1].Private || areaChunks[1].Version == 0 || areaChunks[1].CommitTimeMS == 0 {
				t.Fatalf("mixed private/committed area: %+v, %v", areaChunks, readErr)
			}
			return rollback
		}, TxRetries(0))
		if !errors.Is(err, rollback) {
			t.Fatal(err)
		}
		after, err := a.raw(t, point)
		if err != nil || !bytes.Equal(after, raw) {
			t.Fatal("rolled-back form changed committed state", err)
		}
	})
}

func TestIntegrationChunkModelIdentityHistory(t *testing.T) {
	modelReadCases(t, func(t *testing.T, tlsEnabled bool, dims int) {
		backups, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		server := startServer(t, serverConfig{tls: tlsEnabled, args: []string{"--backup-dir", backups, "--checkpoint-updates", "1"}})
		c := connectIntegration(t, server, func(o *Options) { o.TLSInsecure = tlsEnabled })
		spec := TableSpec{Columns: []ColumnDef{{Name: "n", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2}
		if dims == 3 {
			spec.ChunkDepth = 2
		}
		createTable(t, c, "identities", spec)
		schema, err := c.Describe(t.Context(), "identities")
		if err != nil {
			t.Fatal(err)
		}
		a := modelReadAPI{c, "identities", dims}
		points := []ChunkCoord3D{{CX: -1}, {}, {CX: 1}}
		if dims == 3 {
			points[0].CZ, points[2].CZ = -1, 1
		}
		chunk := NewChunk(schema)
		chunk.SetBlock(0, 0, Record{"n": 7})
		versions := make(map[ChunkCoord3D]uint64)
		for _, point := range points {
			versions[point] = a.put(t, point, chunk)
		}
		deleted := a.put(t, points[1], NewChunk(schema))
		versions[points[1]] = deleted
		stats, err := c.Do(t.Context(), "SHOW TABLE STATS identities")
		count, found := stats.Lookup("tombstones")
		tombstones, valid := count.Uint64()
		if err != nil || !found || !valid || tombstones != 1 {
			t.Fatalf("checkpoint did not retain the deletion: %+v, %v", stats, err)
		}
		empty, err := a.get(t, points[1])
		if err != nil || empty == nil || empty.Version != deleted || empty.CommitTimeMS == 0 || slices.Contains(empty.Present, true) {
			t.Fatalf("retained empty chunk: %+v, %v", empty, err)
		}
		raw, err := a.raw(t, points[1])
		if err != nil || binary.LittleEndian.Uint64(raw[24:]) != empty.CommitTimeMS {
			t.Fatalf("retained commit time: %x, %v", raw, err)
		}
		blockRead := func() (BlockRead, error) {
			if dims == 3 {
				return c.GetBlock3DWithMetadata(t.Context(), "identities", 0, 0, 0)
			}
			return c.GetBlockWithMetadata(t.Context(), "identities", 0, 0)
		}
		if read, err := blockRead(); err != nil || read.Record != nil || read.Version == nil || *read.Version != deleted || read.SchemaVersion != schema.Version {
			t.Fatalf("absent block in a retained deletion: %+v, %v", read, err)
		}
		if absent, err := a.get(t, ChunkCoord3D{CX: 99}); err != nil || absent != nil {
			t.Fatalf("unwritten chunk: %+v, %v", absent, err)
		}
		for _, withData := range []bool{false, true} {
			zero := uint64(0)
			opts := ScanOptions3D{Since: &zero, Limit: 1, WithData: withData, Epoch: schema.Epoch}
			for i, point := range points {
				page, err := a.scan(t, opts)
				if err != nil || len(page.Chunks) != 1 || page.More != (i < len(points)-1) || page.Position == nil || *page.Position != deleted || page.Epoch != schema.Epoch || page.RetainedDeletionFloor != 0 {
					t.Fatalf("identity page %d: %+v, %v", i, page, err)
				}
				got := page.Chunks[0]
				if (ChunkCoord3D{got.CX, got.CY, got.CZ}) != point || got.Version != versions[point] || (!withData && (got.Deleted != nil || got.Chunk != nil)) {
					t.Fatalf("identity row: %+v", got)
				}
				if withData && (got.Deleted == nil || *got.Deleted != (i == 1) || (got.Chunk == nil) != (i == 1)) {
					t.Fatalf("deletion row: %+v", got)
				}
				opts.After = &point
			}
			page, err := a.scan(t, opts)
			if err != nil || len(page.Chunks) != 0 || page.More {
				t.Fatalf("page after last coordinate: %+v, %v", page, err)
			}
		}
		ordinary, err := a.scan(t, ScanOptions3D{WithData: true})
		if err != nil || len(ordinary.Chunks) != 2 || ordinary.Chunks[0].CX != -1 || ordinary.Chunks[1].CX != 1 {
			t.Fatalf("ordinary scan retained deletion: %+v, %v", ordinary, err)
		}
		area, err := a.area(t, true, false)
		if err != nil || len(area.Chunks) != 2 || area.Chunks[0].CX != -1 || area.Chunks[1].CX != 1 {
			t.Fatalf("area retained deletion: %+v, %v", area, err)
		}
		quiet, err := a.scan(t, ScanOptions3D{Since: &deleted})
		if err != nil || len(quiet.Chunks) != 0 || quiet.Position == nil || *quiet.Position != deleted {
			t.Fatalf("completed frontier: %+v, %v", quiet, err)
		}
		if _, err := c.Do(t.Context(), "BACKUP TO 'snapshot'"); err != nil {
			t.Fatal(err)
		}
		readServer := startServer(t, serverConfig{tls: tlsEnabled, dataDir: filepath.Join(backups, "snapshot"), args: []string{"--read-only"}})
		reader := connectIntegration(t, readServer, func(o *Options) { o.TLSInsecure = tlsEnabled })
		readAPI := modelReadAPI{reader, "identities", dims}
		zero := uint64(0)
		readPage, err := readAPI.scan(t, ScanOptions3D{Since: &zero, WithData: true})
		if err != nil || readPage.Position != nil || len(readPage.Chunks) != 3 || readPage.Chunks[1].Deleted == nil || !*readPage.Chunks[1].Deleted {
			t.Fatalf("read-only frontier and deletion: %+v, %v", readPage, err)
		}
		retained, err := readAPI.get(t, points[1])
		if err != nil || retained == nil || retained.Version != deleted || retained.CommitTimeMS != empty.CommitTimeMS || slices.Contains(retained.Present, true) {
			t.Fatalf("retained backup identity: %+v, %v", retained, err)
		}
		reply, err := c.Do(t.Context(), fmt.Sprintf("COMPACT TOMBSTONES identities BEFORE %d", deleted+1))
		floor, valid := reply.Uint64()
		if err != nil || !valid || floor != deleted {
			t.Fatalf("compaction floor: %+v, %v", reply, err)
		}
		if forgotten, err := a.get(t, points[1]); err != nil || forgotten != nil {
			t.Fatalf("forgotten identity: %+v, %v", forgotten, err)
		}
		if read, err := blockRead(); err != nil || read.Record != nil || read.Version != nil || read.SchemaVersion != schema.Version {
			t.Fatalf("absent block after compaction: %+v, %v", read, err)
		}
		_, err = a.scan(t, ScanOptions3D{Since: &zero})
		var lost *Error
		if !errors.As(err, &lost) || lost.ServerCode != CodeResyncRequired || lost.Metadata.Reason != "history_lost" || lost.Metadata.Floor == nil || *lost.Metadata.Floor != floor || lost.Metadata.Epoch != schema.Epoch {
			t.Fatalf("lost-history metadata: %v", err)
		}
		page, err := a.scan(t, ScanOptions3D{Since: &floor, WithData: true})
		if err != nil || page.RetainedDeletionFloor != floor || len(page.Chunks) != 0 {
			t.Fatalf("retained floor page: %+v, %v", page, err)
		}
		if err := c.Ping(t.Context()); err != nil {
			t.Fatal("resync reply consumed another response", err)
		}
	})
}
