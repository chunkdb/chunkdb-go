package chunkdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"unicode/utf8"
)

const maxSectionedVariableBytes = 64 * 1024 * 1024
const maxDecodedChunkCells = 4 * 1024 * 1024

func sectionedGeometry(schema *Schema) (int, int, error) {
	if schema == nil || (schema.Dims != 0 && schema.Dims != 2 && schema.Dims != 3) {
		return 0, 0, protocolErrorf("", "invalid chunk dimensions")
	}
	count := schema.BlockCount()
	if count == 0 || len(schema.Columns) > 1024 || len(schema.ChunkColumns) > 1024 {
		return 0, 0, protocolErrorf("", "invalid chunk geometry or column count")
	}
	depth := 1
	if schema.Dims == 3 {
		depth = schema.ChunkDepth
	}
	return count, depth, nil
}

func packedFormValue(span formBlockSpan, block int) []byte {
	raw := make([]byte, (span.width+7)/8)
	for bit := 0; bit < span.width; bit += 8 {
		raw[bit/8] = byte(readBits(span.values, block*span.width+bit, min(8, span.width-bit)))
	}
	return raw
}

func decodeMetadataValue(column Column, raw []byte, null bool) (any, error) {
	bad := func() (any, error) {
		return nil, protocolErrorf("", "invalid chunk metadata value for %s", column.Name)
	}
	if null {
		if !column.Null {
			return bad()
		}
		return nil, nil
	}
	if column.Type.Kind == ColumnUnknown {
		return bytes.Clone(raw), nil
	}
	if err := column.Type.validate(); err != nil {
		return bad()
	}
	if width, fixed := column.Type.fixedWidth(); fixed {
		if !validArrayPadding(raw, uint64(width)) {
			return bad()
		}
		return readFixed(column.Type, raw, 0, width), nil
	}
	if len(raw) > column.Type.Size || (column.Type.Kind == ColumnText && !utf8.Valid(raw)) {
		return bad()
	}
	if column.Type.Kind == ColumnText {
		return string(raw), nil
	}
	return append([]byte{}, raw...), nil
}

// Decode the checked section contract for every active dimension.
func decodeSectionedChunk(schema *Schema, data []byte, columns ...string) (*Chunk, error) {
	count, depth, err := sectionedGeometry(schema)
	if err != nil {
		return nil, err
	}
	form, err := parseSectionedForm(data, math.MaxUint32, false)
	if err != nil {
		return nil, err
	}
	bad := func(what string) (*Chunk, error) {
		return nil, protocolErrorf("", "chunk form of table %s: %s", schema.Table, what)
	}
	if form.header.schema != schema.Version {
		return bad("schema version mismatch")
	}
	if form.header.flags&chunkFormProjected != 0 && len(columns) == 0 {
		return bad("projection flag mismatch")
	}
	spans, err := parseFormBlockSpans(form, count)
	if err != nil {
		return nil, err
	}
	if len(form.section(sectionFixed))+len(form.section(sectionValidity)) > 64*1024*1024 {
		return bad("fixed payload exceeds its byte limit")
	}
	indexes, err := formColumns(schema, columns)
	if err != nil {
		return nil, err
	}
	if len(indexes) != 0 && count > maxDecodedChunkCells/len(indexes) {
		return bad("decoded chunk exceeds 4194304 column values; select fewer columns or read the raw form")
	}
	if form.header.flags&chunkFormProjected == 0 && len(columns) != 0 {
		if len(indexes) != len(schema.Columns) {
			return bad("full form read as a partial projection")
		}
		for i, index := range indexes {
			if index != i {
				return bad("full form read in another column order")
			}
		}
	}
	byID := make(map[uint32]formBlockSpan, len(spans))
	for _, span := range spans {
		byID[span.id] = span
	}
	seen := make(map[uint32]bool, len(indexes))
	names := make(map[string]bool, len(indexes))
	variables := make(map[uint32]Column)
	consumed := 0
	for _, index := range indexes {
		column := schema.Columns[index]
		if column.ID == 0 || seen[column.ID] || names[column.Name] {
			return bad("column IDs and names must be unique")
		}
		seen[column.ID] = true
		names[column.Name] = true
		span, described := byID[column.ID]
		width, fixed := column.Type.fixedWidth()
		if column.Type.Kind == ColumnUnknown {
			fixed = described
		} else if err := column.Type.validate(); err != nil {
			return bad("invalid schema column type")
		}
		if fixed {
			if !described || (column.Type.Kind != ColumnUnknown && span.width != width) || (len(span.validity) != 0) != column.Null {
				return bad("fixed descriptor differs from schema")
			}
			if consumed >= len(spans) || spans[consumed].id != column.ID {
				return bad("fixed descriptor order differs from projection")
			}
			consumed++
		} else {
			if described {
				return bad("variable column has a fixed descriptor")
			}
			variables[column.ID] = column
		}
	}
	if consumed != len(spans) {
		return bad("unexpected fixed descriptors")
	}
	metadata, err := parseFormChunkColumns(form)
	if err != nil {
		return nil, err
	}
	if len(metadata) != len(schema.ChunkColumns) {
		return bad("chunk metadata differs from schema")
	}
	vars := form.section(sectionVariable)
	if len(vars) > maxSectionedVariableBytes {
		return bad("variable data exceeds its hard byte limit")
	}
	chunk := &Chunk{Version: form.header.version, SchemaVersion: form.header.schema, CommitTimeMS: form.header.commitTime,
		Projected: form.header.flags&chunkFormProjected != 0, Private: form.header.flags&chunkFormPrivate != 0,
		Width: schema.ChunkWidth, Height: schema.ChunkHeight, Depth: depth,
		Present: make([]bool, count), Columns: make(map[string][]any, len(indexes))}
	presence := form.section(sectionPresence)
	for i := range count {
		chunk.Present[i] = bitAt(presence, i)
	}
	for _, index := range indexes {
		column := schema.Columns[index]
		values := make([]any, count)
		chunk.Columns[column.Name] = values
		span, fixed := byID[column.ID]
		for i := range count {
			if !chunk.Present[i] {
				continue
			}
			if !fixed {
				if !column.Null {
					values[i] = emptyValue(column.Type)
				}
				continue
			}
			if column.Null && !bitAt(span.validity, i) {
				continue
			}
			if column.Type.Kind == ColumnUnknown {
				values[i] = packedFormValue(span, i)
			} else {
				values[i] = readFixed(column.Type, span.values, i*span.width, span.width)
			}
		}
	}
	varBytes := uint64(len(vars))
	var lastID, lastBlock uint32
	first := true
	for offset := 0; offset < len(vars); {
		if len(vars)-offset < 12 {
			return bad("truncated variable entry")
		}
		id, block, length := binary.LittleEndian.Uint32(vars[offset:]), binary.LittleEndian.Uint32(vars[offset+4:]), uint64(binary.LittleEndian.Uint32(vars[offset+8:]))
		offset += 12
		if length > uint64(len(vars)-offset) {
			return bad("truncated variable value")
		}
		column, ok := variables[id]
		if !ok || int64(block) >= int64(count) || !chunk.Present[int(block)] || (!first && (id < lastID || (id == lastID && block <= lastBlock))) {
			return bad("unordered, unknown or absent variable entry")
		}
		value := vars[offset : offset+int(length)]
		offset += int(length)
		if (length == 0 && !column.Null) || (column.Type.Kind != ColumnUnknown && length > uint64(column.Type.Size)) || (column.Type.Kind == ColumnText && !utf8.Valid(value)) {
			return bad("invalid variable value")
		}
		if column.Type.Kind == ColumnText {
			chunk.Columns[column.Name][int(block)] = string(value)
		} else {
			chunk.Columns[column.Name][int(block)] = append([]byte{}, value...)
		}
		lastID, lastBlock, first = id, block, false
	}
	if len(metadata) != 0 {
		chunk.Metadata = make(Record, len(metadata))
	}
	metaSchema := make(map[uint32]Column, len(schema.ChunkColumns))
	metaNames := make(map[string]bool, len(schema.ChunkColumns))
	for _, column := range schema.ChunkColumns {
		if column.ID == 0 {
			return bad("invalid chunk metadata ID")
		}
		if _, duplicate := metaSchema[column.ID]; duplicate || metaNames[column.Name] {
			return bad("duplicate chunk metadata ID or name")
		}
		metaSchema[column.ID] = column
		metaNames[column.Name] = true
	}
	for _, entry := range metadata {
		column, ok := metaSchema[entry.id]
		if !ok {
			return bad("unknown chunk metadata ID")
		}
		value, err := decodeMetadataValue(column, entry.value, entry.null)
		if err != nil {
			return nil, err
		}
		chunk.Metadata[column.Name] = value
		if _, fixed := column.Type.fixedWidth(); !fixed && column.Type.Kind != ColumnUnknown && !entry.null && (column.Null || len(entry.value) != 0) {
			varBytes += 12 + uint64(len(entry.value))
		}
	}
	// A lowered table quota does not invalidate already stored values.
	if varBytes > maxSectionedVariableBytes {
		return bad("combined variable data exceeds its hard byte limit")
	}
	for _, section := range form.sections {
		if section.kind < sectionPresence || section.kind > sectionBlockSpans {
			if chunk.UnknownSections == nil {
				chunk.UnknownSections = make(map[uint16][]byte)
			}
			chunk.UnknownSections[section.kind] = bytes.Clone(section.body)
		}
	}
	return chunk, nil
}
