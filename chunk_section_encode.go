package chunkdb

import (
	"encoding/binary"
	"math"
	"slices"
)

func normalizedFormValue(column Column, raw any) (any, error) {
	if raw == nil && !column.Null {
		if column.Required && column.Default == nil {
			return nil, requestErrorf("SET CHUNK", "column %s is REQUIRED", column.Name)
		}
		raw = column.Default
		if raw == nil {
			raw = zeroValue(column.Type)
		}
	}
	return normalize(column.Name, column.Type, raw)
}

func encodeMetadataRow(schema *Schema, metadata Record) ([]byte, uint64, error) {
	if len(schema.ChunkColumns) == 0 {
		if len(metadata) != 0 {
			return nil, 0, requestErrorf("SET CHUNK", "table has no chunk columns")
		}
		return nil, 0, nil
	}
	byName := make(map[string]Column, len(schema.ChunkColumns))
	columns := slices.Clone(schema.ChunkColumns)
	slices.SortFunc(columns, func(a, b Column) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for i, column := range columns {
		if column.ID == 0 || (i != 0 && columns[i-1].ID == column.ID) {
			return nil, 0, requestErrorf("SET CHUNK", "invalid chunk metadata ID")
		}
		if _, duplicate := byName[column.Name]; duplicate {
			return nil, 0, requestErrorf("SET CHUNK", "duplicate chunk metadata name")
		}
		if column.Type.Kind == ColumnUnknown {
			return nil, 0, requestErrorf("SET CHUNK", "cannot write unknown metadata type %s", column.Type.RawType)
		}
		if err := column.Type.validate(); err != nil {
			return nil, 0, err
		}
		byName[column.Name] = column
	}
	for name := range metadata {
		if _, ok := byName[name]; !ok {
			return nil, 0, requestErrorf("SET CHUNK", "unknown chunk column %s", name)
		}
	}
	body := make([]byte, 4+16*len(columns))
	binary.LittleEndian.PutUint32(body, uint32(len(columns)))
	var variableBytes uint64
	for i, column := range columns {
		rawValue, supplied := metadata[column.Name]
		if !supplied {
			rawValue = column.Default
			if rawValue == nil {
				if column.Required {
					return nil, 0, requestErrorf("SET CHUNK", "chunk column %s is REQUIRED", column.Name)
				}
				if !column.Null {
					rawValue = zeroValue(column.Type)
				}
			}
		}
		if rawValue == nil && !column.Null {
			return nil, 0, requestErrorf("SET CHUNK", "chunk column %s cannot be NULL", column.Name)
		}
		value, err := normalize(column.Name, column.Type, rawValue)
		if err != nil {
			return nil, 0, err
		}
		var raw []byte
		if value != nil {
			if width, fixed := column.Type.fixedWidth(); fixed {
				raw = make([]byte, (width+7)/8)
				writeFixed(column.Type, raw, 0, width, value)
			} else {
				raw = encodeNormalized(value)
				if column.Null || len(raw) != 0 {
					variableBytes += 12 + uint64(len(raw))
				}
			}
		}
		if uint64(len(raw)) > math.MaxUint32-uint64(len(body)) {
			return nil, 0, requestErrorf("SET CHUNK", "chunk metadata exceeds wire limit")
		}
		entry := body[4+16*i:]
		binary.LittleEndian.PutUint32(entry, column.ID)
		if value == nil {
			binary.LittleEndian.PutUint16(entry[4:], 1)
		}
		binary.LittleEndian.PutUint32(entry[8:], uint32(len(body)))
		binary.LittleEndian.PutUint32(entry[12:], uint32(len(raw)))
		body = append(body, raw...)
	}
	return body, variableBytes, nil
}

func encodeSectionedChunk(schema *Schema, chunk *Chunk, columns ...string) ([]byte, error) {
	return encodeSectionedChunkForm(schema, chunk, true, columns)
}

// A SET ... COLUMNS form preserves the stored metadata row, even when every
// block column is selected or the metadata schema contains required fields.
func encodeSectionedBlockMutation(schema *Schema, chunk *Chunk, columns ...string) ([]byte, error) {
	if len(columns) == 0 {
		return nil, requestErrorf("SET CHUNK", "a block-only mutation needs columns")
	}
	return encodeSectionedChunkForm(schema, chunk, false, columns)
}

func encodeSectionedChunkForm(schema *Schema, chunk *Chunk, includeMetadata bool, columns []string) ([]byte, error) {
	count, depth, err := sectionedGeometry(schema)
	if err != nil {
		return nil, err
	}
	bad := func(what string) ([]byte, error) { return nil, requestErrorf("SET CHUNK", "chunk form: %s", what) }
	if chunk == nil {
		return bad("nil chunk")
	}
	chunkDepth := chunk.Depth
	if chunkDepth == 0 && schema.Dims != 3 {
		chunkDepth = 1
	}
	if chunk.Width != schema.ChunkWidth || chunk.Height != schema.ChunkHeight || chunkDepth != depth || len(chunk.Present) != count {
		return bad("geometry differs from schema")
	}
	if len(chunk.UnknownSections) != 0 {
		return bad("unknown_form_section")
	}
	indexes, err := formColumns(schema, columns)
	if err != nil {
		return nil, err
	}
	for name := range chunk.Columns {
		if schema.columnIndex(name) < 0 {
			return bad("unknown block column")
		}
	}
	seenIDs := make(map[uint32]bool, len(indexes))
	seenNames := make(map[string]bool, len(indexes))
	var fixedBytes uint64
	fixedCount := 0
	for _, index := range indexes {
		column := schema.Columns[index]
		if column.ID == 0 || seenIDs[column.ID] || seenNames[column.Name] {
			return bad("duplicate block column")
		}
		seenIDs[column.ID], seenNames[column.Name] = true, true
		if column.Type.Kind == ColumnUnknown {
			return bad("cannot write unknown column type")
		}
		if err := column.Type.validate(); err != nil {
			return nil, err
		}
		if values, ok := chunk.Columns[column.Name]; !ok || len(values) != count {
			return bad("missing block column values")
		}
		if width, fixed := column.Type.fixedWidth(); fixed {
			fixedBytes += (uint64(count)*uint64(width) + 7) / 8
			if column.Null {
				fixedBytes += uint64((count + 7) / 8)
			}
			fixedCount++
		}
	}
	if fixedBytes > 64*1024*1024 {
		return bad("fixed payload exceeds its byte limit")
	}
	presence := make([]byte, (count+7)/8)
	for i, present := range chunk.Present {
		if present {
			presence[i/8] |= 1 << (i % 8)
		}
	}
	spans := make([]byte, 4+24*fixedCount)
	binary.LittleEndian.PutUint32(spans, uint32(fixedCount))
	var fixedValues, validity []byte
	type varEntry struct {
		id    uint32
		block int
		value []byte
	}
	var variables []varEntry
	var variableBytes uint64
	spanIndex := 0
	for _, index := range indexes {
		column := schema.Columns[index]
		width, fixed := column.Type.fixedWidth()
		var values, nulls []byte
		if fixed {
			values = make([]byte, (count*width+7)/8)
			if column.Null {
				nulls = make([]byte, len(presence))
			}
		}
		for block, raw := range chunk.Columns[column.Name] {
			if !chunk.Present[block] {
				continue
			}
			value, err := normalizedFormValue(column, raw)
			if err != nil {
				return nil, err
			}
			if value == nil {
				continue
			}
			if fixed {
				writeFixed(column.Type, values, block*width, width, value)
				if column.Null {
					nulls[block/8] |= 1 << (block % 8)
				}
			} else {
				encoded := encodeNormalized(value)
				if len(encoded) == 0 && !column.Null {
					continue
				}
				variableBytes += 12 + uint64(len(encoded))
				if schema.Options.VarMaxChunkBytes != 0 && variableBytes > schema.Options.VarMaxChunkBytes {
					return bad("variable data exceeds table byte limit")
				}
				variables = append(variables, varEntry{column.ID, block, encoded})
			}
		}
		if fixed {
			entry := spans[4+24*spanIndex:]
			for j, value := range []uint32{column.ID, uint32(width), uint32(len(fixedValues)), uint32(len(values)), 0, uint32(len(nulls))} {
				binary.LittleEndian.PutUint32(entry[4*j:], value)
			}
			if column.Null {
				binary.LittleEndian.PutUint32(entry[16:], uint32(len(validity)))
			}
			fixedValues = append(fixedValues, values...)
			validity = append(validity, nulls...)
			spanIndex++
		}
	}
	slices.SortFunc(variables, func(a, b varEntry) int {
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return a.block - b.block
	})
	var vars []byte
	for _, entry := range variables {
		vars = binary.LittleEndian.AppendUint32(vars, entry.id)
		vars = binary.LittleEndian.AppendUint32(vars, uint32(entry.block))
		vars = binary.LittleEndian.AppendUint32(vars, uint32(len(entry.value)))
		vars = append(vars, entry.value...)
	}
	var metadata []byte
	if includeMetadata {
		var metadataBytes uint64
		metadata, metadataBytes, err = encodeMetadataRow(schema, chunk.Metadata)
		if err != nil {
			return nil, err
		}
		variableBytes += metadataBytes
	}
	if schema.Options.VarMaxChunkBytes != 0 && variableBytes > schema.Options.VarMaxChunkBytes {
		return bad("combined variable data exceeds table byte limit")
	}
	header := chunkFormHeader{version: chunk.Version, schema: schema.Version, commitTime: chunk.CommitTimeMS}
	if len(columns) != 0 {
		header.flags |= chunkFormProjected
	}
	if chunk.Private {
		header.flags |= chunkFormPrivate
	}
	sections := []chunkFormSection{{sectionPresence, presence}}
	if len(fixedValues) != 0 {
		sections = append(sections, chunkFormSection{sectionFixed, fixedValues})
	}
	if len(validity) != 0 {
		sections = append(sections, chunkFormSection{sectionValidity, validity})
	}
	if len(vars) != 0 {
		sections = append(sections, chunkFormSection{sectionVariable, vars})
	}
	if metadata != nil {
		sections = append(sections, chunkFormSection{sectionChunkColumns, metadata})
	}
	sections = append(sections, chunkFormSection{sectionBlockSpans, spans})
	return serializeSectionedForm(sectionedForm{header, sections}, math.MaxUint32)
}
