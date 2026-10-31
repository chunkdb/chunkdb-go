package chunkdb

// sectionedChunkFormLimit bounds a complete mutation before sending its frame.
func sectionedChunkFormLimit(schema *Schema) (uint64, error) {
	count, _, err := sectionedGeometry(schema)
	if err != nil {
		return 0, err
	}
	const command = "SET CHUNK"
	presence := uint64((count + 7) / 8)
	var fixed, metadata uint64
	var descriptors uint64
	for _, column := range schema.Columns {
		if column.Type.Kind == ColumnUnknown {
			return 0, requestErrorf(command, "cannot write unknown block column type %s", column.Type.RawType)
		}
		if err := column.Type.validate(); err != nil {
			return 0, err
		}
		if width, ok := column.Type.fixedWidth(); ok {
			fixed += (uint64(count)*uint64(width) + 7) / 8
			if column.Null {
				fixed += presence
			}
			descriptors++
		}
	}
	if fixed > 64*1024*1024 {
		return 0, requestErrorf(command, "fixed payload exceeds its hard byte limit")
	}
	for _, column := range schema.ChunkColumns {
		if column.Type.Kind == ColumnUnknown {
			return 0, requestErrorf(command, "cannot write unknown chunk column type %s", column.Type.RawType)
		}
		if err := column.Type.validate(); err != nil {
			return 0, err
		}
		if width, ok := column.Type.fixedWidth(); ok {
			metadata += uint64((width + 7) / 8)
		}
	}
	if len(schema.ChunkColumns) != 0 {
		metadata += 4 + 16*uint64(len(schema.ChunkColumns))
	}
	variable := schema.Options.VarMaxChunkBytes
	if variable == 0 {
		variable = maxSectionedVariableBytes
	}
	if variable > maxSectionedVariableBytes {
		return 0, requestErrorf(command, "invalid variable byte limit")
	}
	return sectionedHeaderBytes + 6*sectionDirectoryBytes + presence + fixed + 4 + 24*descriptors + metadata + variable, nil
}
