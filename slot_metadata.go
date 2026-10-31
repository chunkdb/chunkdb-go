package chunkdb

// SlotFilters reports persisted identity separately from current display names.
// Area has lower coordinates followed by upper coordinates; nil means no area.
// Columns and ColumnIDs are both nil for all columns. A nil name retains the
// position of a dropped ID; IDs never change when a column is renamed.
type SlotFilters struct {
	Area             []int64
	Columns          []*string
	ColumnIDs        []uint64
	NoBefore, Chunks bool
	Fields           map[string]Reply
}

func parseSlotFilters(r Reply) (*SlotFilters, error) {
	bad := func() (*SlotFilters, error) { return nil, protocolErrorf("SHOW SLOTS", "invalid persisted filters") }
	if r.Kind != ReplyMap {
		return bad()
	}
	filters := &SlotFilters{Fields: make(map[string]Reply, len(r.Map))}
	for _, pair := range r.Map {
		if pair.Key.Kind != ReplyBulk {
			return bad()
		}
		filters.Fields[string(pair.Key.Bulk)] = cloneReply(pair.Value)
	}
	area, exists := r.Lookup("area")
	if !exists {
		return bad()
	}
	if area.Kind != ReplyNull {
		if area.Kind != ReplyArray || len(area.Array) < 4 || len(area.Array)%2 != 0 {
			return bad()
		}
		filters.Area = make([]int64, len(area.Array))
		for i, value := range area.Array {
			n, ok := value.Int64()
			if !ok {
				return bad()
			}
			filters.Area[i] = n
		}
		dims := len(filters.Area) / 2
		for i := range dims {
			if filters.Area[i] > filters.Area[dims+i] {
				return bad()
			}
		}
	}
	names, hasNames := r.Lookup("columns")
	ids, hasIDs := r.Lookup("column_ids")
	if !hasNames || !hasIDs {
		return bad()
	}
	if names.Kind == ReplyNull || ids.Kind == ReplyNull {
		if names.Kind != ReplyNull || ids.Kind != ReplyNull {
			return bad()
		}
	} else {
		if names.Kind != ReplyArray || ids.Kind != ReplyArray || len(names.Array) == 0 || len(names.Array) != len(ids.Array) {
			return bad()
		}
		filters.Columns = make([]*string, len(names.Array))
		filters.ColumnIDs = make([]uint64, len(ids.Array))
		seen := map[uint64]bool{}
		for i, value := range names.Array {
			if value.Kind != ReplyNull {
				if value.Kind != ReplyBulk || !isName(string(value.Bulk)) {
					return bad()
				}
				name := string(value.Bulk)
				filters.Columns[i] = &name
			}
			n, ok := ids.Array[i].Uint64()
			if !ok || n == 0 || seen[n] {
				return bad()
			}
			seen[n] = true
			filters.ColumnIDs[i] = n
		}
	}
	for key, target := range map[string]*bool{"no_before": &filters.NoBefore, "chunks": &filters.Chunks} {
		value, exists := r.Lookup(key)
		if !exists || value.Kind != ReplyBoolean {
			return bad()
		}
		*target = value.Bool
	}
	return filters, nil
}
