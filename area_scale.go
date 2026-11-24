package chunkdb

import (
	"encoding/binary"
	"strconv"
)

// Reduction picks the value of each sample of a downsampled area read; see
// [AreaPageOptions.Scale].
type Reduction string

// The reductions of GET AREA ... SCALE k USING. MAX and MIN take integer,
// bool and float columns, MODE integer, bool and bits columns, and FIRST and
// ANY every column.
const (
	ReduceMax   Reduction = "MAX"
	ReduceMin   Reduction = "MIN"
	ReduceMode  Reduction = "MODE"
	ReduceFirst Reduction = "FIRST"
	ReduceAny   Reduction = "ANY"
)

const sectionScale uint16 = 7

var reductionCodes = map[uint16]Reduction{1: ReduceMax, 2: ReduceMin, 3: ReduceMode, 4: ReduceFirst, 5: ReduceAny}

func (r Reduction) valid() bool {
	for _, known := range reductionCodes {
		if r == known {
			return true
		}
	}
	return false
}

// scaleClause is " SCALE k USING r", or nothing for a full-resolution read.
func scaleClause(scale int, reduction Reduction) (string, error) {
	if scale == 0 && reduction == "" {
		return "", nil
	}
	if scale < 1 || !reduction.valid() {
		return "", requestErrorf("GET AREA", "SCALE needs a positive factor and one of MAX, MIN, MODE, FIRST or ANY")
	}
	return " SCALE " + strconv.Itoa(scale) + " USING " + string(reduction), nil
}

// formScale reads section 7 of a form: the scale and reduction of a
// downsampled form, or zero and "" for a form without it.
func formScale(form sectionedForm, schema *Schema, depth int) (int, Reduction, error) {
	body := form.section(sectionScale)
	if body == nil {
		return 0, "", nil
	}
	bad := func(what string) (int, Reduction, error) {
		return 0, "", protocolErrorf("", "chunk form of table %s: %s", schema.Table, what)
	}
	if len(body) != 8 || binary.LittleEndian.Uint16(body[6:]) != 0 {
		return bad("invalid scale section")
	}
	scale := binary.LittleEndian.Uint32(body)
	reduction, ok := reductionCodes[binary.LittleEndian.Uint16(body[4:])]
	if !ok || scale == 0 || scale > uint32(schema.ChunkWidth) || schema.ChunkWidth%int(scale) != 0 || schema.ChunkHeight%int(scale) != 0 || (schema.Dims == 3 && depth%int(scale) != 0) {
		return bad("scale does not divide the chunk")
	}
	return int(scale), reduction, nil
}
