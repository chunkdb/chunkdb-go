package chunkdb

import (
	"errors"
	"testing"
)

func TestChunkModelDescribeGeometryBounds(t *testing.T) {
	base := readOne(t, describeReply("t", 7, []string{fakeColumn(1, "n", "u8", false, false, respInt(0))}))
	base = replaceMapValue(base, "dims", readOne(t, respInt(3)))
	base = replaceMapValue(base, "chunk", readOne(t, respArray(respInt(2), respInt(2), respInt(2))))
	base = replaceMapValue(base, "large", readOne(t, respArray(respInt(1000000), respInt(8), respInt(9))))
	schema, err := parseSchema(base)
	if err != nil || schema.BlockCount() != 8 || schema.LargeWidth != 1000000 || schema.LargeDepth != 9 {
		t.Fatalf("valid geometry %+v: %v", schema, err)
	}
	for _, tc := range []struct{ name, field, wire string }{
		{"chunk axis", "chunk", respArray(respInt(2), respInt(2), respInt(4097))},
		{"chunk product", "chunk", respArray(respInt(4096), respInt(4096), respInt(2))},
		{"large axis", "large", respArray(respInt(1000001), respInt(8), respInt(9))},
		{"large depth", "large", respArray(respInt(8), respInt(8), respInt(1000001))},
		{"missing depth", "chunk", respArray(respInt(2), respInt(2))},
		{"zero depth", "chunk", respArray(respInt(2), respInt(2), respInt(0))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseSchema(replaceMapValue(base, tc.field, readOne(t, tc.wire))); !errors.Is(err, ErrProtocol) {
				t.Fatalf("malformed geometry accepted: %v", err)
			}
		})
	}
}
