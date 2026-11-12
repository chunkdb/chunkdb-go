package chunkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"
)

func sectionGolden(t *testing.T) []byte {
	t.Helper()
	// Header and directory are literal wire bytes, independent of the writer.
	data, err := hex.DecodeString("434b4346010000002a0000000000000007000000000000000b0000000000000003000000" +
		"010000004800000001000000020000004900000002000000060000004b00000004000000" + "070b1600000000")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Build malformed inputs independently of the validating serializer.
func sectionFixture(sections []chunkFormSection) []byte {
	data := make([]byte, 36+12*len(sections))
	copy(data, "CKCF")
	binary.LittleEndian.PutUint16(data[4:], 1)
	binary.LittleEndian.PutUint64(data[8:], 42)
	binary.LittleEndian.PutUint64(data[16:], 7)
	binary.LittleEndian.PutUint64(data[24:], 11)
	binary.LittleEndian.PutUint16(data[32:], uint16(len(sections)))
	for i, section := range sections {
		at := 36 + 12*i
		binary.LittleEndian.PutUint16(data[at:], section.kind)
		binary.LittleEndian.PutUint32(data[at+4:], uint32(len(data)))
		binary.LittleEndian.PutUint32(data[at+8:], uint32(len(section.body)))
		data = append(data, section.body...)
	}
	return data
}

func TestSectionedFormGoldenAndBounds(t *testing.T) {
	data := sectionGolden(t)
	form, err := parseSectionedForm(data, uint64(len(data)), true)
	if err != nil {
		t.Fatal(err)
	}
	if form.header != (chunkFormHeader{0, 42, 7, 11}) || len(form.sections) != 3 || !bytes.Equal(form.section(2), []byte{11, 22}) {
		t.Fatalf("form: %+v", form)
	}
	encoded, err := serializeSectionedForm(form, uint64(len(data)))
	if err != nil || !bytes.Equal(encoded, data) {
		t.Fatalf("encode %x: %v", encoded, err)
	}
	for end := 0; end < len(data); end++ {
		if _, err := parseSectionedForm(data[:end], math.MaxUint32, false); err == nil {
			t.Fatalf("accepted truncation at %d", end)
		}
	}
	if _, err := parseSectionedForm(data, uint64(len(data)-1), false); err == nil {
		t.Fatal("read limit ignored")
	}
	if _, err := serializeSectionedForm(form, uint64(len(data)-1)); err == nil {
		t.Fatal("write limit ignored")
	}
	for _, change := range []struct {
		at    int
		value byte
	}{
		{0, 'X'}, {4, 2}, {6, 4}, {16, 0}, {32, 65}, {34, 1}, {38, 1}, {40, 71}, {40, 73}, {44, 255}, {48, 1}, {52, 72}, {56, 0},
	} {
		bad := bytes.Clone(data)
		bad[change.at] = change.value
		if _, err := parseSectionedForm(bad, math.MaxUint32, false); err == nil {
			t.Fatalf("accepted corruption at %d", change.at)
		}
	}
	if _, err := parseSectionedForm(append(bytes.Clone(data), 0), math.MaxUint32, false); err == nil {
		t.Fatal("trailing byte accepted")
	}
	for _, flags := range []uint16{chunkFormPrivate, chunkFormPrivate | chunkFormProjected} {
		private := bytes.Clone(data)
		binary.LittleEndian.PutUint16(private[6:], flags)
		if _, err := parseSectionedForm(private, math.MaxUint32, false); err == nil {
			t.Fatal("private producing identity accepted")
		}
		binary.LittleEndian.PutUint64(private[8:], 0)
		binary.LittleEndian.PutUint64(private[24:], 0)
		if _, err := parseSectionedForm(private, math.MaxUint32, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, at := range []int{8, 16, 24} {
		binary.LittleEndian.PutUint64(data[at:], math.MaxUint64)
	}
	form, err = parseSectionedForm(data, math.MaxUint32, false)
	if err != nil || form.header.version != math.MaxUint64 || form.header.schema != math.MaxUint64 || form.header.commitTime != math.MaxUint64 {
		t.Fatalf("lost unsigned identity: %+v %v", form.header, err)
	}
}

func TestSectionedFormUnknownSections(t *testing.T) {
	data := sectionFixture([]chunkFormSection{{0, nil}, {1, []byte{1}}, {6, []byte{0, 0, 0, 0}}, {99, []byte{0xad, 0, 8}}})
	form, err := parseSectionedForm(data, math.MaxUint32, false)
	if err != nil || !bytes.Equal(form.section(99), []byte{0xad, 0, 8}) {
		t.Fatalf("unknown section lost: %+v %v", form, err)
	}
	if _, err := parseSectionedForm(data, math.MaxUint32, true); err == nil {
		t.Fatal("unknown mutation section accepted")
	}
	if _, err := serializeSectionedForm(form, math.MaxUint32); err == nil {
		t.Fatal("unknown section republished")
	}
	binary.LittleEndian.PutUint32(data[36+3*12+4:], math.MaxUint32)
	if _, err := parseSectionedForm(data, math.MaxUint32, false); err == nil {
		t.Fatal("unknown section bounds ignored")
	}
	for _, sections := range [][]chunkFormSection{{{1, []byte{1}}}, {{6, []byte{0, 0, 0, 0}}}} {
		if _, err := parseSectionedForm(sectionFixture(sections), math.MaxUint32, false); err == nil {
			t.Fatal("required section omitted")
		}
	}
}

func TestSectionedBlockSpansAndChunkRows(t *testing.T) {
	spans := make([]byte, 4+24*3)
	binary.LittleEndian.PutUint32(spans, 3)
	put := func(index int, fields ...uint32) {
		for i, v := range fields {
			binary.LittleEndian.PutUint32(spans[4+24*index+4*i:], v)
		}
	}
	put(0, 20, 3, 0, 2, 0, 0)
	put(1, 17, 9, 2, 4, 0, 1)
	put(2, 4, 8, 6, 3, 0, 0)
	values := []byte{0xe9, 1, 0xff, 1, 4, 0, 11, 22, 33}
	sections := []chunkFormSection{{1, []byte{7}}, {2, values}, {3, []byte{5}}, {6, spans}}
	form, err := parseSectionedForm(sectionFixture(sections), math.MaxUint32, false)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := parseFormBlockSpans(form, 3)
	if err != nil || len(columns) != 3 || columns[1].id != 17 || columns[1].width != 9 || !bytes.Equal(columns[1].validity, []byte{5}) {
		t.Fatalf("descriptors: %+v %v", columns, err)
	}
	for _, defect := range []func([]chunkFormSection){
		func(s []chunkFormSection) { s[0].body[0] = 0xff },
		func(s []chunkFormSection) { s[1].body[1] = 0xff },
		func(s []chunkFormSection) { binary.LittleEndian.PutUint32(s[3].body[4+24:], 20) },
		func(s []chunkFormSection) { binary.LittleEndian.PutUint32(s[3].body[4+24+8:], 1) },
		func(s []chunkFormSection) { binary.LittleEndian.PutUint32(s[3].body[4+24+20:], 0) },
	} {
		changed := make([]chunkFormSection, len(sections))
		for i, s := range sections {
			changed[i] = chunkFormSection{s.kind, bytes.Clone(s.body)}
		}
		defect(changed)
		parsed, err := parseSectionedForm(sectionFixture(changed), math.MaxUint32, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseFormBlockSpans(parsed, 3); err == nil {
			t.Fatal("corrupt block descriptors accepted")
		}
	}
	row := make([]byte, 4+16*2)
	binary.LittleEndian.PutUint32(row, 2)
	binary.LittleEndian.PutUint32(row[4:], 1)
	binary.LittleEndian.PutUint32(row[12:], uint32(len(row)))
	binary.LittleEndian.PutUint32(row[16:], 1)
	binary.LittleEndian.PutUint32(row[20:], 3)
	binary.LittleEndian.PutUint16(row[24:], 1)
	binary.LittleEndian.PutUint32(row[28:], uint32(len(row)+1))
	row = append(row, 9)
	rows, err := parseFormChunkColumns(sectionedForm{sections: []chunkFormSection{{5, row}}})
	if err != nil || len(rows) != 2 || rows[0].id != 1 || !bytes.Equal(rows[0].value, []byte{9}) || !rows[1].null {
		t.Fatalf("chunk row: %+v %v", rows, err)
	}
	for end := 0; end < len(row); end++ {
		if _, err := parseFormChunkColumns(sectionedForm{sections: []chunkFormSection{{5, row[:end]}}}); err == nil {
			t.Fatalf("accepted row truncation at %d", end)
		}
	}
}
