package chunkdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
)

const (
	sectionedHeaderBytes         = 36
	sectionDirectoryBytes        = 12
	chunkFormProjected    uint16 = 1
	chunkFormPrivate      uint16 = 2
	sectionPresence       uint16 = 1
	sectionFixed          uint16 = 2
	sectionValidity       uint16 = 3
	sectionVariable       uint16 = 4
	sectionChunkColumns   uint16 = 5
	sectionBlockSpans     uint16 = 6
)

type chunkFormHeader struct {
	flags                       uint16
	version, schema, commitTime uint64
}

type chunkFormSection struct {
	kind uint16
	body []byte
}

type sectionedForm struct {
	header   chunkFormHeader
	sections []chunkFormSection
}

func (f sectionedForm) section(kind uint16) []byte {
	for _, section := range f.sections {
		if section.kind == kind {
			return section.body
		}
	}
	return nil
}

func validFormHeader(header chunkFormHeader) bool {
	return header.schema != 0 && header.flags & ^uint16(chunkFormProjected|chunkFormPrivate) == 0 &&
		(header.flags&chunkFormPrivate == 0 || (header.version == 0 && header.commitTime == 0))
}

// Validate the complete directory before exposing any section slices.
// Read paths retain unknown sections; mutation paths cannot republish them.
func parseSectionedForm(data []byte, limit uint64, mutation bool) (sectionedForm, error) {
	bad := func(what string) (sectionedForm, error) {
		return sectionedForm{}, protocolErrorf("", "chunk form: %s", what)
	}
	if uint64(len(data)) > min(limit, uint64(math.MaxUint32)) || len(data) < sectionedHeaderBytes {
		return bad("truncated header or byte limit exceeded")
	}
	if !bytes.Equal(data[:4], []byte("CKCF")) || binary.LittleEndian.Uint16(data[4:]) != 1 {
		return bad("unsupported header")
	}
	header := chunkFormHeader{binary.LittleEndian.Uint16(data[6:]), binary.LittleEndian.Uint64(data[8:]),
		binary.LittleEndian.Uint64(data[16:]), binary.LittleEndian.Uint64(data[24:])}
	if !validFormHeader(header) || binary.LittleEndian.Uint16(data[34:]) != 0 {
		return bad("invalid flags, schema or private identity")
	}
	count := int(binary.LittleEndian.Uint16(data[32:]))
	next := sectionedHeaderBytes + count*sectionDirectoryBytes
	if count > 64 || next > len(data) {
		return bad("invalid section count or truncated directory")
	}
	var previous uint16
	var presence, spans bool
	for i := range count {
		entry := data[sectionedHeaderBytes+i*sectionDirectoryBytes:]
		kind := binary.LittleEndian.Uint16(entry)
		offset, length := uint64(binary.LittleEndian.Uint32(entry[4:])), uint64(binary.LittleEndian.Uint32(entry[8:]))
		if (i != 0 && kind <= previous) || binary.LittleEndian.Uint16(entry[2:]) != 0 {
			return bad("unordered kinds or section flags")
		}
		if offset != uint64(next) || length > uint64(len(data)-next) {
			return bad("invalid section bounds")
		}
		if mutation && (kind < sectionPresence || kind > sectionBlockSpans) {
			return bad("unknown_form_section")
		}
		presence = presence || kind == sectionPresence
		spans = spans || kind == sectionBlockSpans
		previous = kind
		next += int(length)
	}
	if next != len(data) || !presence || !spans {
		return bad("missing required sections or trailing bytes")
	}
	form := sectionedForm{header: header, sections: make([]chunkFormSection, 0, count)}
	for i := range count {
		entry := data[sectionedHeaderBytes+i*sectionDirectoryBytes:]
		offset, length := int(binary.LittleEndian.Uint32(entry[4:])), int(binary.LittleEndian.Uint32(entry[8:]))
		form.sections = append(form.sections, chunkFormSection{binary.LittleEndian.Uint16(entry), data[offset : offset+length]})
	}
	return form, nil
}

func serializeSectionedForm(form sectionedForm, limit uint64) ([]byte, error) {
	bad := func(what string) ([]byte, error) { return nil, requestErrorf("SET CHUNK", "chunk form: %s", what) }
	if !validFormHeader(form.header) || len(form.sections) > 64 {
		return bad("invalid header or section count")
	}
	total := uint64(sectionedHeaderBytes + len(form.sections)*sectionDirectoryBytes)
	limit = min(limit, uint64(math.MaxUint32), uint64(math.MaxInt))
	if total > limit {
		return bad("byte limit exceeded")
	}
	var previous uint16
	var presence, spans bool
	for i, section := range form.sections {
		if (i != 0 && section.kind <= previous) || section.kind < sectionPresence || section.kind > sectionBlockSpans {
			return bad("invalid or unknown section kind")
		}
		if uint64(len(section.body)) > limit-total {
			return bad("byte limit exceeded")
		}
		total += uint64(len(section.body))
		presence = presence || section.kind == sectionPresence
		spans = spans || section.kind == sectionBlockSpans
		previous = section.kind
	}
	if !presence || !spans {
		return bad("missing required sections")
	}
	data := make([]byte, sectionedHeaderBytes, int(total))
	copy(data, "CKCF")
	binary.LittleEndian.PutUint16(data[4:], 1)
	binary.LittleEndian.PutUint16(data[6:], form.header.flags)
	binary.LittleEndian.PutUint64(data[8:], form.header.version)
	binary.LittleEndian.PutUint64(data[16:], form.header.schema)
	binary.LittleEndian.PutUint64(data[24:], form.header.commitTime)
	binary.LittleEndian.PutUint16(data[32:], uint16(len(form.sections)))
	offset := sectionedHeaderBytes + len(form.sections)*sectionDirectoryBytes
	for _, section := range form.sections {
		data = binary.LittleEndian.AppendUint16(data, section.kind)
		data = binary.LittleEndian.AppendUint16(data, 0)
		data = binary.LittleEndian.AppendUint32(data, uint32(offset))
		data = binary.LittleEndian.AppendUint32(data, uint32(len(section.body)))
		offset += len(section.body)
	}
	for _, section := range form.sections {
		data = append(data, section.body...)
	}
	return data, nil
}

func validArrayPadding(data []byte, bits uint64) bool {
	return uint64(len(data)) == (bits+7)/8 && (bits%8 == 0 || data[len(data)-1]>>(bits%8) == 0)
}

type formBlockSpan struct {
	id               uint32
	width            int
	values, validity []byte
}

func parseFormBlockSpans(form sectionedForm, blocks int) ([]formBlockSpan, error) {
	bad := func(what string) ([]formBlockSpan, error) { return nil, protocolErrorf("", "chunk form: %s", what) }
	if blocks <= 0 || blocks > 1048576 || !validArrayPadding(form.section(sectionPresence), uint64(blocks)) {
		return bad("invalid presence")
	}
	directory := form.section(sectionBlockSpans)
	if len(directory) < 4 {
		return bad("truncated block descriptors")
	}
	count := uint64(binary.LittleEndian.Uint32(directory))
	if count > 1024 || uint64(len(directory)) != 4+24*count {
		return bad("invalid descriptor count or length")
	}
	values, validity := form.section(sectionFixed), form.section(sectionValidity)
	type span struct{ offset, length int }
	var valueSpans, validitySpans []span
	columns := make([]formBlockSpan, 0, int(count))
	ids := make(map[uint32]bool, int(count))
	for i := range int(count) {
		entry := directory[4+24*i:]
		id, width := binary.LittleEndian.Uint32(entry), binary.LittleEndian.Uint32(entry[4:])
		if id == 0 || ids[id] || width == 0 || width > 65535 {
			return bad("invalid descriptor ID or width")
		}
		ids[id] = true
		vo, vl := uint64(binary.LittleEndian.Uint32(entry[8:])), uint64(binary.LittleEndian.Uint32(entry[12:]))
		no, nl := uint64(binary.LittleEndian.Uint32(entry[16:])), uint64(binary.LittleEndian.Uint32(entry[20:]))
		if vo > uint64(len(values)) || vl > uint64(len(values))-vo || no > uint64(len(validity)) || nl > uint64(len(validity))-no {
			return bad("descriptor array bounds")
		}
		v, n := values[int(vo):int(vo+vl)], validity[int(no):int(no+nl)]
		if !validArrayPadding(v, uint64(width)*uint64(blocks)) || (nl != 0 && !validArrayPadding(n, uint64(blocks))) || (nl == 0 && no != 0) {
			return bad("descriptor array length or padding")
		}
		valueSpans = append(valueSpans, span{int(vo), int(vl)})
		if nl != 0 {
			validitySpans = append(validitySpans, span{int(no), int(nl)})
		}
		columns = append(columns, formBlockSpan{id, int(width), v, n})
	}
	covered := func(spans []span, length int) bool {
		slices.SortFunc(spans, func(a, b span) int { return a.offset - b.offset })
		next := 0
		for _, span := range spans {
			if span.offset != next {
				return false
			}
			next += span.length
		}
		return next == length
	}
	if !covered(valueSpans, len(values)) || !covered(validitySpans, len(validity)) {
		return bad("overlap or unused array bytes")
	}
	return columns, nil
}

type formChunkColumn struct {
	id    uint32
	null  bool
	value []byte
}

func parseFormChunkColumns(form sectionedForm) ([]formChunkColumn, error) {
	bad := func(what string) ([]formChunkColumn, error) { return nil, protocolErrorf("", "chunk form: %s", what) }
	body := form.section(sectionChunkColumns)
	if body == nil {
		for _, section := range form.sections {
			if section.kind == sectionChunkColumns {
				return bad("truncated chunk columns")
			}
		}
		return nil, nil
	}
	if len(body) < 4 {
		return bad("truncated chunk columns")
	}
	count := uint64(binary.LittleEndian.Uint32(body))
	next := uint64(4) + 16*count
	if count > 1024 || next > uint64(len(body)) {
		return bad("invalid chunk column count")
	}
	columns := make([]formChunkColumn, 0, int(count))
	var previous uint32
	for i := range int(count) {
		entry := body[4+16*i:]
		id, flags := binary.LittleEndian.Uint32(entry), binary.LittleEndian.Uint16(entry[4:])
		offset, length := uint64(binary.LittleEndian.Uint32(entry[8:])), uint64(binary.LittleEndian.Uint32(entry[12:]))
		if id <= previous || flags > 1 || binary.LittleEndian.Uint16(entry[6:]) != 0 || offset != next || length > uint64(len(body))-next || (flags == 1 && length != 0) {
			return bad("invalid chunk column descriptor")
		}
		columns = append(columns, formChunkColumn{id, flags == 1, body[int(offset):int(offset+length)]})
		previous, next = id, next+length
	}
	if next != uint64(len(body)) {
		return bad("trailing chunk column bytes")
	}
	return columns, nil
}
