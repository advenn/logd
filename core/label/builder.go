package label

import (
	"encoding/binary"
	"hash/crc32"
	"sort"
)

// On-disk format of a segment's label index (.lidx). Header is fixed; the body is
// uvarint-based (compact for the many small integers). StreamID is a stream's 0-based
// position in the file.
//
//	[header 16B] magic 'LIDX' u32 · version u16 · reserved u16 · streamCount u32 · crc32 u32 (whole file, this field zeroed)
//	[stream × streamCount]
//	   uvarint pairCount; per pair: uvarint keyLen, key, uvarint valLen, val   (pairs sorted by key)
//	   uvarint postingCount; offsets delta+uvarint (first absolute, rest ascending deltas)
const (
	lidxMagic      uint32 = 0x4C494458 // "LIDX"
	lidxVersion    uint16 = 1
	lidxHeaderSize        = 16
)

var lidxCRC = crc32.MakeTable(crc32.IEEE)

// Builder accumulates a segment's label streams while it is active (writer-owned, not
// concurrency-safe). Each distinct label set is interned to a StreamID; each record's
// offset is appended to its stream's posting list.
type Builder struct {
	byCanon map[string]uint32
	streams []*streamData
}

type streamData struct {
	set     Set
	offsets []uint64
}

// NewBuilder returns an empty per-segment builder.
func NewBuilder() *Builder {
	return &Builder{byCanon: make(map[string]uint32)}
}

// Intern returns the StreamID for a label set, assigning a new one the first time the
// set is seen in this segment.
func (b *Builder) Intern(s Set) uint32 {
	c := s.Canonical()
	if id, ok := b.byCanon[c]; ok {
		return id
	}
	id := uint32(len(b.streams))
	b.byCanon[c] = id
	b.streams = append(b.streams, &streamData{set: s})
	return id
}

// AddPosting records that the record at offset belongs to streamID.
func (b *Builder) AddPosting(streamID uint32, offset uint64) {
	if int(streamID) < len(b.streams) {
		b.streams[streamID].offsets = append(b.streams[streamID].offsets, offset)
	}
}

// Flush writes the segment's label index to path atomically. It returns wrote=false
// (and writes nothing) when no stream carries any labels — a label-less segment needs no
// index and its label predicates simply scan.
func (b *Builder) Flush(path string) (bool, error) {
	hasLabels := false
	for _, s := range b.streams {
		if len(s.set) > 0 {
			hasLabels = true
			break
		}
	}
	if !hasLabels {
		return false, nil
	}
	if err := writeFileAtomic(path, b.encode()); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Builder) encode() []byte {
	buf := make([]byte, lidxHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], lidxMagic)
	binary.BigEndian.PutUint16(buf[4:6], lidxVersion)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(b.streams)))
	// crc [12:16] stays zero during checksum.

	for _, s := range b.streams {
		buf = appendUvarint(buf, uint64(len(s.set)))
		for _, p := range s.set {
			buf = appendString(buf, p.Key)
			buf = appendString(buf, p.Value)
		}
		offs := dedupSorted(s.offsets)
		buf = appendUvarint(buf, uint64(len(offs)))
		var prev uint64
		for i, o := range offs {
			d := o
			if i > 0 {
				d = o - prev
			}
			buf = appendUvarint(buf, d)
			prev = o
		}
	}

	crc := crc32.Checksum(buf, lidxCRC)
	binary.BigEndian.PutUint32(buf[12:16], crc)
	return buf
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// dedupSorted returns a sorted copy of offs with duplicates removed. Each record maps to
// one offset in one stream, so duplicates are not expected, but dedup keeps the posting
// list clean and the delta encoding strictly increasing.
func dedupSorted(offs []uint64) []uint64 {
	if len(offs) == 0 {
		return nil
	}
	cp := append([]uint64(nil), offs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	out := cp[:1]
	for _, o := range cp[1:] {
		if o != out[len(out)-1] {
			out = append(out, o)
		}
	}
	return out
}
