package index

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// The .tidx file is a per-(segment, field) sorted flat file of fixed 24-byte records
// (16-byte order-preserving key + 8-byte segment-relative record offset). Because it
// is sorted by key and immutable once written at seal, equality is a binary search
// and a range is a binary search for the lower bound plus a linear walk — the whole
// reason the encoding is order-preserving (design §6.3).
const (
	tidxMagic      uint32 = 0x54494458 // "TIDX"
	tidxVersion    uint16 = 1
	tidxHeaderSize        = 32
	tidxRecordSize        = KeySize + 8 // 16-byte key + 8-byte offset
)

var tidxCRC = crc32.MakeTable(crc32.IEEE)

// Entry is one (key, offset) pair. Offset is the segment-relative BYTE offset of the
// record the key was extracted from.
type Entry struct {
	Key    [KeySize]byte
	Offset uint64
}

// WriteTIDX writes entries to path as a sorted .tidx for a field of the given kind.
// Entries are sorted by (key, then offset) so equal keys are contiguous and the walk
// is deterministic. The write is atomic and durable: tmp file → fsync → rename → dir
// fsync, so a crash never leaves a torn .tidx (a torn one would be rejected on open
// and the segment would degrade to scan — never a wrong answer).
func WriteTIDX(path string, kind ValueKind, segmentID uint64, entries []Entry) error {
	sort.Slice(entries, func(i, j int) bool {
		if c := bytesCompare(entries[i].Key, entries[j].Key); c != 0 {
			return c < 0
		}
		return entries[i].Offset < entries[j].Offset
	})

	buf := make([]byte, tidxHeaderSize+len(entries)*tidxRecordSize)
	binary.BigEndian.PutUint32(buf[0:4], tidxMagic)
	binary.BigEndian.PutUint16(buf[4:6], tidxVersion)
	buf[6] = byte(kind)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(entries)))
	binary.BigEndian.PutUint64(buf[12:20], segmentID)
	// bytes [20:24] crc left zero for now; [24:32] reserved.

	off := tidxHeaderSize
	for _, e := range entries {
		copy(buf[off:off+KeySize], e.Key[:])
		binary.BigEndian.PutUint64(buf[off+KeySize:off+tidxRecordSize], e.Offset)
		off += tidxRecordSize
	}

	// Checksum the ENTIRE file (header + records) with the crc field zeroed, so
	// in-place corruption of a key or offset — not just the header — is detected on
	// open and the segment degrades to scan rather than returning a wrong answer.
	crc := crc32.Checksum(buf, tidxCRC)
	binary.BigEndian.PutUint32(buf[20:24], crc)

	return writeFileAtomic(path, buf)
}

// Reader provides binary-search lookups over a .tidx. It loads the whole file into
// memory: a per-field .tidx for one bounded (~64MB) segment is small, and an in-memory
// slice keeps the search machinery boring and mmap-free.
type Reader struct {
	kind    ValueKind
	entries []Entry
}

// SizeBytes estimates this reader's heap footprint. Used by the query-side reader cache
// to enforce a memory budget: a .tidx for a busy field runs to several megabytes, so the
// cache has to account for what it is holding rather than count entries.
func (r *Reader) SizeBytes() int64 {
	return int64(len(r.entries))*int64(tidxRecordSize) + 64
}

// OpenReader reads and validates a .tidx. A bad magic/version, header CRC mismatch, or
// a size that disagrees with the record count returns an error so the caller can
// degrade to a scan (design §9.1) rather than trust a corrupt index.
func OpenReader(path string) (*Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < tidxHeaderSize {
		return nil, fmt.Errorf("tidx %s: truncated header", path)
	}
	if magic := binary.BigEndian.Uint32(data[0:4]); magic != tidxMagic {
		return nil, fmt.Errorf("tidx %s: bad magic 0x%08X", path, magic)
	}
	if ver := binary.BigEndian.Uint16(data[4:6]); ver != tidxVersion {
		return nil, fmt.Errorf("tidx %s: unsupported version %d", path, ver)
	}
	kind := ValueKind(data[6])
	count := int(binary.BigEndian.Uint32(data[8:12]))

	if len(data) != tidxHeaderSize+count*tidxRecordSize {
		return nil, fmt.Errorf("tidx %s: size %d disagrees with record count %d", path, len(data), count)
	}
	// Verify the whole-file checksum with the crc field treated as zero, streamed so
	// we don't copy the file. A mismatch (header OR record corruption) fails the open
	// and the caller falls back to scanning this segment.
	stored := binary.BigEndian.Uint32(data[20:24])
	h := crc32.New(tidxCRC)
	h.Write(data[:20])
	h.Write([]byte{0, 0, 0, 0})
	h.Write(data[24:])
	if h.Sum32() != stored {
		return nil, fmt.Errorf("tidx %s: crc mismatch (corrupt index)", path)
	}

	entries := make([]Entry, count)
	off := tidxHeaderSize
	for i := 0; i < count; i++ {
		copy(entries[i].Key[:], data[off:off+KeySize])
		entries[i].Offset = binary.BigEndian.Uint64(data[off+KeySize : off+tidxRecordSize])
		off += tidxRecordSize
	}
	return &Reader{kind: kind, entries: entries}, nil
}

// Kind returns the value kind this index holds.
func (r *Reader) Kind() ValueKind { return r.kind }

// Count returns the number of index records.
func (r *Reader) Count() int { return len(r.entries) }

// Entries returns a copy of all index records (sorted by key). Used by verification /
// rebuild-and-compare tooling.
func (r *Reader) Entries() []Entry {
	out := make([]Entry, len(r.entries))
	copy(out, r.entries)
	return out
}

// LookupEqual returns the offsets of all records whose key equals key. Equal keys are
// contiguous (sorted), so it binary-searches the first and walks while equal.
func (r *Reader) LookupEqual(key [KeySize]byte) []uint64 {
	i := r.lowerBound(key)
	var out []uint64
	for ; i < len(r.entries) && r.entries[i].Key == key; i++ {
		out = append(out, r.entries[i].Offset)
	}
	return out
}

// LookupRange returns the offsets of all records whose key is within [lo, hi], with
// inclusivity controlled by incLo/incHi. This is the range scan the differentiator is
// for: one binary search for the lower bound, then a walk to the upper bound.
func (r *Reader) LookupRange(lo, hi [KeySize]byte, incLo, incHi bool) []uint64 {
	start := r.lowerBound(lo)
	if !incLo {
		for start < len(r.entries) && r.entries[start].Key == lo {
			start++
		}
	}
	var out []uint64
	for i := start; i < len(r.entries); i++ {
		c := bytesCompare(r.entries[i].Key, hi)
		if c > 0 || (c == 0 && !incHi) {
			break
		}
		out = append(out, r.entries[i].Offset)
	}
	return out
}

// lowerBound returns the index of the first entry whose key is >= key.
func (r *Reader) lowerBound(key [KeySize]byte) int {
	return sort.Search(len(r.entries), func(i int) bool {
		return bytesCompare(r.entries[i].Key, key) >= 0
	})
}

// bytesCompare compares two fixed keys (mirrors bytes.Compare without a slice alloc).
func bytesCompare(a, b [KeySize]byte) int {
	for i := 0; i < KeySize; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
