package index

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// The .tidx file is a per-(segment, field) sorted flat file of fixed 24-byte records
// (16-byte order-preserving key + 8-byte segment-relative record offset). Because it
// is sorted by key and immutable once written at seal, equality is a binary search
// and a range is a binary search for the lower bound plus a linear walk — the whole
// reason the encoding is order-preserving (design §6.3).
const (
	tidxMagic uint32 = 0x54494458 // "TIDX"
	// tidxVersionPlain is the original format: the record region stored verbatim.
	// tidxVersionCompressed deflates that region — see WriteTIDX for why that is safe here
	// but not for a segment. Both are readable; only the compressed form is written.
	tidxVersionPlain      uint16 = 1
	tidxVersionCompressed uint16 = 2
	tidxHeaderSize               = 32
	tidxRecordSize               = KeySize + 8 // 16-byte key + 8-byte offset
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

	// Build the record region, then deflate it. Nothing addresses INTO a .tidx by file
	// offset — OpenReader loads the whole file and every lookup binary-searches the
	// in-memory slice — so unlike a segment, this file can be compressed wholesale with no
	// block directory and no change to the search machinery.
	//
	// It compresses extremely well by construction: an int key puts its value in bytes
	// [8:16] and zero-pads [0:8], so half of every key is zeros, and a literal-existence
	// field keys EVERY entry under the all-zero key. Measured on real files, the record
	// region shrinks ~5x and the keys alone ~178x.
	body := make([]byte, len(entries)*tidxRecordSize)
	off := 0
	for _, e := range entries {
		copy(body[off:off+KeySize], e.Key[:])
		binary.BigEndian.PutUint64(body[off+KeySize:off+tidxRecordSize], e.Offset)
		off += tidxRecordSize
	}

	var comp bytes.Buffer
	zw, err := flate.NewWriter(&comp, flate.DefaultCompression)
	if err != nil {
		return err
	}
	if _, err := zw.Write(body); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}

	buf := make([]byte, tidxHeaderSize+comp.Len())
	binary.BigEndian.PutUint32(buf[0:4], tidxMagic)
	binary.BigEndian.PutUint16(buf[4:6], tidxVersionCompressed)
	buf[6] = byte(kind)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(entries)))
	binary.BigEndian.PutUint64(buf[12:20], segmentID)
	// [20:24] crc, left zero while computing it below.
	// [24:32] was reserved; it now carries the UNCOMPRESSED body length, so the reader can
	// pre-size its buffer and reject a stream that inflates to the wrong size.
	binary.BigEndian.PutUint64(buf[24:32], uint64(len(body)))
	copy(buf[tidxHeaderSize:], comp.Bytes())

	// Checksum the ENTIRE file AS STORED (header + compressed body) with the crc field
	// zeroed. Deliberately over the stored bytes rather than the decompressed ones: it
	// detects disk corruption before any time is spent inflating, and it keeps a flipped
	// byte anywhere in the file a detected error rather than a wrong answer.
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
	// BOTH versions are accepted. Rejecting v1 would be correct — the caller degrades to a
	// scan — but needlessly slow for every segment sealed before compression existed, until
	// retention aged them out.
	ver := binary.BigEndian.Uint16(data[4:6])
	if ver != tidxVersionPlain && ver != tidxVersionCompressed {
		return nil, fmt.Errorf("tidx %s: unsupported version %d", path, ver)
	}
	kind := ValueKind(data[6])
	count := int(binary.BigEndian.Uint32(data[8:12]))

	// Size check, per version. This is the truncation guard: for a plain file the length is
	// implied by the record count; for a compressed one only the DECOMPRESSED length is,
	// so the stored uncompressed length is what gets cross-checked (below, after inflating).
	if ver == tidxVersionPlain && len(data) != tidxHeaderSize+count*tidxRecordSize {
		return nil, fmt.Errorf("tidx %s: size %d disagrees with record count %d", path, len(data), count)
	}

	// Verify the whole-file checksum with the crc field treated as zero, streamed so
	// we don't copy the file. A mismatch (header OR record corruption) fails the open
	// and the caller falls back to scanning this segment. Checked BEFORE inflating, so a
	// corrupt file costs a CRC pass rather than a decompression.
	stored := binary.BigEndian.Uint32(data[20:24])
	h := crc32.New(tidxCRC)
	h.Write(data[:20])
	h.Write([]byte{0, 0, 0, 0})
	h.Write(data[24:])
	if h.Sum32() != stored {
		return nil, fmt.Errorf("tidx %s: crc mismatch (corrupt index)", path)
	}

	body := data[tidxHeaderSize:]
	if ver == tidxVersionCompressed {
		rawLen := binary.BigEndian.Uint64(data[24:32])
		if rawLen != uint64(count)*uint64(tidxRecordSize) {
			return nil, fmt.Errorf("tidx %s: uncompressed length %d disagrees with record count %d", path, rawLen, count)
		}
		out := make([]byte, rawLen)
		zr := flate.NewReader(bytes.NewReader(body))
		defer zr.Close()
		if _, err := io.ReadFull(zr, out); err != nil {
			return nil, fmt.Errorf("tidx %s: decompressing records: %w", path, err)
		}
		body = out
	}

	entries := make([]Entry, count)
	off := 0
	for i := 0; i < count; i++ {
		copy(entries[i].Key[:], body[off:off+KeySize])
		entries[i].Offset = binary.BigEndian.Uint64(body[off+KeySize : off+tidxRecordSize])
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
