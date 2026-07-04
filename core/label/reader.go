package label

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// Reader is a loaded, validated segment label index. It holds the streams in memory (a
// segment's label index is small: bounded by the number of distinct label combinations).
type Reader struct {
	streams []streamData
}

// OpenReader reads and validates a .lidx. A bad magic/version, whole-file CRC mismatch,
// or a malformed body returns an error so the caller degrades label predicates to a scan
// (never a wrong answer).
func OpenReader(path string) (*Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < lidxHeaderSize {
		return nil, fmt.Errorf("lidx %s: truncated header", path)
	}
	if magic := binary.BigEndian.Uint32(data[0:4]); magic != lidxMagic {
		return nil, fmt.Errorf("lidx %s: bad magic 0x%08X", path, magic)
	}
	if ver := binary.BigEndian.Uint16(data[4:6]); ver != lidxVersion {
		return nil, fmt.Errorf("lidx %s: unsupported version %d", path, ver)
	}
	streamCount := int(binary.BigEndian.Uint32(data[8:12]))

	// Whole-file CRC with the crc field treated as zero.
	stored := binary.BigEndian.Uint32(data[12:16])
	h := crc32.New(lidxCRC)
	h.Write(data[:12])
	h.Write([]byte{0, 0, 0, 0})
	h.Write(data[16:])
	if h.Sum32() != stored {
		return nil, fmt.Errorf("lidx %s: crc mismatch (corrupt index)", path)
	}

	r := &Reader{streams: make([]streamData, 0, streamCount)}
	pos := lidxHeaderSize
	for i := 0; i < streamCount; i++ {
		pairCount, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("lidx %s: bad pair count", path)
		}
		pos += n
		// Each pair needs at least 2 bytes (two uvarint lengths). Cap the pre-alloc so a
		// corrupt-but-CRC-valid or buggy huge count degrades to an error, never an
		// out-of-memory panic (which would bypass the promised degrade-to-scan).
		if pairCount > uint64(len(data)-pos) {
			return nil, fmt.Errorf("lidx %s: pair count %d exceeds remaining bytes", path, pairCount)
		}
		set := make(Set, 0, pairCount)
		for j := uint64(0); j < pairCount; j++ {
			key, np, err := readString(data, pos)
			if err != nil {
				return nil, fmt.Errorf("lidx %s: %w", path, err)
			}
			pos = np
			val, nv, err := readString(data, pos)
			if err != nil {
				return nil, fmt.Errorf("lidx %s: %w", path, err)
			}
			pos = nv
			set = append(set, Pair{Key: key, Value: val})
		}
		postingCount, n2 := binary.Uvarint(data[pos:])
		if n2 <= 0 {
			return nil, fmt.Errorf("lidx %s: bad posting count", path)
		}
		pos += n2
		// Each posting needs at least 1 byte (a uvarint delta); bound the alloc so a
		// crafted count errors instead of OOM-panicking.
		if postingCount > uint64(len(data)-pos) {
			return nil, fmt.Errorf("lidx %s: posting count %d exceeds remaining bytes", path, postingCount)
		}
		offsets := make([]uint64, postingCount)
		var prev uint64
		for k := uint64(0); k < postingCount; k++ {
			d, nd := binary.Uvarint(data[pos:])
			if nd <= 0 {
				return nil, fmt.Errorf("lidx %s: bad posting delta", path)
			}
			pos += nd
			if k == 0 {
				prev = d
			} else {
				prev += d
			}
			offsets[k] = prev
		}
		r.streams = append(r.streams, streamData{set: set, offsets: offsets})
	}
	return r, nil
}

func readString(data []byte, pos int) (string, int, error) {
	l, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return "", 0, fmt.Errorf("bad string length")
	}
	pos += n
	if pos+int(l) > len(data) {
		return "", 0, fmt.Errorf("string extends past data")
	}
	return string(data[pos : pos+int(l)]), pos + int(l), nil
}

// LabelOffsets returns the union of record offsets across every stream whose label set
// contains key=value, sorted ascending. This is the candidate set a label-equality
// pushdown fetches and re-verifies.
func (r *Reader) LabelOffsets(key, value string) []uint64 {
	var acc []uint64
	for i := range r.streams {
		if v, ok := r.streams[i].set.Get(key); ok && v == value {
			acc = append(acc, r.streams[i].offsets...)
		}
	}
	sort.Slice(acc, func(i, j int) bool { return acc[i] < acc[j] })
	return acc
}

// Keys returns all distinct label keys present, sorted.
func (r *Reader) Keys() []string {
	seen := map[string]struct{}{}
	for i := range r.streams {
		for _, p := range r.streams[i].set {
			seen[p.Key] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// Values returns all distinct values for a key, sorted.
func (r *Reader) Values(key string) []string {
	seen := map[string]struct{}{}
	for i := range r.streams {
		if v, ok := r.streams[i].set.Get(key); ok {
			seen[v] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

// Streams returns the distinct label sets (for /series in the Loki compat layer).
func (r *Reader) Streams() []Set {
	out := make([]Set, 0, len(r.streams))
	for i := range r.streams {
		if len(r.streams[i].set) > 0 {
			out = append(out, r.streams[i].set)
		}
	}
	return out
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
