// Package index implements logd's typed-range index: the order-preserving 16-byte
// key encoding, the per-segment .tidx file, and the in-RAM buffer that accumulates
// (key, offset) pairs and flushes them at seal. It is a leaf package (stdlib only)
// so both the extraction side and the storage side can depend on it.
//
// The whole point of the encoding here is the property that makes range queries
// O(log n): for a given field type, bytes.Compare(EncodeKey(a), EncodeKey(b))
// agrees with the type's natural order. A .tidx sorted by these keys is then a flat
// sorted file, and a range query is a binary search for the lower bound plus a walk
// — B-tree-leaf behavior without the tree, exactly because sealed segments are
// immutable (design §6.3, discussion-v2 §3.3).
package index

import (
	"encoding/binary"
	"math"
)

// KeySize is the fixed width of every index key. 16 bytes fits a UUID exactly, an
// int64/float64 with room to left-pad, and a lossy 16-byte string prefix — one width
// for all types means one set of binary-search machinery.
const KeySize = 16

// ValueKind tags the type of a Value (and the type a whole .tidx file holds). Since
// one .tidx is per (segment, field) and a field has a single type, keys within a
// file are always the same kind — cross-kind ordering never matters, only order
// within a kind.
type ValueKind uint8

const (
	KindInt   ValueKind = 0
	KindFloat ValueKind = 1
	KindStr   ValueKind = 2
	KindUUID  ValueKind = 3
)

// Value is a typed value extracted from a log line, before key encoding. Only the
// field named by Kind is meaningful.
type Value struct {
	Kind  ValueKind
	Int   int64
	Float float64
	Str   string
	UUID  [KeySize]byte
}

// KeyedValue is the extraction output handed to the writer: which field the value
// belongs to and its already-encoded order-preserving key. The writer pairs it with
// the record's segment-relative offset and buffers it for the field's .tidx.
type KeyedValue struct {
	Field string
	Key   [KeySize]byte
}

// FieldType names an indexed field and its type. The writer records the set of these
// as a segment's schema so the planner later consults the segment's own schema, not
// live config (design §7.3).
type FieldType struct {
	Name string
	Kind ValueKind
}

// EncodeKey produces the 16-byte order-preserving key for a value. See the encoding
// notes on each helper; the invariant is bytes.Compare(EncodeKey(a),EncodeKey(b))
// == the type's natural compare of a,b (for values of the same Kind).
func EncodeKey(v Value) [KeySize]byte {
	switch v.Kind {
	case KindInt:
		return encodeInt(v.Int)
	case KindFloat:
		return encodeFloat(v.Float)
	case KindStr:
		return encodeStr(v.Str)
	case KindUUID:
		return v.UUID // already 16 raw bytes; order = byte order (equality-only field)
	default:
		return [KeySize]byte{}
	}
}

// encodeInt maps a two's-complement int64 to a big-endian unsigned value that sorts
// correctly by flipping the sign bit: this turns the most-negative int64 into the
// smallest unsigned and the most-positive into the largest. The 8-byte value goes in
// the low half [8:16]; the high half [0:8] is 0x00 padding (all int keys share it, so
// ordering is decided by the meaningful low 8 bytes).
func encodeInt(i int64) [KeySize]byte {
	var k [KeySize]byte
	u := uint64(i) ^ (1 << 63)
	binary.BigEndian.PutUint64(k[8:16], u)
	return k
}

// encodeFloat maps a float64 to an order-preserving big-endian value via the standard
// IEEE-754 total-order transform: if the sign bit is set (negative), flip all bits;
// otherwise flip just the sign bit. Negative zero is normalized to positive zero first
// so −0 and +0 encode identically (design §7.2). NaN/±Inf never reach here — the
// extractor rejects them as extraction failures.
func encodeFloat(f float64) [KeySize]byte {
	var k [KeySize]byte
	var b uint64
	if f == 0 { // true for both +0.0 and −0.0; force the +0.0 bit pattern
		b = 0
	} else {
		b = math.Float64bits(f)
	}
	if b>>63 == 1 {
		b = ^b
	} else {
		b |= 1 << 63
	}
	binary.BigEndian.PutUint64(k[8:16], b)
	return k
}

// encodeStr takes the first 16 bytes of the UTF-8 string verbatim (plain truncation)
// and right-pads with 0x00. Byte-wise comparison of UTF-8 is lexicographic, so this
// sorts strings correctly, and the 0x00 pad makes a shorter string sort before a
// longer one sharing its prefix.
//
// We deliberately do NOT back up to a rune boundary. Doing so — dropping a split
// trailing rune and zero-padding in its place — is NON-MONOTONIC: a 17-byte value
// whose 16-byte cut lands mid-rune would then sort BELOW a 16-byte value it is
// naturally greater than, causing false negatives on range queries. The design's
// "never split a rune" note optimized for a readable key at the cost of correctness;
// here order preservation wins. The key is an opaque, LOSSY sort prefix — a split
// multibyte rune in the key bytes is harmless because equality/range hits on strings
// longer than 16 bytes are candidate sets re-verified against the decoded record.
func encodeStr(s string) [KeySize]byte {
	var k [KeySize]byte
	copy(k[:], s) // copies min(len(s), 16) bytes; the remainder stays 0x00
	return k
}
