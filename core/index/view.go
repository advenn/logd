package index

import "encoding/binary"

// Lookup is the query side of a typed-range index: the record offsets whose key equals a
// key, or lies between two. A sealed segment answers it from its .tidx (Reader, by binary
// search); the active segment answers it from the writer's buffer (FieldView, by one linear
// pass). Both take the same arguments with the same meaning, so the planner builds one
// lookup — one set of bound-widening rules — and runs it against either.
type Lookup interface {
	LookupEqual(key [KeySize]byte) []uint64
	LookupRange(lo, hi [KeySize]byte, incLo, incHi bool) []uint64
}

var (
	_ Lookup = (*Reader)(nil)
	_ Lookup = FieldView{}
)

// FieldView is one field's buffered entries as View saw them, restricted to the records at
// offsets below its bound: the part of the active segment that is on disk.
//
// It holds a copy of the entries' slice header, not of the entries. That is safe while the
// writer keeps appending, because an appended entry is never written again: an append
// either writes past the copied length or moves everything to a new array and leaves the
// old one as it was. The one thing that did rewrite entries in place — sorting them for the
// .tidx at seal — now sorts a copy (see Flush).
type FieldView struct {
	Kind    ValueKind
	Pattern string // the pattern the field's values were extracted with (see FieldType)
	entries []Entry
	below   uint64
}

// View returns a FieldView of every field the buffer indexes, including fields with no
// entries yet: the buffer holds every value extracted from every record it was given, so an
// empty field is a field no record has — unlike a sealed segment, which writes no .tidx for
// it and so cannot tell absent from unindexed.
//
// The caller must hold the lock its writer appends under (see Buffer).
func (b *Buffer) View(below uint64) map[string]FieldView {
	out := make(map[string]FieldView, len(b.byField))
	for name, fe := range b.byField {
		out[name] = FieldView{Kind: fe.kind, Pattern: fe.pattern, entries: fe.entries, below: below}
	}
	return out
}

// LookupEqual returns the viewed offsets whose key equals key, in offset order.
func (v FieldView) LookupEqual(key [KeySize]byte) []uint64 {
	var out []uint64
	for i := range v.entries {
		if e := &v.entries[i]; e.Offset < v.below && e.Key == key {
			out = append(out, e.Offset)
		}
	}
	return out
}

// LookupRange returns the viewed offsets whose key lies within [lo, hi], inclusivity as for
// Reader.LookupRange, in offset order. The entries are in append order, not key order, so
// this is one pass over all of them rather than a binary search; comparing keys as two
// big-endian words keeps that pass cheap.
func (v FieldView) LookupRange(lo, hi [KeySize]byte, incLo, incHi bool) []uint64 {
	loHi, loLo := keyWords(&lo)
	hiHi, hiLo := keyWords(&hi)
	var out []uint64
	for i := range v.entries {
		e := &v.entries[i]
		if e.Offset >= v.below {
			continue
		}
		kh, kl := keyWords(&e.Key)
		if c := compareWords(kh, kl, loHi, loLo); c < 0 || c == 0 && !incLo {
			continue
		}
		if c := compareWords(kh, kl, hiHi, hiLo); c > 0 || c == 0 && !incHi {
			continue
		}
		out = append(out, e.Offset)
	}
	return out
}

// keyWords splits a key into its two big-endian halves. Comparing those as unsigned
// integers orders keys exactly as bytesCompare does, since both are lexicographic.
func keyWords(k *[KeySize]byte) (hi, lo uint64) {
	return binary.BigEndian.Uint64(k[0:8]), binary.BigEndian.Uint64(k[8:16])
}

func compareWords(ah, al, bh, bl uint64) int {
	switch {
	case ah < bh:
		return -1
	case ah > bh:
		return 1
	case al < bl:
		return -1
	case al > bl:
		return 1
	default:
		return 0
	}
}
