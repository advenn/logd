// Package label implements logd's native label index: per-segment stream dictionaries
// (a canonical label set ↔ a StreamID) and posting lists (StreamID → sorted record
// offsets), snapshotted at seal. It accelerates label-equality queries and serves label
// discovery, and it is a leaf package (stdlib only) so both ingest/storage and query can
// depend on it.
//
// The index is a DERIVED accelerator: the authoritative labels live in each record's
// Extra blob, and the scan path reads them from there. This package only indexes the
// subset of keys on the configured allowlist, so the index and the scan always agree
// and pushdown results are identical to a scan (the project's core invariant).
package label

import (
	"encoding/binary"
	"sort"
)

// IndexPath returns the label-index path for a segment base (its .log path minus the
// extension). Both the writer and the query planner use it so they always agree.
func IndexPath(segBase string) string { return segBase + ".labels.lidx" }

// Pair is one label key=value.
type Pair struct {
	Key   string
	Value string
}

// Set is a canonical label set: pairs sorted by key with unique keys. Canonicalizing on
// construction means two records with the same labels produce byte-identical Canonical()
// strings and therefore intern to the same StreamID regardless of input ordering.
type Set []Pair

// NewSet builds a canonical Set from a key→value map (already reduced to the allowlisted,
// stringified labels by the caller).
func NewSet(m map[string]string) Set {
	if len(m) == 0 {
		return nil
	}
	s := make(Set, 0, len(m))
	for k, v := range m {
		s = append(s, Pair{Key: k, Value: v})
	}
	sortByKey(s)
	return s
}

// NewSetPairs canonicalizes pairs in place: sorted by key, duplicates collapsed to the
// LAST occurrence, which is what building a map and ranging over it would have produced.
// It exists so a caller that already knows its keys need not build a throwaway map just to
// get those two properties — on the ingest path that map was allocated and discarded once
// per record.
func NewSetPairs(pairs []Pair) Set {
	if len(pairs) == 0 {
		return nil
	}
	sortByKey(pairs)
	out := pairs[:1]
	for _, p := range pairs[1:] {
		if p.Key == out[len(out)-1].Key {
			out[len(out)-1] = p // last wins, as a map assignment would
			continue
		}
		out = append(out, p)
	}
	return out
}

// sortByKey is a stable insertion sort. Label sets are tiny (an allowlist is a handful of
// keys), where insertion sort beats a general sort outright — and crucially sort.Slice is
// reflection-based, so it ALLOCATES: it was 15% of the ingest path's allocations, entirely
// to order two pairs.
func sortByKey(s []Pair) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Key < s[j-1].Key; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// Canonical returns a stable, UNAMBIGUOUS string identity for the set, used as the dedup
// key when interning. It LENGTH-PREFIXES each key and value rather than using in-band
// delimiters: a delimiter (any byte) can legally appear inside a label value, so two
// distinct sets could otherwise canonicalize to the same string, merge to one StreamID,
// and cause pushdown false negatives. Length-prefixing makes the encoding injective.
func (s Set) Canonical() string {
	if len(s) == 0 {
		return ""
	}
	n := 0
	for _, p := range s {
		n += len(p.Key) + len(p.Value) + 2*binary.MaxVarintLen64
	}
	buf := make([]byte, 0, n)
	var tmp [binary.MaxVarintLen64]byte
	put := func(field string) {
		m := binary.PutUvarint(tmp[:], uint64(len(field)))
		buf = append(buf, tmp[:m]...)
		buf = append(buf, field...)
	}
	for _, p := range s {
		put(p.Key)
		put(p.Value)
	}
	return string(buf)
}

// Get returns the value for key (binary search — Set is sorted by key).
func (s Set) Get(key string) (string, bool) {
	i := sort.Search(len(s), func(i int) bool { return s[i].Key >= key })
	if i < len(s) && s[i].Key == key {
		return s[i].Value, true
	}
	return "", false
}
