package index

import (
	"fmt"
	"strings"
)

// Buffer accumulates a segment's (field, key, offset) triples in RAM while the
// segment is active, and flushes one sorted .tidx per field when the segment seals
// (design §7.4 step 4, §7.5). It is owned by the writer goroutine — not safe for
// concurrent use.
//
// v1 holds everything in memory; the documented bound is
// segment_size/avg_entry × matches × 24B (acceptable at ~64MB segments). The Add/Flush
// interface is shaped so a spill-to-disk implementation can be dropped in later without
// touching callers.
type Buffer struct {
	// byField maps a field name to its accumulated entries and value kind. Kind is
	// carried per field so Flush can stamp each .tidx header with the right type.
	byField map[string]*fieldEntries
}

type fieldEntries struct {
	kind    ValueKind
	entries []Entry
}

// NewBuffer creates an empty buffer that knows the schema (field → kind) it will
// index. Fields not in the schema are ignored by Add (a value for an undeclared field
// is simply not indexed).
func NewBuffer(schema []FieldType) *Buffer {
	b := &Buffer{byField: make(map[string]*fieldEntries, len(schema))}
	for _, f := range schema {
		b.byField[f.Name] = &fieldEntries{kind: f.Kind}
	}
	return b
}

// EntryCount returns the total number of buffered (key, offset) entries across all
// fields — the writer uses it (× the per-entry size) to estimate the buffer's RAM
// footprint for the memory budget.
func (b *Buffer) EntryCount() int {
	n := 0
	for _, fe := range b.byField {
		n += len(fe.entries)
	}
	return n
}

// EntryBytes is the in-memory size of one buffered entry (a 16-byte key + an 8-byte
// offset), used to estimate the buffer's footprint.
const EntryBytes = KeySize + 8

// Add records that a value with the given key was extracted from the record at the
// given segment-relative offset. Unknown fields are ignored.
func (b *Buffer) Add(field string, key [KeySize]byte, offset uint64) {
	fe, ok := b.byField[field]
	if !ok {
		return
	}
	fe.entries = append(fe.entries, Entry{Key: key, Offset: offset})
}

// Flush writes one <segBase>.<field>.tidx per field that accumulated any entries and
// returns the set of fields it actually wrote — that becomes the segment's recorded
// schema, so the planner only ever expects a .tidx that exists. segBase is the
// segment's path without its extension (so index files sit next to the segment and are
// deleted with it — retention = delete the segment's files). Fields with no entries are
// skipped: a query on them finds no .tidx and scans, which is correct.
func (b *Buffer) Flush(segBase string, segmentID uint64) ([]FieldType, error) {
	var written []FieldType
	for field, fe := range b.byField {
		if len(fe.entries) == 0 {
			continue
		}
		path := fmt.Sprintf("%s.%s.tidx", segBase, sanitizeField(field))
		if err := WriteTIDX(path, fe.kind, segmentID, fe.entries); err != nil {
			return written, fmt.Errorf("writing %s: %w", path, err)
		}
		written = append(written, FieldType{Name: field, Kind: fe.kind})
	}
	return written, nil
}

// TidxPath returns the .tidx path for a segment base and field (also used by readers).
func TidxPath(segBase, field string) string {
	return fmt.Sprintf("%s.%s.tidx", segBase, sanitizeField(field))
}

// sanitizeField maps a field name to a filesystem-safe, INJECTIVE filename component by
// percent-encoding every byte outside [A-Za-z0-9_-]. Injectivity is load-bearing: an
// earlier version folded '.', '/', '\' all to '_', so two distinct fields (e.g. the
// literals "a.b" and "a_b") collided onto one .tidx path and clobbered each other,
// while both stayed in the schema — the planner would then push against the wrong index
// and drop true matches. Percent-encoding (with '%' itself encoded) can never collide.
func sanitizeField(field string) string {
	var b strings.Builder
	for i := 0; i < len(field); i++ {
		c := field[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
