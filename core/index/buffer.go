package index

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

// Buffer accumulates a segment's (field, key, offset) triples in RAM while the
// segment is active, and flushes one sorted .tidx per field when the segment seals
// (design §7.4 step 4, §7.5).
//
// One goroutine writes it, the segment's writer, and queries read it while it does, through
// View: that is how the planner pushes predicates into the active segment (design §9.2).
// Buffer takes no lock of its own. Every Add must be serialized with every View by the
// caller's lock — storage.Writer's liveMu, which the writer already holds once per record
// for the rest of the active segment's live state, so a lock in here would be a second one
// per record on the hot path. Only the writer calls the other methods, and they read what
// only the writer writes, so they need no lock.
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
	pattern string // carried through to the written schema (see FieldType.Pattern)
	entries []Entry
}

// NewBuffer creates an empty buffer that knows the schema (field → kind) it will
// index. Fields not in the schema are ignored by Add (a value for an undeclared field
// is simply not indexed).
func NewBuffer(schema []FieldType) *Buffer {
	b := &Buffer{byField: make(map[string]*fieldEntries, len(schema))}
	for _, f := range schema {
		b.byField[f.Name] = &fieldEntries{kind: f.Kind, pattern: f.Pattern}
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

// AddAll is Add for every value extracted from one record.
func (b *Buffer) AddAll(keys []KeyedValue, offset uint64) {
	for _, kv := range keys {
		b.Add(kv.Field, kv.Key, offset)
	}
}

// Flush writes one .tidx (at TidxPath) per field of the schema, empty or not, and
// returns the set of fields it actually wrote — that becomes the segment's recorded
// schema, so the planner only ever expects a .tidx that exists. segBase is the
// segment's path without its extension (so index files sit next to the segment and are
// deleted with it — retention = delete the segment's files).
func (b *Buffer) Flush(segBase string, segmentID uint64) ([]FieldType, error) {
	var written []FieldType
	for field, fe := range b.byField {
		// A field with no entries still gets its (empty) .tidx. Skipping it left the field out
		// of the segment's schema, so a typed predicate on it found "not indexed here" and
		// scanned the whole segment — to return nothing, since no record of the segment
		// yielded a value for it. On the Loghub HDFS replay the last segment of 70,668 lines
		// held no `size` value at all and was scanned by every size query. Written, the empty
		// index answers "no candidates" and the segment is pruned; the scan, re-extracting with
		// the same pattern (the planner checks it is the same), would find none either.
		path := TidxPath(segBase, field)
		// WriteTIDX sorts what it is given, in place, and a query may be reading these very
		// entries through a FieldView taken before the seal (see View). Sorting them under
		// it would hand that query reordered and torn entries, so the file is written from a
		// copy and the buffer keeps its append order.
		sorted := append([]Entry(nil), fe.entries...)
		if err := WriteTIDX(path, fe.kind, segmentID, sorted); err != nil {
			return written, fmt.Errorf("writing %s: %w", path, err)
		}
		written = append(written, FieldType{Name: field, Kind: fe.kind, Pattern: fe.pattern})
	}
	return written, nil
}

// nameMax is the longest file name (one path component, in bytes) the filesystems logd
// runs on accept: NAME_MAX on ext4, xfs, btrfs, tmpfs and overlayfs.
const nameMax = 255

// maxTidxName is the longest .tidx file name kept in its readable form: nameMax less the
// ".tmp" suffix writeFileAtomic adds while the file is written. A readable name within it is
// exactly a name that could be created before hashed names existed, so every sidecar already
// on disk keeps the path readers compute for it.
const maxTidxName = nameMax - len(".tmp")

// hashedFieldMarker starts the field component of a hashed .tidx name. sanitizeField never
// emits '~', so a hashed name can never equal the readable name of another field.
const hashedFieldMarker = "~"

// TidxPath returns the .tidx path for a segment base and field. The writer's Flush and every
// reader compute it here, so the two always agree.
//
// The readable form is <segBase>.<sanitized field>.tidx. Percent-encoding triples every byte
// outside [A-Za-z0-9_-], so a literal of ~70 bytes of Cyrillic already exceeds nameMax; the
// file could not be created, the seal failed, and it failed again on every retry. A name that
// would not fit uses <segBase>.~<hex sha256 of the field>.tidx instead, which is 101 bytes for
// a seg-<ULID> base whatever the field. The field name itself is recorded in the manifest
// schema, so nothing needs to recover it from the file name.
func TidxPath(segBase, field string) string {
	p := segBase + "." + sanitizeField(field) + ".tidx"
	if len(filepath.Base(p)) <= maxTidxName {
		return p
	}
	sum := sha256.Sum256([]byte(field))
	return segBase + "." + hashedFieldMarker + hex.EncodeToString(sum[:]) + ".tidx"
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
