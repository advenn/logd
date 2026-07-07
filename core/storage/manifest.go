package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// SegmentState records whether a segment is accepting appends or sealed.
type SegmentState string

const (
	SegmentActive SegmentState = "active"
	SegmentSealed SegmentState = "sealed"
)

// FieldSchema describes one indexed field a segment was built with. It is the
// forward-declared, JSON-serializable shape of the per-segment "indexed-field
// schema" the design requires (§5, §7.3): the query planner will consult a
// segment's own schema — never the live config — to decide index-vs-scan per
// segment, so that a config change only affects future segments. Extraction and
// the typed-range index arrive in Phase 3; for now this exists so the manifest
// FORMAT already carries it and adding real schemas later is not a format break.
type FieldSchema struct {
	Name string `json:"name"`
	Type string `json:"type"` // "int" | "float" | "str" | "uuid" (design §6.3); unused in Phase 2
}

// SegmentMeta describes a segment file, its time bounds, and the schema it was
// built with.
type SegmentMeta struct {
	ID      string        `json:"id"` // ULID: globally unique, sortable, no coordination (§10)
	MinTS   int64         `json:"min_ts"`
	MaxTS   int64         `json:"max_ts"`
	Path    string        `json:"path"`
	State   SegmentState  `json:"state"`
	Schema  []FieldSchema `json:"schema"`  // fields with a .tidx for this segment
	Records uint64        `json:"records"` // number of records (for the pushdown cost guard)
	// CappedKeys are label keys whose distinct-value cap was breached in this segment
	// (§6.2): their over-cap values are in Extra but not the label index, so the planner
	// must SCAN these keys for this segment rather than push them (else a false negative).
	CappedKeys []string `json:"capped_keys,omitempty"`
	// Indexed distinguishes a fully-indexed segment (true — the planner may trust
	// Schema and prune a field absent from it) from a scan-only segment (false — e.g.
	// crash-recovered; its records may hold values that were never indexed, so the
	// planner must SCAN it and never prune). Without this flag an all-zero-match
	// indexed segment and a recovered scan-only segment are indistinguishable.
	Indexed bool `json:"indexed"`
}

// Manifest is the per-shard catalog of segments, held in memory and persisted to
// manifest.json. Safe for concurrent use: the query path reads it while the writer
// mutates it.
type Manifest struct {
	mu       sync.RWMutex
	segments map[string]*SegmentMeta
	byMinTS  []*SegmentMeta // kept sorted by MinTS for time-range filtering
}

// NewManifest returns an empty manifest.
func NewManifest() *Manifest {
	return &Manifest{segments: make(map[string]*SegmentMeta)}
}

// LoadManifest reads dir/manifest.json, or returns a fresh manifest if the file is
// absent (first run, or a crash before the first seal — recovery re-derives the
// active segment's bounds separately).
func LoadManifest(dir string) (*Manifest, error) {
	m := NewManifest()
	data, err := os.ReadFile(dir + "/manifest.json")
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	var entries []*SegmentMeta
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	for _, e := range entries {
		m.segments[e.ID] = e
		m.byMinTS = append(m.byMinTS, e)
	}
	m.sortLocked()
	return m, nil
}

// Add registers a new segment, assigning it a fresh ULID (returned in meta.ID) under the
// lock. The caller uses meta.ID for naming, so segment naming never races with concurrent
// readers, and the ULID is globally unique so it can't collide across shards (§10).
func (m *Manifest) Add(meta *SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta.ID = newULID()
	m.segments[meta.ID] = meta
	m.byMinTS = append(m.byMinTS, meta)
	m.sortLocked()
}

// Update refreshes an existing segment's metadata and re-sorts.
func (m *Manifest) Update(meta *SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.segments[meta.ID] = meta
	for i, s := range m.byMinTS {
		if s.ID == meta.ID {
			m.byMinTS[i] = meta
			break
		}
	}
	m.sortLocked()
}

// SetBounds updates a segment's time bounds in place (used by the writer to publish
// the active segment's MinTS/MaxTS as pages flush, so recent data is visible to
// time-range queries before the segment is sealed).
func (m *Manifest) SetBounds(id string, minTS, maxTS int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		s.MinTS, s.MaxTS = minTS, maxTS
		m.sortLocked()
	}
}

// SealSegment finalizes a segment's bounds, records the schema it was actually
// indexed with, and flips it to sealed — all under the write lock. Callers must NOT
// mutate SegmentMeta fields directly: doing so races concurrent readers (Filter/Save)
// that read those fields. The schema is the set of fields that have a .tidx for this
// segment; an empty schema means "scan this segment" (design §7.3), which is how a
// crash-recovered segment (whose in-RAM index was incomplete) is marked.
func (m *Manifest) SealSegment(id string, minTS, maxTS int64, schema []FieldSchema, indexed bool, records uint64, cappedKeys []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		s.MinTS, s.MaxTS, s.State, s.Schema, s.Indexed, s.Records, s.CappedKeys = minTS, maxTS, SegmentSealed, schema, indexed, records, cappedKeys
		m.sortLocked()
	}
}

// Remove deletes a segment from the manifest (used by retention). Returns the removed
// meta (nil if unknown) so the caller can delete its files.
func (m *Manifest) Remove(id string) *SegmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.segments[id]
	if !ok {
		return nil
	}
	delete(m.segments, id)
	for i, x := range m.byMinTS {
		if x.ID == id {
			m.byMinTS = append(m.byMinTS[:i], m.byMinTS[i+1:]...)
			break
		}
	}
	return s
}

// SealedBefore returns sealed segments whose entire data is older than cutoff (MaxTS <
// cutoff) — the retention-expired set. The active segment is never included.
func (m *Manifest) SealedBefore(cutoff int64) []*SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*SegmentMeta
	for _, s := range m.byMinTS {
		if s.State == SegmentSealed && s.MaxTS < cutoff {
			out = append(out, s)
		}
	}
	return out
}

// Filter returns segments whose [MinTS, MaxTS] overlaps [minTS, maxTS].
func (m *Manifest) Filter(minTS, maxTS int64) []*SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*SegmentMeta
	for _, s := range m.byMinTS {
		if s.MaxTS >= minTS && s.MinTS <= maxTS {
			out = append(out, s)
		}
	}
	return out
}

// All returns every segment, MinTS-sorted (used by the Phase-2 read-back helper).
func (m *Manifest) All() []*SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*SegmentMeta, len(m.byMinTS))
	copy(out, m.byMinTS)
	return out
}

// Active returns the currently active segment, if any.
func (m *Manifest) Active() *SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.segments {
		if s.State == SegmentActive {
			return s
		}
	}
	return nil
}

// Get returns a segment by ID (nil if unknown).
func (m *Manifest) Get(id string) *SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.segments[id]
}

// Save persists the manifest to dir/manifest.json atomically: write a temp file,
// fsync it, rename over the real file, then fsync the directory so the rename
// itself is durable (a rename can otherwise be lost on crash even though the temp
// file's bytes are on disk). This is the atomic-seal guarantee of §7.5.
func (m *Manifest) Save(dir string) error {
	// Marshal while holding the read lock so a concurrent writer cannot mutate a
	// *SegmentMeta's fields mid-serialization (a torn read). Save is not on the hot
	// path (seal/rotate/recovery only), so holding the lock across marshal is fine.
	m.mu.RLock()
	data, err := json.MarshalIndent(m.byMinTS, "", "  ")
	m.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("serializing manifest: %w", err)
	}

	tmp := dir + "/manifest.json.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("creating manifest tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("writing manifest tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("syncing manifest tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, dir+"/manifest.json"); err != nil {
		return fmt.Errorf("renaming manifest: %w", err)
	}
	return fsyncDir(dir)
}

// sortLocked keeps byMinTS ordered by MinTS. Caller holds m.mu.
func (m *Manifest) sortLocked() {
	sort.Slice(m.byMinTS, func(i, j int) bool { return m.byMinTS[i].MinTS < m.byMinTS[j].MinTS })
}

// fsyncDir flushes a directory entry (e.g. a rename) to disk. Opening a directory
// read-only and syncing it is the portable POSIX way to make a rename durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}
