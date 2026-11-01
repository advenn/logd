package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	// Pattern is the template pattern (or literal) the field's .tidx was built from. The
	// planner pushes a predicate into this index only if the live config defines the field
	// with the same pattern. Empty for segments sealed before patterns were recorded, which
	// the planner therefore scans for typed predicates.
	Pattern string `json:"pattern,omitempty"`
}

// SegmentMeta describes a segment file, its time bounds, and the schema it was
// built with.
type SegmentMeta struct {
	ID    string `json:"id"` // ULID: globally unique, sortable, no coordination (§10)
	MinTS int64  `json:"min_ts"`
	MaxTS int64  `json:"max_ts"`
	// Path is the segment's .log path. In memory it is always resolved against the shard
	// directory the manifest was loaded from (see SegmentPath); on disk it is stored relative
	// to that directory, so a shard folder can be moved or mounted elsewhere.
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
	// LabelKeys is the label allowlist in force while this segment was written, i.e. the
	// keys its label index is complete for. The planner pushes a label predicate only for a
	// key in this list: a key allowlisted after the segment was sealed is absent from its
	// label index, and pushing it would silently return nothing. LabelKeysRecorded
	// distinguishes an empty allowlist from a segment that did not record one (sealed before
	// this field existed, or written without Options.LabelKeys); for those the planner falls
	// back to the keys actually present in the segment's label index.
	LabelKeys         []string `json:"label_keys,omitempty"`
	LabelKeysRecorded bool     `json:"label_keys_recorded,omitempty"`
}

// SealInfo is everything the writer knows about a segment when it seals it.
type SealInfo struct {
	MinTS, MaxTS      int64
	Schema            []FieldSchema
	Indexed           bool
	Records           uint64
	CappedKeys        []string
	LabelKeys         []string
	LabelKeysRecorded bool
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
		e.Path = resolveSegmentPath(dir, e)
		m.segments[e.ID] = e
		m.byMinTS = append(m.byMinTS, e)
	}
	m.sortLocked()
	return m, nil
}

// SegmentPath returns the .log path of a segment inside a shard directory. Segment file
// names are derived from the segment ID alone, so the path never has to be trusted from the
// manifest.
func SegmentPath(dir, id string) string {
	return filepath.Join(dir, "segments", "seg-"+id+".log")
}

// resolveSegmentPath finds a loaded segment's files relative to the directory the manifest
// was loaded from.
//
// Manifests written before this existed stored the full path built from the configured
// data_dir. Trusting that string meant a moved or re-mounted data directory made every
// sealed segment "not found", which the query path deliberately reads as a retention race —
// an empty result with no error, so all historical data silently vanished. The ID-derived
// path is therefore tried first; the stored path is only a fallback for files that exist
// under some other name.
func resolveSegmentPath(dir string, e *SegmentMeta) string {
	derived := SegmentPath(dir, e.ID)
	if segmentFilesExist(derived) {
		return derived
	}
	if e.Path != "" && !filepath.IsAbs(e.Path) {
		if joined := filepath.Join(dir, e.Path); segmentFilesExist(joined) {
			return joined
		}
	}
	if e.Path != "" && segmentFilesExist(e.Path) {
		return e.Path
	}
	return derived
}

// segmentFilesExist reports whether a segment is present, raw or compressed.
func segmentFilesExist(logPath string) bool {
	if _, err := os.Stat(logPath); err == nil {
		return true
	}
	_, err := os.Stat(SegzPath(logPath))
	return err == nil
}

// relativeSegmentPath is the form a segment path is persisted in: relative to the shard
// directory when the segment lives inside it, unchanged otherwise.
func relativeSegmentPath(dir, path string) string {
	rel, err := filepath.Rel(dir, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return path
	}
	return rel
}

// Add registers a new segment. The writer generates the ID (a ULID, globally unique so it
// can't collide across shards, §10) and creates the segment's files under it before
// registering it; a meta without an ID is assigned a fresh one here (returned in meta.ID).
func (m *Manifest) Add(meta *SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if meta.ID == "" {
		meta.ID = newULID()
	}
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
// indexed with, and flips it to sealed, persisting the result to dir/manifest.json.
// Callers must NOT mutate SegmentMeta fields directly: doing so races concurrent
// readers (Filter/Save) that read those fields. The schema is the set of fields that
// have a .tidx for this segment; an empty schema means "scan this segment" (design
// §7.3), which is how a crash-recovered segment (whose in-RAM index was incomplete) is
// marked.
//
// The seal is saved FIRST and applied in memory only once that succeeded. Applied first,
// a failed save left the live manifest saying "sealed, indexed" while the writer, whose
// seal had failed, kept appending to the segment: the planner then pushed predicates into
// a .tidx that lacked every later record, and returned fewer rows than a scan. Nothing
// else mutates the manifest between the save and the commit — every mutation runs on the
// writer goroutine, which is the one sealing — so the saved file is exactly the state
// committed after it.
func (m *Manifest) SealSegment(dir, id string, info SealInfo) error {
	if err := m.save(dir, id, &info); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		info.applyTo(s)
		m.sortLocked()
	}
	return nil
}

// applyTo marks s sealed with the bounds and index facts in info.
func (info *SealInfo) applyTo(s *SegmentMeta) {
	s.State = SegmentSealed
	s.MinTS, s.MaxTS = info.MinTS, info.MaxTS
	s.Schema, s.Indexed, s.Records, s.CappedKeys = info.Schema, info.Indexed, info.Records, info.CappedKeys
	s.LabelKeys, s.LabelKeysRecorded = info.LabelKeys, info.LabelKeysRecorded
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
	return m.save(dir, "", nil)
}

// save is Save, except that when seal is non-nil the segment sealID is written as seal
// describes it rather than as it is in memory (see SealSegment).
func (m *Manifest) save(dir, sealID string, seal *SealInfo) error {
	// Marshal while holding the read lock so a concurrent writer cannot mutate a
	// *SegmentMeta's fields mid-serialization (a torn read). Save is not on the hot
	// path (seal/rotate/recovery only), so holding the lock across marshal is fine.
	// Paths are written relative to dir (copies, so the in-memory entries keep their
	// resolved paths).
	m.mu.RLock()
	out := make([]SegmentMeta, len(m.byMinTS))
	for i, s := range m.byMinTS {
		out[i] = *s
		if seal != nil && s.ID == sealID {
			seal.applyTo(&out[i])
		}
		out[i].Path = relativeSegmentPath(dir, s.Path)
	}
	data, err := json.MarshalIndent(out, "", "  ")
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
