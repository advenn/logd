package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// SegmentState indicates whether a segment is accepting writes or sealed.
type SegmentState string

const (
	SegmentActive SegmentState = "active"
	SegmentSealed SegmentState = "sealed"
)

// SegmentMeta describes a segment file and its time bounds.
type SegmentMeta struct {
	ID    uint64       `json:"id"`
	MinTS int64        `json:"min_ts"`
	MaxTS int64        `json:"max_ts"`
	Path  string       `json:"path"`
	State SegmentState `json:"state"`
}

// Manifest tracks all segments in memory and persists to manifest.json.
// Safe for concurrent use.
type Manifest struct {
	mu       sync.RWMutex
	segments map[uint64]*SegmentMeta
	byMinTS  []*SegmentMeta // sorted by MinTS for binary search
	nextID   uint64
}

// NewManifest creates an empty manifest.
func NewManifest() *Manifest {
	return &Manifest{
		segments: make(map[uint64]*SegmentMeta),
	}
}

// LoadManifest reads manifest.json from a directory, or returns a fresh
// manifest if the file doesn't exist.
func LoadManifest(dirPath string) (*Manifest, error) {
	m := NewManifest()
	path := dirPath + "/manifest.json"

	data, err := os.ReadFile(path)
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
		if e.ID >= m.nextID {
			m.nextID = e.ID + 1
		}
	}
	sort.Slice(m.byMinTS, func(i, j int) bool {
		return m.byMinTS[i].MinTS < m.byMinTS[j].MinTS
	})
	return m, nil
}

// Add registers a new segment and assigns it an ID.
func (m *Manifest) Add(meta *SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	meta.ID = m.nextID
	m.segments[meta.ID] = meta
	m.byMinTS = append(m.byMinTS, meta)
	sort.Slice(m.byMinTS, func(i, j int) bool {
		return m.byMinTS[i].MinTS < m.byMinTS[j].MinTS
	})
}

// Update refreshes the metadata for an existing segment.
func (m *Manifest) Update(meta *SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.segments[meta.ID] = meta
	// Re-sort byMinTS.
	for i, s := range m.byMinTS {
		if s.ID == meta.ID {
			m.byMinTS[i] = meta
			break
		}
	}
	sort.Slice(m.byMinTS, func(i, j int) bool {
		return m.byMinTS[i].MinTS < m.byMinTS[j].MinTS
	})
}

// Filter returns segments whose time range overlaps [minTS, maxTS].
func (m *Manifest) Filter(minTS, maxTS int64) []*SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*SegmentMeta
	for _, s := range m.byMinTS {
		if s.MaxTS >= minTS && s.MinTS <= maxTS {
			result = append(result, s)
		}
	}
	return result
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

// Seal marks a segment as sealed (read-only).
func (m *Manifest) Seal(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.segments[id]; ok {
		s.State = SegmentSealed
	}
}

// Get returns a segment by ID.
func (m *Manifest) Get(id uint64) *SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.segments[id]
}

// Save persists the manifest to dirPath/manifest.json.
func (m *Manifest) Save(dirPath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []*SegmentMeta
	for _, s := range m.byMinTS {
		entries = append(entries, s)
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("serializing manifest: %w", err)
	}

	tmpPath := dirPath + "/manifest.json.tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}
	return os.Rename(tmpPath, dirPath+"/manifest.json")
}
