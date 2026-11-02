package query

import (
	"strings"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/storage"
)

// segmentIndex is what the planner reads one segment's indexes through: a sealed segment's
// sidecar files, or the active segment's in-memory index. Both answer the same questions
// with the same meaning, so plan, the lookups it builds and the re-verification rules are
// one piece of code for either, and a segment answers the same before and after it seals.
type segmentIndex interface {
	// typed returns the typed-range lookup of a field the segment's schema lists; false if
	// it cannot be read, which degrades the whole segment to a scan.
	typed(field string) (index.Lookup, bool)
	// labels returns the segment's label lookup; false likewise.
	labels() (labelLookup, bool)
}

// labelLookup is the query side of a label index: *label.Reader for a sealed segment.
type labelLookup interface {
	LabelOffsets(key, value string) []uint64
	HasKey(key string) bool
}

// sidecars reads a sealed segment's .tidx and .lidx files through the reader cache.
type sidecars struct {
	cache *readerCache
	base  string // the segment's path without ".log"
}

func (e *Engine) sidecarsOf(seg *storage.SegmentMeta) sidecars {
	return sidecars{cache: e.cache, base: strings.TrimSuffix(seg.Path, ".log")}
}

func (s sidecars) typed(field string) (index.Lookup, bool) {
	r, err := s.cache.tidx(index.TidxPath(s.base, field))
	if err != nil {
		return nil, false
	}
	return r, true
}

func (s sidecars) labels() (labelLookup, bool) {
	r, err := s.cache.lidx(label.IndexPath(s.base))
	if err != nil {
		return nil, false
	}
	return r, true
}

// inMemory reads the active segment's index from its writer, through a view taken for this
// query (see storage.ActiveIndex: it covers exactly the pages on disk when it was taken).
type inMemory struct{ v *storage.ActiveIndex }

func (m inMemory) typed(field string) (index.Lookup, bool) { return m.v.Field(field) }
func (m inMemory) labels() (labelLookup, bool)             { return activeLabels{m.v}, true }

type activeLabels struct{ v *storage.ActiveIndex }

func (a activeLabels) LabelOffsets(key, value string) []uint64 { return a.v.LabelOffsets(key, value) }
func (a activeLabels) HasKey(key string) bool                  { return a.v.HasLabelKey(key) }

// indexFor returns the segment as the planner should see it, how to read its indexes, and
// whether that is the active segment's in-memory index.
//
// A sealed segment plans against its manifest entry and sidecars, as always. An active one
// plans against a view of its writer's in-memory index, with the view's facts standing in
// for the ones the manifest records only at seal (activeMeta). The two can disagree about
// which segment is active: Manifest.Filter handed out its entry earlier, and the writer may
// have sealed it since. The view is tied to the segment ID, so then there is none, and the
// manifest is read again: a segment that has sealed is planned like any sealed segment, its
// sidecars being complete before the manifest says so. One still active but without a view
// — a crash-recovered segment whose index misses its pre-crash records, or a shard that
// keeps no in-memory index — stays not indexed, and is scanned.
func (e *Engine) indexFor(sh Shard, seg *storage.SegmentMeta) (*storage.SegmentMeta, segmentIndex, bool) {
	if seg.State != storage.SegmentActive {
		return seg, e.sidecarsOf(seg), false
	}
	if v, ok := sh.ActiveIndex(seg.ID); ok {
		return activeMeta(seg, v), inMemory{v}, true
	}
	if fresh := sh.Manifest().Get(seg.ID); fresh != nil && fresh.State == storage.SegmentSealed {
		return fresh, e.sidecarsOf(fresh), false
	}
	return seg, e.sidecarsOf(seg), false // Indexed is false: plan pushes nothing
}

// activeMeta is an active segment's manifest entry with the facts a seal would record taken
// from its in-memory index instead: the schema and pattern of every field it indexes, the
// label keys it was built for and the ones that hit their cap, and its layout so far. plan
// then applies every identity rule a sealed segment gets — the pattern a field was built
// with must be the one the query engine defines it with (F-03), a label key must be one the
// segment was indexed for and not capped (F-02) — without a rule of its own.
func activeMeta(seg *storage.SegmentMeta, v *storage.ActiveIndex) *storage.SegmentMeta {
	m := *seg
	m.Indexed = true
	m.Schema = v.Schema
	m.CappedKeys = v.CappedKeys
	m.LabelKeys, m.LabelKeysRecorded = v.LabelKeys, v.LabelKeysRecorded
	m.Records, m.Pages, m.BlockPages = v.Records, v.Pages, 0 // raw until it seals
	return &m
}
