package storage

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
)

// activeIndex is the active segment's in-memory index as queries reach it: the writer's
// typed-range buffer, label builder and cardinality cap for ONE segment, and how much of
// that segment is on disk. The writer publishes a new one when it starts a segment and
// withdraws it once the segment's seal is committed (see publishActive), so a query that
// holds one holds a single segment's state, never a mix of two.
//
// The fields are set once, at publication, except the structures they point to — which the
// writer keeps appending to under mu — and the readability boundary below.
type activeIndex struct {
	segID  string
	mu     *sync.Mutex // the writer's liveMu: serializes its appends with a query's reads
	buffer *index.Buffer
	labels *label.Builder
	card   *label.Cardinality
	// complete is false for a crash-recovered segment whose pre-crash records were never
	// re-indexed (no Reindex hook): its index misses them, and a query must scan it.
	complete          bool
	labelKeys         []string // the allowlist its label sets were derived with (sorted)
	labelKeysRecorded bool

	// pages is the readability boundary: the active segment's next page number, stored after
	// each page write has returned, so every page below it is in the file in full. Records
	// past it — in the page still being assembled in RAM, or in one being written — are
	// already in the buffers, but not yet anywhere a read can reach them. records counts the
	// records in those pages.
	pages   atomic.Uint64
	records atomic.Uint64
}

// ActiveIndex is a read view of the active segment's in-memory index, for one query.
//
// It covers exactly the records in the pages that were on disk when it was taken: the same
// records a scan of the segment opened at that moment reads. Lookups never return an offset
// at or past that boundary, so every candidate can be fetched; they never miss a record
// below it, so pushing a predicate into the view is as safe as pushing it into a sealed
// segment's .tidx. A view stays valid after the segment seals — it reads the writer's
// buffers, which a seal leaves as they were — but a query should not take one for a segment
// it no longer sees as active (see Writer.ActiveIndex).
type ActiveIndex struct {
	SegmentID string
	// Pages and Records are the data pages and records the view covers, for the cost guard.
	// The segment is raw until it seals, so its unit of I/O is the page.
	Pages, Records uint64
	// Schema is every typed field the writer indexes, with the pattern it extracts it with.
	// Unlike a sealed segment's, it includes fields no record has produced yet: the buffer
	// holds every value of every record given to it, so an empty field is absent from the
	// data, not unindexed.
	Schema []FieldSchema
	// CappedKeys are the label keys whose value cap was breached in this segment so far.
	CappedKeys []string
	// LabelKeys is the allowlist the segment's label sets were derived with, when recorded.
	LabelKeys         []string
	LabelKeysRecorded bool

	src    *activeIndex
	below  uint64 // byte offset: records at or past it are not on disk
	fields map[string]index.FieldView
}

// Field returns the typed-range lookup for an indexed field.
func (v *ActiveIndex) Field(name string) (index.Lookup, bool) {
	f, ok := v.fields[name]
	return f, ok
}

// LabelOffsets returns the offsets of the viewed records whose label set carries
// key=value, ascending: what a sealed segment's .lidx answers.
func (v *ActiveIndex) LabelOffsets(key, value string) []uint64 {
	if v.src.labels == nil {
		return nil
	}
	v.src.mu.Lock()
	lists := v.src.labels.Postings(key, value)
	v.src.mu.Unlock()
	return label.MergePostings(lists, v.below)
}

// LabelCovers reports whether every label stream of the segment carries key=value. It reads
// the streams interned so far, which can include some whose records are beyond the view;
// that only makes the answer more conservative (a later stream without the pair makes it
// false), never wrong for the viewed records.
func (v *ActiveIndex) LabelCovers(key, value string) bool {
	if v.src.labels == nil {
		return false
	}
	v.src.mu.Lock()
	defer v.src.mu.Unlock()
	return v.src.labels.Covers(key, value)
}

// HasLabelKey reports whether any of the segment's label streams carries key.
func (v *ActiveIndex) HasLabelKey(key string) bool {
	if v.src.labels == nil {
		return false
	}
	v.src.mu.Lock()
	defer v.src.mu.Unlock()
	return v.src.labels.HasKey(key)
}

// ActiveIndex returns a read view of the active segment's in-memory index, if segID is
// still the active segment and its index is complete; false otherwise — the segment has
// sealed since the caller saw it (re-read the manifest), or it is a recovered segment
// indexed only from the restart on (scan it).
//
// The boundary is read BEFORE the entries, and the order is what makes the view complete.
// The writer appends a record's entries when it places the record in the page in RAM, and
// stores the boundary only after that page's write has returned. So once a page is below a
// boundary this load observes, its records' appends happened before the store, and the lock
// taken after the load sees all of them. Read the other way round, a page written between
// the two reads would be inside the boundary with its records missing from the view: a
// false negative, the one thing an index may never produce.
func (w *Writer) ActiveIndex(segID string) (*ActiveIndex, bool) {
	a := w.active.Load()
	if a == nil || a.segID != segID || !a.complete {
		return nil, false
	}
	pages := a.pages.Load()
	records := a.records.Load()
	below := uint64(PageOffset(pages))
	if testHookViewAfterBoundary != nil {
		testHookViewAfterBoundary()
	}
	a.mu.Lock()
	fields := a.buffer.View(below)
	a.mu.Unlock()

	v := &ActiveIndex{
		SegmentID:         segID,
		Pages:             pages - 1, // page 0 is the placeholder
		Records:           records,
		CappedKeys:        a.card.CappedKeys(),
		LabelKeys:         a.labelKeys,
		LabelKeysRecorded: a.labelKeysRecorded,
		src:               a,
		below:             below,
		fields:            fields,
	}
	types := make([]index.FieldType, 0, len(fields))
	for name, f := range fields {
		types = append(types, index.FieldType{Name: name, Kind: f.Kind, Pattern: f.Pattern})
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })
	v.Schema = schemaToManifest(types)
	return v, true
}

// testHookViewAfterBoundary, when set, runs between ActiveIndex's two reads, so a test can
// write a page exactly where the read order matters. Always nil outside tests.
var testHookViewAfterBoundary func()

// publishActive makes the current segment's in-memory index visible to queries. Called once
// a segment's buffers exist — at Start, and by rotateSegment before the manifest names the
// new segment, so a query that finds the segment also finds its index.
func (w *Writer) publishActive() {
	a := &activeIndex{
		segID:    w.currSeg.ID,
		mu:       &w.liveMu,
		buffer:   w.buffer,
		labels:   w.labelBuf,
		card:     w.card,
		complete: !w.indexIncomplete && w.buffer != nil,
	}
	if w.opts.LabelKeys != nil {
		a.labelKeys = append([]string{}, w.opts.LabelKeys...)
		sort.Strings(a.labelKeys)
		a.labelKeysRecorded = true
	}
	a.records.Store(w.segRecords) // a recovered segment's records are all on disk
	a.pages.Store(w.currSeg.PageNum)
	w.active.Store(a)
}

// advanceActive moves the readability boundary past the page just written. Records first,
// pages last: a query loads pages first, so it never pairs a boundary with a smaller count
// of the records below it.
func (w *Writer) advanceActive() {
	if a := w.active.Load(); a != nil && a.segID == w.currSeg.ID {
		a.records.Store(w.segRecords)
		a.pages.Store(w.currSeg.PageNum)
	}
}
