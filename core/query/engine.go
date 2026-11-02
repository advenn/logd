package query

import (
	"errors"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// Shard is the read-side handle the engine needs from one shard — a self-contained
// data_root/<shard_id>/ folder with its own manifest, service dictionary, and live
// labels. *storage.Writer satisfies it, so the daemon fans in over its own writers
// (design §10: run §9 per shard, merge). A read-only shard handle could satisfy it too.
//
// ActiveIndex exposes the active segment's in-memory index, so the planner can push
// predicates into the unsealed segment instead of scanning it (design §9.2). A shard that
// keeps none — a read-only handle — returns false, and its active segment is scanned.
type Shard interface {
	Manifest() *storage.Manifest
	ServiceLookup() *storage.LookupTable
	LiveStreams() []label.Set
	Subscribe(match func(model.LogEntry) bool, buffer int) (<-chan model.LogEntry, func())
	ActiveIndex(segID string) (*storage.ActiveIndex, bool)
}

// Engine executes native queries across one or more shards, merging results. It reads
// each shard's live manifest (so just-flushed active-segment data is visible), uses the
// extraction engine for typed predicates, and (when a label allowlist is configured)
// pushes label equality into the per-segment label index. IMPORTANT: service resolution
// is per-shard (each shard owns its lookup.bin), so a shard's resolver is used only for
// that shard's records.
type Engine struct {
	shards    []Shard
	ex        *extract.Engine     // may be nil (no typed predicates possible)
	labelKeys map[string]struct{} // allowlisted label keys eligible for index pushdown; nil → none
	cache     *readerCache        // opened index sidecars, reused across queries
	// noCostGuard fetches every candidate set by offset, however large. Set only by tests
	// (DisableCostGuard), to time the index path where the guard would have scanned.
	noCostGuard bool
}

// defaultLimit is the row limit of a log query that does not set one.
const defaultLimit = 100

// effectiveLimit is the row limit a log query runs under. Explain uses it too, so it reports
// the access path Execute takes rather than one planned for an unset limit.
func effectiveLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

// costGuard returns the cost guard for one segment of a query that keeps limit rows.
func (e *Engine) costGuard(seg *storage.SegmentMeta, limit int) costGuard {
	if e.noCostGuard {
		limit = 0
	}
	return newCostGuard(seg, limit)
}

// NewEngine builds a single-shard query engine with typed pushdown but no label pushdown
// (label filters scan). Label filters still work — they just aren't accelerated.
func NewEngine(w *storage.Writer, ex *extract.Engine) *Engine {
	return NewEngineWithLabels(w, ex, nil)
}

// NewEngineWithLabels builds a single-shard query engine that also pushes label equality
// on the allowlisted keys into the label index. labelKeys must match the ingester's.
func NewEngineWithLabels(w *storage.Writer, ex *extract.Engine, labelKeys []string) *Engine {
	return NewShardedEngine([]Shard{w}, ex, labelKeys)
}

// NewShardedEngine fans queries out across shards and merges (design §10). labelKeys must
// match the ingester's allowlist.
// SetIndexCacheBytes sizes the opened-sidecar cache (0 disables it). The daemon calls this
// from index_cache_mb; the default suits tests and embedded use.
func (e *Engine) SetIndexCacheBytes(n int64) { e.cache = newReaderCache(n) }

// ForgetSegment drops cached readers for a segment that has been deleted, so retention
// does not leave the cache serving — and holding the memory of — files that are gone.
func (e *Engine) ForgetSegment(segPath string) {
	e.cache.Forget(strings.TrimSuffix(segPath, ".log"))
}

func NewShardedEngine(shards []Shard, ex *extract.Engine, labelKeys []string) *Engine {
	var set map[string]struct{}
	if len(labelKeys) > 0 {
		set = make(map[string]struct{}, len(labelKeys))
		for _, k := range labelKeys {
			set[k] = struct{}{}
		}
	}
	return &Engine{shards: shards, ex: ex, labelKeys: set, cache: newReaderCache(defaultIndexCacheBytes)}
}

// Labels returns the distinct allowlisted label keys present across the active and sealed
// segments of all shards, sorted: the active segments' live streams and every sealed
// segment's .lidx.
func (e *Engine) Labels() []string {
	seen := map[string]struct{}{}
	// Read the ACTIVE segments first, THEN the sealed .lidx files. If a rotate seals an
	// active segment between these two reads, the just-sealed segment's labels are
	// captured here (from LiveStreams, before the seal) — reading it the other way round
	// leaves a window where a stream is in neither snapshot.
	e.forEachLiveStream(func(s label.Set) {
		for _, p := range s {
			seen[p.Key] = struct{}{}
		}
	})
	e.forEachLabelIndex(func(r *label.Reader) {
		for _, k := range r.Keys() {
			seen[k] = struct{}{}
		}
	})
	return sortedStrings(seen)
}

// LabelValues returns the distinct values of a label key across the active and sealed
// segments of all shards, sorted.
func (e *Engine) LabelValues(key string) []string {
	seen := map[string]struct{}{}
	e.forEachLiveStream(func(s label.Set) { // active segments first (see Labels)
		if v, ok := s.Get(key); ok {
			seen[v] = struct{}{}
		}
	})
	e.forEachLabelIndex(func(r *label.Reader) {
		for _, v := range r.Values(key) {
			seen[v] = struct{}{}
		}
	})
	return sortedStrings(seen)
}

// Series returns the distinct label streams (as maps) across sealed segments, for the
// Loki /series endpoint. Deduplicated across segments.
func (e *Engine) Series() []map[string]string {
	seen := map[string]struct{}{}
	var out []map[string]string
	fold := func(streams []label.Set) {
		for _, s := range streams {
			key := s.Canonical()
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			m := make(map[string]string, len(s))
			for _, p := range s {
				m[p.Key] = p.Value
			}
			out = append(out, m)
		}
	}
	e.forEachLiveStream(func(s label.Set) { fold([]label.Set{s}) }) // active first, then sealed
	e.forEachLabelIndex(func(r *label.Reader) { fold(r.Streams()) })
	return out
}

// forEachLiveStream visits every shard's active-segment label streams.
func (e *Engine) forEachLiveStream(fn func(label.Set)) {
	for _, sh := range e.shards {
		for _, s := range sh.LiveStreams() {
			fn(s)
		}
	}
}

// forEachLabelIndex visits the label index of every sealed segment across all shards.
func (e *Engine) forEachLabelIndex(fn func(*label.Reader)) {
	for _, sh := range e.shards {
		for _, seg := range sh.Manifest().All() {
			r, err := e.cache.lidx(label.IndexPath(strings.TrimSuffix(seg.Path, ".log")))
			if err != nil {
				continue // no/corrupt label index for this segment
			}
			fn(r)
		}
	}
}

func sortedStrings(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Execute runs a query with index pushdown where possible (design §9): per segment it
// either pushes pushable predicates into the .tidx and re-verifies the candidates, or
// falls back to a full scan. Results are globally sorted by event time and limited.
func (e *Engine) Execute(q Query) ([]model.LogEntry, error) { return e.execute(q, false) }

// ExecuteScan runs the same query forcing the SCAN path for every segment — the Phase-4
// oracle. Execute and ExecuteScan must return identical results for every query; that
// equality is the correctness contract of the index (and the differential test).
func (e *Engine) ExecuteScan(q Query) ([]model.LogEntry, error) { return e.execute(q, true) }

func (e *Engine) execute(q Query, forceScan bool) ([]model.LogEntry, error) {
	if err := validatePreds(q.Preds, e.ex); err != nil {
		return nil, err
	}
	start := q.Start
	end := q.End
	if end <= 0 {
		end = math.MaxInt64
	}
	limit := effectiveLimit(q.Limit)

	// A bounded top-K accumulator rather than collect-everything-then-sort. Append order
	// within a page is NOT time order (out-of-order arrivals), so the result still has to
	// be ordered — but only the best `limit` records need to be kept while doing it, and
	// knowing the worst of those lets whole segments and pages be skipped. entryLess is the
	// ordering; see collect.go for why it must be total.
	acc := newTopK(limit, q.Direction)
	if err := e.collectInto(acc, q.Preds, start, end, forceScan, q.Direction); err != nil {
		return acc.results(), err
	}
	return acc.results(), nil
}

// collect returns every record in [start, end] matching preds (no sort, no limit), using
// pushdown per segment where possible. The metric path uses this: its aggregations need
// EVERY entry in the window, so it must never be given a bounded accumulator.
func (e *Engine) collect(preds []Predicate, start, end int64, forceScan bool) ([]model.LogEntry, error) {
	acc := &sliceCollector{}
	err := e.collectInto(acc, preds, start, end, forceScan, Backward)
	return acc.results(), err
}

// collectInto drives every shard into acc. When acc is bounded it also prunes: segments are
// visited in the query's direction so the accumulator fills with the best records first,
// and a segment whose time bounds cannot beat the current cutoff is skipped outright.
func (e *Engine) collectInto(acc collector, preds []Predicate, start, end int64, forceScan bool, dir Direction) error {
	for _, sh := range e.shards {
		if err := e.collectShardInto(acc, sh, preds, start, end, forceScan, dir); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) collectShardInto(acc collector, sh Shard, preds []Predicate, start, end int64, forceScan bool, dir Direction) error {
	r := e.resolverFor(sh)
	q := Query{Preds: preds, Direction: dir}

	segs := sh.Manifest().Filter(start, end)
	// Filter returns MinTS-ascending. Out-of-order arrivals widen bounds, so MinTS order is
	// NOT MaxTS order — visiting in the wrong order would fill the accumulator with records
	// that are then all displaced, defeating the pruning. Sort by the bound that actually
	// governs the direction. Cheap: this is a handful of segments, not records.
	ordered := make([]*storage.SegmentMeta, len(segs))
	copy(ordered, segs)
	sort.SliceStable(ordered, func(i, j int) bool {
		if dir == Forward {
			return ordered[i].MinTS < ordered[j].MinTS
		}
		return ordered[i].MaxTS > ordered[j].MaxTS
	})

	for _, seg := range ordered {
		if acc.done() {
			break
		}
		if cut, ok := acc.cutoff(); ok && segmentCannotBeat(seg.MinTS, seg.MaxTS, cut, dir) {
			// Every remaining segment is ordered no better than this one, so nothing after
			// it can beat the cutoff either.
			break
		}
		if err := e.querySegment(acc, sh, seg, q, start, end, r, forceScan); err != nil {
			return err
		}
	}
	return nil
}

// querySegment returns the records in one segment matching the query. It uses index
// pushdown when the planner finds pushable predicates and the segment is indexed — sealed,
// or active with an in-memory index; otherwise (or on any index error) it scans. Either way
// the survivors are re-verified against every predicate the index has not proven, so the
// two paths return identical results.
func (e *Engine) querySegment(acc collector, sh Shard, seg *storage.SegmentMeta, q Query, start, end int64, r *engineResolver, forceScan bool) error {
	scan := func() error {
		// Walk pages in the query's direction so a bounded accumulator fills from the pages
		// it will actually keep, and prune pages that cannot beat what it already holds.
		// Page bounds are only consulted after ValidatePage, so a corrupt page is skipped
		// rather than feeding a bogus bound into the pruning decision.
		//
		// Pruning is per page, never per record: append order within a page is NOT time
		// order, so stopping mid-page on a count would drop records.
		skipPage := func(minTS, maxTS int64) bool {
			cut, ok := acc.cutoff()
			return ok && segmentCannotBeat(minTS, maxTS, cut, q.Direction)
		}
		// The scan path always re-verifies the FULL predicate set: nothing has been proven
		// by an index here, so residuals must not be used.
		err := storage.ScanSegmentPages(seg.Path, start, end, q.Direction == Backward, skipPage,
			func(rec model.LogEntry) bool {
				if matchAll(q.Preds, rec, r) {
					acc.add(rec, r)
				}
				return !acc.done()
			})
		return skipUnreadable(seg.Path, err)
	}

	if forceScan {
		return scan()
	}
	// The guard is priced by how many rows acc keeps: a collector that keeps every match
	// (the metric path) has no limit to over-fetch, and never scans for the guard's sake.
	a := e.access(sh, seg, q.Preds, rowBound(acc))
	if !a.fetch {
		return scan() // nothing pushable, no complete index, an unreadable sidecar, or the guard
	}
	// Materialize candidates and re-verify the RESIDUAL predicates — those the index has
	// not already proven. A predicate settled by an exact (lossless-kind) lookup is
	// skipped here, which is what removes the per-candidate re-extraction; everything else
	// is still checked exactly, so a lossy key's false positives are filtered as before.
	// The per-record time filter is applied here because .tidx offsets are not time-pruned.
	err := storage.FetchRecords(a.seg.Path, a.offsets, func(rec model.LogEntry) bool {
		if ts := rec.TS.UnixNano(); ts < start || ts > end {
			return true
		}
		if matchAll(a.residuals, rec, r) {
			acc.add(rec, r)
		}
		return !acc.done()
	})
	return skipUnreadable(a.seg.Path, err)
}

// segmentAccess is the access path chosen for one segment of one query.
type segmentAccess struct {
	seg       *storage.SegmentMeta // the segment as planned (see indexFor)
	fetch     bool                 // fetch offsets by index; false means scan the segment
	reason    string               // why: one of the Reason* constants
	offsets   []uint64             // the candidates, when fetch
	residuals []Predicate          // the predicates the index has not proven, when fetch
}

// access decides how one segment is read: Execute and Explain both go through it, so the
// path Explain reports is the path Execute takes. limit is the number of rows the query
// keeps (0 = all), which prices the cost guard.
func (e *Engine) access(sh Shard, seg *storage.SegmentMeta, preds []Predicate, limit int) segmentAccess {
	meta, src, inMemory := e.indexFor(sh, seg)
	lookups, residuals := e.plan(meta, src, preds)
	if lookups == nil {
		if !meta.Indexed {
			return segmentAccess{seg: meta, reason: ReasonNotIndexed}
		}
		return segmentAccess{seg: meta, reason: ReasonNoPushdown}
	}
	offsets, tripped, ok := intersectLookups(lookups, e.costGuard(meta, limit))
	switch {
	case !ok:
		return segmentAccess{seg: meta, reason: ReasonIndexMissing} // degrade to scan
	case tripped:
		return segmentAccess{seg: meta, reason: ReasonCostGuard}
	}
	reason := ReasonIndexed
	if inMemory {
		reason = ReasonIndexedInMemory
	}
	return segmentAccess{seg: meta, fetch: true, reason: reason, offsets: offsets, residuals: residuals}
}

// skipUnreadable turns two per-segment failures into "this segment contributes nothing",
// the same way on the index and the scan path:
//
//   - The segment's file no longer exists. Retention may delete a segment between
//     manifest.Filter and the query opening it, and that data was intentionally expired,
//     so this is not a query failure. Files removed behind logd's back look the same,
//     which is why it is logged (once per segment) rather than passed over in silence.
//   - The segment cannot be opened at all (storage.ErrSegmentUnreadable: a compressed
//     segment whose block directory fails its checksum). Its records are lost to every
//     reader, as a corrupt block's are. Failing instead would turn one bad byte into an
//     error for every query over the segment's time range, and the two paths would
//     disagree: an index lookup with no candidates in the segment never opens the file.
func skipUnreadable(segPath string, err error) error {
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		reportSkipped(segPath, "its files are gone (expired by retention during the query, or removed outside logd)")
		return nil
	case errors.Is(err, storage.ErrSegmentUnreadable):
		reportSkipped(segPath, err.Error())
		return nil
	}
	return err
}

// skippedSegments remembers which segments have been reported as skipped, so a dashboard
// re-running its queries does not repeat the same log line every few seconds.
var skippedSegments sync.Map

func reportSkipped(segPath, why string) {
	if _, seen := skippedSegments.LoadOrStore(segPath, struct{}{}); !seen {
		log.Printf("query: skipping segment %s: %s", segPath, why)
	}
}

// Tail delivers records written from now on that match all predicates (no time bound).
// The returned func unsubscribes. For multiple shards it subscribes to each and merges
// their channels into one. WebSocket framing lives in compat; this is the core primitive.
func (e *Engine) Tail(preds []Predicate, buffer int) (<-chan model.LogEntry, func()) {
	if len(e.shards) == 1 {
		sh := e.shards[0]
		r := e.resolverFor(sh)
		match := func(rec model.LogEntry) bool { return matchAll(preds, rec, r) }
		return sh.Subscribe(match, buffer)
	}
	return e.mergedTail(preds, buffer)
}

// mergedTail subscribes to every shard and fans their deliveries into one channel. cancel
// stops the forwarders, unsubscribes all shards, then closes the merged channel (in that
// order, so no forwarder sends on a closed channel).
func (e *Engine) mergedTail(preds []Predicate, buffer int) (<-chan model.LogEntry, func()) {
	out := make(chan model.LogEntry, buffer)
	done := make(chan struct{})
	var wg sync.WaitGroup
	var cancels []func()
	for _, sh := range e.shards {
		r := e.resolverFor(sh) // per-shard resolver (service names resolve per shard)
		match := func(rec model.LogEntry) bool { return matchAll(preds, rec, r) }
		ch, cancel := sh.Subscribe(match, buffer)
		cancels = append(cancels, cancel)
		wg.Add(1)
		go func(ch <-chan model.LogEntry) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case rec, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- rec:
					case <-done:
						return
					}
				}
			}
		}(ch)
	}
	var once sync.Once
	return out, func() {
		once.Do(func() {
			close(done)
			for _, c := range cancels {
				c()
			}
			wg.Wait()
			close(out)
		})
	}
}

func (e *Engine) resolverFor(sh Shard) *engineResolver {
	return &engineResolver{lt: sh.ServiceLookup(), ex: e.ex}
}

// engineResolver resolves labels and extracts typed field values for predicates.
type engineResolver struct {
	lt *storage.LookupTable
	ex *extract.Engine
}

// Label resolves a label key from the record's labels (the Extra blob). The reserved keys
// level and service fall back to the first-class fields — the canonical Level, the
// interned ServiceID — only when Extra does not carry them.
//
// Extra wins because it is what responses show: a Loki push keeps its stream labels in
// Extra, and resolving the fields first made {level="error"} miss rows displayed as
// level="error" (the field says "ERROR") and {service="api"} miss everything (the push path
// never sets ServiceID).
func (r *engineResolver) Label(e model.LogEntry, key string) (string, bool) {
	// Single-key scan rather than ParseExtraLabels: this runs once per record PER label
	// predicate on both the scan and the index re-verify paths, and building a whole map to
	// read one key made it 28.7% of query CPU. ExtraLabel is observationally identical
	// (differential + fuzz tested in core/model).
	if v, ok := model.ExtraLabel(e.Extra, key); ok {
		return v, true
	}
	switch key {
	case "level":
		return e.Level.String(), true
	case "service":
		if name, ok := r.lt.GetName(e.ServiceID); ok && name != "" {
			return name, true
		}
	}
	return "", false
}

// AllLabels returns the record's label set for metric grouping: exactly the Extra labels.
// It does not add the first-class level or service: the log path shows a stream's labels
// as Extra carries them, so a metric over the same records must group by the same set, or
// `sum by (level)` reports series ("INFO", or a renamed "WARN" for a pushed "warning")
// that no stream in the log view carries.
func (r *engineResolver) AllLabels(e model.LogEntry) map[string]string {
	m := model.ParseExtraLabels(e.Extra)
	if m == nil {
		m = map[string]string{}
	}
	return m
}

// FieldValues extracts every typed value for a template field from the record's
// message (empty if the extractor is absent or the field doesn't occur).
func (r *engineResolver) FieldValues(e model.LogEntry, field string) []index.Value {
	if r.ex == nil {
		return nil
	}
	var out []index.Value
	for _, fv := range r.ex.ExtractValues(e.Message) {
		if fv.Field == field {
			out = append(out, fv.Value)
		}
	}
	return out
}
