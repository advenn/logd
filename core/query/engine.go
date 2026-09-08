package query

import (
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
type Shard interface {
	Manifest() *storage.Manifest
	ServiceLookup() *storage.LookupTable
	LiveStreams() []label.Set
	Subscribe(match func(model.LogEntry) bool, buffer int) (<-chan model.LogEntry, func())
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
func NewShardedEngine(shards []Shard, ex *extract.Engine, labelKeys []string) *Engine {
	var set map[string]struct{}
	if len(labelKeys) > 0 {
		set = make(map[string]struct{}, len(labelKeys))
		for _, k := range labelKeys {
			set[k] = struct{}{}
		}
	}
	return &Engine{shards: shards, ex: ex, labelKeys: set}
}

// Labels returns the distinct allowlisted label keys present across sealed segments,
// sorted. (Active-segment labels appear after seal — acceptable eventual consistency for
// discovery.)
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
			r, err := label.OpenReader(label.IndexPath(strings.TrimSuffix(seg.Path, ".log")))
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
	start := q.Start
	end := q.End
	if end <= 0 {
		end = math.MaxInt64
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}

	out, err := e.collect(q.Preds, start, end, forceScan)
	if err != nil {
		return out, err
	}

	// Global sort because append order within a page is NOT guaranteed to be time order
	// (out-of-order arrivals, §16); then apply the limit. Equal timestamps are broken
	// deterministically by client-visible content (message, then labels) so the result
	// set is stable regardless of how records are partitioned across shards — the fan-in
	// invariant (sharded == single-shard) then holds even at a limit boundary, and a
	// given query returns the same result run to run.
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].TS.UnixNano(), out[j].TS.UnixNano()
		if ti != tj {
			if q.Direction == Forward {
				return ti < tj
			}
			return ti > tj
		}
		if out[i].Message != out[j].Message {
			return out[i].Message < out[j].Message
		}
		return out[i].Extra < out[j].Extra
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// collect returns every record in [start, end] matching preds (no sort, no limit), using
// pushdown per segment where possible. It is the shared entry source for log queries
// (Execute) and metric queries (ExecuteMetric).
func (e *Engine) collect(preds []Predicate, start, end int64, forceScan bool) ([]model.LogEntry, error) {
	var out []model.LogEntry
	for _, sh := range e.shards {
		recs, err := e.collectShard(sh, preds, start, end, forceScan)
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// collectShard collects one shard's matching records, resolved with THAT shard's service
// dictionary. The metric evaluator uses this so it can annotate each shard's entries with
// the correct (per-shard) resolver before merging into cross-shard streams.
func (e *Engine) collectShard(sh Shard, preds []Predicate, start, end int64, forceScan bool) ([]model.LogEntry, error) {
	r := e.resolverFor(sh)
	q := Query{Preds: preds} // querySegment only reads q.Preds
	var out []model.LogEntry
	for _, seg := range sh.Manifest().Filter(start, end) {
		recs, err := e.querySegment(seg, q, start, end, r, forceScan)
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// querySegment returns the records in one segment matching the query. It uses index
// pushdown when the planner finds pushable predicates and the segment is indexed;
// otherwise (or on any .tidx error) it scans. Either way the survivors are re-verified
// against the FULL predicate set, so the two paths return identical results.
func (e *Engine) querySegment(seg *storage.SegmentMeta, q Query, start, end int64, r Resolver, forceScan bool) ([]model.LogEntry, error) {
	var recs []model.LogEntry
	keep := func(rec model.LogEntry) {
		if matchAll(q.Preds, rec, r) {
			recs = append(recs, rec)
		}
	}
	scan := func() ([]model.LogEntry, error) {
		recs = recs[:0]
		err := storage.ScanSegmentTimeRange(seg.Path, start, end, func(rec model.LogEntry) bool {
			keep(rec)
			return true
		})
		return recs, skipIfGone(err)
	}

	if forceScan {
		return scan()
	}
	lookups := e.plan(seg, q.Preds)
	if lookups == nil {
		return scan() // nothing pushable, or non-indexed segment
	}
	segBase := strings.TrimSuffix(seg.Path, ".log")
	offsets, ok := intersectLookups(segBase, lookups)
	if !ok {
		return scan() // a .tidx was missing/corrupt → degrade to scan
	}
	// Cost guard: a candidate set covering too much of the segment is cheaper to read
	// sequentially than to fetch by random offset — same reasoning as a DB planner
	// choosing seq-scan over index-scan. Correctness-neutral (scan == pushdown result).
	if costGuardTrips(seg, len(offsets)) {
		return scan()
	}
	// Materialize candidates and re-verify exactly (lossy keys mean the .tidx set is a
	// superset). The per-record time filter is applied here because .tidx offsets are
	// not time-pruned.
	err := storage.FetchRecords(seg.Path, offsets, func(rec model.LogEntry) bool {
		if ts := rec.TS.UnixNano(); ts < start || ts > end {
			return true
		}
		keep(rec)
		return true
	})
	return recs, skipIfGone(err)
}

// skipIfGone turns a "segment file no longer exists" error into a clean empty result:
// retention may delete a segment between manifest.Filter and the query opening it, and
// that data was intentionally expired — not a query failure.
func skipIfGone(err error) error {
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
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

func (e *Engine) resolverFor(sh Shard) Resolver {
	return &engineResolver{lt: sh.ServiceLookup(), ex: e.ex}
}

// engineResolver resolves labels and extracts typed field values for predicates.
type engineResolver struct {
	lt *storage.LookupTable
	ex *extract.Engine
}

// Label resolves the first-class labels level and service, then falls back to the
// Extra JSON blob. (Extra-as-label is a compat-era convenience; the native label index
// arrives in Phase 6. Only string-valued Extra fields resolve.)
func (r *engineResolver) Label(e model.LogEntry, key string) (string, bool) {
	switch key {
	case "level":
		return e.Level.String(), true
	case "service":
		if name, ok := r.lt.GetName(e.ServiceID); ok && name != "" {
			return name, true
		}
		return "", false
	default:
		// Single-key scan rather than ParseExtraLabels: this runs once per record PER
		// label predicate on both the scan and the index re-verify paths, and building a
		// whole map to read one key made it 28.7% of query CPU. ExtraLabel is
		// observationally identical (differential + fuzz tested in core/model).
		return model.ExtraLabel(e.Extra, key)
	}
}

// AllLabels returns the record's full label set: the Extra labels, plus the first-class
// level and service resolved from the record's fields (overriding any Extra value for
// those reserved keys, consistent with Label).
func (r *engineResolver) AllLabels(e model.LogEntry) map[string]string {
	m := model.ParseExtraLabels(e.Extra)
	if m == nil {
		m = map[string]string{}
	}
	m["level"] = e.Level.String()
	if name, ok := r.lt.GetName(e.ServiceID); ok && name != "" {
		m["service"] = name
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
