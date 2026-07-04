# logd_v2 — Phase 4 build spec (native query engine: scan path + live tail)

Builds the **correctness baseline** (design §13 Phase 4, §9 native query model, §11 live tail).
This is the *scan path*: correct but not accelerated. NO index pushdown yet (Phase 5) — but this
becomes the **oracle** Phase 5 must match exactly (indexed result == scan result). Conform to
`docs/logd-design.md` §9, §11, §16. Phases 1–3 done. Learning project: heavily-commented Go.

Governing rule: results are the records matching a time range AND a set of predicates, time-ordered,
limited. The scan path is the semantic ground truth — every predicate is evaluated on the fully
decoded record, exactly (no lossy keys here; the full extracted values are compared).

## New package `core/query` + small storage additions

```
core/query/    native Query model · Predicate types · Engine.Execute (scan) · Engine.Tail (live)
```
Dependency direction: `query` → `storage`, `extract`, `index`, `model` (all core; no cycles — none of
those import query). `storage` gains read/subscribe accessors but imports nothing new.

## 1. Native query model — `core/query/query.go`

```go
type Direction uint8
const ( Backward Direction = 0; Forward Direction = 1 ) // Backward = newest-first (default)

type Query struct {
    Start, End int64        // event-time window [Start,End] unix nano (End<=0 means "open" → live tail territory)
    Preds      []Predicate  // AND-combined
    Limit      int          // 0 → default 100
    Direction  Direction
}

type Predicate interface { Match(e model.LogEntry, r Resolver) bool }
```
`Resolver` gives a predicate what it needs beyond the raw entry: label resolution (service name via
the lookup table, level, Extra-JSON fields) and typed field extraction (via the extract engine). It
is passed in so predicates stay pure and the engine owns the wiring.

Predicate implementations (each with a negate variant where sensible):
- `LineContains{Sub}` / `LineNotContains{Sub}` — substring on Message.
- `LineRegex{re *regexp.Regexp}` / `LineNotRegex` — RE2 on Message.
- `LabelEqual{Key, Value}` / `LabelNotEqual` — resolve Key (`level`, `service`, else Extra JSON) and compare.
- `TypedCompare{Field, Op, Value}` — Op ∈ = != < <= > >=; re-extract Field's typed value(s) from Message and compare EXACTLY (full value, not the lossy key). A record with multiple occurrences matches if ANY occurrence satisfies (OR over occurrences). A record with no such field: satisfies only `!=` (Loki empty-label semantics), like §9 re-verify.

## 2. Value comparison — `core/index/compare.go`

`func Compare(a, b Value) int` for same-Kind values: int/float numeric, str `strings.Compare` (FULL
string), uuid byte order. Used by TypedCompare. (Encoding is order-preserving, but the scan path
compares real values, not keys, so it is exact even for >16-byte strings.)

## 3. Typed extraction of values — `core/extract`

Add `Engine.ExtractValues(message string) []FieldValue` where `FieldValue{ Field string; Value index.Value }`
— the typed values BEFORE key encoding. Refactor `Extract` to be `ExtractValues` + `EncodeKey`, so the
index path and the scan/re-verify path share one extractor and can never disagree. (Literals stay
existence-only; ExtractValues returns template captures.)

## 4. Storage read/subscribe surface — `core/storage`

Add, without new imports:
- `func (w *Writer) Manifest() *Manifest` and `func (w *Writer) ServiceLookup() *LookupTable` — expose the
  LIVE (in-memory) manifest and lookup so queries see just-flushed active-segment data with current
  bounds (the on-disk manifest lags between seals).
- `func ScanSegmentTimeRange(path string, start, end int64, visit func(model.LogEntry) bool) error` — open a
  segment read-only, skip pages whose header [MinTS,MaxTS] doesn't overlap [start,end], decode records,
  and call visit for each record whose TS ∈ [start,end]; visit returns false to stop early. Sealed
  segments are immutable; the active segment's already-flushed pages are immutable too (the writer
  writes whole pages via a separate fd), so concurrent read is safe.
- Live tail: a concurrency-safe subscriber registry. `type tailSub struct{ match func(model.LogEntry) bool; ch chan model.LogEntry }`.
  `func (w *Writer) Subscribe(match func(model.LogEntry) bool, buffer int) (<-chan model.LogEntry, func())` returns
  a receive channel and an unsubscribe func. The writer's `notify(e)` (the reserved §11 seam in
  processEntry) offers e to every subscriber whose match(e) is true, non-blocking (drop if the
  subscriber's buffer is full — a slow tailer must not stall the write path). Registry guarded by its
  own mutex; notify runs on the writer goroutine, Subscribe/unsubscribe from callers.

## 5. Query engine — `core/query/engine.go`

`type Engine struct { w *storage.Writer; ex *extract.Engine }`; `func NewEngine(w, ex) *Engine`. A
`resolver` wraps w.ServiceLookup() + ex.

### Execute (scan path)
```
Execute(q Query) ([]model.LogEntry, error):
  end := q.End; if end<=0 { end = maxInt64 }
  limit := q.Limit; if limit<=0 { limit = 100 }
  var out []model.LogEntry
  for _, seg := range w.Manifest().Filter(q.Start, end):          // time-prune segments
      storage.ScanSegmentTimeRange(seg.Path, q.Start, end, func(e){ // time-prune pages + records
          if allPredsMatch(e) { out = append(out, e) }
          return true                                              // collect all, sort/limit after
      })
  sort out by TS (Backward: desc, Forward: asc); tie-break stable
  if len(out) > limit { out = out[:limit] }
  return out
```
Correctness over cleverness: collect all matches, then global sort + limit. Append order within a page
is NOT assumed to be time order (§16 out-of-order risk), so a global sort is required. Early-stop/
heap merge is a later optimization.

### Tail (live)
```
Tail(preds []Predicate, buffer int) (<-chan model.LogEntry, cancel func()):
  match := func(e){ return allPredsMatch(e) }   // no time bound; "from now on"
  return w.Subscribe(match, buffer)
```
Delivers matching records as they are written. WebSocket framing / Loki `/tail` semantics / historical→
live handoff are compat/later (§11) — this is the core primitive only.

## 6. Tests (this is the oracle — test it hard)

- Time window: records outside [Start,End] excluded; page/segment pruning doesn't drop in-range records.
- Ordering + limit + direction: Backward newest-first, Forward oldest-first, limit truncates AFTER sort; out-of-order arrivals still sort correctly.
- LineContains/regex + negations; LabelEqual on level, service, and an Extra-JSON field; LabelNotEqual empty-label semantics.
- TypedCompare: `latency > 200` returns exactly the records whose extracted value > 200; `=`, `!=`, boundaries; a record with multiple occurrences matches if any qualifies; a record lacking the field matches only `!=`. EXACT for a >16-byte str value (where the lossy key would over-match) — proving the scan path is exact.
- Reads just-flushed active-segment data (query before Close sees flushed pages).
- Live tail: subscribe, write matching + non-matching records, receive only matches; unsubscribe stops delivery; a full buffer drops rather than blocking the writer.
- Concurrency: run Execute/Tail while the writer ingests, under `-race`.

Keep `go build ./... && go vet ./... && go test -race ./...` green throughout.
