# logd_v2 — Phase 5 build spec (planner + index pushdown + re-verify)

Makes indexed queries O(log n) (design §13 Phase 5, §9 execution, §7.3 schema, §6.3 keys,
§15 lossy-range). THE deliverable and test bar: **for every query, the pushdown result set
equals the Phase-4 scan result set, exactly.** Phases 1–4 done. Learning project: commented Go.

Governing invariant (design §2.2): the `.tidx` returns a CANDIDATE superset (false positives OK,
false negatives never); re-verify decodes each candidate and applies the full Phase-4 predicate
set, so the final result is exact. A missing/corrupt `.tidx`, a non-indexed (active/recovered)
segment, or a field absent from a segment's schema → that segment (or predicate) degrades to scan.

## What pushes down

Per time-pruned segment, a predicate is **pushable** only if the segment is `Indexed==true` AND:
- **`TypedCompare{Field, Op, Value}`** with `Op ∈ {=, <, <=, >, >=}` (NOT `!=`) and `Field` in the
  segment's schema. Lookup on the field's `.tidx` (bounds WIDENED to inclusive so lossy str keys
  never drop a true match; re-verify filters the edges, §15):
  - `=`  → `LookupEqual(EncodeKey(v))`
  - `>` `>=` → `LookupRange(EncodeKey(v), maxKey, incLo=true, incHi=true)` (key ≥ encode(v))
  - `<` `<=` → `LookupRange(minKey, EncodeKey(v), true, true)` (key ≤ encode(v))
- **`LineContains{Sub}`** where `Sub` is exactly a configured literal (via `extract.Engine.LiteralField`)
  whose existence field is in the segment's schema → `LookupEqual(zeroKey)` (all offsets that contain it).

Everything else (`!=`, regex, label filters, a `TypedCompare` whose field is NOT in this segment's
schema) is a **residual**: not pushed, but re-checked by the full re-verify. A field absent from an
indexed segment's schema is deliberately NOT pruned to empty — it becomes a residual so the segment is
scanned and query-time re-extraction finds any values (this is what keeps pushdown == scan even after a
config change adds a template; §9.1, §7.3).

## Execution per segment

```
plan(seg):
  if !seg.Indexed:                         -> SCAN (active/recovered)
  lookups := [pushable predicates as above]
  if len(lookups) == 0:                     -> SCAN (nothing selective, e.g. only line/label filters)
  else:                                     -> INDEX

INDEX path:
  for each lookup: open <segBase>.<field>.tidx; on ANY open/read error -> degrade: SCAN the segment
  candidates = INTERSECT the lookups' offset sets (AND across predicates)  // sorted []uint64
  FetchRecords(seg.Path, candidates): decode each candidate record, then
     if rec.TS in [start,end] AND matchAll(q.Preds, rec, resolver):  keep   // FULL re-verify (exact)
SCAN path (unchanged from Phase 4): ScanSegmentTimeRange + matchAll.
```
Re-verify runs `matchAll` over the SAME predicates the scan uses (including the pushed ones — lossy
keys mean a candidate must be confirmed against the decoded record), plus the per-record time filter.
Then Execute globally sorts + limits across segments exactly as Phase 4.

**Cost guard (design §7.4): DEFERRED to hardening.** Phase 5 always uses the index when a predicate is
pushable; a non-selective predicate (e.g. `latency >= 0`) therefore fetches many records by offset —
correct but not faster than scan. Adding a "candidates > X% of segment records → seq-scan" guard needs
a per-segment record count in the manifest; documented as the next optimization, out of scope here.

## New/changed code

- `core/extract`: `func (e *Engine) LiteralField(sub string) (field string, ok bool)` — is `sub` a
  configured literal, and its existence-field name.
- `core/storage`: `func FetchRecords(path string, offsets []uint64, visit func(model.LogEntry) bool) error`
  — sort offsets, read each needed page once (offset/PageSize), `ValidatePage`, `DecodeEntry` the record
  at offset%PageSize, call visit. Corrupt page → skip its offsets (degrade). No new imports.
- `core/query`:
  - `Engine.Execute` refactored to per-segment `plan → scan|index`.
  - `Engine.ExecuteScan(q)` — forces the scan path for ALL segments (the Phase-4 oracle), so the
    differential test can assert `Execute == ExecuteScan`.
  - `Engine.Explain(q) []SegmentPlan{SegmentID, Mode "scan"|"index", Candidates int}` — reports the plan
    per segment (design's index-hit-vs-scan explainability) and lets tests assert pushdown actually fired.
  - `minKey`/`maxKey` (all-0x00 / all-0xFF 16-byte keys) for range bounds.

## Tests (the differential test is the deliverable)

- **Differential oracle:** a fixed dataset spanning ≥2 sealed segments; run a large battery of queries
  (each typed op at boundary/interior/outside values, equality, `!=`, str equality beyond 16 bytes,
  line-literal, label filters, combinations, limits, both directions) and assert `Execute(q)` returns
  the IDENTICAL ordered result to `ExecuteScan(q)` for every one. This is "indexed == scan, always".
- **Pushdown actually fires:** `Explain` reports `index` mode (not `scan`) for a typed query on an
  indexed segment, and `scan` for the active segment / a `!=` query / a field not in schema.
- **Lossy str:** two 17-char ids sharing a 16-byte key prefix — `trip_id = "...1"` returns exactly one
  record via the index (re-verify filters the collision), matching scan.
- **Degrade:** delete/corrupt a `.tidx` → the segment silently scans and returns the same result.
- **Range boundary:** `>`, `>=`, `<`, `<=` at the exact bound value return the same as scan (inclusive
  widening + re-verify handles `>` vs `>=`).
- **Multi-predicate intersect:** `latency > 100 AND latency < 900` (or two different fields) intersects
  candidate sets; result equals scan.
- Run the differential battery under `-race` with a concurrent tail to be safe.

Keep `go build ./... && go vet ./... && go test -race ./...` green throughout.
