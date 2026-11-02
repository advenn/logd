# logd — Master Design Document

Authoritative design spec for `logd`, a log storage daemon in Go with a Loki-compatible API. Supersedes prior notes and the original `prompt.md` where they conflict (see "Deviations" at the end). Phase build prompts must conform to this document.

Status markers: ✅ locked · ⚙️ resolved this revision · ⏳ open / future.

---

## 1. Purpose, differentiator, non-goals

`logd` stores logs and serves them over a Loki-compatible API. Differentiator: **range queries on typed values extracted from unstructured log text** (extract `247` from `took 247ms`, answer `duration > 200`). Competitors either need pre-structured typed columns (ClickHouse, Quickwit, Elasticsearch) or support presence-only filtering (Loki and VictoriaLogs bloom filters — no ranges).

**Non-goals (v1):** distributed consensus, cross-machine query fan-out, cloud object-storage read path. The format must not foreclose these (§10, §16); they are not built.

---

## 2. Invariants — never violate

1. **Segment is the source of truth.** Every index is derived, always rebuildable from segment data.
2. **Indexes are pruning structures.** False positives allowed (re-verify on decode); false negatives never. Missing/corrupt index degrades to scan — never a wrong answer.
3. **Append-only.** Written records are never mutated.
4. **One segment, one writer goroutine, for life.** Concurrency = multiple writers on disjoint folders, never multiple writers on one segment.
5. **Shared-nothing across writers.** Each writer owns a folder. Only shared read-side state: the list of shard folders.
6. **Segments are self-describing and relocatable.** Segment-relative offsets; all dictionaries needed to decode a segment are snapshotted inside it.
7. **Core is protocol-agnostic.** `core/*` never imports `compat/*`. Core has a native ingest API and a native query model; Loki/OTLP/ES are compat translations. No Loki assumption (nanosecond-string timestamps, `{label=...}` selector syntax) leaks into core.
8. **Index structure matches question shape.** Range → sorted order-preserving index. Equality/set → dictionary + posting lists. Time → min/max bounds.

---

## 3. Architecture & package layout ✅

Single binary, modular by package, dependency rule per invariant 7.

```
logd/
  cmd/logd/main.go        # wiring only
  core/
    model/                # LogEntry, LogLevel
    storage/              # record, page, segment, manifest, dictionaries
    index/                # time pruning, label index, typed-range (.tidx)
    ingest/               # writer goroutine(s), native ingest API
    query/                # native query engine, planner, live tail
    config/               # extraction/label config
  compat/
    loki/push/            # Loki push JSON  → core ingest
    loki/logql/           # LogQL           → core query plan
    loki/grafana/         # discovery endpoints, tail WS, metric stubs
    otlp/                 # later
    elastic/              # later
```

---

## 4. Data model ✅

### LogEntry

```go
type LogEntry struct {
    TS         time.Time // event time
    IngestedAt time.Time // arrival time (set by logd)
    Level      LogLevel  // DEBUG=0 INFO=1 WARN=2 ERROR=3
    ServiceID  uint16    // interned service name (hot-path filter)
    StreamID   uint32    // interned label set (label-index ref; 0 until label phase)
    Message    string
    Extra      string    // raw JSON: high-cardinality fields
}
```

`StreamID` is canonical label reference; `ServiceID` is a denormalized fast path (also a member of the stream's label set).

### Binary record format

All integers BigEndian. Variable fields length-prefixed. Max field 65535 bytes (`ErrEntryTooLarge`).

```
[8] TS int64 unixnano   [8] IngestedAt   [1] Level   [2] ServiceID
[4] StreamID   [2] MsgLen   [N] Message   [2] ExtraLen   [M] Extra
```

`EncodedSize = 27 + N + M`. Decode bounds-checks before every read; truncation → `ErrMalformedRecord`. Implemented in `record.go`.

---

## 5. On-disk storage format ✅

- **Pages:** fixed 4096 B, unit of I/O. Records never cross page boundaries. Pages assembled in RAM; only whole pages written.
- **Page header (32 B):** Magic `0x4C30474E`, MinTS, MaxTS, EntryCount, FreeSpaceOffset, Checksum, Reserved.
- **Checksum: CRC32(IEEE) over the entire 4096-byte page** with checksum field zeroed (supersedes header-only; detects torn record data).
- **Manifest (per shard):** per segment — ULID id, state (active|sealed), MinTS/MaxTS, file paths, and the **indexed-field schema the segment was built with** (load-bearing for §7.5). Crash-safe via temp + fsync + rename + dir fsync.

---

## 6. The three indexes

Execution order: **time → label → typed-value → line filter** (cheapest pruning first).

### 6.1 Time ✅
Page headers + manifest carry `[MinTS, MaxTS]`. Prune segments, then pages, by bounds. Risk: out-of-order arrivals widen bounds (§16).

### 6.2 Label index ✅ (structure) / ⏳ (encodings)
Equality/set questions. Per segment: stream dictionary (canonical label set ↔ StreamID; keys sorted before canonicalization), posting lists (StreamID → sorted record offsets), service lookup. All snapshotted on seal. Not sorted by value — equality wants maps, not binary search. Serves `/labels` and `/label/<k>/values` without log scans. Cardinality guard: 5–10 label keys; high-cardinality values belong in `Extra`.
Posting-list encoding ⚙️: delta + varint over sorted ascending offsets (standard inverted-index practice; decoded sequentially).
Cardinality guard ⚙️: label keys are an explicit config allowlist; undeclared keys → `Extra`. Per declared key, max distinct values per segment (default 1000). On breach: new values stop creating streams, the key=value routes to `Extra` for those records, a counter increments. Never an ingest error, never a silent drop.

### 6.3 Typed-range index `.tidx` ✅
Range questions. Per (segment, indexed field), one file. Record: `[16 B order-preserving key][8 B segment-relative record offset]`, sorted by key. Active segment accumulates unsorted pairs in RAM, queryable in place while it is active (§9.2); sorted and flushed on seal. Record-level pointers (precision serves the differentiator; re-verify handles lossy keys; page-level is the fallback lever if disk size becomes a problem).

Key encodings (BigEndian is load-bearing — byte order = value order):
- **int** → flip sign bit (add 2^63), BigEndian, left-pad to 16 B.
- **float** → sign bit 0: flip sign bit; sign bit 1: flip all bits; BigEndian. Bounded precision in v1 — no exact-equality guarantee on far-apart decimals.
- **str** → longest valid-UTF-8 prefix ≤ 16 B (never split a multibyte rune; use `utf8.RuneStart` to back up to a boundary), right-pad `0x00`. **Lossy** → re-verify mandatory.
- **uuid** → 16 B binary form, exact fit.

---

## 7. Extraction & indexing configuration ⚙️ (was undesigned; core decisions now made)

### 7.1 Input shapes

```yaml
index:
  literals:                      # existence-indexed exact substrings
    - "panic:"
    - "nil pointer dereference"
  templates:                     # typed captures, value-indexed
    - name: latency
      pattern: "took {ms:int}ms"
      min: 0                     # optional params
      max: 100000
    - name: trip
      pattern: "trip:{id:str}"
      max_len: 32
```

### 7.2 Decisions ⚙️

| Topic | Decision |
|---|---|
| Types (v1) | `int`, `float`, `str`, `uuid`; extensible. Numeric → range ops; str/uuid → equality/presence only. |
| Literal case | Case-sensitive (matches Loki `|=`; byte-exact). |
| Naming | Field names and template names unique across config. Queryable field = capture name (compound naming still open ⏳). |
| Overlapping declarations | Error at config load. "Overlap" = identical or subsumed patterns. Shared anchors between a literal and a template are legal — both index on match. Definition needs final precision at implementation ⏳. |
| Extraction failure | Anchor matches but capture isn't the declared type → **no index entry, line stored normally** (found by scan). Never an ingest error. Must increment a per-template failure counter — silent failure is a debugging trap. |
| Multiple matches per line | Index every occurrence (multiple `.tidx` entries → same offset). No cap in v1: a cap creates false negatives unless the field is flagged partial. |
| NaN / ±Inf | Not numbers: extraction failure for the numeric field (unindexed). Never silently indexed into a str field. Negative zero encodes equal to zero. |
| Template params | v1: **parse-and-ignore** — `min`/`max`/`max_len` are accepted in config so adding semantics later isn't a format break, but enforce nothing. Correctness never depended on them (re-verify covers it). Validation + encoding hints (e.g. `max_len ≤ 16` ⇒ lossless str keys) deferred. |
| Anchor rule | Every template needs ≥ 1 literal fragment ≥ 3 B (Aho-Corasick anchor). Unanchored patterns rejected. |
| Adjacent captures | `{a:str}{b:int}` rejected — ambiguous without a literal separator. |

### 7.3 Config changes & schema history ⚙️ resolved

Config changes apply **only to future ingestion**. Old segments are never reindexed or stripped.

Mechanism: each segment's manifest entry records the schema it was built with (§5). **The planner consults the segment's own schema, never the live config**, to decide index-vs-scan per segment. Live config governs ingestion only.

Consequences: a removed template still accelerates queries on old segments; new segments scan. A query spanning the change gets mixed performance — correct, documented behavior.

### 7.4 Ingestion flow ✅

1. Compat translates wire format → `LogEntry` (labels → StreamID/ServiceID; high-cardinality → `Extra`).
2. **Extraction in handler goroutines** (not the writer): Aho-Corasick over `Message`, typed parse per capture, params validated. Produces `(field, typed_value)` pairs.
3. Bundle `(LogEntry, values)` → channel → shard's single writer.
4. Writer: encode, place in RAM page, flush whole pages; record offset = segment-relative position. Append `(key, offset)` to typed-range buffer; update stream dictionary + posting list; update page bounds.
5. Every record is stored identically — indexing is additive metadata; one storage path.

### 7.5 Seal ✅
Sort typed-range buffer → write `.tidx` + dictionaries + posting lists → fsync → fsync dir → rename → manifest update (including schema) → drop RAM buffers.

Triggers ⚙️: the active segment seals at a page boundary when it reaches `segment_size_mb`, when its in-RAM index exceeds `index_mem_budget_mb`, or when its first record is older than `max_segment_age_minutes` (default 60; 0 = off). The age trigger is checked on every flush tick, so an idle segment seals too. Without it a low-volume shard kept one segment active for days — uncompressed, indexed only in memory, and immune to retention, which deletes sealed segments only. A recovered segment's age is its first surviving record's ingest time; an empty segment never seals for age.

---

## 8. Durability & crash recovery ✅

- fsync: group commit (per page or per N ms), documented loss window.
- Whole-page writes only; torn trailing page detected by full-page CRC → truncate to last valid page.
- Atomic seal per §7.5; crash leaves old state or new state, never in-between.
- Recovery: read manifest → trust sealed segments → scan active segment → truncate torn page → **re-run extraction to rebuild the in-RAM typed-range buffer, stream dictionary, and posting lists** → resume.
- Recovery cost is bounded by max segment size ⏳ (bound unquantified).

---

## 9. Native query model ✅ (core-only terms)

Query = time range + predicate set: label-equality, line-contains (substring/regex), typed-value range. Results: matching records, time-ordered, limit + direction. Live tail = a query with no end bound, served by writer fan-out (§11).

Execution per shard:
1. Time: manifest prunes segments.
2. Label: resolve equality → StreamIDs → union posting lists → candidates.
3. Typed value: **check the segment's schema (§7.3)** → if indexed, binary-search `.tidx` for bounds (the active segment: one pass over its in-RAM buffer, §9.2), collect offsets, intersect with candidates; else mark segment for scan.
4. Decode survivors; apply line filters; **re-verify typed predicates** (lossy keys).
5. Merge across segments and shards, time-order, apply limit/direction.

### 9.1 Unindexed queries ✅ (defined behavior, not an error path)
- Arbitrary substring (undeclared): full scan within time-pruned range. Supported always.
- Range on an undeclared field: scan + extract at query time + filter. Supported, O(n). ✅ Implemented as the dynamic predicate (`query.DynamicCompare`, §9.3); in LogQL, a parser stage (`| regexp`, `| pattern`, `| logfmt`, `| json`) followed by a filter on what it extracts.
- **Fast ranges require a declared template.** Honest limit; document it.
- Missing/corrupt/version-mismatched `.tidx`: silently degrade to scan for that segment.

### 9.2 Active-segment queries ⚙️ revised: in-memory index (was: scan)

**Decision changed, because it was measured.** The first resolution was to always scan the unsealed segment — bounded size, predictable cost, no insert-time structure — with "background incremental sort" as the escape hatch if profiling demanded. The evaluation demanded it: with ~24% of a 500k-line corpus in the active segment (T4), logd's latency rose 2.2–6.4× over the all-sealed layout and fell behind Loki at every selectivity. A live deployment always has an active segment, so that is the steady state, not an edge case. No sort was needed: the writer already holds the segment's typed-range buffer and label postings in RAM for the seal, and a linear pass over them is cheap next to a scan that decodes and re-extracts every record.

**Mechanism.** The planner pushes typed, literal and allowlisted label-equality predicates into the active segment exactly as into a sealed one; only where the offsets come from differs.
- *Concurrent reads.* One writer appends; queries read while it does. The writer appends a record's keys and posting under the lock it already takes once per record (for live label discovery), so the hot path gains no lock for labelled records. A query copies the slices' headers under that lock and reads the elements without it: an appended element is never written again (an append writes past the copied length or moves to a new array, leaving the old one intact), and the seal now sorts a *copy* for the `.tidx` instead of the buffer in place.
- *Readability boundary.* Buffered offsets run ahead of the file: a record's keys are buffered when it enters the page in RAM. After each successful page write the writer publishes the next page number; a query reads that boundary **first**, then copies the buffers, and keeps only offsets below it. Every page below a boundary it observed was written after its records were buffered, so the copy holds all of them (reading the other way round could put a page inside the boundary with its records missing — a false negative). The scan path reads the same thing — the pages in the file — so both paths see the pages written so far, and neither sees the page in RAM.
- *Segment identity.* The view is tied to a segment ID. The writer publishes it before the manifest names the segment and withdraws it once the seal is committed. A query that listed the segment as active before it sealed finds no view, re-reads the manifest, and plans against the now-sealed segment's sidecars; a view already taken stays valid for its segment, sealed or not.
- *Lookups.* Equality and ranges are one linear pass over the field's entries comparing 16-byte keys; label equality unions the matching streams' posting lists. Both implement the same lookup interface as the `.tidx`/`.lidx` readers, so the planner builds one lookup with one set of bound-widening and settled/residual rules for either.
- *Identity rules unchanged.* The view stands in for the facts a seal would record — every indexed field with its pattern, the label allowlist the writer derives label sets with, the keys capped so far — and the planner applies the same F-02/F-03 checks to them. Unlike a sealed schema it lists fields with no entries yet, which is sound because the buffer holds every value of every record: an active segment can prune to zero.
- *Recovery.* A crash-recovered active segment re-indexed by the reindex hook is complete and gets a view; without the hook its index misses the pre-crash records, so none is offered and it is scanned (`not_indexed`).
- *Explain* reports `state: active` with reason `indexed_in_memory`, or the scan reason.

**Cost.** Write path: one lock per indexed record, which labelled records already took; the writer's per-record CPU measured within noise (±10%) of the old code, interleaved on one machine. Read path: a lookup is one linear pass over the field's entries (~1 ms per 200k), against a scan that decodes and re-extracts every record. On a 200k-record active segment (`BenchmarkActiveSegment`) a range matching 5% / 1% / 0.1% returns every row in 25 / 35 / 12 ms against the scan's 470 / 415 / 335 ms; at limit 100 it takes 15 vs 43 ms (1%) and 10 vs 222 ms (0.1%), while at 5% the cost guard (§9.3) hands the segment to the scan. Memory: nothing is copied but slice headers; a view pins the buffers it read until its query ends, so a sealed segment's buffers can outlive the seal by one query.

### 9.3 Query decisions ⚙️ resolved

**Matcher (LogQL pattern ≡ declared template):** strict structural equality. Normalize both sides to (literal fragments, capture names); match iff fragments are byte-identical, captures align positionally, names are equal. Types come from config (queries carry none). Any deviation — whitespace, extra fragment, renamed capture — is no match → scan. Rationale: a false match uses the wrong index (wrong answers); a false no-match only costs a scan. Upgrade path: explain output (index-hit vs scan per query) so users can see why a query scanned.

✅ Implemented (`compat/loki/logql/structural.go`, which carries the full argument). Precisions made at implementation:
- *Names:* a capture's name is compared with the template's queryable field name, `<template>_<capture>` — the name the native syntax and results use. `<_>took=<latency_ms>ms<_>` matches template `latency: took={ms:int}ms`.
- *The ends:* the pattern must start with `<_>` (Loki anchors a pattern that starts with a literal at the start of the line; a template matches anywhere). A trailing `<_>` is dropped: Loki never anchors the end, so the pattern extracts the same labels with or without it. A template ending in a capture never matches (Loki's last capture runs to the end of the line). The literal after an int/float/uuid capture must not start with a byte the value may contain (`+`, `-`; hex for uuid).
- *Scope:* the pattern must be the query's only parser stage, and every filter on the template's fields must be `=`, `<`, `<=`, `>`, `>=` with a number on an int/float field (a bound of the field's type) or `=` with a non-empty string on a str field. Anything else (`!=`, a regex, `= "200"` on a number) gives the match up for the whole query.
- *Semantics:* on a *conforming* line — the template matches at the first occurrence of its first literal and nowhere later — Loki's capture equals the template's value and the comparisons agree (ints within ±2^53). Elsewhere they differ: several occurrences (Loki reads the first, the template any), a first value not of the type (`took=12.5ms` under int; Loki converts it, or keeps an unconvertible one flagged `__error__`), a missing or displaced literal, ints beyond 2^53. No rewrite closes that gap, so **a structural match uses the template's semantics** — the rows of the native form — and a test pins the differences. Result labels are still re-extracted with Loki's semantics.
- *Explain:* `/logd/api/v1/explain` reports per parser stage and filter whether it was mapped to a template (index) or runs as a dynamic predicate (scan), and why.

**Operator/type mismatch:** two-level.
- Core native API is **strict**: an operator unsupported by the field's declared type is a plan-time error. Core separately offers an explicit *dynamic predicate* (decode → attempt conversion → filter), a scan-time operation.
- Compat layers choose their own mapping. Loki layer maps numeric filters on untyped/str fields to the dynamic predicate (reproduces Loki's convert-per-line behavior). Other layers may map strictly.

✅ Implemented. `query.DynamicCompare{Name, Extract, Op, Value, Numeric, Re}`: `Extract(entry, resolver) (string, bool)` is supplied by the caller; numeric comparisons convert both sides to float64 per record and fail on a missing or unconvertible value whatever the operator; string comparisons read a missing value as `""`; a regex matches or must not. The planner has no case for it, so it is always a residual — evaluated on scan, on re-verify and in tail, keeping `Execute == ExecuteScan`. `MetricQuery.SeriesLabels`/`UnwrapValue` let metric queries group by and unwrap such values. The Loki layer maps every label filter after a parser stage, on a name the stage may extract, to it. Deliberate difference from Loki: a numeric filter on a value that does not convert does not match, where Loki keeps the line flagged `__error__=LabelFilterErr` (and skips numeric filters on a line that already carries an error); the rows agree for a query with `| __error__=""` after its last numeric filter.
Core stays protocol-agnostic (invariant 7); compat semantics live in compat.

**Predicate ordering:** fixed pipeline, no cost-based planner in v1: time → label (union posting lists per label match, intersect across labels) → typed-range (intersect with prior set) → decode-time line filters + re-verify. LogQL pipelines are AND; OR exists only inside regex, evaluated at decode. Sorted offset lists make intersect/union linear merges. Order is correctness-independent (set intersection), so selectivity-aware reordering can be added later without breaking anything.

**Cost guard ⚙️ (index fetch vs. scan, per segment):** a bounded query scans a segment instead of fetching its index candidates by offset only when both hold: the candidates exceed √(limit × the segment's records), and they touch more than 90% of the segment's I/O units (compressed blocks, or pages when uncompressed — taken from the layout the segment records in the manifest, not from configuration). An unbounded collection (metric path) never scans for it. Rationale: an offset fetch reads each page or block once, in order, so it never costs more I/O than a scan; it decodes only its candidates, while a scan re-extracts every record. The scan's only advantage is its top-K early exit, which with evenly spread matches decodes ≈ limit·n/candidates records — equal to the fetch's `candidates` at √(limit·n); the coverage term is what makes "evenly spread" hold. Measured, not guessed (`TestCostGuardSelectivitySweep`, `testbed/results/costguard-sweep-2026-11-02.txt`): the crossover moved ~2–3× for a 10× limit, as √ predicts and a fixed over-fetch ratio does not. Segments without a recorded layout never trip. Known gap: matches clustered at the end a scan starts from make the scan cheaper than the rule assumes; the index is kept then, at the cost every query paid before the guard existed.

**Still open ⏳:** merge mechanics with live tail at the seam (historical→live handoff).

---

## 10. Multi-writer / shared-nothing ✅

`data_root/<shard_id>/`, each shard self-contained (own manifest, segments). Writers = goroutines or separate processes; per-segment dictionary snapshots make segments readable anywhere (no global ID space). ULID segment IDs. Readers enumerate `data_root/*` (shard registry by convention). Fan-in at query: run §9 per shard, merge.

**Data directory:** single daemon-owned directory, configurable (`data_root`). Defaults: `/var/lib/logd` on Linux, volume mount in containers, PVC per instance in k8s. logd writes nowhere else; nothing else writes into it. In production, keep it on a separate volume from the sources being collected (a log flood must not fill logd's disk). A flock-based `LOCK` file at each shard root prevents two processes opening the same shard.

---

## 11. Live tailing ✅ (seam reserved)

Core primitive: subscribe to matching records at write time. The writer, after appending a record, offers it to active subscribers whose predicates match (same predicate evaluation as §9, applied to one record; no indexes involved). The writer loop must reserve this notify step — retrofitting a broadcast into a tuned hot loop is the thing to avoid. WebSocket framing, Loki `/tail` semantics, historical→live handoff policy: compat/later ⏳.

---

## 12. Compat layer (not core) ✅

- **Loki push** → native ingest. Timestamp arrives as nanosecond string; structured metadata (flat string→string) → `Extra`.
- **LogQL** → native plan. `| pattern "<_>took <latency_ms>ms<_>" | latency_ms > 200` maps to declared template `latency: took {ms:int}ms` when the matcher can prove equivalence → index pushdown; else scan (§9.3). Index invisible to Grafana. ✅ Implemented with the parser stages `pattern`, `regexp`, `logfmt`, `json` (plain forms); like Loki, results carry the extracted labels (re-extracted per returned line; a name the stream already has becomes `<name>_extracted`).
- **Grafana endpoints:** `/loki/api/v1/labels`, `/label/<n>/values`, `/series` (from stream dictionary); `/query_range`, `/query`; `/tail` (WS); `/ready`; metric-query stub early (Grafana health check runs an instant metric query). Responses Prometheus-shaped. Tenancy header accepted.
- OTLP, ES bulk: later. Severity strings normalize into `Level`.

---

## 13. Build plan ✅

1. **Phase 1 (current):** `record.go` ✅ written · `page.go`, `lookup.go` next.
2. Segment + writer + durability + manifest (incl. schema field).
3. Extraction engine + key encoding + `.tidx` (§7 decisions apply).
4. Query + scan path + live tail (correctness baseline).
5. Planner + index pushdown + re-verify (results must equal Phase-4 scan).
6. Label index + compat (push, LogQL, Grafana discovery).
7. Hardening: metric queries, retention (drop sealed segments), RAM budget, backpressure.

---

## 14. Deferred / future ⏳

- Cloud tiering: write path clean (immutable segments → S3); **read path is a subsystem** — local hot cache + range-GET coalescing (record-level offsets → merged large reads); metastore only if fully disaggregated. Not one step.
- Cluster layer (insert/select gateways, HA by replication fan-out) — separate plan.
- Trigram/bloom acceleration for undeclared substrings (presence only, never ranges).
- Compression, encryption.

---

## 15. Known risks ⏳

- Out-of-order logs widen time bounds, weaken pruning. Policy undecided (accept widening vs lateness bound).
- String truncation collisions: covered by re-verify. Range bounds on truncated keys: encode the query bound with the same truncation, take the inclusive slice, re-verify filters the edges — no separate rule needed.
- Recovery time on large active segments unquantified.
- Extraction CPU under high line rates: Aho-Corasick amortizes, unbenchmarked.

---

## Deviations from `prompt.md`

1. Page checksum is **full-page**, not header-only. `CalculateChecksum`/`Validate` differ from prompt.md.
2. Record format adds **StreamID uint32** after ServiceID; layout and `EncodedSize` differ by 4 bytes.

Everything else in prompt.md Phase-1 scope stands.
