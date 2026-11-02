# logd — Master Design Document

Authoritative design spec for `logd`, a log storage daemon in Go with a Loki-compatible API. Supersedes prior notes and the original `prompt.md` where they conflict (see "Deviations" at the end). Phase build prompts must conform to this document.

Status markers: ✅ locked · ⚙️ resolved this revision · ⏳ open / future.

---

## 1. Purpose, differentiator, non-goals

`logd` stores logs and serves them over a Loki-compatible API. Differentiator: **index-accelerated range predicates on values extracted from unstructured log text at ingest, by declared templates**, without the client pre-structuring the data. A template `took {ms:int}ms` extracts `247` from `took 247ms` as the line arrives; `latency_ms > 200` is then answered from a sorted per-segment index (§6.3) instead of by reading every line in the time range.

What the others do with the same question:
- **Loki** indexes stream labels; its bloom filters answer presence, not ranges. A value inside the line is extracted at query time by a parser stage (`| pattern`, `| regexp`) and compared line by line.
- **VictoriaLogs** does support numeric range filters (`field:>N`, `field:range(a, b)`), and its blocks keep per-column min/max for numeric fields, so it can skip blocks — for a field that was *stored as a field at ingest*, i.e. sent structured. A number inside unstructured text must be pulled out at query time with `extract`/`extract_regexp`, and no index helps with that.
- **ClickHouse, Quickwit, Elasticsearch** need the value as a pre-structured typed column or field.
- **PostgreSQL** with a STORED generated column (regex extraction at insert) and a B-tree on it achieves the same access path for one field. It needs a schema change per field, and it is not a log store: no Loki API, no label/stream model, retention by partitioning. The testbed runs it as the relational baseline (`testbed/README.md`, E-3).

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
- **Manifest (per shard):** per segment — ULID id, state (active|sealed), MinTS/MaxTS, file paths, and the **indexed-field schema the segment was built with** (load-bearing for §7.5). Also, as implemented: the record count, the data-page count and compressed block size (the layout the cost guard prices, §9.3), the label allowlist the segment was written with and the label keys that hit their cap (§6.2), and whether its index is complete. Crash-safe via temp + fsync + rename + dir fsync.

---

## 6. The three indexes

Execution order: **time → label → typed-value → line filter** (cheapest pruning first).

### 6.1 Time ✅
Page headers + manifest carry `[MinTS, MaxTS]`. Prune segments, then pages, by bounds. Risk: out-of-order arrivals widen bounds (§16).

### 6.2 Label index ✅ (structure) / ⏳ (encodings)
Equality/set questions. Per segment: stream dictionary (canonical label set ↔ StreamID; keys sorted before canonicalization), posting lists (StreamID → sorted record offsets), service lookup. All snapshotted on seal. Not sorted by value — equality wants maps, not binary search. Serves `/labels` and `/label/<k>/values` without log scans. Cardinality guard: 5–10 label keys; high-cardinality values belong in `Extra`.
Posting-list encoding ⚙️: delta + varint over sorted ascending offsets (standard inverted-index practice; decoded sequentially).
Cardinality guard ⚙️: label keys are an explicit config allowlist; undeclared keys → `Extra`. Per declared key, max distinct values per segment (default 1000). On breach: new values stop creating streams, the key=value routes to `Extra` for those records, a counter increments. Never an ingest error, never a silent drop. ✅ The counter: per label key, the records whose value the cap kept out of the index since the daemon started, served as `label_cap_drops` at `/logd/api/v1/index_stats` (per tenant with multitenancy on). It is written only when a value is dropped, and a crash-recovered segment's rebuilt index does not count its records again. The seal also logs the segment's breaches.

### 6.3 Typed-range index `.tidx` ✅
Range questions. Per (segment, indexed field), one file. Record: `[16 B order-preserving key][8 B segment-relative record offset]`, sorted by key. Active segment accumulates unsorted pairs in RAM, queryable in place while it is active (§9.2); sorted and flushed on seal. Record-level pointers (precision serves the differentiator; re-verify handles lossy keys; page-level is the fallback lever if disk size becomes a problem).

Key encodings (BigEndian is load-bearing — byte order = value order):
- **int** → flip sign bit (add 2^63), BigEndian, left-pad to 16 B.
- **float** → sign bit 0: flip sign bit; sign bit 1: flip all bits; BigEndian. Bounded precision in v1 — no exact-equality guarantee on far-apart decimals.
- **str** → the first 16 bytes verbatim (plain truncation, which may split a multibyte rune), right-pad `0x00`. **Lossy** → re-verify mandatory. ⚙️ Changed at implementation: the original rule — back up to a rune boundary with `utf8.RuneStart` so the key stays valid UTF-8 — is **non-monotonic**. A 17-byte value whose cut lands mid-rune would drop that rune and pad, sorting *below* a 16-byte value it is greater than: a false negative on a range. Order preservation wins; the key is an opaque sort prefix (`core/index/key.go`).
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
| Types (v1) | `int`, `float`, `str`, `uuid`; extensible. Numeric → range ops; str/uuid → equality/presence only. ⚙️ As implemented, every type takes `=`, `!=`, `<`, `<=`, `>`, `>=`. Numbers order by value; `str` orders byte-wise (lexicographic UTF-8) and `uuid` by its 16 raw bytes. A `str` key is a lossy 16-byte prefix (§6.3), so a range on it is pushed down with its strict bounds widened to inclusive — a value whose truncated key equals the bound's is kept — and every candidate is re-verified against the full value; `=` likewise returns every record sharing the prefix, re-verified. int, float and uuid keys are lossless and need neither. |
| Literal case | Case-sensitive (matches Loki `|=`; byte-exact). |
| Naming | Field names and template names unique across config. Queryable field = `<template>_<capture>` (`latency: took {ms:int}ms` → `latency_ms`). Duplicate template names, duplicate queryable fields and duplicate literals are config errors. |
| Overlapping declarations | Error at config load when two templates have the same **normalized shape**: the same literal fragments byte for byte, with captures of the same types in the same places. Capture names do not count, so `took {a:int}ms` and `took {b:int}ms` overlap, while `took {a:int}ms` and `took {a:float}ms` do not. Subsumption — one pattern matching a subset of another's lines — is **not** detected (⏳ a possible refinement). Shared anchors between a literal and a template are legal — both index on match. |
| Extraction failure | Anchor matches but capture isn't the declared type → **no index entry, line stored normally** (found by scan). Never an ingest error. Must increment a per-template failure counter — silent failure is a debugging trap. ✅ Implemented with three counters (`/logd/api/v1/index_stats`): an attempt is a *candidate* once the anchor aligns; a value that does not parse, or parses only partway (`took 12.5ms` under `took {ms:int}ms`), is a *failure*. Only records the writer accepted are counted. |
| Multiple matches per line | Index every occurrence (multiple `.tidx` entries → same offset). No cap in v1: a cap creates false negatives unless the field is flagged partial. |
| NaN / ±Inf | Not numbers: extraction failure for the numeric field (unindexed). Never silently indexed into a str field. Negative zero encodes equal to zero. |
| Template params | v1: **parse-and-ignore** — `min`/`max`/`max_len` are accepted in config so adding semantics later isn't a format break, but enforce nothing. Correctness never depended on them (re-verify covers it). Validation + encoding hints (e.g. `max_len ≤ 16` ⇒ lossless str keys) deferred. |
| Anchor rule | Every template needs ≥ 1 literal fragment ≥ 3 B (Aho-Corasick anchor). Unanchored patterns rejected. As implemented, a template must also *start* with a literal (its occurrences are the candidate start positions). |
| Adjacent captures | `{a:str}{b:int}` rejected — ambiguous without a literal separator. Likewise a numeric capture followed by a literal starting with a byte its scanner would consume (a digit; for float also `.`, `e`, `E`), which would swallow the separator and silently lose every match. |

### 7.3 Config changes & schema history ⚙️ resolved

Config changes apply **only to future ingestion**. Old segments are never reindexed or stripped.

Mechanism: each segment's manifest entry records the schema it was built with (§5), including the pattern that defined each field. ⚙️ Revised at implementation (F-03): the planner consults **both the segment's schema and the live config**. A typed predicate is pushed into a segment's index only when the field is in that segment's schema *and* the live config defines the field with the same pattern the index was built from; every other segment is scanned for it, as is a segment that recorded no pattern. The original rule — the segment's schema only, never the live config — was unsound: the scan path extracts with the live config, so an index built from an edited or removed template answers for a different definition of the field than the scan does, and `Execute` would no longer equal `ExecuteScan`. Label predicates follow the same idea (F-02): a key is pushed only if it was in the allowlist the segment was written with.

Consequences: an edited or removed template no longer accelerates queries on the segments built with it — they are scanned, and the scan extracts with the live config (for a removed template, nothing); segments written after the change use their own index. A query spanning the change gets mixed performance — correct, documented behavior.

### 7.4 Ingestion flow ✅

1. Compat translates wire format → `LogEntry` (labels → StreamID/ServiceID; high-cardinality → `Extra`).
2. **Extraction in handler goroutines** (not the writer): Aho-Corasick over `Message`, typed parse per capture (template params are parsed and ignored, §7.2). Produces `(field, typed_value)` pairs.
3. Bundle `(LogEntry, values)` → channel → shard's single writer.
4. Writer: encode, place in RAM page, flush whole pages; record offset = segment-relative position. Append `(key, offset)` to typed-range buffer; update stream dictionary + posting list; update page bounds.
5. Every record is stored identically — indexing is additive metadata; one storage path.

### 7.5 Seal ✅
Sort typed-range buffer → write `.tidx` + dictionaries + posting lists → fsync → fsync dir → rename → manifest update (including schema) → drop RAM buffers.

As implemented, two orderings make a failure at any step leave the old state: the next segment's `.log` and `.idx` are created (and the directory fsynced) *before* the current segment seals, so a failed create leaves the current one open and active; and the manifest is saved with the seal applied *before* the seal takes effect in memory, so a failed save never shows queries a sealed segment whose `.tidx` lacks the records appended after the attempt. The seal sorts a copy of the buffer, not the buffer, which queries may be reading (§9.2).

Triggers ⚙️: the active segment seals at a page boundary when it reaches `segment_size_mb`, when its in-RAM index exceeds `index_mem_budget_mb` (an absent key or 0 means no cap, the code's default; the shipped configs set 256), or when its first record is older than `max_segment_age_minutes` (default 60; 0 = off). The age trigger is checked on every flush tick, so an idle segment seals too. Without it a low-volume shard kept one segment active for days — uncompressed, indexed only in memory, and immune to retention, which deletes sealed segments only. A recovered segment's age is its first surviving record's ingest time; an empty segment never seals for age.

---

## 8. Durability & crash recovery ✅

- fsync: group commit (per page or per N ms), documented loss window. ⚙️ Resolved as per N ms: a written page is fsynced once `sync_interval_ms` (default 50) has passed since the last fsync, checked after each page write and on every flush tick; a negative value fsyncs every page. Seal, rotation and Close always fsync.
- **Acknowledgement.** A push is acknowledged once its entries are queued for the shard's writer, not once they are durable. Every write either enters the queue before Close starts the final drain or is refused (`ErrWriterClosed`); the daemon closes its writers only after the HTTP server has let in-flight pushes finish.
- **Loss window, by stage.** Where an acknowledged record is decides what can take it:
  1. *Queued* for the writer (up to 4,096 records per shard): lost on a process crash.
  2. *In the page being assembled in RAM*, which is written when it fills or on the flush tick (`flush_interval_ms`, default 500): lost on a process crash. At a low ingest rate this stage, not the fsync interval, bounds the loss: at most one page, or one flush tick of records, whichever is fewer.
  3. *Written but not yet fsynced*, in the kernel's page cache: survives a process crash, lost on a machine crash (power cut, kernel panic). Up to `sync_interval_ms` plus one flush tick.

  A process crash loses stages 1–2; a machine crash 1–3. Nothing becomes corrupt either way: a partially written page fails its full-page CRC and is truncated on recovery.
- **Measured (E-8, `testbed/results/2026-11-02-e8/`).** `testbed/scripts/e8-crashloss.sh` kills the real daemon with kill -9 at a random moment, restarts it and counts the acknowledged lines missing; 10 runs per setting. At the defaults (50/500) it lost a median of **30 lines at 10,000 lines/s (12–44, ≈3 ms of data)** and **22 lines at 200 lines/s (6–42, ≈110 ms, max 210 ms)**. With a 50 ms flush tick (10/50) the 200 lines/s loss fell to 4 lines (2–8, ≈20 ms); a kill -9 spares written pages whatever the fsync interval, so that is the tick's doing (stage 2). Every loss was a clean tail, with no duplicates; restart to `/ready` took a median 220 ms and 48 ms. These numbers are indicative: the machine was under load from other builds (load average ~7 on 8 threads). A kill -9 cannot show stage 3, because the page cache outlives the process.
- **Power cut, approximated.** `TestMachineCrashLossApproximation` (`core/storage/machinecrash_test.go`) drives the writer's own steps in real time under the real group-commit policy and, at a random moment, truncates a copy of the shard to its last fsynced page and recovers it as a restart would. It is the worst case the policy allows — it assumes the kernel wrote back nothing unsynced on its own and loses every page since the last fsync whole — and it has no queue in front of the writer, so its process-crash figure is the open page alone. At the defaults it lost a median of 172 lines at 10,000 lines/s (≈17 ms, max 48 ms) and 38 at 200 lines/s (≈190 ms, max 245 ms), on the same loaded machine (`machine-crash-approx.md`). Measuring a real power cut needs a VM that can be reset or a block device that drops unsynced writes.
- **Whole-page writes only.** A successful page write is the point of no return: the page number advances and the page is reset before the sparse-index append and the fsync, so a page is never written twice. A failed write leaves the page in RAM and is retried, so a disk that keeps failing becomes backpressure on ingest rather than silent loss (once Close has begun, the writer makes one last attempt and then gives up, so Close cannot hang on a dead disk).
- Atomic seal per §7.5; crash leaves old state or new state, never in-between.
- **Recovery:** read manifest → trust sealed segments → scan every page of the active segment → truncate the **torn tail**, the run of invalid pages after the last valid one → **re-run extraction to rebuild the in-RAM typed-range buffer, stream dictionary, and posting lists** → resume. As implemented:
  - An invalid page *before* the last valid one is damage, not a torn write (pages are written in order). It is kept, logged and skipped by readers, instead of truncating every valid, fsynced page after it.
  - Re-extraction runs through the writer's `Options.Reindex` hook, which the daemon sets; the recovered segment then seals fully indexed and is queryable from memory meanwhile (§9.2). Without the hook its pre-crash records are not in the index, so it seals with an empty schema and is scanned — never given a partial index.
  - A manifest naming an active segment whose file is missing (deleted, lost with a disk, or a crash between the manifest save and the create) loses that segment's unsealed records but no longer stops the shard: the entry is dropped, with a log line, and a fresh segment is started. Any other failure to read the active segment still stops the shard, since its data may be recoverable.
  - A recovered segment keeps its age, read from its first surviving record (§7.5).
- Recovery cost is bounded by max segment size ⏳ (bound unquantified; E-8's restarts, with small active segments, are no measure of a full 64 MB one).

---

## 9. Native query model ✅ (core-only terms)

Query = time range + predicate set: label-equality, line-contains (substring/regex), typed-value range. Results: matching records, time-ordered, limit + direction. Live tail = a query with no end bound, served by writer fan-out (§11).

Execution per shard:
1. Time: manifest prunes segments.
2. Label: resolve equality → StreamIDs → union posting lists → candidates.
3. Typed value: **check the segment's schema (§7.3)** → if indexed, binary-search `.tidx` for bounds (the active segment: one pass over its in-RAM buffer, §9.2), collect offsets, intersect with candidates; else mark segment for scan.
4. Cost guard (§9.3): a bounded query whose candidates over-fetch its limit and cover nearly the whole segment scans it instead.
5. Decode survivors; apply line filters; **re-verify every predicate the index has not proven** (lossy str keys; a predicate answered by a lossless int/float/uuid key is settled and skipped).
6. Merge across segments and shards, time-order, apply limit/direction.

### 9.1 Unindexed queries ✅ (defined behavior, not an error path)
- Arbitrary substring (undeclared): full scan within time-pruned range. Supported always.
- Range on an undeclared field: scan + extract at query time + filter. Supported, O(n). ✅ Implemented as the dynamic predicate (`query.DynamicCompare`, §9.3); in LogQL, a parser stage (`| regexp`, `| pattern`, `| logfmt`, `| json`) followed by a filter on what it extracts.
- **Fast ranges require a declared template.** Honest limit; document it.
- Missing/corrupt/version-mismatched `.tidx`: silently degrade to scan for that segment.
- A segment whose *data* cannot be read at all — a compressed segment whose header or block directory fails its checks, or one whose files are gone — is skipped, identically on the index and the scan path, and logged once per segment. Its records are lost to every reader, the same degrade a corrupt block gets. Failing instead would turn one bad byte into an error for every query over the segment's range, and the paths would disagree: an index lookup with no candidates there never opens the file. (Retention deletes a segment's files before its manifest entry, so a file vanishing mid-query is not an error.)

### 9.2 Active-segment queries ⚙️ revised: in-memory index (was: scan)

**Decision changed, because it was measured.** The first resolution was to always scan the unsealed segment — bounded size, predictable cost, no insert-time structure — with "background incremental sort" as the escape hatch if profiling demanded. The evaluation demanded it: with ~24% of a 500k-line corpus in the active segment (T4), logd's latency rose 2.2–6.4× over the all-sealed layout and fell behind Loki at every selectivity. A live deployment always has an active segment, so that is the steady state, not an edge case. No sort was needed: the writer already holds the segment's typed-range buffer and label postings in RAM for the seal, and a linear pass over them is cheap next to a scan that decodes and re-extracts every record.

**Mechanism.** The planner pushes typed, literal and allowlisted label-equality predicates into the active segment exactly as into a sealed one; only where the offsets come from differs.
- *Concurrent reads.* One writer appends; queries read while it does. The writer appends a record's keys and posting under the lock it already takes once per record (for live label discovery), so the hot path gains no lock for labelled records (and a record with neither keys nor labels, which changes nothing a query reads, takes none). A query copies the slices' headers under that lock and reads the elements without it: an appended element is never written again (an append writes past the copied length or moves to a new array, leaving the old one intact), and the seal now sorts a *copy* for the `.tidx` instead of the buffer in place.
- *Readability boundary.* Buffered offsets run ahead of the file: a record's keys are buffered when it enters the page in RAM. After each successful page write the writer publishes the next page number; a query reads that boundary **first**, then copies the buffers, and keeps only offsets below it. Every page below a boundary it observed was written after its records were buffered, so the copy holds all of them (reading the other way round could put a page inside the boundary with its records missing — a false negative). The scan path reads the same thing — the pages in the file — so both paths see the pages written so far, and neither sees the page in RAM. (A scan opening the file in the instant between a page's write and the boundary store sees that page one query earlier; with the writer idle the two paths read identical pages, which is what the differential tests compare.)
- *Segment identity.* The view is tied to a segment ID. The writer publishes it before the manifest names the segment and withdraws it once the seal is committed. A query that listed the segment as active before it sealed finds no view, re-reads the manifest, and plans against the now-sealed segment's sidecars; a view already taken stays valid for its segment, sealed or not.
- *Lookups.* Equality and ranges are one linear pass over the field's entries comparing 16-byte keys; label equality unions the matching streams' posting lists. Both implement the same lookup interface as the `.tidx`/`.lidx` readers, so the planner builds one lookup with one set of bound-widening and settled/residual rules for either.
- *Identity rules unchanged.* The view stands in for the facts a seal would record — every indexed field with its pattern, the label allowlist the writer derives label sets with, the keys capped so far — and the planner applies the same F-02/F-03 checks to them. Unlike a sealed schema it lists fields with no entries yet, which is sound because the buffer holds every value of every record: an active segment can prune to zero.
- *Recovery.* A crash-recovered active segment re-indexed by the reindex hook is complete and gets a view; without the hook its index misses the pre-crash records, so none is offered and it is scanned (`not_indexed`).
- *Explain* reports `state: active` with reason `indexed_in_memory`, or the scan reason.

**Cost.** Write path: one lock per indexed record, which labelled records already took; the writer's per-record CPU measured within noise (±10%) of the old code, interleaved on one machine. Read path: a lookup is one linear pass over the field's entries (~1 ms per 200k), against a scan that decodes and re-extracts every record. On a 200k-record active segment (`BenchmarkActiveSegment`) a range matching 5% / 1% / 0.1% returns every row in 21 / 10 / 9 ms against the scan's 461 / 494 / 447 ms; at limit 100 it takes 9.5 vs 33 ms (1%) and 7 vs 196 ms (0.1%), while at 5% the cost guard (§9.3) hands the segment to the scan — after the lookups, which cost 18 ms against the scan's 11. Memory: nothing is copied but slice headers; a view pins the buffers it read until its query ends, so a sealed segment's buffers can outlive the seal by one query.

### 9.3 Query decisions ⚙️ resolved

**Matcher (LogQL pattern ≡ declared template):** strict structural equality. Normalize both sides to (literal fragments, capture names); match iff fragments are byte-identical, captures align positionally, names are equal. Types come from config (queries carry none). Any deviation — whitespace, extra fragment, renamed capture — is no match → scan. Rationale: a false match uses the wrong index (wrong answers); a false no-match only costs a scan. Upgrade path: explain output (index-hit vs scan per query) so users can see why a query scanned.

✅ Implemented (`compat/loki/logql/structural.go`, which carries the full argument). Precisions made at implementation:
- *Names:* a capture's name is compared with the template's queryable field name, `<template>_<capture>` — the name the native syntax and results use. `<_>took=<latency_ms>ms<_>` matches template `latency: took={ms:int}ms`.
- *The ends:* the pattern must start with `<_>` (Loki anchors a pattern that starts with a literal at the start of the line; a template matches anywhere). A trailing `<_>` is dropped: Loki never anchors the end, so the pattern extracts the same labels with or without it. A template ending in a capture never matches (Loki's last capture runs to the end of the line). The literal after an int/float/uuid capture must not start with a byte the value may contain (`+`, `-`; hex for uuid).
- *Scope:* the pattern must be the query's only parser stage, and every filter on the template's fields must be `=`, `<`, `<=`, `>`, `>=` with a number on an int/float field (a bound of the field's type) or `=` with a non-empty string on a str field. Anything else (`!=`, a regex, `= "200"` on a number) gives the match up for the whole query.
- *Semantics:* on a *conforming* line — the template matches at the first occurrence of its first literal and nowhere later — Loki's capture equals the template's value and the comparisons agree (ints within ±2^53). Elsewhere they differ: several occurrences (Loki reads the first, the template any), a first value not of the type (`took=12.5ms` under int; Loki converts it, or keeps an unconvertible one flagged `__error__`), a missing or displaced literal, ints beyond 2^53. No rewrite closes that gap, so **a structural match uses the template's semantics** — the rows of the native form — and a test pins the differences. Result labels are still re-extracted with Loki's semantics.
- *Explain:* `/logd/api/v1/explain` reports per parser stage and filter whether it was mapped to a template (index) or runs as a dynamic predicate (scan), and why.

**Operator/type mismatch:** two-level.
- Core native API is **strict**: an operator unsupported by the field's declared type is a plan-time error. Core separately offers an explicit *dynamic predicate* (decode → attempt conversion → filter), a scan-time operation. (As implemented, every declared type accepts every comparison operator, §7.2, so what the plan-time check rejects is a predicate with no meaningful answer: a non-finite float bound, a typed comparison whose value is of another kind than the one the live extractor declares for the field (`latency_ms > 2.5` on an int field, which compared by kind alone), and a dynamic predicate with no extractor, a non-finite numeric bound, or a regex under an ordering operator. The Loki layer converts a number literal to the field's kind — `| dur_s > 2` on a float field compares with 2.0 — so valid LogQL never builds the mismatched comparison.)
- Compat layers choose their own mapping. Loki layer maps numeric filters on untyped/str fields to the dynamic predicate (reproduces Loki's convert-per-line behavior). Other layers may map strictly.

✅ Implemented. `query.DynamicCompare{Name, Extract, Op, Value, Numeric, Re}`: `Extract(entry, resolver) (string, bool)` is supplied by the caller; numeric comparisons convert both sides to float64 per record and fail on a missing or unconvertible value whatever the operator; string comparisons read a missing value as `""`; a regex matches or must not. The planner has no case for it, so it is always a residual — evaluated on scan, on re-verify and in tail, keeping `Execute == ExecuteScan`. `MetricQuery.SeriesLabels`/`UnwrapValue` let metric queries group by and unwrap such values. The Loki layer maps every label filter after a parser stage, on a name the stage may extract, to it. Deliberate difference from Loki: a numeric filter on a value that does not convert does not match, where Loki keeps the line flagged `__error__=LabelFilterErr` (and skips numeric filters on a line that already carries an error); the rows agree for a query with `| __error__=""` after its last numeric filter.
Core stays protocol-agnostic (invariant 7); compat semantics live in compat.

**Predicate ordering:** fixed pipeline, no cost-based planner in v1: time → label (union posting lists per label match, intersect across labels) → typed-range (intersect with prior set) → decode-time line filters + re-verify. LogQL pipelines are AND; OR exists only inside regex, evaluated at decode. Order is correctness-independent (set intersection), so selectivity-aware reordering can be added later without breaking anything. ⚙️ As implemented, the lookups for one segment all run first (one that matches nothing ends the work at once), and the intersection is a hash set built from the *smallest* candidate set, which the larger ones only probe. In predicate order, `{app="checkout"} | latency_ms > T` built a set of every record in the segment — the label matches all of them — to keep the few percent the range matched. The candidates are not sorted there; the fetch sorts its own copy.

**Cost guard ⚙️ (index fetch vs. scan, per segment):** a bounded query scans a segment instead of fetching its index candidates by offset only when both hold: the candidates exceed √(limit × the segment's records), and they touch more than 90% of the segment's I/O units (compressed blocks, or pages when uncompressed — taken from the layout the segment records in the manifest, not from configuration; for the active segment, the pages its in-memory view covers). An unbounded collection (metric path) never scans for it. Rationale: an offset fetch reads each page or block once, in order, so it never costs more I/O than a scan; it decodes only its candidates, while a scan re-extracts every record. The scan's only advantage is its top-K early exit, which with evenly spread matches decodes ≈ limit·n/candidates records — equal to the fetch's `candidates` at √(limit·n); the coverage term is what makes "evenly spread" hold. Measured, not guessed (`TestCostGuardSelectivitySweep`, `testbed/results/costguard-sweep-2026-11-02.txt`): the crossover moved ~2–3× for a 10× limit, as √ predicts and a fixed over-fetch ratio does not. Segments without a recorded layout never trip. Known gap: matches clustered at the end a scan starts from make the scan cheaper than the rule assumes; the index is kept then, at the cost every query paid before the guard existed.

**Still open ⏳:** merge mechanics with live tail at the seam (historical→live handoff).

---

## 10. Multi-writer / shared-nothing ✅

`data_root/<shard_id>/`, each shard self-contained (own manifest, segments). Writers = goroutines or separate processes; per-segment dictionary snapshots make segments readable anywhere (no global ID space). ULID segment IDs. Readers enumerate `data_root/*` (shard registry by convention). Fan-in at query: run §9 per shard, merge. ⏳ As implemented, the daemon opens `data_root/shard-NNNN/` for the configured `shards` count, and its query engine fans in over exactly those; enumerating `data_root/*`, for a reader in another process, is not built.

**Data directory:** single daemon-owned directory, configurable (`data_root`). Defaults: `/var/lib/logd` on Linux, volume mount in containers, PVC per instance in k8s. logd writes nowhere else; nothing else writes into it. In production, keep it on a separate volume from the sources being collected (a log flood must not fill logd's disk). A flock-based `LOCK` file at each shard root prevents two processes opening the same shard.

---

## 11. Live tailing ✅ (seam reserved)

Core primitive: subscribe to matching records at write time. The writer, after appending a record, offers it to active subscribers whose predicates match (same predicate evaluation as §9, applied to one record; no indexes involved). The writer loop must reserve this notify step — retrofitting a broadcast into a tuned hot loop is the thing to avoid. WebSocket framing and Loki `/tail` semantics: ✅ in compat (`compat/loki/ws.go`), including the close handshake — a client's Close is echoed, and a shutdown ends each tail with Close 1001. Historical→live handoff policy ⏳: a tail starts live, with no history.

---

## 12. Compat layer (not core) ✅

- **Loki push** → native ingest. Timestamp arrives as nanosecond string; structured metadata (flat string→string) → `Extra`.
- **LogQL** → native plan. `| pattern "<_>took <latency_ms>ms<_>" | latency_ms > 200` maps to declared template `latency: took {ms:int}ms` when the matcher can prove equivalence → index pushdown; else scan (§9.3). Index invisible to Grafana. ✅ Implemented with the parser stages `pattern`, `regexp`, `logfmt`, `json` (plain forms); like Loki, results carry the extracted labels (re-extracted per returned line; a name the stream already has becomes `<name>_extracted`).
- **Grafana endpoints:** `/loki/api/v1/labels`, `/label/<n>/values`, `/series` (from stream dictionary); `/query_range`, `/query`; `/tail` (WS); `/ready`; metric-query stub early (Grafana health check runs an instant metric query). Responses Prometheus-shaped. Tenancy header accepted. ✅ As implemented, the stub answers only Grafana's exact health-check expression, `vector(1)+vector(1)`, with 2; any other unparseable query is a 400. With multitenancy on, `X-Scope-OrgID` scopes every read, and the internal `__tenant__` label is never returned. Request parsing (timestamps, `since`, `limit`, `direction`, `step`, push encodings, the per-request entry cap) follows Loki's; the README's "Loki HTTP API" section lists each rule.
- OTLP, ES bulk: later. Severity strings normalize into `Level`.

---

## 13. Build plan ✅

All seven steps are done (the README's Status table tracks them as Phases 1–9, and the work since).

1. **Phase 1:** `record.go`, `page.go`, `lookup.go`.
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
- Encryption. (Compression is done: sealed segments are compressed in blocks, `compress_block_pages`, and `.tidx` files are deflated.)

---

## 15. Known risks ⏳

- Out-of-order logs widen time bounds, weaken pruning. Policy undecided (accept widening vs lateness bound).
- String truncation collisions: covered by re-verify. Range bounds on truncated keys: encode the query bound with the same truncation, take the inclusive slice, re-verify filters the edges — no separate rule needed.
- Recovery time on large active segments unquantified.
- Extraction CPU under high line rates: Aho-Corasick amortizes. Benchmarked since (E-4: `core/ingest/bench_templates_test.go`, `testbed/scripts/rq3-templates.sh`): after the literal search became a transition table and the per-line allocations went, `Engine.Extract` takes about 0.5–0.7 µs per line with one template and 2.0–2.4 µs with twenty, 80% of lines matching, on a loaded machine. The recorded E-4 run (`testbed/results/2026-11-02-rq3/`) predates that work; both need a rerun on a quiet machine.

---

## Deviations from `prompt.md`

1. Page checksum is **full-page**, not header-only. `CalculateChecksum`/`Validate` differ from prompt.md.
2. Record format adds **StreamID uint32** after ServiceID; layout and `EncodedSize` differ by 4 bytes.

Everything else in prompt.md Phase-1 scope stands.

---

## Deviations and changes after v1

v1 is the tag `thesis-eval-1` (commit `8240b62`), the baseline against which the final evaluation measures what the review fixes and the features since changed; the testbed runs it beside the current daemon (`testbed/docker-compose.v1.yml`).

Deviations from this document that v1 already had, recorded where they were decided:

1. **str keys** are plain 16-byte truncation, not a rune-boundary prefix, which is non-monotonic (§6.3).
2. **Overlap** means an identical normalized shape; subsumption is not detected (§7.2).
3. **Queryable field names** are `<template>_<capture>` (§7.2).
4. **Every type takes range operators**; `str` ranges are pushed down with inclusive-widened bounds and re-verified (§7.2).
5. **The planner consults the live config too**: a typed predicate is pushed only where the live pattern equals the one the segment's index was built from (F-03), a label only where the segment's recorded allowlist has the key (F-02) (§7.3).

Changes after v1, each explained in the section named:

1. The active segment is answered from the writer's in-memory index instead of being scanned (§9.2).
2. The LogQL parser stages and the dynamic predicate exist; a `| pattern` structurally equal to a declared template uses its index (§9.1, §9.3, §12).
3. The cost guard scans only when the candidates over-fetch √(limit·n) *and* cover more than 90% of the segment's I/O units, priced from the layout the manifest records (§5, §9.3); it used to scan above half the segment's records.
4. The active segment also seals by age, and rotation is checked on the flush tick, not only when a page fills (§7.5).
5. Durability and recovery: writes are refused once Close begins; a failed page write is retried, never reset or written twice; only the torn tail is truncated; a missing active-segment file no longer stops the shard; the seal creates the next segment first and commits in memory only after the manifest save (§7.5, §8).
6. A segment whose data cannot be opened is skipped on both paths and logged once (§9.1); counts in `.tidx`, `.lidx` and `.logz` headers are checked against the bytes that hold them before they size or index anything.
7. Candidate sets are intersected smallest first (§9.3).
8. The Loki layer reads requests as Loki does and answers only Grafana's exact health check (§12).
9. Extraction searches literals with a transition table and matches without per-line maps or slices (§15).
