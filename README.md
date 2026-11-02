# logd

A lightweight, single-node log storage daemon with a Loki-compatible API. Its
differentiator is **index-accelerated range predicates on typed values extracted from
unstructured log text at ingest, by declared templates** — the template `took {ms:int}ms`
pulls `247` out of `took 247ms` as the line arrives, and `latency_ms > 200` is then a
lookup in a sorted per-segment index, without the client having to send the value as a
structured field. Loki extracts such a value at query time and compares it line by line.
VictoriaLogs has numeric range filters and skips blocks by their per-column min/max, but
only for fields stored as fields at ingest; a number inside the message text needs
`extract` at query time, and no index helps with that. ClickHouse and Quickwit need
pre-structured columns. PostgreSQL with a generated column and a B-tree reaches the same
access path for one field, at the price of a schema per field and without a log store's
API, label model or retention (design §1).

This tree is the **fresh `core/` + `compat/` rewrite** on the master design
(`docs/logd-design.md`). It treats the previous implementation as a *quarry*, not
trash: correct, tested subsystems are ported; subsystems that deviate from the spec
are rebuilt against it. See `docs/logd-design.md` §3 for the target layout and
`docs/PHASE-1-2-SPEC.md` for the build spec this foundation was written against.

## Layout (design §3)

```
core/                     protocol-agnostic; never imports compat/
  model/    LogEntry, LogLevel
  storage/  record · page · segment · manifest · writer · lookup · sparse time index · compression
  index/    typed-range .tidx + the active segment's in-RAM buffer  (Phase 3)
  extract/  template compiler + Aho-Corasick extraction engine       (Phase 3)
  label/    stream dictionary + posting lists (.lidx)                (Phase 6a)
  ingest/   native ingest API                                        (Phase 3+)
  query/    native query engine/planner, metric evaluation, tail     (Phase 4-5, 8)
  config/   daemon + extraction/label config                         (Phase 3)
compat/                   protocol translations (Loki today); may import core
  loki/     HTTP server, Grafana discovery, WebSocket tail            (Phase 6b, 9)
  loki/push · loki/logql                                              (Phase 6b)
cmd/logd/                 the daemon: load config, wire core → compat, serve
```

## Status

| Phase | Scope | State |
|---|---|---|
| 1 | record + page + lookup | ✅ done |
| 2 | segment + writer + durability + manifest | ✅ done |
| 3 | extraction + key encoding + typed-range `.tidx` | ✅ done |
| 4 | native query + scan path + live tail | ✅ done |
| 5 | planner + index pushdown + re-verify | ✅ done |
| 6a | native label index (stream dict + posting lists) | ✅ done |
| 6b | Loki compat (push, LogQL→plan, Grafana discovery) | ✅ done |
| 7 | hardening: cost guard, retention, live label discovery | ✅ done |
| 8 | metric queries (count_over_time/rate/unwrap aggs, vector agg + by/without) | ✅ done |
| 9 | WebSocket live `/tail`, RAM-budget (seal-early on index memory) | ✅ done |

### Since Phase 9

The phases built the daemon; the evaluation and a code review then changed it. Each item has
its own section below.

| Change | What it does | Section |
|---|---|---|
| Active-segment index | the unsealed segment is answered from the writer's in-memory index instead of a scan | Querying the active segment |
| Parser stages | `\| pattern`, `\| regexp`, `\| logfmt`, `\| json`; a pattern equal to a declared template uses its index | LogQL API: parser stages |
| Cost guard v2 | a segment is scanned only when its candidates over-fetch the limit *and* cover nearly all of it | Cost guard v2 |
| Seal by age | `max_segment_age_minutes` (default 60) seals a segment that has gone quiet | Seal by age |
| Review fixes | durability, recovery, corrupt and hostile files, query edge cases, Loki API compatibility | Review fixes, Loki HTTP API |
| Extraction speedup | the literal search as a transition table, no per-line maps or slices: 3.4–4.5× faster | Extraction speedup |

### What Phases 1–2 deliver

- **Record codec** (`core/storage/record.go`): 27-byte fixed prefix `[8 TS][8 IngestedAt][1 Level][2 ServiceID][4 StreamID][2 MsgLen]…[2 ExtraLen]…`, big-endian, fully bounds-checked (`ErrMalformedRecord` on truncation, never a panic).
- **Full-page checksum** (`core/storage/page.go`): CRC32-IEEE over the entire 4096-byte page, so corruption in *record* data — not just the header — is detected. This is what makes torn-page recovery possible.
- **Durable writer** (`core/storage/writer.go`): one writer goroutine, whole-page appends, group-commit fsync (`sync_interval_ms`, default 50 ms; negative fsyncs every page), rotation by size (later also by index memory and segment age), atomic seal (tmp → fsync → rename → dir fsync). Extraction is deliberately *not* here — the write path carries a reserved seam for pre-extracted values (§7.4) and a reserved live-tail notify hook (§11). What a crash can lose, stage by stage, is in design §8.
- **Crash recovery** (`NewWriter` + `OpenSegment`): on restart, the active segment is rescanned, its real time bounds are republished to the manifest (fixing the old "SIGKILL hides recent data from time-range queries" bug), and a torn tail — the invalid pages after the last valid one, detected via the full-page checksum — is truncated. An invalid page *before* a valid one is damage, not a torn write: it is kept, logged and skipped by readers, rather than truncating every fsynced page after it. A manifest naming an active segment whose file is gone loses that segment's unsealed records but no longer keeps the shard from opening: the entry is dropped, with a log line, and a fresh segment is started. Covered by `TestCrashRecoveryRepublishesBounds`, `TestTornPageTruncatedOnRecovery` and `TestMissingActiveSegmentDoesNotBlockTheShard`.
- **Per-segment schema field** in the manifest, so the future planner consults each segment's own build-time schema (§7.3) — carried in the format now even though extraction is Phase 3. (The planner that came later also requires the live config to define a field with the pattern the segment's index was built from, F-03; otherwise it scans, because the scan extracts with the live config.)

Ported verbatim from the quarry: `LookupTable` (service interning) and the sparse
time index.

### What Phase 3 delivers (the differentiator)

- **Order-preserving key encoding** (`core/index/key.go`): 16-byte keys where byte order equals value order — sign-flipped int, IEEE-754 total-order float (−0 normalized), the first 16 bytes of a str (plain truncation: backing up to a rune boundary, as the design first said, would break the order), raw uuid. Property-tested across thousands of pairs.
- **Typed-range `.tidx`** (`core/index`): per-(segment, field) sorted file of 16-byte key + 8-byte segment-relative offset records (deflated on disk since format v2 and loaded whole), CRC-headered, written atomically at seal. `Reader` does O(log n) equality and range lookups; an in-RAM `Buffer` accumulates keys per active segment, answers queries while it does (`View`), and flushes them at seal.
- **Extraction engine** (`core/extract`): config templates like `took {ms:int}ms` compile to literal fragments + typed captures; one Aho-Corasick automaton (a dense transition table since the extraction speedup) finds all literal positions in a single pass; hand-written byte scanners parse int/float/str/uuid (rejecting NaN/±Inf); the §7.2 rules (≥3-byte anchor, no adjacent captures, no leading capture, no numeric capture followed by a literal its scanner would swallow, no two templates of identical normalized shape) are enforced at compile; per-template counters — candidates (the anchor aligned), matches and failures (a value that does not parse, or parses only partway) — are served at `/logd/api/v1/index_stats`, counted only for records the writer accepted.
- **Native ingest** (`core/ingest`): runs extraction off the write path and hands the writer pre-encoded `(field, key)` pairs, which it pairs with each record's on-disk offset (the reserved §7.4 seam).
- **No-false-negative recovery**: the daemon re-extracts a crash-recovered active segment's records through the writer's `Options.Reindex` hook, rebuilding its in-RAM index, so the segment seals fully indexed (and is queryable from memory until then). Without the hook — an embedder that sets none — the segment is marked incomplete and sealed with an **empty schema**, so the planner scans it rather than trusting a partial `.tidx`.
- Verified by a **rebuild-and-compare** test: re-extracting every stored record reproduces the exact `.tidx` contents.

Query-side index pushdown is Phase 5; Phase 3 builds and verifies the index.

### What Phase 4 delivers (the query oracle)

- **Native query model** (`core/query`): a time window + AND-combined predicates (`LineContains`/regex + negations, `LabelEqual`/`NotEqual` over level/service/Extra, `TypedCompare` for `= != < <= > >=` on template fields; since Phase 9 also `DynamicCompare`, which extracts a value no template declares at query time), a limit, and a direction. Protocol-agnostic — no Loki assumptions.
- **Scan path** (`Execute` then, `ExecuteScan` since Phase 5): time-prunes segments (live manifest) and pages (header bounds), decodes records, evaluates every predicate on the **fully decoded record with exact values**, then global-sorts by event time and applies the limit. Because it compares real values (not the lossy 16-byte keys), it is the exact correctness oracle Phase 5's index result must equal.
- **Live tail** (`Engine.Tail`): activates the writer's reserved `notify()` seam — a concurrency-safe subscriber registry fans each newly-written record out to matching tailers, non-blocking (a slow tailer drops rather than stalling the write path). WebSocket/Loki `/tail` framing came in Phase 9.
- Reads just-flushed active-segment data via the live in-memory manifest; safe to query under concurrent ingestion (verified under `-race`).

### What Phase 5 delivers (the payoff)

- **Query planner + index pushdown** (`core/query/plan.go`): per time-pruned, indexed segment (sealed, or since Phase 9 the active one through its in-memory index), a `TypedCompare` (`= < <= > >=`, kind-matched) or a `LineContains` on a configured literal is pushed into the field's `.tidx` — binary-search equality or a range-walk with **inclusive-widened bounds** (so a lossy string key equal to the bound is never dropped). Candidate offset sets are intersected across predicates, smallest set first.
- **Materialize + re-verify** (`storage.FetchRecords`): only candidate records are read (each page or block once, in offset order), then **every predicate the index has not proven is re-applied exactly** — lossy-key false positives are filtered, while a predicate answered exactly by a lossless int/float/uuid key is settled and not re-extracted — so the result is identical to a scan.
- **Always-correct fallbacks**: a non-indexed (crash-recovered, never re-indexed) segment, a `!=`, a field absent from a segment's schema, or a missing/corrupt `.tidx` all degrade to scan (never a wrong answer). A cross-kind predicate — a value of another kind than the field's declared type — has no meaningful answer and is refused at plan time.
- **`Explain`** reports index-vs-scan per segment; **`ExecuteScan`** forces the scan path.

The contract is enforced by a **differential oracle test**: `Execute(q)` (pushdown) must equal `ExecuteScan(q)` (scan) for a large battery of queries over multiple segments — typed ranges/equality/`!=`, lossy string ranges beyond 16 bytes, negative-int bounds, literals, labels, intersections, limits, directions, degrade-to-scan, cross-kind (refused by both), and config change. It does, for every one.

### What Phase 6a delivers (native label index)

- **`core/label`**: a per-segment stream dictionary (canonical label set ↔ `StreamID`) + posting lists (`StreamID` → delta+uvarint sorted offsets), snapshotted at seal in a CRC'd `.lidx`. Fills in the `StreamID` reserved on `LogEntry` since Phase 1.
- **Derived accelerator**: the label index is built from the **allowlisted** keys already in each record's `Extra` (read with `model.ExtraLabel`, the parser the query resolver uses too), so the scan path stays the untouched oracle and the index just accelerates — the allowlist is the primary cardinality guard.
- **`LabelEqual` pushdown**: an allowlisted, non-reserved key resolves to candidate offsets via the segment's `.lidx` (intersected with typed/literal lookups), then the survivors are re-verified exactly. Reserved keys (`level`, `service`: the record's own label, falling back to `entry.Level`/`ServiceID` when it has none — values the Extra-derived index cannot hold), non-allowlisted keys, and `LabelNotEqual` degrade to scan. `Engine.Labels()`/`LabelValues()` enumerate from the sealed `.lidx` files and the active segment's live streams.
- Verified by the same differential oracle: `Execute` (label pushdown) equals `ExecuteScan` across a battery (single/combined label equality, label∩label, label∩typed, non-allowlisted, `!=`, reserved keys, numeric labels, degrade-to-scan).

### What Phase 6b delivers (Grafana can point at logd)

- **`compat/loki`** — the Loki-compatible HTTP surface. Core stays protocol-agnostic; all Loki assumptions live here.
- **Push** (`compat/loki/push`): decodes Loki push (snappy+protobuf *and* JSON, gzip/deflate `Content-Encoding` too) into native entries, labels → `Extra` (so the label index derives consistently). Hardened against decompression bombs, body-size abuse and entry-count abuse — see [Loki HTTP API](#loki-http-api).
- **LogQL** (`compat/loki/logql`): a hand-written lexer/parser/AST + an **AST→native translator**. `| latency_ms > 200` on a declared template capture becomes a native `TypedCompare` — so a Grafana LogQL query **transparently drives logd's typed-range pushdown**. Labels → equality/regex/numeric-compare; line filters → contains/regex; Loki empty-label semantics. The parser stages `| pattern`, `| regexp`, `| logfmt` and `| json` are supported too, and a Loki `| pattern` equal to a declared template uses its index — see [LogQL API: parser stages](#logql-api-parser-stages).
- **HTTP** (`compat/loki/server.go`): `/loki/api/v1/push`, `/query_range`, `/query` (+ Grafana health stub), `/labels`, `/label/{n}/values`, `/series`, `/ready`, returning Grafana-shaped JSON.
- **`cmd/logd`**: the real daemon — load YAML config, wire `core` → `compat`, serve, graceful shutdown. Verified with a live `curl` round-trip: push a line, then `query_range` with `{region="eu"} | latency_ms > 200` returns exactly the matching record as Loki JSON, index-accelerated.

Run it: `go run ./cmd/logd -config config.yaml`, then point a Grafana Loki datasource at `http://localhost:7100`.

### What Phase 7 delivers (hardening)

- **Index cost guard**: the planner's seq-scan-vs-index-scan choice, made per segment. Correctness-neutral — the differential oracle still holds — so it only changes the access path; `Explain` reports `cost_guard` when it fires. Phase 7 scanned whenever the candidates exceeded half a segment's records; that rule was measured and replaced (see Cost guard v2 below).
- **Retention**: a per-segment record count and a background sweep delete sealed segments whose data is entirely past the window (`retention_days`), plus their sidecar index files — trivial given per-segment files, no compaction. Runs on the writer goroutine (serialized with seals). Queries degrade gracefully on a retention race (a segment deleted mid-query is skipped, not an error, and logged once).
- **Live label discovery**: the active segment's label streams are tracked concurrency-safely, so `/labels`, `/label/values`, and `/series` populate Grafana's dropdowns immediately — not only after the first seal.

### What Phase 8 delivers (metric queries → Grafana graph panels)

LogQL metric queries are evaluated natively into a Loki matrix/vector:

- **Range aggregations** over a `[range]` window per stream: `count_over_time`, `rate` (count/sec), and the unwrap forms `sum_over_time`/`avg_over_time`/`max_over_time`/`min_over_time` over `| unwrap <field>` (a template capture or a numeric label). `rate(... | unwrap x [r])` is the value-rate `sum(x)/r`.
- **Vector aggregations** across streams: `sum`/`avg`/`max`/`min`/`count`, with `by(labels)` / `without(labels)`.
- **Evaluation** (`core/query/metric.go`, protocol-agnostic): step across `[start,end]`, per-stream `(t-range, t]` windows, range-aggregate, then vector-aggregate — Prometheus/Loki-compatible (empty windows are gaps). The LogQL metric grammar lives in `compat/loki/logql`; `TranslateMetric` maps it to the native `MetricQuery`.
- **Hardened**: resolution is capped (11,000 steps), and collection stops once more than 20M entries match, keeping 16 bytes per entry rather than the record, so a tiny `step` or a huge range can't OOM the server; the step arithmetic is safe at the edges of int64; durations must be positive; trailing junk is rejected. Without a `step` the resolution is Loki's default, max(1s, range/250).

Verified live: `sum by(region)(sum_over_time({region="eu"} | unwrap latency_ms [30s]))` against the running daemon returns the correct per-step windowed sums as a Loki matrix.

### What Phase 9 delivers (live tail + a RAM bound)

- **WebSocket `/tail`** (`compat/loki/ws.go`): a hand-rolled minimal RFC 6455 server (no third-party dependency — a tail only needs server→client text frames, pings and the close handshake). `/loki/api/v1/tail?query={…}` upgrades the connection and streams matching entries live as Loki tail messages, driven by the existing non-blocking tail fan-out (a slow client drops entries; the writer never blocks). Frame writes are mutex-serialized (a client ping's pong can't interleave), carry a write deadline (a non-reading client can't pin the goroutine), and oversized/ control frames are rejected. A client's Close is answered with a Close echoing its status code, and a daemon shutdown ends each tail with Close 1001 (going away) instead of dropping the socket. Verified end-to-end with a real handshake in the test suite.
- **RAM budget** (`index_mem_budget_mb`; absent or 0 means no cap, which is the code's default, and the shipped configs set 256): the writer estimates the active segment's in-memory index footprint and seals early when it exceeds the budget — bounding index RAM by making segments smaller, *without* reintroducing the global merge/compaction the per-segment design deliberately avoids. It reuses the same seal path (valid `.tidx`/`.lidx`/manifest), so a budget-sealed segment is indistinguishable from a size-sealed one.

With Phases 1–9 done, logd is a complete Loki-compatible daemon: durable ingest, typed-range + label pushdown, Loki push/LogQL (log **and** metric queries), Grafana discovery, live tail, retention, and a RAM bound.

### Querying the active segment (design §9.2)

The unsealed segment used to be scanned on every query. With a quarter of the corpus unsealed, that cost logd 2.2–6.4× in the evaluation (T4) and put it behind Loki — and a live deployment always has an active segment. It is now answered from the writer's own in-memory index, the typed-range buffer and label postings it keeps for the seal anyway:

- **Same planner, same rules.** Typed, literal and allowlisted label-equality predicates are pushed into the active segment through the same lookup interface the sealed `.tidx`/`.lidx` readers implement (a linear pass over the buffered entries instead of a binary search), with the same bound widening, settled/residual split, template-pattern check (F-03), recorded-allowlist check (F-02) and label-cap check.
- **Readable while the writer appends.** The writer appends under the per-record lock it already took for live label discovery; a query copies slice headers under that lock and reads the entries without it. Entries are never rewritten once appended, and the seal now sorts a copy for the `.tidx`.
- **Only what is on disk.** The writer publishes how many pages are written after each page write; a query reads that boundary first and drops candidates past it, so the index never offers a record the file — or the scan — cannot yet read.
- **Seals mid-query are safe.** A view is tied to its segment ID; a query whose segment sealed meanwhile re-reads the manifest and uses the new sidecars.
- **Recovery.** A crash-recovered segment rebuilt by the reindex hook is queryable from memory; without the hook it is scanned.
- **`Explain`** reports `"state": "active"` with reason `indexed_in_memory` (or the scan reason).

Measured in-process on a 200k-record active segment with scattered matches (`BenchmarkActiveSegment`, `{app="bench"} | latency_ms > T`); before this change both columns were the scan:

| selectivity | every row: index / scan | limit 100: index / scan |
|---|---|---|
| 5% | 21 ms / 461 ms | 18 ms / 11 ms (the cost guard scans, after the lookups) |
| 1% | 10 ms / 494 ms | 9.5 ms / 33 ms |
| 0.1% | 9 ms / 447 ms | 7 ms / 196 ms |

### Cost guard v2 (design §9.3)

Phase 7's guard scanned a segment whenever its candidates exceeded half its records. Measured, that made the queries it fired on 2–4× slower: `FetchRecords` already reads each page or block once, in order, so there was no random I/O for a scan to save, while the scan decodes and re-extracts every record. And at ~30 records per page and 8 pages per block, half the records already cover every block, so the rule measured the wrong quantity.

What a scan does have is an early exit: a bounded query's scan stops decoding once its top-K is full and no remaining page can beat it. A bounded query now scans a segment instead of fetching its candidates by offset only when both hold:

- the candidates **over-fetch the limit** — more than √(limit × the segment's records), where, with matches spread evenly, the scan's early exit starts decoding fewer records than the fetch;
- they **cover more than 90% of the segment's I/O units** — compressed blocks, or pages when uncompressed — counted from the layout each segment records in the manifest at seal and compression (`pages`, `block_pages`), not from the daemon's configuration; for the active segment, from the pages its in-memory view covers. Coverage is the condition under which "spread evenly" holds.

An unbounded collection — the metric path — never trips it: there is no early exit to trade for. Nor does a segment sealed before the layout was recorded. The thresholds come from a measured selectivity sweep at two limits over compressed and uncompressed, scattered and clustered corpora (`TestCostGuardSelectivitySweep`, `LOGD_SWEEP=1`; output in `testbed/results/costguard-sweep-2026-11-02.txt`), re-run on the final planner once it intersected smallest-first: every cell the rule sends to the scan had the scan faster, and every cell it keeps on the index had the index faster but for two ties at the threshold. Known gap: matches clustered at the end a scan starts from make the scan cheaper than the rule assumes; the index is kept then, at the cost every query paid before the guard existed.

### Seal by age (design §7.5)

`max_segment_age_minutes` (default 60, `0` = off) seals the active segment once its first record is older than this, whatever its size. The check runs on every flush tick, including ticks with nothing to write, so a stream that has gone quiet still seals. Size alone left a low-volume shard on one active segment for days: uncompressed, its indexes only in memory, and out of retention's reach (retention deletes sealed segments only). A crash-recovered segment keeps its age (taken from its first record), so frequent restarts cannot postpone the seal; an empty segment is never sealed for age. Unlike the other numeric knobs, an explicit `0` is honoured, and a negative value is rejected. The testbed sets it to `0`, keeping rotation size-only so its deliberately unsealed layout stays unsealed for the whole sweep.

Rotation moved into the flush tick as part of the same work: it used to be checked only when a page filled, so at a low ingest rate, where the tick writes every page before it fills, a segment grew past `segment_size_mb` without bound and never sealed.

### Review fixes

A code review of the daemon led to the fixes below; the commit messages carry the details and the tests that pin them. In brief:

- **Acknowledged writes.** A write is refused (`ErrWriterClosed`) once Close begins, instead of being acknowledged into a queue nobody drains; the daemon closes its writers only after the HTTP server has let in-flight pushes finish (and the compose files give it 30 s to stop). A page that fails to write stays in RAM and is retried — never reset, which lost its records and left their index entries pointing at the next page, and never written twice — so a disk that keeps failing becomes backpressure on ingest. The next segment's files are created before the current one seals, so a failed create no longer leaves the writer on a closed file; a seal takes effect in memory only once the manifest has saved it. A level outside DEBUG..ERROR is refused at write rather than stored unreadable.
- **Recovery.** Only the torn tail is truncated, and a missing active-segment file no longer keeps the shard from opening (Phases 1–2 above).
- **Corrupt and hostile files.** Counts in `.tidx`, `.lidx` and `.logz` headers are checked against the bytes that hold them before anything is allocated or indexed by them, so a damaged or crafted file degrades its segment instead of exhausting memory or panicking. A compressed segment that cannot be opened is skipped the same way on the index and the scan path — its records are lost to every reader, as a corrupt block's are — and logged once. A field whose readable `.tidx` file name would exceed 255 bytes gets a hashed one. Retention finds a segment's files by listing the directory, not with a glob that a `[` in the data path broke.
- **Query edge cases.** A non-finite float bound is refused instead of being answered differently by the two paths; a metric query's arithmetic is safe at the edges of int64 and its memory is bounded by its entry cap; result-order ties break on the service name, not its per-shard ID, so a limit returns the same rows for any shard count; manifest entries are handed out as copies, and the sidecar cache checks that a path still names the file it loaded, so a query never plans against a half-sealed entry or a sidecar a retried seal has replaced; a reader looks for the `.logz` once more before calling a segment that is being compressed gone.
- **Loki API.** Parameters, push encodings, the health check, the tail's close handshake, tenancy and label resolution now behave as Loki's do: see [Loki HTTP API](#loki-http-api).

### Extraction speedup

Profiling extraction on the ingest path put about 70% of its CPU time in Go map lookups inside the Aho-Corasick search; once those were gone, allocation was the largest cost. The trie is now compiled into a dense transition table — one array lookup per input byte, with the fail links folded in and the bytes that occur in no pattern sharing one column — and a line is matched without per-line maps or slices: hits go into a stack buffer, captures straight into the result, keys are de-duplicated by a linear check up to 32 values, and the counters are held per template index. Extraction results are unchanged (a differential run over 3,000 random configurations, 200 lines each, found no difference). `BenchmarkTemplateExtract`, 80% of lines matching, old and new code alternated three times:

| templates | ns/line before | ns/line after | allocs/line |
|---|---|---|---|
| 1 | 2425–3059 | 511–734 | 5 → 1 |
| 5 | 4308–5945 | 1039–1404 | 19 → 3 |
| 20 | 6614–8290 | 1986–2415 | 25 → 5 |

The machine was at load 2–4, so the absolute figures move; the ratio, 3.4–4.5×, held in every round.

### Multi-writer shard fan-in (design §10)

Set `shards: N` to run N shared-nothing writers, one per `data_root/shard-NNNN/` folder (each with its own manifest, service dictionary, label index, and a flock `LOCK`). Ingest routes each record to a shard (a labelled stream hashes to a stable shard; label-less records round-robin). Queries **fan in**: the engine runs the per-segment query on every shard and merges — log results are globally sorted (with a deterministic tiebreaker, ending on the service name, so a limit returns the same set regardless of partitioning), metric aggregations combine across shards, and label discovery unions. The correctness contract is **fan-in == single-shard**: an N-shard engine returns exactly what one engine over the same data would. (Routing never affects results — it only changes write balance and locality.) Segment IDs are ULIDs, unique across shards without coordination. Cross-process shard enumeration — finding shards by listing `data_root` rather than from the configured `shards` count — remains deferred; it matters only for relocating segments *between* shards, which fan-in doesn't do.

## Loki HTTP API

logd serves Loki's `/loki/api/v1/push`, `query_range`, `query`, `labels`,
`label/{name}/values`, `series`, `tail`, `index/stats`, `format_query` and
`status/buildinfo`, plus `/ready`, and its own `/logd/api/v1/explain`, `query_scan` and
`index_stats`. Where a request could mean two things, it means what it means to Loki: the
parameter parsing is a port of Loki's `pkg/loghttp`, and a value logd cannot read is a 400,
never silently replaced by a default — an unparseable `start` used to mean "the last hour",
and an answer to a question the client did not ask is worse than an error.

**Push** (`POST /loki/api/v1/push`).

- The body is snappy-compressed protobuf unless `Content-Type` is `application/json` (the
  media type is parsed, so `application/json; charset=utf-8` is JSON). No `Content-Type` at
  all means protobuf, as in Loki.
- `Content-Encoding: gzip` and `deflate` (raw DEFLATE, as Loki reads it) are inflated; none
  and `snappy` pass through; anything else is a 400 naming the encoding. The body may be
  32 MB, and may decompress to 128 MB.
- At most **100,000 entries** per request; above that the push is answered **413** ("send
  it in smaller batches") and nothing from it is stored. The byte caps bound bytes, not
  memory: an empty entry is two bytes on the wire and a full record once decoded. Real
  shippers batch far below the cap (the testbed generator sends 1,000 per request).
- A line too large for a page (line, labels and a 27-byte prefix must fit in 4,064 bytes) is
  rejected alone: the other entries are stored, and the answer is a 400 counting the
  rejections, which shippers do not retry.
- `204` means queued for the writer, not yet durable (design §8). A full queue is
  backpressure: the push waits for space rather than failing partway, because the protocol
  cannot say "the first 412 of your 1,000 entries were stored".

**Query parameters** (`query_range`, `query`, `query_scan`, `explain`).

- `start`, `end`, `time`: a value containing `.` that parses as a float is seconds (the
  fraction rounded to the millisecond); an integer of at most 10 digits is seconds, a longer
  one nanoseconds; anything else must be RFC3339. A value outside 1970-01-01 to 2262-01-01
  is a 400: int64 nanoseconds end in April 2262, and `time=9223372036854775807` once made a
  metric query loop until the daemon ran out of memory.
- `end` defaults to now (`time` stands in for it on an instant query); `start` defaults to
  `since` before `end`, or before now when `end` is in the future. `since` takes a duration,
  default `1h`.
- `limit`: default 100; a positive integer of at most 1,000,000, the role of Loki's
  `max_entries_limit_per_query` (Loki's default is 5,000; logd's is high because the
  testbed reads whole result sets).
- `direction`: `backward` (the default) or `forward`, case-insensitive; anything else is a
  400.
- `step`: float seconds or a duration (`15s`, `1m30s`); zero or negative is a 400. Without
  one, the resolution is Loki's default, max(1s, range/250) in whole seconds, instead of a
  fixed 15s. A query that would need more than 11,000 steps is refused.
- Durations (`since`, `step`, range selectors) take both of Loki's forms, Prometheus'
  (`1d12h`, `1w`, `1y`) and Go's (`1.5h`, `500ms`); a sign, an unknown unit or a value that
  overflows int64 nanoseconds is an error rather than a wrapped number.

**Labels and tenancy.**

- `level` and `service` resolve from the record's labels as pushed, so `{level="error"}`
  matches the rows shown as `level="error"`. Only a record whose labels lack the key falls
  back to its own level or service field. A metric groups by exactly the pushed labels; no
  `level` is synthesized. Both keys are listed by discovery when allowlisted, but are always
  scanned, never pushed into the label index, which does not hold the fallback values.
- With `multitenancy: true`, `X-Scope-OrgID` (default `default`) tags every push and scopes
  every read: queries, tail, `labels`, `label/{name}/values`, `series`, the stream count of
  `index/stats` and the label keys of `index_stats`. The internal `__tenant__` label is never
  returned, whether or not multitenancy is on, so data stored while it was on does not
  surface it later. The template counters and `index/stats`' chunk count stay daemon-wide:
  they describe configuration and shared layout, not anyone's data.

**Health check.** logd has no `vector()`. Grafana's "Save & test" sends
`vector(1)+vector(1)` and checks that the answer is 2, so exactly that expression
(whitespace aside) is answered with a one-sample vector of 2. Any other query that does not
parse is a 400 with the parse error; answering every unparseable query containing
`vector(` made `count_over_time(...) or vector(0)` return 1, a number nobody computed.

**Tail.** `/loki/api/v1/tail` completes the WebSocket close handshake: a client's Close is
answered with a Close echoing its status code (RFC 6455 §5.5.1), and a daemon shutdown ends
every tail with Close 1001 (going away), so clients see a deliberate end of stream rather
than an abnormal closure. Nothing is written after a Close.

**LogQL syntax**, beyond the parser stages of the next section:

- Label filters join with `and` or `,` as well as `|`; all mean AND, each filter a stage of
  its own. `or` between label filters is refused: pipelines are AND by design (design
  §9.3), and alternatives on one label belong in a regex, `=~"warn|error"`.
- `==` is accepted as `=` in label filters (not in selectors, as in Loki).
- A double-quoted string is decoded with Go's escape rules (`strconv.Unquote`: `\x41`,
  `é`, octal), as Loki decodes it, and an invalid escape (`\q`, or `\d` outside a raw
  string) is an error. A single-quoted string follows the same rules with `\'`; a backtick
  string is raw. An unterminated string is an error, no longer a value running to the end
  of the query.
- `| drop __error__` (and `__error_details__`, optionally with a matcher) is accepted, so
  the log-volume histogram Grafana runs beside every log query,
  `sum by (level) (count_over_time(<expr> | drop __error__ [$__auto]))`, renders. Other
  `drop` and `keep` lists are refused.
- A float bound that is not finite (`NaN`, `Inf`) on a typed field is a 400: no extracted
  value is ever non-finite, so the query has no meaningful answer.

## LogQL API: parser stages

logd accepts the plain forms of Loki's four parser stages, `| pattern "<expr>"`,
`| regexp "<re>"`, `| logfmt` and `| json`, in log queries, metric queries and `/tail`.
Their semantics are ported from Loki's source (`pkg/logql/log`, cited in
`compat/loki/logql/stages.go`):

- **pattern**: anchored at the start of the line unless it starts with a capture; a capture
  runs to the first occurrence of the next literal; `<_>` skips; a missing literal makes the
  capture take the rest of the line and stops matching; a line that does not match yields no
  labels, never an error.
- **regexp**: Go RE2 syntax (as in Loki), the named groups of the leftmost match; at least
  one named group is required.
- **logfmt**: Loki's decoder in its default mode — bad pairs skipped, empty values not set,
  never an error.
- **json**: nested values named by their path joined with `_`, numbers kept as written,
  arrays and nulls skipped; a line that is not a JSON object gets `__error__=JSONParserErr`
  and keeps the labels read before the error.
- Keys are sanitized as Loki does; a name the record already carries (a stream label or
  structured metadata) is extracted as `<name>_extracted`; the first value set under a name
  wins.

**Filters after a stage.** A label filter on a name the preceding stages may extract becomes
a *dynamic predicate* (`query.DynamicCompare`): the value is re-extracted from each line and
compared at query time — a scan of the time-pruned range, O(n) (design §9.1). A number
literal (`| x > 200`, `| x = 200`) makes a numeric comparison (float64, as in Loki), a string
(`| x = "200"`) a string comparison, and `=~`/`!~` an anchored regex. A filter on a name no
preceding stage can extract translates as without the stage: a stream label still uses the
label index, and the logd-native `| latency_ms > 200` on a declared field is unchanged.

**A pattern that equals a template uses its index.** When a `| pattern` is structurally
equal to a declared template — it starts with `<_>`, then the template's literal fragments
byte for byte, with captures where the template has them, each named as the template's
field (`<template>_<capture>`), and optionally a trailing `<_>` — filters on its captures
become that template's typed comparison and are pushed into the typed-range index. With the
testbed's template `latency: took={ms:int}ms`,

```
{app="checkout"} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200
```

returns exactly the rows of `{app="checkout"} | latency_ms > 200`, from the index. Any
deviation (whitespace, a renamed capture, an extra fragment, no leading `<_>`, a template
ending in a capture, a second parser stage, or a filter on the field that is not `=`, `<`,
`<=`, `>`, `>=` with a bound of the field's type) is no match, and the query scans with
Loki's semantics.

*Where this differs from Loki.* On every line where the template matches at the first
occurrence of its first literal and nowhere later, Loki and the template agree (ints within
±2^53; the argument is in `compat/loki/logql/structural.go`). On other lines they can
differ, and no rewrite closes the gap, so the documented rule decides: **a structural match
uses the template's semantics**, not Loki's. Concretely: on a line with several occurrences
(`took=5ms … took=500ms`) Loki compares only the first and the template matches on any; a
first value that is not of the declared type (`took=12.5ms` under `int`) is converted and
compared by Loki but has no value in the template; a missing or displaced literal
(`took=7` at the end of the line) still gives Loki a value; ints beyond 2^53 are rounded by
Loki. `TestStructuralMatchDiffersFromLokiOnlyOnNonConformingLines` pins these cases. Every
line the testbed generator emits conforms, so there the three engines agree.

**Numeric filters on values that do not convert** — the one rule chosen differently from
Loki. A numeric comparison on an extracted value that is missing *or does not convert to a
number* does not match. Loki drops a missing one too, but keeps a line whose value does not
convert and flags it `__error__=LabelFilterErr`; it also skips numeric filters entirely on a
line that already carries an error (such as `JSONParserErr`), and a metric query over such
lines fails unless the error is filtered out. Reproducing that would make every filter's
answer depend on the error state left by the filters before it; the simpler rule returns
the same rows as Loki for the queries Loki's documentation recommends, those with
`| __error__=""` after their last numeric filter. Unwrap follows the same rule: an entry
whose unwrapped value is missing is dropped, as in Loki, and so is one that does not convert,
which Loki flags `__error__=SampleExtractionErr`.

**`__error__`.** Only `| json` sets it. `| __error__ = ""`, `!= ""` or `= "JSONParserErr"`
after a json stage filter on it, and `| drop __error__` (optionally with a matcher) removes
it. `__error_details__` is never set. In a metric query the error is an ordinary series label;
logd does not fail the query.

**Result labels.** Like Loki, each result entry carries the labels its line yields — the
stream labels plus every extracted label (and `__error__`) — so a result stream is keyed by
both, in `query_range`, `query`, `query_scan` and `tail`. They are re-extracted from each
returned line with Loki's semantics, also when the rows were selected through a template (on
a line with several `took=`, the label is the first). An extracted label with an empty value
is omitted. In metric queries, series are keyed by the extracted labels too, without the
unwrapped one, as in Loki.

**Explain.** `/logd/api/v1/explain` reports, in `pipeline`, how each parser stage and each
filter after one was translated — `template` / `typed` (the index), `dynamic` (a per-line
scan) or `label` — and why: the near miss that kept a pattern from a template, the filter
that gave the match up, or that the stage is not a pattern at all.

**Not supported:** `| json` and `| logfmt` with parameters (`| json a="b.c"`, `| logfmt a`)
or flags (`--strict`, `--keep-empty`); `line_format`, `label_format`, `unpack`,
`decolorize`, `distinct`; `drop`/`keep` of labels other than `__error__` and
`__error_details__`; `or` between label filters; duration and byte-size comparisons; IP
filters. Each is refused with an error rather than ignored.

## Build & test

```sh
go build ./...
go vet ./...
go test ./...
```

Phases 1–2 are stdlib-only; external deps (yaml, snappy, protobuf) arrive with their
phases.
