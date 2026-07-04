# logd

A lightweight, single-node log storage daemon with a Loki-compatible API. Its
differentiator is **range queries on typed values extracted from unstructured log
text** — pull `247` out of `took 247ms` and answer `duration > 200` in O(log n),
which Loki/VictoriaLogs (presence-only) and ClickHouse/Quickwit (need pre-structured
columns) can't do the same way.

This tree is the **fresh `core/` + `compat/` rewrite** on the master design
(`docs/logd-design.md`). It treats the previous implementation as a *quarry*, not
trash: correct, tested subsystems are ported; subsystems that deviate from the spec
are rebuilt against it. See `docs/logd-design.md` §3 for the target layout and
`docs/PHASE-1-2-SPEC.md` for the build spec this foundation was written against.

## Layout (design §3)

```
core/                     protocol-agnostic; never imports compat/
  model/    LogEntry, LogLevel
  storage/  record · page · segment · manifest · writer · lookup · sparse time index
  index/    typed-range .tidx            (Phase 3)
  ingest/   native ingest API            (Phase 3+)
  query/    native query engine/planner  (Phase 4-5)
  config/   extraction/label config       (Phase 3)
compat/                   Loki/OTLP/ES translations; may import core
  loki/push · loki/logql · loki/grafana   (Phase 6)
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

### What Phases 1–2 deliver

- **Record codec** (`core/storage/record.go`): 27-byte fixed prefix `[8 TS][8 IngestedAt][1 Level][2 ServiceID][4 StreamID][2 MsgLen]…[2 ExtraLen]…`, big-endian, fully bounds-checked (`ErrMalformedRecord` on truncation, never a panic).
- **Full-page checksum** (`core/storage/page.go`): CRC32-IEEE over the entire 4096-byte page, so corruption in *record* data — not just the header — is detected. This is what makes torn-page recovery possible.
- **Durable writer** (`core/storage/writer.go`): one writer goroutine, whole-page appends, group-commit fsync per flushed page, size-based rotation, atomic seal (tmp → fsync → rename → dir fsync). Extraction is deliberately *not* here — the write path carries a reserved seam for pre-extracted values (§7.4) and a reserved live-tail notify hook (§11).
- **Crash recovery** (`NewWriter` + `OpenSegment`): on restart, the active segment is rescanned, its real time bounds are republished to the manifest (fixing the old "SIGKILL hides recent data from time-range queries" bug), and any torn trailing page is detected via the full-page checksum and truncated. Covered by `TestCrashRecoveryRepublishesBounds` and `TestTornPageTruncatedOnRecovery`.
- **Per-segment schema field** in the manifest, so the future planner consults each segment's own build-time schema, never live config (§7.3) — carried in the format now even though extraction is Phase 3.

Ported verbatim from the quarry: `LookupTable` (service interning) and the sparse
time index.

### What Phase 3 delivers (the differentiator)

- **Order-preserving key encoding** (`core/index/key.go`): 16-byte keys where byte order equals value order — sign-flipped int, IEEE-754 total-order float (−0 normalized), longest valid-UTF-8 str prefix, raw uuid. Property-tested across thousands of pairs.
- **Typed-range `.tidx`** (`core/index`): per-(segment, field) sorted flat file (16-byte key + 8-byte segment-relative offset), CRC-headered, written atomically at seal. `Reader` does O(log n) equality and range lookups; an in-RAM `Buffer` accumulates keys per active segment and flushes them at seal.
- **Extraction engine** (`core/extract`): config templates like `took {ms:int}ms` compile to literal fragments + typed captures; one Aho-Corasick automaton finds all literal positions in a single pass; hand-written byte scanners parse int/float/str/uuid (rejecting NaN/±Inf); the §7.2 rules (≥3-byte anchor, no adjacent captures, no leading capture, no identical-pattern overlap) are enforced at compile; a per-template failure counter records anchor-matched-but-unparseable values.
- **Native ingest** (`core/ingest`): runs extraction off the write path and hands the writer pre-encoded `(field, key)` pairs, which it pairs with each record's on-disk offset (the reserved §7.4 seam).
- **No-false-negative recovery**: a crash-recovered segment (whose in-RAM index was never rebuilt) is sealed with an **empty schema** so the planner scans it, rather than getting a partial `.tidx`.
- Verified by a **rebuild-and-compare** test: re-extracting every stored record reproduces the exact `.tidx` contents.

Query-side index pushdown is Phase 5; Phase 3 builds and verifies the index.

### What Phase 4 delivers (the query oracle)

- **Native query model** (`core/query`): a time window + AND-combined predicates (`LineContains`/regex + negations, `LabelEqual`/`NotEqual` over level/service/Extra, `TypedCompare` for `= != < <= > >=` on template fields), a limit, and a direction. Protocol-agnostic — no Loki assumptions.
- **Scan-path `Execute`**: time-prunes segments (live manifest) and pages (header bounds), decodes records, evaluates every predicate on the **fully decoded record with exact values**, then global-sorts by event time and applies the limit. Because it compares real values (not the lossy 16-byte keys), it is the exact correctness oracle Phase 5's index result must equal.
- **Live tail** (`Engine.Tail`): activates the writer's reserved `notify()` seam — a concurrency-safe subscriber registry fans each newly-written record out to matching tailers, non-blocking (a slow tailer drops rather than stalling the write path). WebSocket/Loki `/tail` framing is compat/later.
- Reads just-flushed active-segment data via the live in-memory manifest; safe to query under concurrent ingestion (verified under `-race`).

### What Phase 5 delivers (the payoff)

- **Query planner + index pushdown** (`core/query/plan.go`): per time-pruned, indexed segment, a `TypedCompare` (`= < <= > >=`, kind-matched) or a `LineContains` on a configured literal is pushed into the field's `.tidx` — binary-search equality or a range-walk with **inclusive-widened bounds** (so a lossy string key equal to the bound is never dropped). Candidate offset sets are intersected across predicates.
- **Materialize + re-verify** (`storage.FetchRecords`): only candidate records are read (each page once, in offset order), then the **full predicate set is re-applied exactly** — so lossy-key false positives are filtered and the result is identical to a scan.
- **Always-correct fallbacks**: a non-indexed (active/recovered) segment, a `!=`, a field absent from a segment's schema, a cross-kind predicate, or a missing/corrupt `.tidx` all degrade to scan (never a wrong answer).
- **`Explain`** reports index-vs-scan per segment; **`ExecuteScan`** forces the scan path.

The contract is enforced by a **differential oracle test**: `Execute(q)` (pushdown) must equal `ExecuteScan(q)` (scan) for a large battery of queries over multiple segments — typed ranges/equality/`!=`, lossy string ranges beyond 16 bytes, negative-int bounds, literals, labels, intersections, limits, directions, degrade-to-scan, cross-kind, and config change. It does, for every one.

### What Phase 6a delivers (native label index)

- **`core/label`**: a per-segment stream dictionary (canonical label set ↔ `StreamID`) + posting lists (`StreamID` → delta+uvarint sorted offsets), snapshotted at seal in a CRC'd `.lidx`. Fills in the `StreamID` reserved on `LogEntry` since Phase 1.
- **Derived accelerator**: the label index is built from the **allowlisted** keys already in each record's `Extra` (via the shared `model.ParseExtraLabels`), so the scan path stays the untouched oracle and the index just accelerates — the allowlist is the primary cardinality guard.
- **`LabelEqual` pushdown**: an allowlisted, non-reserved key resolves to candidate offsets via the segment's `.lidx` (intersected with typed/literal lookups), then the survivors are re-verified exactly. Reserved first-class keys (`level`, `service`, resolved from `entry.Level`/`ServiceID`), non-allowlisted keys, and `LabelNotEqual` degrade to scan. `Engine.Labels()`/`LabelValues()` enumerate from the indexes.
- Verified by the same differential oracle: `Execute` (label pushdown) equals `ExecuteScan` across a battery (single/combined label equality, label∩label, label∩typed, non-allowlisted, `!=`, reserved keys, numeric labels, degrade-to-scan).

### What Phase 6b delivers (Grafana can point at logd)

- **`compat/loki`** — the Loki-compatible HTTP surface. Core stays protocol-agnostic; all Loki assumptions live here.
- **Push** (`compat/loki/push`): decodes Loki push (snappy+protobuf *and* JSON) into native entries, labels → `Extra` (so the label index derives consistently). Hardened against decompression bombs and body-size abuse.
- **LogQL** (`compat/loki/logql`): a hand-written lexer/parser/AST + an **AST→native translator**. `| latency_ms > 200` on a declared template capture becomes a native `TypedCompare` — so a Grafana LogQL query **transparently drives logd's typed-range pushdown**. Labels → equality/regex/numeric-compare; line filters → contains/regex; Loki empty-label semantics.
- **HTTP** (`compat/loki/server.go`): `/loki/api/v1/push`, `/query_range`, `/query` (+ Grafana health stub), `/labels`, `/label/{n}/values`, `/series`, `/ready`, returning Grafana-shaped JSON.
- **`cmd/logd`**: the real daemon — load YAML config, wire `core` → `compat`, serve, graceful shutdown. Verified with a live `curl` round-trip: push a line, then `query_range` with `{region="eu"} | latency_ms > 200` returns exactly the matching record as Loki JSON, index-accelerated.

Run it: `go run ./cmd/logd -config config.yaml`, then point a Grafana Loki datasource at `http://localhost:3100`.

### What Phase 7 delivers (hardening)

- **Index cost guard**: when a candidate set covers more than half a segment's records, the planner scans sequentially instead of fetching by random offset (a DB planner's seq-scan-vs-index-scan choice). Correctness-neutral — the differential oracle still holds — so it only changes the access path. `Explain` reports the chosen path.
- **Retention**: a per-segment record count and a background sweep delete sealed segments whose data is entirely past the window (`retention_days`), plus their sidecar index files — trivial given per-segment files, no compaction. Runs on the writer goroutine (serialized with seals). Queries degrade gracefully on a retention race (a segment deleted mid-query is skipped, not an error).
- **Live label discovery**: the active segment's label streams are tracked concurrency-safely, so `/labels`, `/label/values`, and `/series` populate Grafana's dropdowns immediately — not only after the first seal.

### What Phase 8 delivers (metric queries → Grafana graph panels)

LogQL metric queries are evaluated natively into a Loki matrix/vector:

- **Range aggregations** over a `[range]` window per stream: `count_over_time`, `rate` (count/sec), and the unwrap forms `sum_over_time`/`avg_over_time`/`max_over_time`/`min_over_time` over `| unwrap <field>` (a template capture or a numeric label). `rate(... | unwrap x [r])` is the value-rate `sum(x)/r`.
- **Vector aggregations** across streams: `sum`/`avg`/`max`/`min`/`count`, with `by(labels)` / `without(labels)`.
- **Evaluation** (`core/query/metric.go`, protocol-agnostic): step across `[start,end]`, per-stream `(t-range, t]` windows, range-aggregate, then vector-aggregate — Prometheus/Loki-compatible (empty windows are gaps). The LogQL metric grammar lives in `compat/loki/logql`; `TranslateMetric` maps it to the native `MetricQuery`.
- **Hardened**: resolution is capped (~11k steps) and entry loading is bounded, so a tiny `step` or a huge range can't OOM the server; durations must be positive; trailing junk is rejected.

Verified live: `sum by(region)(sum_over_time({region="eu"} | unwrap latency_ms [30s]))` against the running daemon returns the correct per-step windowed sums as a Loki matrix.

### What Phase 9 delivers (live tail + a RAM bound)

- **WebSocket `/tail`** (`compat/loki/ws.go`): a hand-rolled minimal RFC 6455 server (no third-party dependency — a tail only needs server→client text frames plus close detection). `/loki/api/v1/tail?query={…}` upgrades the connection and streams matching entries live as Loki tail messages, driven by the existing non-blocking tail fan-out (a slow client drops entries; the writer never blocks). Frame writes are mutex-serialized (a client ping's pong can't interleave), carry a write deadline (a non-reading client can't pin the goroutine), and oversized/ control frames are rejected. Verified end-to-end with a real handshake in the test suite.
- **RAM budget** (`index_mem_budget_mb`): the writer estimates the active segment's in-memory index footprint and seals early when it exceeds the budget — bounding index RAM by making segments smaller, *without* reintroducing the global merge/compaction the per-segment design deliberately avoids. It reuses the same seal path (valid `.tidx`/`.lidx`/manifest), so a budget-sealed segment is indistinguishable from a size-sealed one.

With Phases 1–9 done, logd is a complete Loki-compatible daemon: durable ingest, typed-range + label pushdown, Loki push/LogQL (log **and** metric queries), Grafana discovery, live tail, retention, and a RAM bound.

### Multi-writer shard fan-in (design §10)

Set `shards: N` to run N shared-nothing writers, one per `data_root/shard-NNNN/` folder (each with its own manifest, service dictionary, label index, and a flock `LOCK`). Ingest routes each record to a shard (a labelled stream hashes to a stable shard; label-less records round-robin). Queries **fan in**: the engine runs the per-segment query on every shard and merges — log results are globally sorted (with a deterministic tiebreaker so a limit returns the same set regardless of partitioning), metric aggregations combine across shards, and label discovery unions. The correctness contract is **fan-in == single-shard**: an N-shard engine returns exactly what one engine over the same data would. (Routing never affects results — it only changes write balance and locality.) ULID segment IDs and cross-process shard enumeration remain deferred; they matter only for relocating segments *between* shards, which fan-in doesn't do.

## Build & test

```sh
go build ./...
go vet ./...
go test ./...
```

Phases 1–2 are stdlib-only; external deps (yaml, snappy, protobuf) arrive with their
phases.
