# logd testbed — head-to-head with Loki and VictoriaLogs

One `docker compose up` brings up **logd, Grafana Loki, VictoriaLogs and Grafana**, feeds
all three identical traffic, and lets you compare them on the same queries in the same UI.

```sh
make up          # logd + Loki + VictoriaLogs + Grafana
make load        # generate a corpus, push identical bytes to all three
make explain     # which access path did logd choose?
make compare     # logd's index path vs its own forced scan
make bench       # interleaved latency across logd, Loki and VictoriaLogs
open http://localhost:3000    # Explore → switch datasource → same query, three engines
```

This is a separate Go module (`replace ../`), so the daemon's `go build ./...`,
`go vet ./...` and its test suite are untouched, and its `go.mod` keeps its deliberate
three dependencies.

---

## Why a corpus, and not just "send some logs"

A comparison between log stores is only worth publishing if you can show all three
returned the **right** answer, not merely the same answer — two engines can agree and both
be wrong.

So `loggen -corpus` emits a fixed, seeded body of lines and writes a **manifest**
(`corpus/*.jsonl`) recording, per line, its exact timestamp, labels, text and the duration
it actually carries. The expected answer to any query is then computed in plain Go from the
manifest, with no backend involved.

Two details make that work, and both were bugs before they were features:

- **Timestamps are synthetic and strictly monotonic.** `ts_ns` is the primary key for
  diffing result sets; at a million lines, wall-clock time collides constantly.
- **`-base-ts` defaults to `auto`** (the corpus ends one minute before now). A corpus dated
  in the past is *silently invisible* to Loki: it is accepted with HTTP 204, held in the
  ingester, and returns zero rows, because Loki only consults ingesters for data within
  `query_ingesters_within` (3h) and the chunks have not flushed to store yet. No error
  anywhere explains it. VictoriaLogs has the same hazard via its retention window, which is
  why the compose file sets `-retentionPeriod=100y`.

## Why the generator pushes directly, rather than through Alloy

One encoded batch, N URLs, closed-loop: every backend must answer before the next batch.

The obvious alternative — point one Grafana Alloy at three `loki.write` endpoints — looks
equivalent and is not. Each endpoint block carries its own queue, backoff and retry state,
so the moment one backend is slower the three receive *different data*, and the throughput
figure measures Alloy's queue rather than any store. Alloy's own CPU and memory are also
shared across all three and unattributable.

Alloy is still here, as `--profile alloy`, to demonstrate that **a real shipper talks to
logd unmodified**. That is a compatibility claim, and it never runs during a measured run.

---

## Results (8-core dev box)

Real numbers from this harness, not estimates. Each section states its own corpus size:
latency was measured at 500,000 lines, correctness/storage/ingest at 50,000. Reproduce with
`./scripts/bench-vs.sh`, which interleaves samples across engines and refuses to report
timings unless every engine returned the same non-zero row count.

Corpus: unstructured application logs, e.g.
`GET /api/v1/orders 200 took=247ms trace_id=…`. 81% of lines carry a `took=` duration.

### Correctness — all three agree with ground truth (50,000 lines)

| query | ground truth | logd | Loki | VictoriaLogs |
|---|---|---|---|---|
| all lines | 50,000 | 50,000 | 50,000 | 50,000 |
| `latency_ms > 200` | 2,806 | **2,806** | **2,806** | **2,806** |

Three engines, three query languages, identical answers:

```
logd          {app="checkout"} | latency_ms > 200
Loki          {app="checkout"} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200
VictoriaLogs  _stream:{app="checkout"} "took=" | extract "took=<latency_ms>ms" | latency_ms:>200
```

Loki's line filter `|= "took="` is deliberately included. Omitting it would sandbag Loki,
and the first reader would say so.

### Query latency — 500,000 lines

Medians of 10–15 interleaved samples per engine. Every row verified to return the identical
result set from all engines first.

| `latency_ms >` | rows | logd (index) | logd (scan) | Loki (`pattern`) | Loki (`regexp`) | vs Loki |
|---|---|---|---|---|---|---|
| 200 | 26,996 (5.4%) | **151 ms** | 1665 ms | 435 ms | 530 ms | **2.9×** |
| 1000 | 3,935 (0.79%) | **56 ms** | 1732 ms | 344 ms | 441 ms | **6.1×** |
| 3000 | 1,046 (0.21%) | **41 ms** | 1681 ms | 307 ms | 398 ms | **7.5×** |
| 5000 | 482 (0.10%) | **38 ms** | 1681 ms | 304 ms | 388 ms | **8.0×** |

**The shape is the result.** logd's time tracks the size of the *answer* (151 → 38 ms as the
result set shrinks 56×). Loki's is comparatively flat (435 → 304 ms) because it re-scans and
re-parses every line regardless of how few match. logd's own forced-scan column is flat too
(~1700 ms), confirming flatness is the signature of scanning rather than anything specific
to Loki.

That is what the typed index is for: converting work proportional to the *data* into work
proportional to the *answer*.

⚠️ These numbers post-date a round of read-path optimization (see *Optimization history*).
An earlier measurement had logd at 386 ms for the 5.4% row — a **tie** with Loki — because
the query was dominated by result serialization rather than lookup. Loki's own numbers also
moved between the two runs (369 → 435 ms at 5.4%), so treat the cross-engine ratios as
same-run comparisons only, not as evidence about Loki.

### "Last N lines" — the query every Explore session opens with

| limit | logd | Loki |
|---|---|---|
| 1 | 14.4 ms | 13.8 ms |
| 100 | **16.4 ms** | 14.3 ms |
| 1000 | 21.1 ms | 15.7 ms |

This was logd's worst result by a wide margin — **1558 ms against Loki's 15 ms**, a ~100×
gap — because the engine collected every matching record, sorted all of them, and only then
applied the limit. It is now at parity.

### logd vs itself (500,000 lines)

The cross-engine numbers always invite "you configured Loki wrong". This one cannot: same
binary, same data, same page cache, one variable — whether the `.tidx` is consulted.

| `latency_ms >` | index | forced scan | speedup |
|---|---|---|---|
| 200 | 151 ms | 1665 ms | **11×** |
| 1000 | 56 ms | 1732 ms | **31×** |
| 3000 | 41 ms | 1681 ms | **41×** |
| 5000 | 38 ms | 1681 ms | **44×** |

`make compare` runs this and refuses to print timings if the two paths disagree on a row.

### Optimization history

The read path was profiled and reworked in four commits. Benchmarks are
`core/query/bench_test.go` (200k records, medians of 3):

| | LabelOnly/100 | LabelOnly/1 | TypedSelective | TypedBroad |
|---|---|---|---|---|
| baseline | 426 ms | 436 ms | 26 ms | 781 ms |
| single-key label resolution | 262 | 244 | 22 | 651 |
| index reader cache + cost guard | 156 | 156 | 10 | 479 |
| **limit pushdown** | **9.3** | **9.4** | **9.8** | **37** |

What each fixed, in order of how much it mattered:

1. **The limit was never pushed down.** `execute` collected every match, sorted, then
   truncated — so `limit=1` cost the same as `limit=100`. Now a bounded top-K accumulator
   prunes whole segments and pages that cannot beat what it already holds.
2. **No index reader cache.** Every query re-read and re-decoded each sidecar from disk, per
   segment *per predicate* — 7.2 MB for one field's `.tidx` here. That, not the sort, is why
   a broad indexed query used to be *slower* than the scan it fell back to.
3. **The label blob was JSON-parsed per record, per predicate** (28.7% of query CPU),
   building a whole map to read one key. A single-key scanner is ~21× faster and
   allocation-free.
4. **Re-verification re-extracted every candidate** (20.6% of CPU), re-running Aho-Corasick
   to re-derive what the index already knew. Skipped now for int/float/uuid, whose 16-byte
   keys are exact; `str` keys are lossy prefixes and still re-verify.

### Storage — logd loses, clearly (50,000 lines)

| | on disk | vs logd |
|---|---|---|
| VictoriaLogs | **0.81 MB** | 12× smaller |
| Loki | 5.14 MB | 1.9× smaller |
| logd | 9.60 MB | — (8.52 data + 1.06 index) |

logd stores records uncompressed in 4 KB pages. There is no block compression anywhere in
the write path. This is a real, structural disadvantage and it is not close.

### Ingest (50,000 lines)

Both a finding and a fix. The first run showed logd requiring **225 retries** to absorb
50,000 lines while Loki and VictoriaLogs needed zero, and — worse — storing **169,692 rows
for 50,000 pushed lines**. See the bug below. After the fix: zero retries, exact counts,
and end-to-end throughput up from 3,158 to ~10,200–11,100 lines/s.

---

## The bug this harness found on its first real run

logd's push handler ingested a batch **entry by entry** into a bounded queue that
**dropped** once full, then returned `503` for the whole request. Because the Loki protocol
cannot express *"I accepted the first 412 of your 1,000 entries"*, every real shipper
retries the entire batch — and the already-stored prefix is written again.

The failure is silent. No error, no data loss, just duplicates. Nothing short of a corpus
with known ground truth would have surfaced it.

Fixed by `storage.WriteExtractedCtx` / `ingest.IngestCtx`, which **wait** for queue space
instead of dropping, so backpressure costs latency rather than correctness — which is what
Loki and VictoriaLogs do and what shippers already expect. Pinned by
`core/storage/backpressure_test.go`.

---

## Honest caveats

Read these before quoting any number above.

- **Selectivity decides the answer.** Quoting one speedup figure for "logd vs Loki" is
  meaningless — the same corpus gives 2.9× and 8.0× depending only on how many rows match.
  Quote the table, or quote nothing.
- **The cross-engine numbers come from separate runs.** Loki's own timings moved between
  measurement rounds (369 → 435 ms on the same query and corpus), so a ratio that improved
  is not by itself evidence that logd improved — the logd-vs-itself column and the
  `core/query` benchmarks are the controlled comparisons.
- **500,000 lines is still modest**, and one node. Both engines fit the working set in
  page cache, so none of this says anything about behaviour at disk-bound scale.
- **Shared dev box**, other containers running throughout. Latency figures are medians of
  12–15 interleaved samples; the disk and ingest figures are single runs with no
  percentiles. Treat the latter as directional.
- **Warm page cache.** Five warm-ups discarded per measurement. No cold-cache numbers.
- **Result serialization is inside every number.** All engines pay it and it is not
  separated out, which is exactly why the 5.4% row is a tie: at 27,000 rows the marshalling
  dominates the lookup. A benchmark isolating engine time would show a larger gap; this one
  measures what a client actually waits for.
- **All latencies include HTTP and JSON serialization** of ~2,806 rows, identical across
  engines, which compresses the visible ratio relative to raw engine time.
- **logd's advantage requires the template to be declared in advance.** On an undeclared
  field it falls back to scan and the advantage disappears entirely. That is the honest
  shape of the trade: logd is not a drop-in Loki replacement, it asks the operator to
  say up front what will be queried by value.
- **Loki was deliberately tuned to be fair**, not to look bad. Every deviation from its
  defaults is commented in `stack/loki/loki.yaml`; the important ones are raising
  `ingestion_rate_mb` from 4 (the default would 429 the generator and hand logd a
  meaningless ingest win), disabling `query_range.cache_results` (or runs 2..N measure the
  cache), and turning off `discover_service_name`/`discover_log_levels` (they synthesise
  labels the other two do not have, breaking parity for reasons unrelated to storage).

---

## Tuning the index list

logd only accelerates values it was told about, so choosing templates is the operator's
real work. `/logd/api/v1/index_stats` (`make index-stats`) is the feedback signal:

```
candidates == 0        the anchor never appears — this pattern is for other data
matches < candidates   the anchor fires but the pattern doesn't align after it
failures > 0           it aligns, but a capture won't parse as the declared type
matches ≈ candidates    healthy
```

A worked example is built in. `stack/logd/logd.yaml` deliberately does **not** declare the
warn-level `waited={ms:int}ms` branch (~4% of lines), so there is a real gap to find and
close.

This also catches a mistake that is otherwise invisible: the repo's example `config.yaml`
declares `took {ms:int}ms` **with a space**, while the generator emits `took=247ms`. The
anchor never matches, the `.tidx` stays empty, every range query silently degrades to a
scan, and the daemon reports nothing wrong. `candidates: 0` is how you see it.

---

## Layout

```
docker-compose.yml            logd + Loki + VictoriaLogs + Grafana (+ alloy profile)
Makefile                      up / load / explain / compare / index-stats / verify-loki
stack/logd/logd.yaml          templates + label allowlist for the testbed
stack/loki/loki.yaml          single-binary filesystem Loki; every deviation commented
stack/grafana/provisioning/   all three datasources
cmd/loggen/                   corpus generator + multi-target Loki push driver
internal/loggen/              ported generator; corpus.go and push.go are new
scripts/bench-vs.sh           interleaved A/B latency across all engines
scripts/compare-self.sh       index vs forced scan, with a correctness gate first
scripts/window.sh             derives the query window from a manifest
```

## Operational notes

- `make verify-loki` runs Loki's own `-verify-config`. Config keys drift between minors;
  re-run it on every image bump.
- **Neither Loki nor VictoriaLogs ships a shell** (both are distroless/scratch), so neither
  can run a container-local healthcheck — `CMD-SHELL` fails with
  `stat /bin/sh: no such file or directory`, which Docker reports as "unhealthy" and is
  indistinguishable from the service being broken. A one-shot `waiter` container polls all
  three from the outside instead.
- VictoriaLogs speaks the Loki **push** protocol but not the Loki **query** API; its
  language is LogsQL, so Grafana needs the `victoriametrics-logs-datasource` plugin
  (installed via `GF_INSTALL_PLUGINS`, which downloads at container start and is the
  flakiest part of `up`).
- `docker compose down -v` between benchmark runs. Without `-v` the volumes persist and the
  disk figures include the previous run.
