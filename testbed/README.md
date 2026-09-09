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

### Query latency — 500,000 lines, all three fed identically

Medians of 7 runs, 3 warm-ups discarded. Every row verified to return the identical result
set from all three first (26,996 / 3,935 / 1,046 / 482 — matching the manifest exactly).

**Typed range over a value inside unstructured text** — the differentiator:

| `latency_ms >` | rows | logd | Loki | VictoriaLogs |
|---|---|---|---|---|
| 200 | 26,996 (5.4%) | 230 ms | 703 ms | **145 ms** |
| 1000 | 3,935 (0.79%) | **128 ms** | 455 ms | 148 ms |
| 3000 | 1,046 (0.21%) | **71 ms** | 411 ms | 133 ms |
| 5000 | 482 (0.10%) | **69 ms** | 391 ms | 124 ms |

**VictoriaLogs is the real competitor here, not Loki.** It is flat and fast across the whole
sweep (124–148 ms) — it is clearly not brute-forcing the way Loki is (703 → 391 ms). logd
only wins once the query is *selective*: it is 1.8× faster at 0.10% but **loses at 5.4%**.
The honest summary is that logd beats Loki everywhere by 3–5.7×, and beats VictoriaLogs only
on needle-in-haystack lookups.

logd's time tracks the size of the *answer* (230 → 69 ms as the result shrinks 56×), which
is what the typed index is for. Loki's tracks the size of the *data*. VictoriaLogs' tracks
neither, which is worth understanding before claiming a win over it.

**"Last N lines"** — the query every Explore session opens with:

| limit | logd | Loki | VictoriaLogs |
|---|---|---|---|
| 1 | 29.3 ms | **21.2 ms** | 48.8 ms |
| 100 | 26.2 ms | **24.9 ms** | 53.2 ms |
| 1000 | **25.1 ms** | 27.4 ms | 70.6 ms |

Effectively a three-way tie. This was logd's worst result by far — **1558 ms against Loki's
15 ms** — before the limit was pushed down.

### Ingest

Identical 500 requests of 1,000 entries, closed-loop, zero retries everywhere, fresh
volumes:

| | accept time | lines/s | vs logd |
|---|---|---|---|
| VictoriaLogs | 2.37 s | 211k | 2.6× faster |
| Loki | 3.48 s | 144k | 1.8× faster |
| logd | **6.23 s** | **80k** | — |

logd was **59.9 s (8.3k lines/s)** before the write path was profiled — 27× behind
VictoriaLogs and 13× behind Loki. It is now 2.6× and 1.8× behind. What changed:

- **fsync was the whole bottleneck.** `flushPage` fsynced *twice* per 4 KB page; at ~35
  records per page that is ~28,000 fsyncs for this corpus. One of the two was pure waste —
  the sparse `.idx` is a derived accelerator that nothing reads and recovery rebuilds, so
  it is now synced once at seal instead of every page. That alone was 13.7k → 20.8k lines/s
  with no durability change.
- **Group commit** (`sync_interval_ms`, default 50 ms) took the isolated writer from 20.8k
  to **1.05M lines/s** — 51×. A crash risks at most ~50 ms of accepted logs; a partial page
  is still CRC-caught and truncated on recovery, so data is never corrupt, only absent.
  Negative restores fsync-every-page.
- **Label derivation** was then the biggest remaining per-record cost (46%, 31 of 38
  allocations) — it built the whole label map to keep 3 allowlisted keys, the same waste
  already fixed on the read path. 6998 → 4811 ns/record.

⚠️ The isolated writer benchmark shows 51×; end-to-end shows 9.4×. That gap is the point:
once fsync stopped dominating, HTTP, snappy/protobuf decode and extraction became the
bottleneck, under a 2-CPU cgroup limit. There is more to get here, and it is no longer in
storage.

⚠️ **Benchmark these on a real disk.** `b.TempDir()` honours `$TMPDIR`, and `/tmp` is tmpfs
on most Linux dev boxes, where `fsync` is a no-op — the write benchmark reports ~1.3M
lines/s there and tells you nothing. `core/storage/write_bench_test.go` requires
`LOGD_BENCH_DIR` and skips rather than silently producing a RAM-disk number.

### Storage — 500,000 identical lines

| | on disk | vs logd |
|---|---|---|
| VictoriaLogs | **7.80 MB** | 2.1× smaller |
| logd | **16.4 MB** | — |
| Loki | 57.5 MB | **3.5× larger** |

logd was **92.3 MB** before any compression — larger than Loki and 12× larger than
VictoriaLogs. Two changes got it here:

| | before | after | |
|---|---|---|---|
| `.logz` (segment data) | 81.7 MB | 13.2 MB | 6.2× |
| `.tidx` (typed-range index) | 9.35 MB | **1.86 MB** | **5.0×** |
| `.lidx` + `.idx` | 1.27 MB | 1.27 MB | — |
| **total** | **92.3 MB** | **16.4 MB** | **5.6×** |

**Segments** could not simply be gzipped: records are addressable by segment-relative byte
offset (the `.tidx` stores exactly those, and the query path divides to get
`(page, in-page offset)`), and a variable-length stream has no O(1) offset mapping. Sealed
segments are therefore rewritten into a `.logz` sidecar with a block directory that
preserves logical page numbering. Block size is the tradeoff — 4 KB pages give 4.1×, 32 KB
blocks 6.2× (default), whole-file 6.9×.

**`.tidx` had no such constraint**, which is why it was a much smaller change: `OpenReader`
already loads the whole file and materializes every record, and the `Reader` holds no file
handle at all — lookups binary-search an in-memory slice. So the record region is simply
deflated wholesale. It compresses 5× because an int key puts its value in bytes `[8:16]` and
zero-pads `[0:8]` (keys alone: **178×**), and literal-existence fields key *every* entry
under the all-zero key.

`.logz` is now 81% of what remains. Further disk work means attacking the log text itself —
a stronger codec or a shared dictionary — which is a different kind of change.

### Compression costs query latency

Not free, and worth stating plainly — every page read now decompresses a 32 KB block:

| query | uncompressed | after segment compression | after `.tidx` too |
|---|---|---|---|
| `latency_ms > 200` | 230 ms | 372 ms | 364 ms |
| `latency_ms > 5000` | 69 ms | 89 ms | 94 ms |
| last 100 lines | 26 ms | 66 ms | 71 ms |

**Segment compression cost roughly 1.3–2.5× on reads; `.tidx` compression cost nothing
measurable** — the index is decompressed once per file at open and then cached, whereas
segment pages are inflated per block on every read.

The one visible `.tidx` cost is a colder first query: 144 ms immediately after a restart vs
83 ms warm, the ~61 ms being two `.tidx` files inflating into the reader cache. Bounded, and
paid once per file.

`compress_block_pages` tunes the segment trade (smaller blocks decompress less per lookup
and compress worse); a negative value turns segment compression off entirely. The
before/after columns come from separate runs on a shared box, so treat the smaller deltas as
directional.

### logd vs itself

The cross-engine numbers always invite "you configured it wrong". This one cannot: same
binary, same data, same page cache, one variable — whether the `.tidx` is consulted. Earlier
run, same corpus:

| `latency_ms >` | index | forced scan | speedup |
|---|---|---|---|
| 200 | 151 ms | 1665 ms | **11×** |
| 5000 | 38 ms | 1681 ms | **44×** |

`make compare` runs this and refuses to print timings if the two paths disagree on a row.

### Optimization history

Four rounds of work: read path, write path, segment compression, index compression. The read
path was profiled and reworked in four commits. Benchmarks are
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

### Earlier 50,000-line run

Superseded by the 500k numbers above, but the ingest figures there are what surfaced the
duplication bug described next: logd needed **225 retries** to absorb 50,000 lines while
Loki and VictoriaLogs needed zero, and stored **169,692 rows for 50,000 pushed lines**.

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

- **Selectivity decides the answer, and which competitor you name decides it too.** On the
  same corpus logd is 3–5.7× faster than Loki at every threshold, but versus VictoriaLogs it
  ranges from 0.63× (slower, at 5.4%) to 1.8× (faster, at 0.10%). Quote the table, or quote
  nothing.
- **logd wins on query, still trails on ingest and disk.** A fair summary includes all
  three. After the write-path and compression work logd is 2.9× slower to ingest than
  VictoriaLogs (was 27×) and 2.1× larger on disk (was 12×) — but 3.5× *smaller* than Loki,
  which it used to lose to on both.
- **Segment compression trades read latency for disk**; `.tidx` compression did not. 5.6×
  less disk overall cost roughly 1.3–2.5× on query latency, all of it from the segment side.
  Tunable via `compress_block_pages`, disableable entirely.
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
