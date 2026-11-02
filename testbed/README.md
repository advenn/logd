# logd testbed — head-to-head with Loki, VictoriaLogs and PostgreSQL

One `docker compose up` brings up **logd, Grafana Loki, VictoriaLogs and Grafana**, feeds
all three identical traffic, and lets you compare them on the same queries in the same UI.
**PostgreSQL** runs alongside as the relational baseline, loaded from the same manifest and
queried over HTTP through a small shim, so the harness can time it the same way.

```sh
make up          # logd + Loki + VictoriaLogs + PostgreSQL (+ shim) + Grafana
make load        # generate a corpus, push identical bytes to all three log stores
make pg-load     # load the same corpus into PostgreSQL, print load time and sizes
make gate        # every engine's row count against the manifest's ground truth
make explain     # which access path did logd choose?
make compare     # logd's index path vs its own forced scan
make bench       # interleaved latency across logd, Loki and VictoriaLogs
make hdfs        # the Loghub HDFS replay, end to end (destructive: fresh volumes)
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
timings unless every engine returned the same non-empty result set (the sorted
timestamp/line pairs, compared byte for byte) and every timed sample answered 200 with a
response the size of the gate's.

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

logd also accepts the Loki query as written: its `| pattern` equals the declared template
`took={ms:int}ms`, so it is answered from the same index as logd's own form and returns the
same rows (`/logd/api/v1/explain` shows the mapping; see the main README's LogQL section).

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
  to **1.05M lines/s** — 51×. Pushes are acknowledged once queued, so the window of
  accepted-but-lost logs is wider than the sync interval alone: a process crash loses the
  queue and the page being filled (up to `flush_interval_ms`, 500 ms by default), and a
  machine crash can additionally lose pages not yet fsynced (up to `sync_interval_ms` plus
  one flush tick). A partial page is still CRC-caught and truncated on recovery, so data is
  never corrupt, only absent. Negative restores fsync-every-page.
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
| VictoriaLogs | **7.81 MB** | 1.9× smaller |
| logd | **15.0 MB** | — |
| Loki | 57.5 MB | **3.8× larger** |

logd was **92.3 MB** before any compression — larger than Loki and 12× larger than
VictoriaLogs. Three changes got it here:

| | before | after | |
|---|---|---|---|
| `.logz` (segment data) | 81.7 MB | 11.8 MB | 6.9× |
| `.tidx` (typed-range index) | 9.35 MB | 1.86 MB | 5.0× |
| `.lidx` + `.idx` | 1.27 MB | 1.27 MB | — |
| **total** | **92.3 MB** | **15.0 MB** | **6.2×** |

**Segments** could not simply be gzipped: records are addressable by segment-relative byte
offset (the `.tidx` stores exactly those, and the query path divides to get
`(page, in-page offset)`), and a variable-length stream has no O(1) offset mapping. Sealed
segments are rewritten into a `.logz` sidecar with a block directory that preserves logical
page numbering.

**A preset dictionary matters more than the compression level.** A 32 KB block cannot build
a useful window for itself — deflate starts each block empty, so the repeated shape of log
lines is re-learned 2,048 times per segment. Priming it with a sample of the segment's own
content, stored in the file:

| | ratio |
|---|---|
| flate -6 | 6.15× |
| flate -9 | 6.32× |
| flate -6 + dictionary | 6.72× |
| **flate -7 + dictionary** | **6.91×** |
| flate -9 + dictionary | 7.04× (3.3× the seal cost) |

No dependency was added, and the measurements are why: stdlib `flate -7 +dict` (6.91×) beats
`zstd -9` (6.81×), and `flate -9 +dict` (7.04×) matches `zstd -19` (7.03×). Only `zstd -19`
with a trained dictionary did better (7.87×).

**`.tidx` had no offset constraint at all**, which is why it was a much smaller change:
`OpenReader` already loads the whole file and the `Reader` holds no file handle, so the
record region is simply deflated wholesale. It compresses 5× because an int key puts its
value in bytes `[8:16]` and zero-pads `[0:8]` (keys alone: **178×**).

`.logz` is still 79% of what remains. Closing the last gap to VictoriaLogs would mean
changing what is stored, not how it is compressed.

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

**The preset dictionary cost nothing either**, which is worth stating because a later run
appeared to show it had: logd's numbers rose ~1.3×, but so did Loki's and VictoriaLogs'
(117 → 149 ms on a query touching neither of the changed files), so the whole session was
slow. An isolated benchmark settles it — inflating dictionary-primed blocks is *faster*
(603 vs 487 MB/s), because the blocks are smaller and the 32 KB window prefill is a cheap
memcpy. **Cross-session latency comparisons on this box are unreliable; only same-run
comparisons and isolated benchmarks are.**

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

## PostgreSQL baseline (E-3)

The question it answers: *why not just use PostgreSQL with an extracted column and a B-tree?*
So it is configured the way a competent DBA would configure a small dedicated box — every
deviation from the defaults is commented in `stack/postgres/postgresql.conf` — and gets the
same 2 CPUs / 3 GB as the other backends. Image: `postgres:17` pinned by digest (17.11).

### Schema: two variants, one database

`stack/postgres/schema.sql`, with the reasoning inline. Both variants share one server, one
buffer pool and one config, and differ only in how the value is stored:

| table | variant | value | indexes |
|---|---|---|---|
| `logs` | **pre-structured** | `latency_ms integer GENERATED ALWAYS AS ((substring(line FROM 'took=([0-9]+)ms'))::integer) STORED` | `(latency_ms, ts_ns) WHERE latency_ms IS NOT NULL`, `(ts_ns)` |
| `logs_raw` | **unstructured** | extracted at query time with the same expression | `(ts_ns)` |
| `hdfs_logs` / `hdfs_logs_raw` | the same, for the HDFS replay | `size_bytes bigint … 'size ([0-9]+)'` | the same shapes |

- **The pre-structured variant is what a DBA would deploy** for a field known up front, and it
  needs exactly the up-front knowledge a logd template needs. The unstructured one is the
  fallback for a question nobody anticipated — PostgreSQL's scan, the counterpart of logd's
  forced scan.
- **Generated column, not loader extraction**, so PostgreSQL pays for its own extraction at
  insert, as logd does at ingest. Adding one to an existing table rewrites it under an exclusive
  lock; for that case the right tool is an expression index built `CONCURRENTLY`.
- **`(latency_ms, ts_ns)`**: the value predicate is the selective one, so it leads; `ts_ns`
  second lets the time window be checked inside the index (it changes nothing when the window
  covers the table, as here, and is what keeps "slow requests in the last hour" cheap over
  weeks of data). **Partial**, because 19% of corpus lines and 68% of HDFS lines carry no value,
  and every comparison implies `IS NOT NULL`.
- **Labels in a `streams` dictionary**, not a jsonb per row (58 bytes per line for
  `{app, env, region}`, over a quarter of what the row would be). **`ts_ns bigint`**, not `timestamptz`, which
  cannot hold the manifest's nanosecond primary key.
- **Partitioning by time** (daily, `DROP PARTITION` for retention) is how production would
  expire data; it is described in the schema and not used, because the corpora span minutes.

### Loading: `make pg-load` / `scripts/pg-load.sh`

`cmd/pgload` COPYs the manifest in the push driver's 1,000-line batches, each its own
transaction (durable when COPY returns: `synchronous_commit` is left on), with the indexes in
place, each variant alone on the server. Every load starts from a `CHECKPOINT`; `VACUUM (ANALYZE)`
runs after it and is timed separately. It fails if the generated column extracted a value from a
different number of rows than the manifest says carry one.

One run each, on the shared dev box (load average 4–8 during the run) — directional:

| dataset | variant | load | lines/s | WAL written | heap | indexes | **total** |
|---|---|---:|---:|---:|---:|---:|---:|
| corpus, 500k | structured | 7.58 s | 65,936 | 128.5 MB | 74.6 MB | 26.4 MB (value 15.2, ts 11.3) | **101.0 MB** |
| corpus, 500k | unstructured | 3.59 s | 139,218 | 94.4 MB | 72.8 MB | 11.3 MB | **84.1 MB** |
| HDFS, 2M | structured | 17.85 s | 112,016 | 512.9 MB | 378.9 MB | 65.4 MB (value 20.5, ts 44.9) | **444.3 MB** |
| HDFS, 2M | unstructured | 11.27 s | 177,468 | 456.7 MB | 372.7 MB | 44.9 MB | **417.7 MB** |

MB = 10⁶ bytes; total = `pg_total_relation_size`, WAL excluded. For scale, the 2026-09-27 run
measured logd at 15.6 MB, Loki at 10.4 MB (flushed) and VictoriaLogs at 8.1 MB for a 500k corpus
from the same seed. PostgreSQL's heap is uncompressed — a row is only TOAST-compressed once it
exceeds ~2 KB — and each line carries a 24-byte tuple header and a 4-byte line pointer.

### Querying: `pgshim` on port 7300

Every other engine is timed over HTTP including result serialisation, so PostgreSQL is too:
`cmd/pgshim` runs the query, groups rows into streams and returns Loki's JSON, newest first
with the limit applied (Loki's backward direction). The query is one whitelisted predicate —
`<field> <op> <integer>`, op one of `> >= < <= =` — parsed strictly and always bound as a
parameter; nothing from the request becomes SQL. The variant is required and travels in the
URL, which is how bench-vs.sh passes engine-specific parameters:

```
GET http://localhost:7300/loki/api/v1/query_range?variant=structured     query=latency_ms > 200
GET http://localhost:7300/loki/api/v1/query_range?variant=unstructured   query=latency_ms > 200
    &start=<ns>&end=<ns>&limit=<n>    (window [start, end); limit default 100)
GET http://localhost:7300/loki/api/v1/query_range?variant=structured&dataset=hdfs    query=size_bytes < 90000
```

As bench-vs.sh rows:

```sh
add "PostgreSQL (structured)"   "http://localhost:7300/loki/api/v1/query_range?variant=structured"   "latency_ms > $T" loki
add "PostgreSQL (unstructured)" "http://localhost:7300/loki/api/v1/query_range?variant=unstructured" "latency_ms > $T" loki
```

The SQL behind them (`internal/pgdata`, held to `schema.sql` by a test):

```sql
-- structured
SELECT stream_id, ts_ns, line FROM logs WHERE latency_ms > $1::bigint
  AND ts_ns >= $2 AND ts_ns < $3 ORDER BY ts_ns DESC LIMIT $4
-- unstructured: the LIKE prefilter is the counterpart of Loki's |= "took="
SELECT stream_id, ts_ns, line FROM logs_raw WHERE line LIKE '%took=%'
  AND (substring(line FROM 'took=([0-9]+)ms'))::integer > $1::bigint
  AND ts_ns >= $2 AND ts_ns < $3 ORDER BY ts_ns DESC LIMIT $4
```

**Correctness:** both variants return the manifest's exact rows (not just counts) at every
threshold — 27,089 / 3,861 / 1,066 / 469 on the 500k corpus, and the HDFS counts below.

What the planner does (`EXPLAIN ANALYZE`, server side, 500k corpus): the structured variant is a
bitmap scan of the partial index plus an in-memory sort — 38.7 ms at 5.4%, 0.8 ms at 0.094%. The
unstructured app query walks the `ts_ns` index backwards evaluating the regex per row
(2.14 s at 5.4%, single process); forcing a parallel seq scan would be 1.63 s, and is not done,
because that would be tuning the planner per query. On the 2M-line HDFS table the planner picks the
parallel seq scan itself (1.1 s). JIT stays at its default and never fires: the costliest plan is
costed just under `jit_above_cost`.

**Fairness notes to quote with any PostgreSQL number:**

- **Serialisation runs outside PostgreSQL's 2 CPUs.** The other engines serialise inside their own
  budget; the shim is a separate container capped at 1 CPU / 512 MB. It encodes one response on
  one goroutine, so the cap does not throttle it, but the PostgreSQL side has up to 3 CPUs in all.
- **Durability is not equal, and favours the others**: PostgreSQL's numbers are for batches
  fsynced before COPY returns; logd acknowledges at enqueue and group-commits every 50 ms; Loki and
  VictoriaLogs acknowledge from memory.
- **The load is not concurrent with the log stores' push**: they are pushed to together by one
  fan-out driver, PostgreSQL is loaded afterwards, alone.
- `plan_cache_mode = force_custom_plan` on the shim's connections, so every threshold is planned
  with its real value rather than a generic plan's fixed 33% estimate after five executions.

---

## Public dataset: Loghub HDFS replay (E-2)

The synthetic corpus was designed alongside logd's template, so they agree by construction.
`loggen -replay` pushes a real dataset through the same driver instead, with a ground-truth
manifest in the same schema. `make hdfs` (`scripts/hdfs-run.sh`) does the whole run: fresh
volumes, replay push to logd/Loki/VictoriaLogs, PostgreSQL load, gate per threshold, queries.

```sh
go run ./cmd/loggen -replay ~/.cache/logd-datasets/HDFS.log -replay-count 2000000 \
  -replay-value-regex 'size ([0-9]+)' -replay-value-name size_bytes -label app=hdfs \
  -manifest ~/.cache/logd-bench/hdfs-2000000.jsonl -push … -push … -push '…?_stream_fields=app,component'
```

- **Line text byte-for-byte.** Only the terminator is removed — HDFS.log is CRLF throughout, and
  the CR goes with the LF. Nothing is prefixed; the original `081109 203518` stays in the text.
- **Timestamps synthetic**, `base + i·1ms`, strictly monotonic, `-base-ts=auto` ending a minute
  before now — the source has one-second resolution, and an old replay is invisible to Loki.
- **Labels**: `app=hdfs` plus `component`, the logger's outer class lowercased
  (`dfs.DataNode$PacketResponder` → `datanode`), capped at 10 values with `other` as overflow.
  First 2M lines: **4 values** — datanode 1,432,269, fsnamesystem 500,833, datablockscanner 55,989,
  fsdataset 10,909. The whole file has 5 (plus `pendingreplicationblocks`, 47 lines).
- **Manifest**: corpus schema plus `value` / `value_name` (`size_bytes`) on lines that carry one,
  so `window.sh` and `gate.py` read it unchanged. ~530 MB at 2M lines: it goes to
  `~/.cache/logd-bench`, not `corpus/`, because `/tmp` is often a small tmpfs.
- **logd** uses `stack/logd/logd-hdfs.yaml` (template `size {bytes:int}` → field `size_bytes`;
  allowlist `app`, `component`), selected with `LOGD_CONFIG`, which hdfs-run.sh sets.

### What the data looks like (first 2,000,000 lines)

633,736 lines (31.7%) carry a size, in five forms; none carries two, so logd's any-occurrence
predicate and the engines' first-match extraction cannot disagree:

| lines | form |
|---:|---|
| 316,774 | `INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: IP is added to blk_X size N` |
| 316,494 | `INFO dfs.DataNode$PacketResponder: Received block blk_X of size N from /IP` |
| 280 | `INFO dfs.DataNode$DataXceiver: Received block blk_X src: /IP dest: /IP of size N` |
| 182 | `WARN dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: Redundant addStoredBlock request received for blk_X on IP size N` |
| 6 | `INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: addStoredBlock request received for blk_X on IP size N But it does not belong to any file.` |

The distribution is a point mass: **602,065 of the 633,736 sizes (95%) are exactly 67108864**, a
full 64 MiB block; the rest clump around 3.53–3.55 MB and 28.48–28.50 MB, plus 181 below
90 KB (min 0). So the selective end of a sweep is *small* blocks, `size_bytes < T`:

| `size_bytes <` | rows | of all lines |
|---:|---:|---:|
| 67108864 | 31,671 | 1.58% |
| 28490000 | 20,483 | 1.02% |
| 3535000 | 2,123 | 0.106% |
| 90000 | 181 | 0.0091% |

**5% is not reachable with one threshold**: everything below 64 MiB is 1.58%, and `>= 67108864`
jumps to 30.1% (602,065 rows, above bench-vs.sh's 200,000 limit).

### Queries, all verified against the manifest

```
logd          {app="hdfs"} | size_bytes < T                     (/loki/api/v1/query_range; forced scan: /logd/api/v1/query_scan)
Loki regexp   {app="hdfs"} |= "size " | regexp "size (?P<size_bytes>[0-9]+)" | size_bytes < T
Loki pattern  {app="hdfs"} |= "size " | pattern "<_> size <size_bytes> <_>" | pattern "<_> size <size_bytes>" | size_bytes < T
VictoriaLogs  _stream:{app="hdfs"} "size" | extract_regexp "size (?P<size_bytes>[0-9]+)" | size_bytes:<T
VictoriaLogs  _stream:{app="hdfs"} "size" | extract "size <size_bytes> " | extract if (size_bytes:"") "size <size_bytes>" | size_bytes:<T
PostgreSQL    http://localhost:7300/loki/api/v1/query_range?variant=structured&dataset=hdfs     query=size_bytes < T
              http://localhost:7300/loki/api/v1/query_range?variant=unstructured&dataset=hdfs   query=size_bytes < T
```

**The real dataset exposed a query-language difference the synthetic one could not.** In two of
the five forms the number is followed by text, in three it ends the line. logd's template ends in
its capture, so the digits end it and one template covers all five. A single Loki `| pattern
"<_> size <size_bytes>"` captures `N from /10.x` on the first kind, the numeric filter fails, and
Loki keeps those lines with an `__error__` label instead of dropping them: **200,000 rows (the
limit), 199,740 of them errors, for 2,123 expected** — a wrong answer with no error status. Two
pattern stages are exact (the first parser to extract a label wins in Loki 3.7). VictoriaLogs'
single `extract` returns 1,136 or 987 of the 2,123 rows (at `< 3535000`), depending on the
trailing space; `extract_regexp`, or a second `extract if (size_bytes:"")`, is exact.

**Gate (one run):** at all four thresholds, all eight rows — logd index, logd forced scan, Loki
pattern, Loki regexp, VictoriaLogs extract_regexp and extract, PostgreSQL structured and
unstructured — returned the manifest's count, and at `< 3535000` the exact (ts, line) rows.

Ingest during that run (2,000 requests of 1,000 lines, concurrent fan-out, zero retries, one run on
a loaded box): VictoriaLogs 12.6 s (159k lines/s), Loki 17.9 s (112k), logd 18.8 s (106k).

bench-vs.sh's queries are written for the synthetic corpus's field (`latency_ms`, `took=`; only
`APP` and `THRESHOLD` are variables), so an HDFS sweep needs the queries above, with
`eval "$(./scripts/window.sh <manifest> env)"` for the window.

---

## Honest caveats

Read these before quoting any number above.

- **Selectivity decides the answer, and which competitor you name decides it too.** On the
  same corpus logd is 3–5.7× faster than Loki at every threshold, but versus VictoriaLogs it
  ranges from 0.63× (slower, at 5.4%) to 1.8× (faster, at 0.10%). Quote the table, or quote
  nothing.
- **logd wins on query, still trails on ingest and disk.** A fair summary includes all
  three. After the write-path and compression work logd is 2.8× slower to ingest than
  VictoriaLogs (was 27×) and 1.9× larger on disk (was 12×) — but 3.8× *smaller* than Loki,
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
- **PostgreSQL with a generated column and a partial B-tree is a serious baseline**, not a straw
  man: in the gate's single unwarmed samples (loaded box, logd's active segment unsealed) the
  structured variant answered in 4–73 ms on the corpus and 6–144 ms on HDFS, below every log
  store in the same run. It pays on disk (5.4–6.5× logd's settled 15.6 MB), at ingest, and in a
  schema per field; measure it on a quiet machine before quoting any latency.
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
docker-compose.yml            logd + Loki + VictoriaLogs + PostgreSQL + pgshim + Grafana (+ alloy)
Makefile                      up / load / pg-load / gate / hdfs / explain / compare / ...
stack/logd/logd.yaml          templates + label allowlist for the synthetic corpus
stack/logd/logd-hdfs.yaml     the same for the HDFS replay (LOGD_CONFIG selects it)
stack/loki/loki.yaml          single-binary filesystem Loki; every deviation commented
stack/postgres/               postgresql.conf (every non-default commented) + schema.sql
stack/pgshim/Dockerfile       the shim's image, built from testbed/ alone
stack/grafana/provisioning/   the three log-store datasources
cmd/loggen/                   corpus generator, replay mode, multi-target Loki push driver
cmd/pgload/                   manifest -> PostgreSQL via COPY, with load time and sizes
cmd/pgshim/                   PostgreSQL behind a Loki-shaped query_range endpoint
internal/loggen/              ported generator; corpus.go, replay.go and push.go are new
internal/pgdata/              PostgreSQL tables, extraction expressions, predicate parser
scripts/bench-vs.sh           interleaved A/B latency across all engines
scripts/compare-self.sh       index vs forced scan, with a correctness gate first
scripts/resultset.py          canonical result set of a Loki-JSON or LogsQL-JSONL response
scripts/gate.py               every engine vs the manifest's ground truth; the exact queries
scripts/hdfs-run.sh           the HDFS replay end to end
scripts/pg-load.sh            load a manifest into PostgreSQL
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
  disk figures include the previous run. `COMPOSE_PROJECT_NAME=<other>` runs a second, isolated
  set of volumes (container names are fixed, so not at the same time as the first).
- PostgreSQL's schema is applied by `/docker-entrypoint-initdb.d` on a **fresh** volume only. After
  editing `schema.sql`, `docker compose down -v` (or drop the tables by hand); `pgload` truncates
  and reloads data but never alters tables.
