# logd testbed — head-to-head with Loki and VictoriaLogs

One `docker compose up` brings up **logd, Grafana Loki, VictoriaLogs and Grafana**, feeds
all three identical traffic, and lets you compare them on the same queries in the same UI.

```sh
make up          # logd + Loki + VictoriaLogs + Grafana
make load        # generate a corpus, push identical bytes to all three
make explain     # which access path did logd choose?
make compare     # logd's index path vs its own forced scan
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

## Results (50,000 lines, 8-core dev box)

These are real numbers from this harness, not estimates. They are also **small-scale** —
enough to prove the mechanism works, not enough to publish as a benchmark. The
index-vs-scan comparison was repeated four times; everything else is a single run. See
*Honest caveats* below.

Corpus: unstructured application logs, e.g.
`GET /api/v1/orders 200 took=247ms trace_id=…`. 81% of lines carry a `took=` duration;
`latency_ms > 200` selects 5.6% of them.

### Correctness — all three agree with ground truth

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

### Query latency — the differentiator

| | mean | notes |
|---|---|---|
| **logd (typed index)** | **42–62 ms** | `explain`: 1 segment indexed, 2,824 candidates of 50,000 |
| logd (forced scan) | 294–441 ms | *same binary, same data, same page cache* — index off |
| Loki (`\| pattern`) | 59 ms | |

Ranges, not points: across four runs the index/scan speedup landed at **6.5×, 6.5×, 6.9×
and 7.9×**. That spread on an otherwise-identical workload is the honest measure of how
noisy this box is, and the reason none of these should be quoted as a single figure.

**The logd-vs-logd number is the one that matters.** An index-vs-scan result against
another product always invites "you configured it wrong"; a comparison against the same
binary over the same data has exactly one variable — whether the `.tidx` is consulted.
`make compare` runs it and refuses to print timings if the two paths disagree on a row.

Note logd narrowed 50,000 records to 2,824 candidates, then re-verified down to the exact
2,806. The 18 extra are lossy-key false positives that the re-verify pass removes — by
design, the index over-approximates and is never trusted for the final answer.

### Storage — logd loses, clearly

| | on disk | vs logd |
|---|---|---|
| VictoriaLogs | **0.81 MB** | 12× smaller |
| Loki | 5.14 MB | 1.9× smaller |
| logd | 9.60 MB | — (8.52 data + 1.06 index) |

logd stores records uncompressed in 4 KB pages. There is no block compression anywhere in
the write path. This is a real, structural disadvantage and it is not close.

### Ingest

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

- **50,000 lines is small.** Loki at 59 ms is doing brute force over a corpus small enough
  that brute force is cheap. logd's advantage should widen with scale, and that is a
  hypothesis this harness has not yet tested, not a result.
- **Shared dev box**, other containers running throughout. Only the index-vs-scan
  comparison was repeated (4 runs, spread 6.5x-7.9x); the cross-engine latency and all the
  disk and ingest figures are single runs with no percentiles. Treat them as directional.
- **Warm page cache.** Five warm-ups discarded per measurement. No cold-cache numbers.
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
