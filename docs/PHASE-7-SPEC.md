# logd_v2 — Phase 7 build spec (hardening: cost guard · retention · live labels)

Production-hardening. Three tractable items with clear correctness stories (design §13 Phase 7,
§7.4 cost guard, §14 retention). Metric-query evaluation (rate/count_over_time) and WebSocket
`/tail` are larger follow-ups, out of scope here. Invariant preserved: `Execute == ExecuteScan`.

## 1. Index cost guard (design §7.4 / discussion-v2 §7.4)

A non-selective pushdown (e.g. `latency >= 0` matching most records) fetches many records by
random-access offset — slower than a sequential scan. Guard: when the intersected candidate set
exceeds a fraction of the segment's record count, discard it and SCAN instead (a DB planner's
seq-scan-over-index-scan choice). Correctness-neutral: both paths return identical results, so the
differential oracle still holds; the guard only changes the access path.

- `storage.SegmentMeta` gains `Records uint64` (json). The writer counts records per active segment
  (`segRecords`, reset on rotate, incremented per stored record) and records it at seal via the
  sealed manifest entry. Recovered segments record 0 (scan-only anyway).
- `core/query`: after `intersectLookups`, if `seg.Records > 0 && len(candidates) > costGuardFraction *
  seg.Records`, fall back to `scan()`. `costGuardFraction = 0.5` (constant; configurable later).
- `Explain` reports `scan` when the guard would trip (so it matches what Execute does).

## 2. Retention (design §14, §12)

Bound disk by deleting SEALED segments whose data is entirely older than the retention window. Per-
segment files make this trivial — no compaction/tombstones (the whole reason the index is per-segment).

- `config.RetentionDays` (already loadable via YAML; 0 = keep forever).
- `storage.Manifest` gains `Remove(id)` (under the write lock). `Writer.enforceRetention(now time.Time)`
  deletes every SEALED segment with `MaxTS < now - retention`: remove its manifest entry, then delete
  its `.log`, `.idx`, and all `.tidx`/`.lidx` sidecar files (glob `<segBase>.*`), then `Save` the
  manifest. Never touches the active segment.
- A background goroutine (started in `Start` when retention > 0) calls `enforceRetention(time.Now())`
  on a ticker (hourly); it stops on shutdown. `enforceRetention` is exported-for-test via a controlled
  `now`.
- **Query degrade on a retention race:** a segment can be deleted between `Manifest.Filter` and the
  query opening its file. `querySegment` must treat a missing segment file (`os.IsNotExist`) as an
  empty result for that segment (skip), NOT an error — the data was intentionally retention-expired.
  Apply to both the scan and the fetch paths.

## 3. Active-segment live label discovery

Today `/labels`,`/label/values`,`/series` enumerate only sealed segments' `.lidx`, so a freshly-started
daemon shows no labels until the first seal (Grafana dropdowns empty). Fix: the writer tracks the ACTIVE
segment's label streams in a concurrency-safe structure, and enumeration unions it with the sealed indexes.

- `storage.Writer`: a mutex-guarded `liveStreams map[string]label.Set` (canonical → set), updated in
  `processEntry` for each labeled record (deduped by canonical), RESET on rotate (the just-sealed
  segment's labels are then served from its `.lidx`, so no loss and the structure stays bounded by the
  active segment's cardinality). `Writer.LiveStreams() []label.Set` returns a snapshot under the lock.
- `core/query`: `Engine.Labels()/LabelValues()/Series()` union the sealed `.lidx` enumeration with
  `w.LiveStreams()`. (Enumeration is discovery — approximate/eventually-consistent is fine; a live label
  appearing immediately is strictly better than after-seal-only.)

## Tests

- **Cost guard:** a query whose candidate set exceeds the fraction → `Explain` reports `scan`, and
  `Execute == ExecuteScan` (unchanged result). A selective query still reports `index`. Record count is
  recorded in the sealed manifest.
- **Retention:** seal segments across a time span, `enforceRetention(now)` with a cutoff deletes the old
  ones (files gone, manifest updated), keeps the recent ones; a query after retention returns only
  surviving data and does not error. A retention race (delete a segment's files, then query) degrades to
  skipping it, not a 500.
- **Live labels:** ingest labeled records WITHOUT sealing → `Engine.Labels()`/`Series()` already show
  them (before any seal); after seal they still show (deduped, no double-count).
- All under `-race` (live labels add cross-goroutine access — the mutex must hold up).

Keep `go build ./... && go vet ./... && go test -race ./...` green throughout.
