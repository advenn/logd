# logd_v2 — Phase 1–2 build spec (worker-2 handoff)

Authoritative build instructions for the storage foundation of the fresh `core/` + `compat/`
tree. Conform to `docs/logd-design.md` (the master design); this file pins the exact,
non-negotiable decisions so there is zero ambiguity. **This is a learning project** — write
detailed, heavily-commented Go that a human will re-type by hand to learn from. Comment the
*why* (byte layouts, invariants, crash semantics), not just the *what*.

Module: `github.com/advenn/logd`, `go 1.26.2`, **stdlib only** for Phases 1–2 (no yaml/snappy/
protobuf yet). Everything must end with `go build ./...`, `go vet ./...`, `go test ./...` green.

---

## Already in place (worker-1, do not rewrite)

- `core/storage/lookup.go` (+ `lookup_test.go`) — `LookupTable` string↔uint16 service interning, ported verbatim. Package `storage`.
- `core/storage/timeindex.go` — sparse time index (`IndexWriter`/`IndexReader`, 16-byte `[ts][pageNum]` records, binary search), ported verbatim. Package `storage`.
- `core/storage/errors.go` — `ErrPageCorrupted`, `ErrEntryTooLarge`, `ErrMalformedRecord`.

---

## Package boundary (design §3)

- `core/model` (package `model`): pure types, **no logd imports**. `LogEntry`, `LogLevel`, `LogLevel.String`, `ParseLogLevel`.
- `core/storage` (package `storage`): binary codec, page, segment, manifest, writer. **Imports `core/model`, never the reverse, never `compat/*`.**
- No `config` import in Phase 2. The writer takes an explicit `Options` struct. The elaborate extraction/label config (design §7.1) is Phase 3+; do not build it now.

---

## PHASE 1 — record + page

### `core/model/entry.go`

```go
type LogLevel uint8
const ( LogLevelDebug LogLevel = 0; LogLevelInfo = 1; LogLevelWarn = 2; LogLevelError = 3 )
// String() and ParseLogLevel(s string) (LogLevel, error) — port the case-insensitive
// alias table from the quarry (trace→debug, warning→warn, fatal/critical/panic→error, …).

type LogEntry struct {
    TS         time.Time // event time
    IngestedAt time.Time // arrival time (stamped by logd)
    Level      LogLevel  // DEBUG=0 INFO=1 WARN=2 ERROR=3
    ServiceID  uint16    // interned service name (hot-path filter)
    StreamID   uint32    // interned label set (label-index ref; 0 until the label phase)
    Message    string
    Extra      string    // raw JSON: high-cardinality fields
}
```

### `core/storage/record.go` — **REWRITE vs quarry (adds StreamID; 27-byte prefix)**

Free functions operating on `model.LogEntry` (LogEntry lives in another package, so these are
functions, not methods):

```go
func EncodedSize(e model.LogEntry) int
func EncodeEntry(e model.LogEntry) ([]byte, error)
func DecodeEntry(data []byte) (model.LogEntry, error)
```

**Binary record format — all BigEndian (design §4):**

```
[8]  TS int64 unixnano
[8]  IngestedAt int64 unixnano
[1]  Level
[2]  ServiceID uint16
[4]  StreamID  uint32      ← NEW vs quarry, sits after ServiceID
[2]  MsgLen uint16   [N] Message
[2]  ExtraLen uint16 [M] Extra
```

- **`EncodedSize = 27 + N + M`** (was 23; the +4 is StreamID). Minimum record = 27 bytes.
- Field offsets: TS[0:8] IngestedAt[8:16] Level[16] ServiceID[17:19] StreamID[19:23] MsgLen[23:25] Message[25:25+N] then ExtraLen, Extra.
- `EncodeEntry`: reject Message/Extra > 65535 → `ErrEntryTooLarge` (wrap with `%w`).
- `DecodeEntry`: bounds-check **before every read**; truncation → `ErrMalformedRecord`; `Level > LogLevelError` → `ErrMalformedRecord`. `time.Unix(0, nano)` to rebuild timestamps.
- Timestamps use `UnixNano()`. (Note the well-known `IsZero`/`UnixNano` edge — keep it simple: store `e.TS.UnixNano()`; a zero `time.Time` is fine to round-trip as its nano value.)

### `core/storage/page.go` — **REWRITE the checksum vs quarry (full-page, not header-only)**

Keep: `PageSize = 4096`, `PageHeaderSize = 32`, `PageMagic uint32 = 0x4C30474E`, the 32-byte
`PageHeader` struct + field offsets (Magic[0:4] MinTS[4:12] MaxTS[12:20] EntryCount[20:22]
FreeSpaceOffset[22:24] Checksum[24:28] Reserved[28:32]), `NewPageHeader`, `encode/decode/Read/
Write PageHeader`, `PageOffset`, `CanFitEntry`. **Fix the magic comment** — `0x4C30474E` is ASCII
`"L0GN"`, not `"L0GD"`; state the bytes correctly or drop the ASCII claim.

**The change (design §5, "Checksum: CRC32(IEEE) over the entire 4096-byte page"):** the checksum
must cover the **whole 4096-byte page** (header + all record bytes) with the checksum field
`[24:28]` zeroed — this is what detects torn/corrupt *record* data, which a header-only checksum
cannot. So the checksum is computed over the assembled page buffer, not the header struct alone:

```go
// ChecksumPage computes CRC32-IEEE over the full 4096-byte page with bytes [24:28] zeroed.
func ChecksumPage(page []byte) uint32
// ValidatePage checks magic (bytes [0:4]) and full-page checksum; returns ErrPageCorrupted.
func ValidatePage(page []byte) error
```

The writer, at flush, encodes the header into the page buffer, then sets the header's Checksum =
`ChecksumPage(page)`, re-encodes the checksum field, and writes. Readers call `ValidatePage` on
the whole 4096-byte page they read. (You may keep a header-level helper for convenience, but the
authoritative check is full-page. Torn-page recovery in Phase 2 relies on this.)

### Phase 1 tests

- `core/model` needs no test file unless helpful; `String`/`ParseLogLevel` table test is welcome.
- `core/storage/record_test.go` (adapt the quarry's cases): round-trip with **all fields incl. StreamID** populated; empty Message; empty Extra; truncated slice → `ErrMalformedRecord`; `EncodedSize` == `len(EncodeEntry)`; Message at 65535 ok; Message at 65536 → `ErrEntryTooLarge`; every LogLevel round-trips; level byte 4 → `ErrMalformedRecord`. Assert the **27-byte** minimum explicitly.
- `core/storage/page_test.go`: header round-trip; **full-page checksum test that flips a byte in the DATA region (offset > 32) and asserts `ValidatePage` fails** — this is the behavior a header-only checksum would miss and is the whole point of the change; `CanFitEntry`; `PageOffset`.

---

## PHASE 2 — segment + writer + durability + manifest

### `core/storage/segment.go` — port from quarry, adapt to full-page checksum

Port `Segment`, `CreateSegment`, `OpenSegment`, `WritePage`, `ReadPage`, `ReadPageHeader`,
`Sync`, `Close`. Adaptations:
- `CreateSegment` writes the initial page-0 placeholder header with a **full-page** checksum.
- `OpenSegment` validates pages with **`ValidatePage`** (full page), not header-only. Keep the
  "scan pages to re-derive MinTS/MaxTS and next page number" behavior — it is load-bearing for
  crash recovery below.
- Page 0 stays a header-only placeholder; data starts at page 1. Document this.

### `core/storage/manifest.go` — port from quarry, **add the per-segment schema field (design §5/§7.3)**

Port `Manifest`, `SegmentMeta`, `SegmentState` (active|sealed), `NewManifest`, `LoadManifest`,
`Add`, `Update`, `Filter`, `Active`, `Seal`, `Get`, `Save` (atomic tmp+rename). Changes:
- **`SegmentMeta` gains an indexed-field schema field**: the schema the segment was built with,
  so the future planner consults the segment's own schema, never live config (§7.3). Extraction
  is Phase 3, so the concrete type is a forward-declared, JSON-serializable placeholder now — e.g.
  `Schema []FieldSchema` where `FieldSchema{ Name string; Type string }` (empty slice for Phase 2).
  The point is the manifest **format** carries it now so adding real schemas later is not a format
  break. Add a `json:"schema"` tag.
- **After `Save` (rename), fsync the directory** so the manifest swap is durable (crash-safe seal).
- Keep `Save` atomic (tmp + rename).

### `core/storage/writer.go` — **REWRITE (this is the heart of Phase 2)**

The quarry writer entangles extraction, template-index building, and Loki-label tracking into the
write path. Strip all of that. The new writer is lean and owns durability.

**Construction / options (no config import):**
```go
type Options struct {
    SegmentSizeBytes int64         // rotate when the active segment reaches this
    FlushInterval    time.Duration // group-commit cadence
    // (reserved) SyncOnFlush bool — see durability; default true
}
type Writer struct { /* single-writer-goroutine storage engine */ }
func NewWriter(dir string, opts Options) (*Writer, error) // MkdirAll(dir/segments), load/recover manifest+lookup, run recovery (below)
func (w *Writer) Start(ctx context.Context) error         // launch the writer goroutine
func (w *Writer) Write(e model.LogEntry) error            // non-blocking enqueue onto buffered channel
func (w *Writer) Close() error                            // drain, flush, seal, persist, fsync
```

**Invariants to hold (design §2, §7.4):**
- One writer goroutine is the sole disk writer. Handlers enqueue on a buffered channel. Append-only.
- **Extraction is NOT here.** No template import. The write input is a bare `model.LogEntry`.
  **Reserve the seam**: document that the future input is a `(LogEntry, []extractedValue)` bundle
  (Phase 3) and shape the channel/`processEntry` so adding the values slice is additive, not a
  rewrite.
- **Reserve the live-tail notify seam** (design §11): in the writer loop, right after a record is
  appended, leave a documented no-op hook (`w.notify(e)`) where the future subscriber fan-out goes.
  Retrofitting a broadcast into a tuned hot loop is the thing to avoid.

**Durability — fix the SIGKILL data-loss bug in one motion (design §8):** the quarry only fsyncs at
seal and only persists manifest bounds at seal, so a crash loses recent pages AND leaves the active
segment with stale/zero bounds (dropped from time-range queries on restart). Do both of these:

1. **Group-commit fsync on flush.** When a page is flushed to the segment, `Sync()` the segment (and
   the time index) on the flush cadence / per flushed page. Document the loss window (≤ one
   `FlushInterval`). This makes flushed pages durable without fsync-per-write.
2. **Crash recovery on open.** In `NewWriter`, after `LoadManifest`, for the **active** segment:
   `OpenSegment` (which re-scans pages to derive MinTS/MaxTS) and **write the recovered bounds back
   into the manifest**, so recent data is visible to queries even though the crash never ran `Save`
   at seal. Then continue appending to it (or seal+rotate — your call; re-opening and appending is
   fine and simpler to test).
3. **Torn-page truncation.** During that active-segment scan, the first page that fails
   `ValidatePage` (a partially-written trailing page from a crash mid-write) marks the truncation
   point: truncate the segment file to the last valid page boundary (`f.Truncate(lastGoodPageEnd)`)
   before resuming. A torn page must never be read as data.

**Seal (design §7.5, minus the Phase-3 .tidx):** fsync segment → update manifest (final bounds,
state=sealed, **schema snapshot**) → `Save` (atomic + dir fsync) → drop buffers. No `.tidx` yet.

**Service interning:** keep the `LookupTable` for service names; persist to `lookup.bin` on
seal/close. For Phase 2, resolving `ServiceID` may stay a small writer helper (a test can also set
`ServiceID` directly). Note in a comment that interning moves to the ingest/compat layer in Phase 6
(design §7.4 step 1). Do **not** re-introduce Loki `Extra`-label tracking — that is the label-index
phase.

**Rotation:** when `segment.Size >= opts.SegmentSizeBytes`, seal and create a new active segment.
Name segments without the manifest data race the quarry had — use the `meta.ID` returned by
`manifest.Add` for the filename; never read `manifest.nextID` outside the manifest's lock.

### Minimal read-back for tests (not the full query engine)

Phase 4 is the real native query engine. For Phase 2 you only need enough to *prove durability*:
a small unexported helper (or `ReadAll(dir) ([]model.LogEntry, error)`) that enumerates segments
from the manifest, reads each data page, `ValidatePage`, and `DecodeEntry`s the records in
`[PageHeaderSize:FreeSpaceOffset]` sequentially. Keep it minimal and clearly labeled as a test/aid
scaffold, not the query layer.

### Phase 2 tests (the durability tests are the important ones)

Use `t.TempDir()`. Cover:
- **Round-trip:** open writer, write N entries across multiple pages, close, reopen, read all back — count and field-equality (TS to nanosecond, Level, Message, Extra, ServiceID).
- **Rotation:** small `SegmentSizeBytes` forces ≥2 segments; manifest lists them; all entries still read back.
- **Seal:** after close, `manifest.json` shows the sealed state and the `schema` field is present (even if empty).
- **SIGKILL / ungraceful-crash recovery (regression for the bug):** write + flush + fsync entries, then **simulate a crash — construct a *new* `Writer` on the same dir WITHOUT having called `Close()` on the first** (drop it). Assert: recovered entries are readable AND the active segment's manifest bounds are non-zero / correct so a time-range read returns them. This proves fix #2.
- **Torn trailing page:** after a clean write, append junk bytes (a partial page) to the active segment file, then reopen; assert recovery truncates the torn page and all previously-valid entries survive (fix #3). Full-page checksum (Phase 1) is what makes this detectable.

Keep every file heavily commented for the re-type-to-learn workflow.
