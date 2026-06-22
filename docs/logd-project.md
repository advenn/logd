# `logd` — Project Design Document

## What It Is

A lightweight, standalone log storage daemon written in Go. Loki-compatible query API, designed for small/medium single-node deployments where the full Grafana/Loki/Promtail stack is too heavy.

### Real motivation

Running Django + Celery on a VPS with the LGTM stack produced this:

```
Loki:       155MB RAM
Grafana:    287MB RAM
Promtail:   104MB RAM
Total:      ~547MB

Django app:        383MB
Celery worker:     403MB
Celery beat:       187MB
App total:         ~973MB
```

The observability stack consumes 56% the memory of the actual application. On a $10-20/mo VPS this is unsustainable.

### The pitch

> "Lightweight append-only log storage daemon with Loki-compatible query API. Drop-in replacement for Loki on single-node deployments. Target footprint: <50MB RAM."

---

## Why Loki Is Heavy

- Loki holds logs **in memory as chunks** per stream, flushing only when chunk hits size limit or `max_chunk_age` (default: 2 hours). Each unique label combination = a separate chunk in RAM.
- Loki is a distributed system squeezed into one process for single-node use — it carries the memory cost of that architecture regardless of workload size.
- Promtail loads a full pipeline (read → parse → label → batch → ship) sized for large workloads, even when tailing 3 containers.
- Grafana loads all plugins on startup, runs alerting engine, session store — most of it unused in a solo dev setup.

---

## Core Design Philosophy

- **Append-only** — never mutate written records. Readers and writers never contend on same data.
- **Single process** — no distributor, ingester, querier separation. One binary, one writer goroutine, one file store.
- **File-first** — filesystem is the source of truth. No external dependencies in v1.
- **Page-oriented I/O** — fixed-size pages like Postgres. Unit of I/O is always a full page, not individual records.
- **Loki-compatible query API** — plug into existing Grafana without a custom plugin.
- **Two ingestion paths** — Loki push format for Promtail users, native structured JSON for direct use.

---

## Log Entry Schema

### On the wire (JSON)

```json
{
  "ts": "2026-04-25T10:00:00+05:00",
  "level": "ERROR",
  "service": "celery_worker",
  "trace_id": "4bf92f35",
  "span_id": "00f067aa",
  "msg": "task failed: timeout",
  "order_id": "998"
}
```

- `ts` — RFC3339 with timezone offset. Optional on send, stamped by `logd` if missing.
- `ingested_at` — always stamped by `logd` on arrival. Never sent by client.
- `service` — injected from `LOGD_SERVICE` env var by the Python handler. No code change per service.

### On disk (binary)

```
Page header (32 bytes):
  [4 bytes:  magic 0x4C30474E]   ← "L0GD", detects corruption
  [8 bytes:  min_ts]             ← skip entire page without reading entries
  [8 bytes:  max_ts]
  [2 bytes:  entry_count]
  [2 bytes:  free_space_offset]
  [4 bytes:  checksum]
  [4 bytes:  reserved]

Per log entry:
  [8 bytes:  ts unix nano]
  [8 bytes:  ingested_at unix nano]
  [1 byte:   level]              ← 0x00=DEBUG 0x01=INFO 0x02=WARN 0x03=ERROR
  [2 bytes:  service_id]         ← lookup table, not repeated string
  [2 bytes:  msg_len]
  [N bytes:  msg]
  [2 bytes:  extra_len]
  [M bytes:  extra JSON]         ← trace_id, span_id, and all other fields
```

**Why binary:** Plain JSONL repeats field names thousands of times. Same entry in JSONL is ~3x larger. Binary header lets you scan forward without parsing JSON. Page `min_ts`/`max_ts` lets you skip entire pages during time-range scans.

**Lookup tables** (stored in a small separate file):

```
service_id:   0x0001=django_app  0x0002=celery_worker  0x0003=celery_beat
level:        0x00=DEBUG  0x01=INFO  0x02=WARN  0x03=ERROR
```

---

## Storage Layout

### Filesystem structure

```
/data/
  segments/
    2026/04/25/
      10.log          ← active segment (current hour)
      09.log          ← sealed
      09.idx          ← sparse time index for segment 09
  index/
    order_id.idx      ← global inverted index (template-based)
    driver_id.idx
    payment_id.idx
    status.idx
  manifest.json       ← in-memory segment registry, persisted on change
  templates.bin       ← compiled template cache
  checkpoint.json     ← last flush position for crash recovery
  lookup.bin          ← service_id and enum lookup tables
```

### Time-based folders

Segments organized by `YYYY/MM/DD/HH`. Time-range queries naturally map to folder paths — no manifest scan needed to eliminate irrelevant hours/days.

### Segment lifecycle

```
active (.log) → sealed (.log, read-only) → compressed (.log.zst) [post-v1]
```

Rotation triggers at 64MB. On seal: fsync, update manifest, open new segment.

---

## Write Path

```
Incoming HTTP entry
      ↓
Validate + parse
Stamp ingested_at
Lookup/assign service_id
      ↓
Push to writer channel
      ↓
Single writer goroutine (only one touches the file)
      ↓
Serialize to binary record
Append to current page in memory
      ↓
Every 500ms or 4KB:
  flush page to active segment file
  append to sparse index (ts → page_number)
  write checkpoint.json
      ↓
Segment hits 64MB:
  fsync
  seal segment (mark read-only in manifest)
  open new segment file
  merge index entries into global inverted indexes
```

**Why single writer goroutine:** eliminates all file locking. HTTP handlers push to a channel, writer goroutine is the only one doing I/O. Clean, simple, correct.

---

## Read Path

### Two-timestamp problem

Logs don't always arrive in order. A Celery task may log after a delay. Solution: store both timestamps, index both.

- `ingested_at` → physical write order (what the file guarantees)
- `ts` → event time (what queries use)

On query for `[T1, T2]`:
- Seek to `T1 - tolerance_window` (configurable, default 5 min)
- Scan forward
- Filter entries where `ts` falls in `[T1, T2]`

Small extra scan cost, zero data loss, zero write delay.

### Sparse time index per segment

Flat binary file, one entry per flush (not per log line):

```
[8 bytes: ts unix nano][8 bytes: page_number]
```

Fixed 16-byte records → record N is always at byte `N * 16`. Binary search is O(log n) with direct seeks. No parsing needed.

### Segment manifest (in memory)

```go
type SegmentMeta struct {
    ID      uint64
    MinTS   time.Time
    MaxTS   time.Time
    Path    string
    State   string    // "active", "sealed", "compressed"
}
```

Time-range query first filters manifest in memory — eliminates most segments without any disk access. Then binary search index for remaining segments.

### Full query flow

```
Query [T1, T2] with filter level=ERROR service=celery_worker
      ↓
1. Filter manifest by time range          → O(segments), in memory
2. Check template inverted indexes        → direct page refs if applicable  
3. Binary search sparse index per segment → land on right page
4. pread() full pages                     → fixed size, predictable I/O
5. Filter entries by ts, level, service   → in memory
6. Stream results
```

---

## Late Log Handling

**Problem:** `LOGD_SERVICE=celery_worker` container processes a task and logs 20 seconds after the HTTP request completed. Logs arrive out of order at `logd`.

**Solution:** dual-timestamp + tolerance window query. See Read Path above.

**Rejected alternative:** heap-based reorder buffer (hold logs 15s before writing). Rejected because: 15s write delay for all logs, up to 15s data loss on crash, added memory pressure. Not worth it for log data.

---

## Loki-Compatible API

Implements just enough of Loki's HTTP API to work with Grafana's built-in Loki data source. No custom Grafana plugin needed.

### Endpoints

```
POST /loki/api/v1/push          ← ingest (Loki push format, for Promtail)
GET  /loki/api/v1/query_range   ← time range query (core query endpoint)
GET  /loki/api/v1/labels        ← label keys (Grafana needs this on startup)
GET  /loki/api/v1/label/{name}/values
```

### Native structured endpoint (additional)

```
POST /v1/ingest                 ← native JSON, used by Python handler
```

Same storage backend. Two ingestion paths. Loki push format loses field structure (plain string log lines). Native endpoint preserves full binary record structure.

---

## Writer Identity

All Django, Celery worker, and Celery beat containers share the same Python logging config. Logs arrive at `logd` looking identical.

**Solution:** environment variable per container, read once at startup by the Python handler:

```yaml
# docker-compose.yml
django_app:
  environment:
    - LOGD_SERVICE=django_app

celery_worker:
  environment:
    - LOGD_SERVICE=celery_worker

celery_beat:
  environment:
    - LOGD_SERVICE=celery_beat
```

Python handler reads `LOGD_SERVICE`, attaches it to every log entry. One logging config, three identities. No application code change.

---

## Template-Based Dynamic Indexing

### The problem

Searching `order_id=1234` across logs requires full sequential scan. Loki's structured metadata solves extraction but not indexing — it still scans. Traditional database indexes don't apply because log structure is implicit in free-text strings.

### The solution

User-defined typed templates teach `logd` how to extract fields. Matched fields get inverted indexes. Search becomes O(log n).

### Template config

```yaml
templates:
  - name: order_created
    pattern: "order-{order_id:uint32} created"
    fields:
      - name: order_id
        type: uint32
        index: true

  - name: driver_assigned
    pattern: "driver-{driver_id:uint32} assigned to order-{order_id:uint32}"
    fields:
      - name: driver_id
        type: uint32
        index: true
      - name: order_id
        type: uint32
        index: true

  - name: payment_status
    pattern: "payment-{payment_id:string} status={status:enum}"
    fields:
      - name: payment_id
        type: string
        index: true
      - name: status
        type: enum
        values: [pending, completed, failed, refunded]
        index: true
```

### Supported field types

| Type | Index structure | Search |
|---|---|---|
| `uint32` / `uint64` | sorted flat file | binary search O(log n) |
| `string` | sorted flat file | binary search O(log n) |
| `enum` | bitmap per value | bitmap AND/OR O(1) |

### How templates compile

```
"order-{order_id:uint32} created"
  → ^order-(?P<order_id>\d{1,10}) created$

"payment-{payment_id:string} status={status:enum}"
  → ^payment-(?P<payment_id>\S+) status=(?P<status>pending|completed|failed|refunded)$
```

Compiled once on startup. Per ingested log line: run each regex, on match extract fields, write to index buffer.

### Inverted index file format (uint32/string)

```
[header]
  magic:        4 bytes
  field_type:   1 byte
  entry_count:  8 bytes

[entries — sorted by value]
  [4 bytes: value]
  [4 bytes: ref_count]
  [ref_count × 12 bytes: page refs]
    each ref: [8 bytes: segment_id][4 bytes: page_number]
```

### Bitmap index (enum)

```
[header]
  magic:       4 bytes
  enum_count:  1 byte
  page_count:  8 bytes

[per enum value]
  [1 byte:  value_id]
  [N bytes: bitmap — one bit per page across all segments]
```

`status=failed AND service=celery_worker` → AND two bitmaps → read only matching pages.

### Index write timing

Index entries buffered in memory during active segment. On segment seal: sort buffer, merge into global `.idx` files (merge sort, same pattern as LSM compaction). Keeps hot write path fast.

### Query with template index

```
query: order_id=1234
  → open order_id.idx
  → binary search for 1234
  → get page references
  → read exactly those pages
  → done. No segment scanning.
```

---

## Crash Recovery

- `checkpoint.json` — written after every flush. Contains last written segment ID and page number.
- On startup: read checkpoint → open last active segment → scan from last known good page → truncate any partial write at end.
- Sealed segments are safe — they were fsync'd before sealing.
- Maximum data loss: one flush interval (500ms) of logs. Acceptable for observability data.

---

## Security (v1 scope)

- TLS on HTTP — `crypto/tls`, self-signed cert for internal use
- API key auth — `X-Logd-Key` header, two keys: write-only and read-only
- At-rest encryption — stubbed interface, not implemented in v1. Designed so AES-GCM per segment can be added as a `StorageBackend` wrapper later.

---

## Python Handler

Drop-in Python `logging.Handler` subclass. No changes to application logging config except adding the handler.

```python
import logging
from logd import LogdHandler

logger = logging.getLogger(__name__)
logger.addHandler(LogdHandler(host="logd", port=3100))
```

Reads `LOGD_SERVICE` env var on init. Sends structured JSON to `POST /v1/ingest`. Batches entries, retries on failure.

---

## Configuration

```yaml
port: 3100
data_dir: /var/logd/data
retention_days: 30
flush_interval_ms: 500
segment_size_mb: 64
query_tolerance_minutes: 5

security:
  tls: true
  cert_file: /etc/logd/cert.pem
  key_file:  /etc/logd/key.pem
  write_key: changeme-write
  read_key:  changeme-read

templates:
  - name: order_created
    pattern: "order-{order_id:uint32} created"
    fields:
      - name: order_id
        type: uint32
        index: true
```

---

## Go Project Structure

```
logd/
├── cmd/
│   └── logd/
│       └── main.go              ← entry point, wires everything together
│
├── internal/
│   ├── config/
│   │   └── config.go            ← YAML parsing, defaults, validation
│   │
│   ├── ingestion/
│   │   ├── handler.go           ← HTTP handlers for /push and /v1/ingest
│   │   ├── parser.go            ← parse Loki push format and native JSON → LogEntry
│   │   └── queue.go             ← channel-based write queue
│   │
│   ├── storage/
│   │   ├── storage.go           ← Storage interface definition
│   │   ├── segment.go           ← segment lifecycle: open, rotate, seal
│   │   ├── writer.go            ← single writer goroutine
│   │   ├── reader.go            ← pread, page scan, filter
│   │   ├── index.go             ← sparse time index: write and binary search
│   │   ├── manifest.go          ← in-memory segment registry
│   │   ├── record.go            ← binary encode/decode of LogEntry
│   │   └── page.go              ← page header, page read/write
│   │
│   ├── template/
│   │   ├── template.go          ← template config parsing and compilation
│   │   ├── matcher.go           ← per-entry regex matching and extraction
│   │   └── indexer.go           ← inverted index write, merge, bitmap
│   │
│   ├── query/
│   │   ├── handler.go           ← HTTP handlers for /query_range, /labels
│   │   ├── engine.go            ← orchestrates manifest → index → reader
│   │   └── filter.go            ← filter by level, service, ts tolerance
│   │
│   └── server/
│       └── server.go            ← net/http setup, routing, middleware, graceful shutdown
│
├── pkg/
│   └── lokicompat/
│       ├── push.go              ← Loki push request/response types
│       └── query.go             ← Loki query response types
│
├── config.yaml
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

### Key design patterns

**Storage interface** — everything depends on this, not on the filesystem implementation:
```go
type Storage interface {
    Write(entry LogEntry) error
    Query(filter QueryFilter) ([]LogEntry, error)
    Labels() ([]string, error)
    Close() error
}
```

**Dependency injection in main.go** — no global state:
```go
store  := storage.NewFileStorage(cfg)
tmpl   := template.NewEngine(cfg.Templates)
ingest := ingestion.NewHandler(store, tmpl)
query  := query.NewHandler(store)
srv    := server.New(cfg, ingest, query)
```

**Graceful shutdown:**
```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()
srv.RunContext(ctx)
```

**Structured errors** — not fmt.Errorf strings:
```go
type SegmentError struct {
    SegmentID uint64
    Op        string
    Err       error
}
```

---

## V1 Scope — Build This First

| Feature | In v1 |
|---|---|
| Binary page-based storage | ✅ |
| Single writer goroutine | ✅ |
| Sparse time index + binary search | ✅ |
| Segment manifest in memory | ✅ |
| Dual timestamp (ts + ingested_at) | ✅ |
| Loki push endpoint | ✅ |
| Native structured ingest endpoint | ✅ |
| Loki query_range + labels endpoints | ✅ |
| Grafana plug-in (via Loki datasource) | ✅ |
| Python logging handler | ✅ |
| TLS + API key auth | ✅ |
| YAML config | ✅ |
| Crash recovery via checkpoint | ✅ |
| Template-based inverted indexes | ✅ |
| Segment rotation at 64MB | ✅ |

## Post-V1 Roadmap

| Feature | Notes |
|---|---|
| zstd block compression | Block-level, not whole-file. Index stores block_offset + within_block_offset |
| S3 / MinIO storage backend | Via StorageBackend interface swap |
| ClickHouse analytics backend | Dual-write: files for durability, ClickHouse for aggregation |
| Live tail (`/loki/api/v1/tail`) | WebSocket streaming |
| `logd-agent` | Tail Docker stdout log files, ship to logd. Replaces Promtail |
| Per-service API keys | Access control per writer |
| At-rest AES-GCM encryption | Per-segment, plugged into StorageBackend wrapper |
| Retention / compaction | TTL-based segment deletion |
| Bloom filters | Per-segment, for non-time field pre-filtering on large ranges |
| LRU block cache | Cache recently decompressed blocks, Postgres buffer pool style |

---

## Postgres Internals Lessons Applied

| Postgres concept | logd application |
|---|---|
| 8KB fixed pages | 4KB fixed pages. Unit of I/O is always one page |
| Page header with min/max stats | Page header with min_ts/max_ts — skip pages without reading entries |
| Item pointers | entry_count + free_space_offset in page header |
| Checksum per page | 4-byte checksum in page header for corruption detection |
| MVCC (no in-place updates) | Append-only by design — never mutate written records |
| Shared buffer pool | Post-v1: LRU page cache in front of pread() |
| Checkpoint record | checkpoint.json after every flush |
| WAL replay on recovery | Scan from checkpoint offset on startup |
| TOAST (overflow storage) | Post-v1: extra JSON > 1KB written to .overflow file, pointer in record |

---

## Thesis Angle

### Research question

> "Can user-defined template-based field extraction with typed inverted indexes outperform full-scan approaches for structured log retrieval, and what are the tradeoffs in index maintenance overhead vs query performance?"

### Why it's novel

Existing log parsing research (Drain, Spell, IPLoM) focuses on **automatic template discovery for anomaly detection and ML pipelines** — not for indexed retrieval. Nobody has built a log storage engine where user-defined typed templates drive inverted index construction.

Loki's structured metadata solves extraction but explicitly does not index — it still scans. Your contribution is the missing piece: **template extraction → typed inverted index → O(log n) field search on append-only log storage**.

### What makes it valid MSc research

- Clear gap in existing literature
- Measurable hypothesis: query latency, index size, ingestion overhead benchmarked against naive scan and Loki
- Novel combination: template extraction + typed indexes + append-only page storage
- Practical artifact: `logd` is the implementation

### Suggested paper titles to read

- Drain: An Online Log Parsing Approach with Fixed Depth Tree (He et al., 2017)
- Spell: Online Streaming Parsing of Large Unstructured System Logs (Du et al.)
- Database Internals — Alex Petrov (book, chapters 3-5)
- Designing Data-Intensive Applications — Martin Kleppmann (book)

---

## External Dependencies (v1)

| Package | Purpose |
|---|---|
| `gopkg.in/yaml.v3` | Config file parsing |
| Standard library only | Everything else |

No web framework. No ORM. No message broker. Single binary.

---

*Document generated from design discussion. Last updated: May 2026.*
