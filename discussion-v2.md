# logd Design Doc — Template Indexing & LogQL Engine

Status: proposal for Phases 5–8
Depends on: Phases 0–4 (storage format, segments, write path, read path) — complete
Scope: LogQL query language support, template-based typed indexing, query planner with index pushdown

---

## 1. Problem statement

Today every non-time predicate triggers a full segment scan: every page is read, every
entry decoded, and `matchLabels()` runs `json.Unmarshal` on every entry's `Extra` field.
Time range is the only pruned dimension (manifest min/max + sparse ts index).

Goal: let users declare *what is worth indexing* via config — literal strings and typed
patterns ("templates") — and have queries that hit those patterns run in O(log n) per
segment instead of O(n), including range operators (`>`, `<`, `>=`, `<=`) on numeric,
timestamp, and other ordered types. This mirrors what a B-tree index does for a database
column: the column here is "a typed value extracted from unstructured log text."

Constraints carried over from the project:

- Single binary, no external deps beyond stdlib + yaml.
- Append-only, 4KB pages, single writer goroutine.
- ~50MB RAM target.
- Loki wire compatibility: Grafana must be able to point at logd with a one-line change.

---

## 2. Core design invariant: indexes are pruning structures, not truth

Every index lookup returns a **candidate set** of entry references. The executor always
re-applies the full predicate on the decoded entries before returning them.

Consequences:

- False positives in an index are *allowed*. This permits lossy keys (e.g. 16-byte string
  prefixes), which keeps index records fixed-size and binary-searchable.
- False negatives are *not* allowed. An index must never miss a matching entry, or it is
  unusable. This is the only correctness obligation on the index builder.
- A missing, corrupt, or version-mismatched index file degrades to a scan, never to a
  wrong answer. Indexes are derived data and can always be rebuilt from segments.

This invariant is what makes the whole feature safe to ship incrementally.

---

## 3. Template model

### 3.1 Config syntax

```yaml
index:
  # Bare literal strings. Existence-indexed: "which entries contain this exact substring".
  literals:
    - "nil pointer dereference"
    - "panic:"
    - "ERROR"

  # Typed patterns. Each {name:type} capture is extracted and value-indexed.
  templates:
    - name: order            # template name, must be unique
      pattern: "order-{id:uuid}"
    - name: trip
      pattern: "trip:{id:str}"
    - name: latency
      pattern: "took {ms:int}ms"
    - name: payment
      pattern: "payment {amount:float} {currency:str}"
```

A pattern is a sequence of literal fragments and captures. Rules enforced at config load:

1. **Anchor rule**: every template must contain at least one literal fragment of ≥ 3
   bytes. This is what makes matching cheap (§4). A pattern like `{ts:timestamp}` with no
   anchor is rejected in v1. (A dedicated "scan for any timestamp" mode can come later;
   it has a fundamentally different cost profile.)
2. Two captures may not be adjacent without a literal separator between them
   (`{a:str}{b:int}` is ambiguous → rejected).
3. Capture names within a template are unique; `templatename_capturename` must not
   collide across templates (this compound name becomes a queryable field, §7.3).

### 3.2 Type system

Each type defines: a parser (consume bytes at a position, produce a value or fail), a
canonical **order-preserving 16-byte key encoding**, and a supported operator set.

| type      | parse rule                                                      | key encoding (16B)                          | operators            |
|-----------|------------------------------------------------------------------|---------------------------------------------|----------------------|
| `int`     | optional `-`, digits, bounded to int64                           | sign-flipped big-endian int64, zero-padded   | `= != > >= < <=`     |
| `float`   | digits, optional `.digits`, optional exponent → float64          | IEEE754 total-order transform¹, padded       | `= != > >= < <=`     |
| `uuid`    | 8-4-4-4-12 hex with dashes                                       | raw 16 bytes                                 | `= !=`               |
| `hex`     | hex chars, length bounded by config (e.g. `{id:hex16}`)          | raw bytes, zero-padded                       | `= !=`               |
| `str`     | run of non-delimiter bytes², max 256                             | first 16 bytes, lexicographic (lossy)        | `= != > >= < <=` ³   |
| `ts`      | RFC3339 / RFC3339Nano                                            | sign-flipped big-endian unix-nano int64      | `= != > >= < <=`     |
| `dur`     | Go duration syntax (`150ms`, `2.5s`)                             | nanoseconds as int                           | `= != > >= < <=`     |

¹ Standard trick: if sign bit set, flip all 64 bits; else flip only the sign bit. The
result compares correctly with `bytes.Compare`.
² Delimiters: whitespace, `"`, `,`, `}`, `]`, and the next literal fragment of the
pattern. `str` is greedy up to the first delimiter.
³ `str` keys are lossy prefixes — range and equality results on strings longer than 16
bytes are candidate sets, verified post-decode (§2). This is invisible to the user.

The shared 16-byte key width means every value index has **fixed 24-byte records**
(16B key + 8B entry ref), so the same binary-search machinery serves all types — exactly
like the existing sparse ts index (`index.go`), just with a typed key instead of a
timestamp.

### 3.3 Why order-preserving encodings matter

This is the answer to "how do databases make `>` fast and how do we keep that": a B-tree
is fast for ranges because keys are stored *sorted under a total order that matches the
type's semantic order*. If we encode every value so that `bytes.Compare(enc(a), enc(b))`
agrees with the type's natural `<`, then:

- equality = binary search for the key,
- range = binary search for the lower bound, then a linear walk to the upper bound,
- the index file needs zero type-specific logic at read time.

We get B-tree-leaf-level behavior from a flat sorted file because segments are immutable
once sealed — no inserts mean no need for the tree part, only the sorted-leaf part. This
is the LSM observation: append-only storage turns indexes into "sort once at seal time."

---

## 4. Match engine (ingestion side)

### 4.1 Where matching runs

Extraction is CPU work; the writer goroutine must stay I/O-bound. Therefore:

- **HTTP handler goroutines** run the matcher on each message and attach the results
  (`[]Match{TemplateID, CaptureIdx, Key [16]byte}`) to the entry before enqueuing it on
  the existing write channel. Matching parallelizes across connections for free.
- The **writer goroutine** is the only place where the entry's final location
  (pageNum, slot) is known, so it pairs each Match with its ref and appends to the
  per-segment index buffers (§5.2).

This splits the work along the already-existing channel boundary and adds no new
concurrency primitives.

### 4.2 Matching algorithm

Naive approach — run every template's regex against every line — is O(templates ×
line length) with large constants. Instead:

1. At startup, collect every literal fragment (template anchors + bare literals) into a
   single **Aho-Corasick automaton**. One pass over the message (O(message length))
   yields all literal hits and their positions.
2. A bare-literal hit immediately produces an existence match.
3. A template-anchor hit nominates that template at that position. The engine then runs
   the template's **typed scanner**: verify the literal fragments around the anchor,
   parse each capture with its type parser. Typed parsers are hand-written byte loops
   (no regexp), and fail fast on the first mismatched byte.
4. A line can produce multiple matches (different templates, or the same template at
   multiple positions). All are recorded.

Aho-Corasick is ~150 lines of stdlib-only Go (goto/fail/output links over a byte trie).
Go's `regexp` (RE2) is linear-time too, but a combined alternation of N patterns carries
much higher constants and gives no positions-per-pattern without submatch tracking.

Estimated cost: one automaton pass + a handful of short typed scans per line. Target
< 1µs per typical 200-byte line; benchmark in `template/matcher_bench_test.go`.

### 4.3 Config changes and reindexing

Templates are identified by `(name, pattern, version)` and assigned stable uint16 IDs
persisted next to `lookup.bin` (`templates.bin`, same lookup-table format). If a
template's pattern changes, it gets a new ID; old segments simply lack index sections
for the new ID and fall back to scan for it. A `logd reindex` subcommand (post-v1)
can rebuild index files for sealed segments offline — possible precisely because
indexes are derived data.

---

## 5. Index storage

### 5.1 Per-segment, not global

One index file per segment (`seg000123.tidx`), built when the segment seals. Reasons:

- **Retention becomes trivial**: deleting a segment deletes its index. A global index
  would need tombstones/compaction — the exact complexity logd exists to avoid.
- Sealed segments are immutable → their indexes are write-once, sorted, checksummed.
- Query fan-out across segments already exists (`querySegments`); each segment's index
  is consulted independently after manifest time pruning.

### 5.2 Building during the active segment

The writer keeps per-template in-memory buffers of `(key, ref)` pairs. Sizing concern:
a 64MB segment at ~200B/entry is ~335K entries; at 1.5 matches/entry × 24B that's
~12MB — too close to the RAM budget to ignore.

Design: **sorted-run spilling**.

- Buffer up to `index.spill_threshold` records (default 64K ≈ 1.5MB) per segment.
- On overflow, sort the buffer and append it as a run to `seg000123.tidx.tmp`.
- At seal: k-way merge of spilled runs + final in-memory buffer → final sorted
  sections → write `.tidx` → fsync → atomic rename → mark `hasIndex` in manifest.

v1 simplification permitted: skip spilling, hold everything in memory, document the
bound (`segment_size / avg_entry × matches × 24B`), and make it acceptable by keeping
the default segment at 64MB. The spill path is the durable answer if RAM pressure shows
up in practice. The interface (`indexBuilder.Add(templateID, key, ref)` /
`indexBuilder.Seal(w io.Writer)`) is identical either way, so this is swappable later.

**Active-segment queries**: the query path asks the writer (via a small RW-locked
snapshot or a channel request) for the current buffer, sorts a copy on demand. Active
segments are small and hot; this is cheap and avoids any index-file machinery for the
unsealed segment. Falling back to scanning just the active segment is also acceptable
for v1 — it is bounded at 64MB.

### 5.3 File format: `segNNNNNN.tidx`

```
[file header — 32 bytes]
  magic        u32   "TIDX" 0x54494458
  version      u16   1
  sectionCount u16   number of directory entries
  segmentID    u64
  reserved     u64
  headerCRC    u32   CRC32-IEEE of header with this field zeroed

[directory — sectionCount × 24 bytes, sorted by (templateID, captureIdx)]
  templateID   u16   0xFFFF reserved; literal IDs share the template ID space
  captureIdx   u8    which capture within the template (0 for literals)
  kind         u8    0=value index, 1=existence index
  recordCount  u32
  sectionOff   u64   absolute file offset of the section's records
  sectionCRC   u32   CRC32 over the section bytes

[value section — recordCount × 24 bytes, sorted by key, then ref]
  key      [16]byte  order-preserving encoding (§3.2)
  pageNum  u32
  slot     u16       entry's ordinal within the page
  reserved u16

[existence section — recordCount × 8 bytes, sorted by ref]
  pageNum  u32
  slot     u16
  reserved u16
```

Notes:

- An **entry ref** is `(pageNum, slot)`. No byte offsets are stored: to materialize slot
  *k*, the reader decodes the page's entries sequentially up to *k*. Pages are 4KB and
  decoding is a tight loop — sequential decode of one page is cheaper than maintaining a
  per-entry offset table, and it reuses `decodePageEntries` as-is.
- Fixed-size records → `sort.Search` over `ReadAt`, identical access pattern to the
  existing `IndexReader`. No mmap needed (keeps it boring and portable).
- All sections in one file per segment: one fd, one fsync, one rename.

### 5.4 Crash story

The `.tidx` write happens at seal: write tmp → fsync → rename → update manifest. If logd
dies mid-seal, the manifest lacks `hasIndex` and queries scan that segment (correct,
slower). Startup may optionally rebuild missing indexes in the background. The segment
data path is unchanged — indexes never gate durability of log data.

---

## 6. LogQL engine

### 6.1 Supported grammar (target end-state)

```
query        := selector pipeline*
selector     := '{' matcher (',' matcher)* '}'
matcher      := label ('=' | '!=' | '=~' | '!~') string
pipeline     := lineFilter | parserStage | labelFilter
lineFilter   := ('|=' | '!=' | '|~' | '!~') string
parserStage  := '|' ('json' | 'logfmt')
labelFilter  := '|' label op value          // op: = != =~ !~ > >= < <=
```

Out of scope for now (rejected with a clear error): metric queries (`rate`,
`count_over_time`, aggregations, `unwrap`), `| pattern`/`| regexp` parsers,
`line_format`/`label_format`. Metric queries are the most-requested follow-up because
Grafana dashboards need them; they layer cleanly on top of the entry iterator (§8,
Phase 8).

### 6.2 Implementation

Hand-written lexer + recursive-descent parser → AST. ~600–800 lines including tests.
No Loki imports (their parser drags in a huge dependency tree and a yacc grammar).

```
internal/logql/
  lexer.go      // tokens: IDENT STRING NUMBER OPERATORS BRACES PIPES
  parser.go     // recursive descent → ast
  ast.go        // Selector, LineFilter, ParserStage, LabelFilter nodes
  parser_test.go
```

The existing ad-hoc selector parsing in `internal/query` is replaced by this package;
`Engine.Execute` consumes the AST.

### 6.3 Semantics of each stage (scan path — always exists)

Executed per entry, in pipeline order, exactly like Loki:

- **Line filters** operate on `Message` (finally giving full-text matching on the
  message body, which today doesn't exist even in the scan path). `|~`/`!~` use
  compiled `regexp` (RE2, linear time).
- **`| json` / `| logfmt`** parse `Extra` (and message for logfmt) into an extracted
  label map for subsequent label filters.
- **Label filters** apply to stream labels, extracted labels, and template fields
  (§7.3), with numeric comparison when both sides parse as numbers (Loki semantics).

This scan path is the semantic ground truth. The planner (§7) only decides *which
entries get fed into it*.

---

## 7. Query planner: index pushdown

### 7.1 Pipeline position

```
parse LogQL → AST
   → manifest time pruning                    (exists)
   → per segment:
       plan(AST, segment.indexDirectory) →
         candidateRefs (sorted) | SCAN_ALL
   → fetch pages for refs, decode, run full pipeline (§6.3) on candidates
   → group into streams                       (exists)
```

### 7.2 Pushdown rules for line filters

Given `|= "s"` (the only directly pushable line filter; `!=`, `|~`, `!~` always run as
residuals):

1. **Exact literal**: `s` equals a configured literal → use its existence section
   directly. Result is exact, but the residual check still runs (free correctness).
2. **Template-shaped string**: try to fully parse `s` against every template whose
   anchor occurs in `s` (reuse the ingestion matcher on the query string!). If `s` ≡
   `order-7f3a…` matches template `order`, do an equality lookup for the encoded key in
   that segment's value section.
3. **Literal-substring containment**: if a configured literal `L` is a substring of `s`,
   every entry containing `s` necessarily contains `L`, so `postings(L)` is a valid
   candidate superset. Pick the rarest such `L` (smallest recordCount — the directory
   gives this for free, acting as cheap statistics).
4. Otherwise: no pushdown → this filter contributes SCAN_ALL.

Multiple pushable predicates → **intersect** their sorted ref lists (linear merge).
Any non-pushable conjunct just stays a residual; it does not force a scan as long as at
least one conjunct produced a candidate set.

### 7.3 Pushdown rules for label filters / range queries

Template captures are exposed as queryable fields named `{template}_{capture}`
(`order_id`, `latency_ms`, `payment_amount`). Two ways a user reaches them:

- Explicitly, Loki-style: `{app="api"} | logfmt | latency_ms > 100` — works today as a
  scan; the planner additionally recognizes `latency_ms` as a template field.
- Implicitly: a label filter on a known template field is accepted **without a parser
  stage** (`{app="api"} | latency_ms > 100`). Loki would reject this; we accept it as a
  strict superset of LogQL. Grafana sends raw query text, so this works in Explore.

Pushdown for `field op value` where `field` is a template capture:

- `=`  → encode value with the capture's type → binary-search equality.
- `>` `>=` `<` `<=` → encode bound → binary-search the boundary → walk to section end /
  from section start, collecting refs. This is the database-style range scan the
  project is after: O(log n + result size) per segment.
- `!=`, `=~`, `!~` → residual only.

If the type parser rejects the query value (`latency_ms > "abc"`), return an empty
result with a 400-style error message — matching the typed-column behavior of a DB.

### 7.4 Cost guard

If a candidate set exceeds `index.scan_threshold` (default 20%) of the segment's entry
count (known from segment metadata), discard it and scan: sequential page reads beat
random page fetches for large result fractions, same reasoning as a DB planner choosing
seq-scan over index-scan. Ref lists are sorted by (page, slot), so page fetches are at
least monotonic and each page is fetched once even when many refs share it.

---

## 8. Implementation phases

**Phase 5 — LogQL parser + scan-path pipeline.**
`internal/logql` (lexer/parser/AST), pipeline executor over the existing scan path,
line filters on Message, `| json` / `| logfmt`, label filters. Replaces the current
selector-only parsing. *Deliverable: Grafana Explore line filters work, correctly,
slowly.* This ships user-visible value before any indexing exists and becomes the
ground-truth oracle for index correctness tests (scan result == indexed result, always).

**Phase 6 — Template engine + index build.**
`internal/template`: config parsing, pattern compiler, type parsers, Aho-Corasick
matcher, ingestion-side extraction, writer-side index buffers, `.tidx` writer at seal,
manifest `hasIndex` flag, `templates.bin` ID persistence. No query-side changes yet.
*Deliverable: index files exist and are verifiably correct (test: rebuild-and-compare).*

**Phase 7 — Planner pushdown.**
`.tidx` reader (binary search / range walk), pushdown rules §7.2–7.3, intersection,
cost guard, active-segment buffer queries. *Deliverable: indexed queries are O(log n);
benchmarks demonstrating scan vs index on 1M+ entries.*

**Phase 8 — Production hardening (interleaves with the missing v1 items).**
Metric queries (`rate`, `count_over_time`, `sum by` — entry-iterator layering),
retention (per-segment delete, which the index design already made trivial),
checkpoint-based crash recovery, TLS + API keys, optional background reindex command.
Optional research follow-up: per-segment trigram bloom filters for arbitrary `|=`
strings not covered by any template (covers the "I didn't configure it" gap; this is
what Loki's bloom-filter work targets).

Ordering rationale: parser before indexes (correct-but-slow beats fast-but-untestable);
index build before index read (files can be validated standalone); hardening last
because every earlier phase degrades gracefully without it.

---

## 9. Memory & size budgets

| component                          | estimate                                            |
|------------------------------------|-----------------------------------------------------|
| Aho-Corasick automaton             | ~(total literal bytes) × ~40B/node; 100 patterns ≈ <1MB |
| Active-segment index buffers       | ≤ spill_threshold × 24B per spill (default ~1.5MB), or ~12MB worst case without spilling |
| `.tidx` per 64MB segment           | matches × 24B; 500K matches ≈ 12MB (~19% of segment) |
| Query-time ref lists               | bounded by cost guard at 20% of segment entries × 8B ≈ 0.5MB/segment |
| LogQL AST / compiled regexes       | negligible                                          |

Index size is the number to watch: it scales with how aggressively users configure
templates. Documented guidance: index identifiers you search by (IDs, UUIDs), not
free-text words.

---

## 10. Open questions (decide before Phase 6)

1. **Spill vs in-memory for v1** (§5.2). Recommendation: in-memory with the interface
   shaped for spilling, ship spilling when a real workload demands it.
2. **`str` range semantics**: lexicographic ranges on lossy 16-byte prefixes are
   correct (candidates verified post-decode) but can over-fetch on long shared
   prefixes. Acceptable, or cap `str` to equality-only in v1?
3. **Field naming**: `{template}_{capture}` vs a config-declared `label:` per capture.
   Config-declared is friendlier but adds a collision-checking surface.
4. **Active segment**: query the in-memory buffer vs always scan the active segment.
   Scanning is simpler and bounded at 64MB; buffer queries shave the last bit of
   latency. Recommendation: scan in v1.
5. **Literal matching case sensitivity**: exact bytes only (recommended — predictable,
   fast), or optional case-insensitive literals (doubles automaton size)?