# logd_v2 — Phase 3 build spec (extraction + key encoding + typed-range `.tidx`)

Builds the **differentiator**: range queries on typed values extracted from unstructured log
text. Conform to `docs/logd-design.md` §6.3, §7, §9.1; this pins the exact decisions.
Deliverable (discussion-v2 Phase 6): **index files exist and are verifiably correct** —
a rebuild-and-compare test (re-extract a segment, compare against its `.tidx`). **No query-side
pushdown yet** (that's Phase 5); but the `.tidx` reader is built now so correctness is testable.

Invariant that governs everything (design §2.2): indexes are **pruning structures**. False
positives are fine (re-verify on decode); **false negatives are never allowed**. A missing/
corrupt `.tidx`, or a segment with no schema, degrades to a full scan — never a wrong answer.

Learning project: heavily-commented code. Phases 1–2 are done (`core/model`, `core/storage`).

## New packages

```
core/index/    key encoding (order-preserving 16B) · .tidx writer/reader · in-RAM buffer   [leaf: stdlib only]
core/extract/  config compile · Aho-Corasick matcher · typed parsers · Extract()           [imports core/index]
core/config/   index config structs + validation (literals, templates)                     [leaf]
core/ingest/   native ingest: run extraction, encode keys, feed the writer                  [imports extract, index, storage, model]
```
Dependency direction (no cycles, core never imports compat): `index` ← `extract` ← `ingest` → `storage`; `storage` ← `ingest`; `storage` → `index` (buffer + tidx + KeyedValue).

## 1. Key encoding — `core/index/key.go` (THE correctness crux)

One `.tidx` is per (segment, field); a field has ONE type, so keys within a file are all the same
type — cross-type ordering is irrelevant, only order *within* a type matters. Every key is 16 bytes;
`bytes.Compare(EncodeKey(a), EncodeKey(b))` must have the same sign as the type's natural order.

```go
type ValueKind uint8
const ( KindInt ValueKind = iota; KindFloat; KindStr; KindUUID )
type Value struct { Kind ValueKind; Int int64; Float float64; Str string; UUID [16]byte }
func EncodeKey(v Value) [16]byte
type KeyedValue struct { Field string; Key [16]byte } // extraction output handed to the writer
```

Encodings (design §6.3 / §7.2), all BigEndian, byte order == value order:
- **int** → `u := uint64(i) ^ (1<<63)` (flip sign bit), BigEndian into bytes **[8:16]**, [0:8]=0x00 (left-pad). Sign-flip makes two's-complement ints compare correctly as unsigned.
- **float** → total-order transform: `b := math.Float64bits(f); if b>>63==1 { b = ^b } else { b |= 1<<63 }`, BigEndian into [8:16], [0:8]=0x00. **Normalize −0.0 → +0.0 first** (`if f==0 { b = math.Float64bits(0) }`) so −0 and +0 encode identically (§7.2). NaN/±Inf never reach here — extraction rejects them.
- **str** → longest **valid-UTF-8** prefix ≤16B: copy bytes until 16, but if that would split a multibyte rune, back up to the rune boundary (`utf8.RuneStart`); right-pad 0x00. **Lossy** ⇒ re-verify mandatory (Phase 5). 0x00 pad makes a shorter string sort before a longer one sharing its prefix.
- **uuid** → the 16 raw bytes, exact fit.

Property tests are mandatory: for many random pairs of each type, assert `sign(bytes.Compare(EncodeKey(a),EncodeKey(b))) == sign(cmp(a,b))`, including negatives, ±0, MinInt/MaxInt, subnormals, rune-boundary strings, and shared-prefix strings.

## 2. `.tidx` format — `core/index/tidx.go`

Per (segment, field), written at seal. Sorted fixed-size records enable `sort.Search` + range walk.

```
[header 32B] magic 'TIDX' 0x54494458 (u32) · version u16=1 · kind u8 (0=int 1=float 2=str 3=uuid) ·
             reserved u8 · recordCount u32 · segmentID u64 · headerCRC u32 (CRC32-IEEE, this field zeroed) · reserved u32
[records recordCount × 24B, sorted by (key, offset)]  key [16]byte · offset u64 (segment-relative BYTE offset of the record)
```
- `Writer`: accumulate `(key,offset)` pairs, sort by key then offset at flush, write header+records, fsync, atomic rename, one `<field>.tidx` per field inside the segment's own dir (retention = delete segment dir).
- `Reader`: `sort.Search` for equality and for range lower bound; `LookupEqual(key) []uint64`, `LookupRange(lo, hi [16]byte, incLo, incHi bool) []uint64` returning offsets. Validate header CRC + record-count vs file size; on mismatch return an error so the caller degrades to scan.
- Segment-relative byte offset: `PageOffset(pageNum) + inPageByteOffset`. To materialize, seek page = offset/PageSize, validate page, decode the record at offset%PageSize.

## 3. In-RAM index buffer — `core/index/buffer.go`

Per active segment (writer-owned). `Buffer.Add(field string, key [16]byte, offset uint64)`; `Buffer.Flush(segDir string, segmentID uint64, schema []FieldType)` sorts each field's pairs and writes its `.tidx`. v1 keeps everything in RAM (documented bound: segment_size/avg_entry × matches × 24B, acceptable at 64MB segments); the interface is spill-ready.

## 4. Config — `core/config/index.go`

```yaml
index:
  literals: ["panic:", "nil pointer dereference"]
  templates:
    - {name: latency, pattern: "took {ms:int}ms", min: 0, max: 100000}
    - {name: trip,    pattern: "trip:{id:str}", max_len: 32}
```
Go structs + a `Validate()`; YAML file loading is deferred to the daemon phase (structs are the config now). Rules enforced at compile (design §7.2): field/template names unique across config; **every template has a literal fragment ≥3 bytes** (anchor) else reject; **adjacent captures `{a}{b}` rejected**; **overlapping declarations** (identical/subsumed patterns) rejected; types ∈ {int,float,str,uuid}; params `min`/`max`/`max_len` **parse-and-ignore** (accepted, enforced nowhere — re-verify covers correctness).

## 5. Extraction — `core/extract/`

- `compile.go`: parse each template pattern into `[]fragment` (literal | capture{name,type}); collect all literal fragments (template anchors + bare literals) for the automaton.
- `ahocorasick.go`: one Aho-Corasick automaton (goto/fail/output over a byte trie) built from all literals; one O(len(message)) pass yields every literal hit + position. Stdlib only, ~150 lines, heavily commented (learning goal).
- `parse.go`: hand-written typed byte scanners (no regexp) — `parseInt` (optional `-`, digits, int64 bounds), `parseFloat` (digits/./exp → float64; **reject NaN/±Inf**), `parseStr` (greedy run up to a delimiter: whitespace, `" , } ]`, or the next literal fragment; max 256 then the key encoder truncates), `parseUUID` (8-4-4-12 hex → 16B).
- `engine.go`: `Engine.Extract(message string) []index.KeyedValue`:
  1. run the automaton;
  2. a bare-literal hit → existence match (Phase 3 can record literals as a presence entry or defer — at minimum they must be *matchable*; a literal is indexed as its own field with a constant/no key or a dedicated existence section — keep simple: a literal produces a `KeyedValue{Field: literalName, Key: zero}` OR skip literal indexing in Phase 3 and note it. Decide: index literals as existence via a per-literal field with a single zero key; document);
  3. a template-anchor hit → verify surrounding literal fragments, run each capture's typed parser; on success emit `KeyedValue{Field, EncodeKey(value)}`; **on a typed-parse failure: no entry, increment `Engine.Failures[template]`, line still stored** (never an ingest error, never silent — §7.2);
  4. **index every occurrence** (multiple matches per line → multiple entries) — no cap.
- Expose `Engine.IndexedFields() []FieldType` (name+type) so the writer records the segment schema, and `Engine.Failures` counters.

## 6. Ingest + writer wiring — `core/ingest/ingest.go` + edits to `core/storage/writer.go`

- `core/ingest.Ingester{ engine *extract.Engine; w *storage.Writer }`; `Ingest(e model.LogEntry) error` = `w.WriteExtracted(e, engine.Extract(e.Message))`. This keeps extraction OUT of the writer (design §7.4) — the writer receives already-encoded `[]index.KeyedValue`.
- `writer.go`:
  - `record` struct's reserved seam becomes `keys []index.KeyedValue`. Add `WriteExtracted(e, keys)`; keep `Write(e)` = `WriteExtracted(e, nil)`.
  - On construction, the writer takes the segment schema (`[]index.FieldType`) so it can record it in the manifest and build a per-segment `index.Buffer`.
  - `processEntry`: compute the record's **segment-relative byte offset** = `PageOffset(currSeg.PageNum) + int64(freeSpaceOffsetBeforeCopy)` (the page it will be flushed to), and `buffer.Add(kv.Field, kv.Key, offset)` for each key.
  - **Seal**: `buffer.Flush(segDir, segID, schema)` writes the per-field `.tidx` files, fsync, then the manifest seal records the real `schema` (§7.5). Fresh buffer for the next segment.
  - **Recovery of the active segment (§8 / no-false-negatives):** the pre-crash records were never indexed in RAM and the segment has no `.tidx`. Do NOT write a partial `.tidx` (that would drop pre-crash records = false negatives). Instead **seal the recovered segment with an EMPTY schema** so the planner scans it (correct, slower), and start a fresh active segment. (Re-extraction-on-recovery per §8 is a valid alternative but needs the engine in the writer; sealing-as-scanned is simpler and equally correct for v1 — document the choice.)

## 7. Tests (correctness is the deliverable)

- Key encoding: order-preservation property tests per type (above).
- `.tidx`: write a shuffled set, read back; equality lookup; range lower/upper inclusive/exclusive; corrupt header → reader errors (caller degrades).
- Extract: `"took 247ms"` → int 247 keyed on `latency`; multiple matches; anchor-rule/adjacency/overlap rejected at compile; NaN/±Inf → failure counter++, no entry; unanchored pattern rejected.
- **Rebuild-and-compare (the headline):** ingest N entries through the Ingester, seal, then independently re-extract each stored record from the segment and assert the `.tidx` contains exactly those `(key, offset)` pairs — index == derived-from-truth.
- Recovery: crash mid-active-segment, reopen; assert the recovered segment is sealed with empty schema (→ scanned) and all pre-crash records are still readable; new writes build a correct `.tidx`.

Keep `go build ./... && go vet ./... && go test -race ./...` green throughout.
