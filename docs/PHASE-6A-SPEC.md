# logd_v2 — Phase 6a build spec (native label index)

Builds the label side of design §6.2/§4/§9: intern label sets to a per-segment StreamID,
snapshot a stream dictionary + posting lists at seal, and push label-equality into that
index. Fills in the `StreamID` reserved on `LogEntry` since Phase 1. Phases 1–5 done.

Governing invariant (unchanged): the SCAN path is the oracle. Label resolution during scan
reads labels from `Extra` (the resolver, Phase 4). The label index is a DERIVED accelerator
built from the allowlisted keys already in `Extra`, so `Execute` (label pushdown) must equal
`ExecuteScan` for every query — same contract as Phase 5's typed pushdown.

## Design decisions

- **Labels stay in `Extra`.** All labels a caller provides live in the `Extra` JSON blob (as
  today). The label index is DERIVED from the subset of keys on the **allowlist** — so the
  index and the scan read the exact same values (both via the shared `jsonScalarString`
  helper), and consistency is automatic. No ingest API rewrite.
- **Allowlist is the primary cardinality guard** (design §6.2): only declared label keys are
  indexed; everything else is scan-only via `Extra`. The secondary per-key *value* cap is
  DEFERRED to Phase 7 (like the query cost guard) — so the label index is always COMPLETE,
  which keeps pushdown trivially correct (no "incomplete index" false negatives).
- **Per-segment stream dictionary + posting lists** (§6.2, §10): each segment interns its own
  label sets → StreamID (per-segment id space, self-describing/relocatable), and stores
  StreamID → sorted record offsets. Snapshotted at seal alongside the `.tidx`. Serves label
  pushdown AND (6b) `/series`.
- A crash-recovered / active segment has no label index file → label predicates scan it
  (same degrade as `.tidx`; file-existence gates pushdown, no new manifest field).

## New package `core/label`

```go
type Pair struct{ Key, Value string }
type Set  []Pair                          // canonicalized: sorted by key, unique keys
func NewSet(m map[string]string) Set       // sort + dedup
func (s Set) Canonical() string            // "k1=v1\x00k2=v2" for dedup / fingerprint
func (s Set) Get(key string) (string, bool)

type Builder struct{ ... }                 // per active segment, writer-owned (no locking)
func NewBuilder() *Builder
func (b *Builder) Intern(s Set) uint32     // dedup → StreamID (0-based, per segment)
func (b *Builder) AddPosting(streamID uint32, offset uint64)
func (b *Builder) Flush(path string) (wrote bool, err error) // writes <segBase>.labels.lidx; false if no label keys

type Reader struct{ ... }
func OpenReader(path string) (*Reader, error)   // validates whole-file CRC; error → caller scans
func (r *Reader) LabelOffsets(key, value string) []uint64 // union of postings of streams with key=value, sorted
func (r *Reader) Keys() []string
func (r *Reader) Values(key string) []string
func (r *Reader) Streams() []Set               // for /series (6b)
```

### `.lidx` on-disk format (per segment, one file)
```
[header 16B] magic 'LIDX' u32 · version u16=1 · reserved u16 · streamCount u32 · crc32 u32 (whole file, field zeroed)
[streams × streamCount]  StreamID = its 0-based index:
   pairCount u16; per pair: keyLen u16, key, valLen u16, val   (pairs sorted by key)
   postingCount u32; offsets as delta+uvarint (first absolute, rest ascending deltas)
```
Whole-file CRC like `.tidx`; a mismatch/short read → OpenReader errors → the segment's label
predicates degrade to scan. Atomic write (tmp→fsync→rename→dir fsync) via the index pkg's
`writeFileAtomic` pattern (re-implemented locally to keep `label` a leaf).

## Ingest + writer wiring

- `config`: add `Labels []string` (the allowlist). Threaded into the ingester.
- `ingest.Ingester` gains the allowlist. `Ingest` derives the label `Set` from the entry's
  `Extra` (allowlisted keys only, via the same `jsonScalarString` the resolver uses) and passes
  it to the writer alongside the extracted typed keys.
- `storage.Writer`: the `record` seam gains `labels label.Set`. `WriteExtracted(e, keys, labels)`.
  processEntry: intern `labels` → StreamID in the per-segment `*label.Builder`, set
  `e.StreamID`, and `AddPosting(streamID, segRelOffset)` (mirrors the `.tidx` buffer). On seal:
  `Builder.Flush(segBase + ".labels")`; on rotate: fresh Builder; recovered segment: no flush
  (scan-only). No manifest change.

## Query pushdown + enumeration

- `core/query/plan.go`: a `LabelEqual{Key,Value}` is pushable when Key is on the allowlist AND
  the segment's `.lidx` opens → `LabelOffsets(Key,Value)` as a `plannedLookup` (intersected with
  typed lookups). `LabelNotEqual` stays a residual (like `!=`). Everything re-verified by
  `matchAll` (which reads `Extra`) → identical to scan. The engine needs the allowlist to know
  which label keys are pushable; pass it into `NewEngine`.
- `Engine.Labels() []string` and `Engine.LabelValues(key string) []string`: union across sealed
  segments' `.lidx` readers (active-segment labels appear after seal — documented eventual
  consistency, acceptable for Grafana discovery).

## Tests (differential oracle again)

- **Differential:** extend the battery — `LabelEqual` on allowlisted keys (single + combined with
  typed predicates + combined with each other), on multiple segments, must equal `ExecuteScan`.
  A non-allowlisted `LabelEqual` and `LabelNotEqual` stay scan and must still equal.
- **Explain:** `LabelEqual` on an allowlisted key reports `index`; on a non-allowlisted key reports `scan`.
- **StreamID set:** ingested records carry a non-zero StreamID reflecting their label set;
  two records with identical label sets share a StreamID within a segment.
- **Enumeration:** `Labels()`/`LabelValues()` return the allowlisted keys/values actually ingested.
- **Degrade:** delete a `.lidx` → label queries scan → same result.
- **Cardinality/allowlist:** a label key NOT on the allowlist is never indexed (scan only) but
  still queryable (resolver reads Extra) and returns the same as scan.
- Run under `-race`.

Keep `go build ./... && go vet ./... && go test -race ./...` green throughout.
