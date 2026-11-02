package query

import (
	"sort"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// zeroKey is the all-zero 16-byte key: the range minimum, and the existence key a bare
// literal is indexed under (EncodeKey of the empty string). maxKey is the range maximum.
var (
	zeroKey [index.KeySize]byte
	maxKey  = func() (k [index.KeySize]byte) {
		for i := range k {
			k[i] = 0xFF
		}
		return
	}()
)

// plannedLookup is one pushable predicate. run resolves it against the segment's index (a
// sealed segment's sidecar, or the active segment's in-memory index) and returns the
// candidate offsets, or ok=false if the index can't be read — in which case the caller
// degrades the whole segment to a scan (design §9.1).
type plannedLookup struct {
	run func() (offsets []uint64, ok bool)
}

// plan decides which predicates can be pushed into this segment's indexes, read through
// src. A non-indexed segment (crash-recovered and never re-indexed, or an active segment
// without an in-memory index) pushes nothing. A predicate whose field/key isn't
// indexed here is left as a residual (handled by the full re-verify), which keeps
// pushdown results identical to scan. Returns nil to mean "scan this segment".
// It also returns the RESIDUAL predicates: those the index has not already proven. A
// predicate is settled (and so omitted from the residuals) only when the index answer is
// exact — see losslessKind. Settled predicates are skipped during re-verify on the
// index-fetch path, which is where the Aho-Corasick re-extraction cost lived; they are
// never skipped on any scan fallback, where nothing has been proven.
func (e *Engine) plan(seg *storage.SegmentMeta, src segmentIndex, preds []Predicate) ([]plannedLookup, []Predicate) {
	if !seg.Indexed {
		return nil, preds
	}
	var lookups []plannedLookup
	var residuals []Predicate
	keepResidual := func(p Predicate) { residuals = append(residuals, p) }
	for _, p := range preds {
		switch pred := p.(type) {
		case TypedCompare:
			kind, pattern, ok := schemaField(seg, pred.Field)
			// Push only when: not != (not selective); the field is indexed in this
			// segment; the query value's Kind MATCHES the field's indexed kind (a cross-kind
			// predicate would encode its key in the wrong kind-space; validatePreds refuses
			// one against the live extractor, but Explain plans without it); and the index was
			// built from the SAME pattern that defines the field now. The scan path extracts
			// with the live config, so an index built from an edited — or since removed —
			// template would answer for a different field than the scan does. Otherwise the
			// predicate stays a residual and the scan path handles it.
			if pred.Op == OpNe || !ok || pred.Value.Kind != kind || !e.patternMatches(pred.Field, pattern) {
				keepResidual(p)
				continue
			}
			// A lossless kind's key IS the value, and with exact (non-widened) bounds the
			// lookup returns precisely the matching records — so the predicate is proven
			// and needs no re-extraction. A lossy str key stays a residual.
			if !losslessKind(kind) {
				keepResidual(p)
			}
			field, run := pred.Field, typedRun(pred, losslessKind(kind))
			lookups = append(lookups, plannedLookup{run: func() ([]uint64, bool) {
				l, ok := src.typed(field)
				if !ok {
					return nil, false
				}
				return run(l), true
			}})
		case LineContains:
			// Always a residual. The existence index would very likely settle it, but
			// re-verifying is a strings.Contains that never touches the extraction engine,
			// so there is no cost to recover and no reason to take the risk.
			keepResidual(p)
			if e.ex == nil {
				continue
			}
			field, ok := e.ex.LiteralField(pred.Sub)
			if !ok || !schemaHas(seg, field) {
				continue
			}
			lookups = append(lookups, plannedLookup{run: func() ([]uint64, bool) {
				l, ok := src.typed(field)
				if !ok {
					return nil, false
				}
				return l.LookupEqual(zeroKey), true // existence: offsets that contain the literal
			}})
		case LabelEqual:
			// Only allowlisted, non-reserved keys are pushed. A non-allowlisted key
			// scans (resolver reads Extra); a reserved key (level/service) scans too,
			// because the resolver falls back to the first-class field when Extra lacks
			// the key and the index (derived from Extra) holds no such value — pushing it
			// would drop those matches. An empty-string value
			// also scans: {k=""} matches records that LACK k (absent==empty), which the
			// index (only records that HAVE k) can't surface. LabelNotEqual is a residual.
			// A key whose value-cardinality cap breached in this segment has over-cap
			// values only in Extra, not the index — pushing it would miss them, so scan.
			// Always a residual: after the single-key resolver change a label compare is
			// ~200ns and allocation-free, so settling it from the index would trade a real
			// (if small) divergence risk — the index was built at ingest under the then-
			// current allowlist — for no measurable gain.
			keepResidual(p)
			if model.IsReservedLabelKey(pred.Key) || pred.Value == "" || !e.labelAllowed(pred.Key) || segCapped(seg, pred.Key) {
				continue
			}
			// The engine's allowlist says what is indexed NOW; the segment's label index is
			// complete only for the keys allowlisted when it was written. A key added since
			// is simply absent from it, and pushing it would return no rows for a segment
			// full of matches.
			if !e.segmentIndexesLabel(seg, src, pred.Key) {
				continue
			}
			key, value := pred.Key, pred.Value
			lookups = append(lookups, plannedLookup{run: func() ([]uint64, bool) {
				l, ok := src.labels()
				if !ok {
					return nil, false
				}
				return l.LabelOffsets(key, value), true
			}})
		default:
			keepResidual(p) // no pushdown case for this type: always re-verified
		}
	}
	if len(lookups) == 0 {
		return nil, preds
	}
	return lookups, residuals
}

// losslessKind reports whether a kind's 16-byte index key preserves the value exactly, so
// an index hit is proof the predicate holds and re-verification is redundant.
//
// int is a sign-flipped int64 and float an IEEE-754 total-order transform (both bijective
// in 8 bytes with 8 bytes of padding); uuid IS the 16 raw bytes. Only str is lossy — it is
// a truncated 16-byte prefix, which is precisely why re-verify exists (core/index/key.go).
func losslessKind(k index.ValueKind) bool {
	return k == index.KindInt || k == index.KindFloat || k == index.KindUUID
}

// typedRun builds the index-level lookup for a typed comparison. The same lookup runs
// against a sealed segment's .tidx (binary search) and the active segment's buffer (linear
// pass), so both get exactly these bounds.
//
// For a LOSSY (str) kind the bounds are widened inclusively so a truncated key equal to the
// bound is never dropped, and re-verify then filters > from >=. For a LOSSLESS kind the key
// is the exact value, so the strict bound can be used directly — the lookup returns
// precisely the matching records and no re-verification is needed.
func typedRun(pred TypedCompare, lossless bool) func(r index.Lookup) []uint64 {
	key := index.EncodeKey(pred.Value)
	switch pred.Op {
	case OpEq:
		return func(r index.Lookup) []uint64 { return r.LookupEqual(key) }
	case OpGt:
		incLo := !lossless // lossy: keep the bound, re-verify drops equals
		return func(r index.Lookup) []uint64 { return r.LookupRange(key, maxKey, incLo, true) }
	case OpGe:
		return func(r index.Lookup) []uint64 { return r.LookupRange(key, maxKey, true, true) }
	case OpLt:
		incHi := !lossless
		return func(r index.Lookup) []uint64 { return r.LookupRange(zeroKey, key, true, incHi) }
	case OpLe:
		return func(r index.Lookup) []uint64 { return r.LookupRange(zeroKey, key, true, true) }
	default:
		return func(index.Lookup) []uint64 { return nil }
	}
}

// patternMatches reports whether a segment's index for field was built from the pattern the
// query engine currently defines field with. A segment that recorded no pattern (sealed
// before patterns were recorded) never matches: its index cannot be proven to agree with the
// scan, so it is scanned.
func (e *Engine) patternMatches(field, segPattern string) bool {
	if e.ex == nil || segPattern == "" {
		return false
	}
	live, ok := e.ex.FieldPattern(field)
	return ok && live == segPattern
}

// segmentIndexesLabel reports whether a segment's label index is complete for key.
//
// A segment that recorded its allowlist answers directly. One that did not falls back to the
// keys present in its label index: a key that appears there was allowlisted for the whole
// segment (the allowlist cannot change while a segment is being written, and a crash-recovered
// segment is re-indexed from scratch), so the index is complete for it. A key that does not
// appear stays a residual — scanned, never wrongly pruned. If the index cannot be opened the
// lookup is still planned; it then fails and degrades the segment to a scan as before. The
// active segment answers the same way, from the allowlist its writer derives label sets with.
func (e *Engine) segmentIndexesLabel(seg *storage.SegmentMeta, src segmentIndex, key string) bool {
	if seg.LabelKeysRecorded {
		for _, k := range seg.LabelKeys {
			if k == key {
				return true
			}
		}
		return false
	}
	l, ok := src.labels()
	if !ok {
		return true
	}
	return l.HasKey(key)
}

func (e *Engine) labelAllowed(key string) bool {
	_, ok := e.labelKeys[key]
	return ok
}

// intersectLookups runs each lookup against the segment and intersects the candidate
// offset sets (predicates are ANDed).
//
// Returns ok=false if any lookup's index can't be opened, so the caller degrades to a full
// scan rather than a partial set. Returns guardTripped=true when the cost guard sends the
// segment to the scan instead (see costguard.go).
func intersectLookups(lookups []plannedLookup, g costGuard) (offsets []uint64, guardTripped, ok bool) {
	sets := make([][]uint64, 0, len(lookups))
	for _, l := range lookups {
		offs, lookupOK := l.run()
		if !lookupOK {
			return nil, false, false
		}
		// A lookup that matched nothing empties the intersection: the rest need not run.
		if len(offs) == 0 {
			return nil, false, true
		}
		sets = append(sets, offs)
	}
	// With a single lookup its result IS the candidate set, so the guard is decided before
	// paying for the dedup map, which on a set the guard rejects is pure waste. With several
	// it is decided only after intersecting: the intersection can only shrink, so a large set
	// says nothing about the final candidate count — TestExplainReportsAccessPath, where
	// `{region="eu"} | latency_ms > 200` intersects 3 typed hits down to 2, caught a guard
	// that bailed on the first set.
	if len(sets) == 1 && g.trips(sets[0]) {
		return nil, true, true
	}
	// Smallest set first: the map is built from it and every larger set only probes it. In
	// predicate order, `{app="checkout"} | latency_ms > 1000` built a map of every record in
	// the segment — the label matches all of them — to keep the 1% the range matched; the
	// map, not the lookups, was most of the index path's time, and it was paid even when the
	// cost guard then chose the scan.
	sort.Slice(sets, func(i, j int) bool { return len(sets[i]) < len(sets[j]) })
	result := make(map[uint64]struct{}, len(sets[0]))
	for _, o := range sets[0] {
		result[o] = struct{}{}
	}
	for _, offs := range sets[1:] {
		next := make(map[uint64]struct{}, len(result))
		for _, o := range offs {
			if _, in := result[o]; in {
				next[o] = struct{}{}
			}
		}
		result = next
		if len(result) == 0 {
			break
		}
	}
	// No sort: FetchRecords sorts its own copy of the offsets (core/storage/readback.go),
	// and nothing else depends on the order, so sorting here was pure duplicated work.
	out := make([]uint64, 0, len(result))
	for o := range result {
		out = append(out, o)
	}
	if len(sets) > 1 && g.trips(out) {
		return nil, true, true
	}
	return out, false, true
}

// SegmentPlanReason explains why a segment took the path it did.
const (
	ReasonNotIndexed      = "not_indexed"       // no complete index: a crash-recovered segment never re-indexed
	ReasonNoPushdown      = "no_pushdown"       // no predicate could be pushed here
	ReasonIndexMissing    = "index_missing"     // a sidecar was absent or corrupt
	ReasonCostGuard       = "cost_guard"        // candidates over-fetch the limit and cover the segment; a scan is cheaper
	ReasonIndexed         = "indexed"           // pushdown used, into the sealed segment's sidecars
	ReasonIndexedInMemory = "indexed_in_memory" // pushdown used, into the active segment's in-memory index
)

// segCapped reports whether a label key's value cap was breached in this segment (so its
// index is incomplete and the query must scan it).
func segCapped(seg *storage.SegmentMeta, key string) bool {
	for _, k := range seg.CappedKeys {
		if k == key {
			return true
		}
	}
	return false
}

func schemaHas(seg *storage.SegmentMeta, field string) bool {
	for _, f := range seg.Schema {
		if f.Name == field {
			return true
		}
	}
	return false
}

// schemaField returns the indexed value kind of a field in the segment's schema and the
// pattern its index was built from. ok is false if the field isn't indexed here or its
// recorded type is unrecognized (either way → don't push, scan instead).
func schemaField(seg *storage.SegmentMeta, field string) (kind index.ValueKind, pattern string, ok bool) {
	for _, f := range seg.Schema {
		if f.Name != field {
			continue
		}
		switch f.Type {
		case "int":
			return index.KindInt, f.Pattern, true
		case "float":
			return index.KindFloat, f.Pattern, true
		case "str":
			return index.KindStr, f.Pattern, true
		case "uuid":
			return index.KindUUID, f.Pattern, true
		default:
			return 0, "", false
		}
	}
	return 0, "", false
}

// SegmentPlan is how Explain reports the chosen access path per segment.
type SegmentPlan struct {
	SegmentID string
	// State is the segment's state as planned: "active" when it was answered — or scanned —
	// as the unsealed segment, "sealed" otherwise, including a segment that sealed while the
	// query was being planned.
	State string
	Mode  string // "index" or "scan"
	// Reason names WHY this mode was chosen. Previously a cost-guard trip and a missing
	// index both reported Mode:"scan" with Candidates:0, indistinguishably — so a query
	// that silently stopped using the index looked identical to one that never could.
	Reason     string
	Candidates int // candidate offsets when Mode=="index"
}

// Explain reports, per time-pruned segment, whether the query would use the index or
// scan, and how many candidates the index yields (design §9.3's index-vs-scan
// explainability). It runs the index lookups but not the fetch/re-verify, through the same
// access-path decision Execute makes.
func (e *Engine) Explain(q Query) []SegmentPlan {
	end := q.End
	if end <= 0 {
		end = maxInt64
	}
	var plans []SegmentPlan
	for _, sh := range e.shards {
		for _, seg := range sh.Manifest().Filter(q.Start, end) {
			a := e.access(sh, seg, q.Preds, effectiveLimit(q.Limit))
			p := SegmentPlan{SegmentID: seg.ID, State: string(a.seg.State), Mode: "scan", Reason: a.reason}
			if a.fetch {
				p.Mode, p.Candidates = "index", len(a.offsets)
			}
			plans = append(plans, p)
		}
	}
	return plans
}

const maxInt64 = int64(^uint64(0) >> 1)
