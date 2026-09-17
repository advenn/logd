package query

import (
	"strings"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
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

// plannedLookup is one pushable predicate. run resolves it against a segment (opening the
// appropriate typed or label index for that segment's base path) and returns the
// candidate offsets, or ok=false if the index can't be opened — in which case the caller
// degrades the whole segment to a scan (design §9.1).
type plannedLookup struct {
	run func(segBase string) (offsets []uint64, ok bool)
}

// plan decides which predicates can be pushed into this segment's indexes. A non-indexed
// segment (active or crash-recovered) pushes nothing. A predicate whose field/key isn't
// indexed here is left as a residual (handled by the full re-verify), which keeps
// pushdown results identical to scan. Returns nil to mean "scan this segment".
// It also returns the RESIDUAL predicates: those the index has not already proven. A
// predicate is settled (and so omitted from the residuals) only when the index answer is
// exact — see losslessKind. Settled predicates are skipped during re-verify on the
// index-fetch path, which is where the Aho-Corasick re-extraction cost lived; they are
// never skipped on any scan fallback, where nothing has been proven.
func (e *Engine) plan(seg *storage.SegmentMeta, preds []Predicate) ([]plannedLookup, []Predicate) {
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
			// predicate would encode its key in the wrong kind-space); and the index was
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
			cache := e.cache
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := cache.tidx(index.TidxPath(segBase, field))
				if err != nil {
					return nil, false
				}
				return run(r), true
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
			cache := e.cache
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := cache.tidx(index.TidxPath(segBase, field))
				if err != nil {
					return nil, false
				}
				return r.LookupEqual(zeroKey), true // existence: offsets that contain the literal
			}})
		case LabelEqual:
			// Only allowlisted, non-reserved keys are indexed. A non-allowlisted key
			// scans (resolver reads Extra); a reserved key (level/service) scans too,
			// because the index derives it from Extra while the resolver reads the
			// first-class field — pushing it could drop matches. An empty-string value
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
			if !e.segmentIndexesLabel(seg, pred.Key) {
				continue
			}
			key, value := pred.Key, pred.Value
			cache := e.cache
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := cache.lidx(label.IndexPath(segBase))
				if err != nil {
					return nil, false
				}
				return r.LabelOffsets(key, value), true
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

// typedRun builds the reader-level lookup for a typed comparison.
//
// For a LOSSY (str) kind the bounds are widened inclusively so a truncated key equal to the
// bound is never dropped, and re-verify then filters > from >=. For a LOSSLESS kind the key
// is the exact value, so the strict bound can be used directly — the lookup returns
// precisely the matching records and no re-verification is needed.
func typedRun(pred TypedCompare, lossless bool) func(r *index.Reader) []uint64 {
	key := index.EncodeKey(pred.Value)
	switch pred.Op {
	case OpEq:
		return func(r *index.Reader) []uint64 { return r.LookupEqual(key) }
	case OpGt:
		incLo := !lossless // lossy: keep the bound, re-verify drops equals
		return func(r *index.Reader) []uint64 { return r.LookupRange(key, maxKey, incLo, true) }
	case OpGe:
		return func(r *index.Reader) []uint64 { return r.LookupRange(key, maxKey, true, true) }
	case OpLt:
		incHi := !lossless
		return func(r *index.Reader) []uint64 { return r.LookupRange(zeroKey, key, true, incHi) }
	case OpLe:
		return func(r *index.Reader) []uint64 { return r.LookupRange(zeroKey, key, true, true) }
	default:
		return func(*index.Reader) []uint64 { return nil }
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
// lookup is still planned; it then fails and degrades the segment to a scan as before.
func (e *Engine) segmentIndexesLabel(seg *storage.SegmentMeta, key string) bool {
	if seg.LabelKeysRecorded {
		for _, k := range seg.LabelKeys {
			if k == key {
				return true
			}
		}
		return false
	}
	r, err := e.cache.lidx(label.IndexPath(strings.TrimSuffix(seg.Path, ".log")))
	if err != nil {
		return true
	}
	return r.HasKey(key)
}

func (e *Engine) labelAllowed(key string) bool {
	_, ok := e.labelKeys[key]
	return ok
}

// intersectLookups runs each lookup against the segment and intersects the candidate
// offset sets (predicates are ANDed).
//
// Returns ok=false if any lookup's index can't be opened, so the caller degrades to a full
// scan rather than a partial set. Returns guardTripped=true when the candidate set grows
// past what is worth fetching by offset — see below.
func intersectLookups(segBase string, lookups []plannedLookup, maxCandidates int) (offsets []uint64, guardTripped, ok bool) {
	var result map[uint64]struct{}
	for _, l := range lookups {
		offs, lookupOK := l.run(segBase)
		if !lookupOK {
			return nil, false, false
		}
		if result == nil {
			// Early bail is only sound when this is the ONLY lookup, because the
			// intersection can only shrink: a large first set says nothing about the final
			// candidate count. Tripping the guard on it anyway would demote segments whose
			// real intersection is tiny — caught immediately by TestExplainReportsAccessPath,
			// where `{region="eu"} | latency_ms > 200` intersects 3 typed hits down to 2.
			bailAt := 0
			if len(lookups) == 1 {
				bailAt = maxCandidates
			}
			result = make(map[uint64]struct{}, len(offs))
			for _, o := range offs {
				result[o] = struct{}{}
				// Applied DURING materialization, not after: building the whole set and
				// then discarding it is what made a broad indexed query slower than the
				// scan it fell back to. Bailing bounds the work at maxCandidates+1
				// insertions. Exact, not an estimate — a raw index-entry count would
				// over-count, since one record matching a template twice yields two entries
				// for the same offset.
				if bailAt > 0 && len(result) > bailAt {
					return nil, true, true
				}
			}
			// A first lookup that matched nothing means the intersection is empty. The old
			// code only checked this from the SECOND lookup onward, so a zero-candidate
			// first predicate still ran every remaining lookup in full.
			if len(result) == 0 {
				return nil, false, true
			}
			continue
		}
		next := make(map[uint64]struct{})
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
	return out, false, true
}

// costGuardFraction: when candidate offsets exceed this fraction of a segment's records,
// a sequential scan beats random-access fetches (constant; configurable later).
const costGuardFraction = 0.5

// costGuardTrips reports whether an index candidate set is too large to be worth fetching
// by offset versus scanning the segment.
func costGuardTrips(seg *storage.SegmentMeta, candidates int) bool {
	return seg.Records > 0 && uint64(candidates) > uint64(costGuardFraction*float64(seg.Records))
}

// costGuardLimit is the candidate count above which the guard trips, or 0 when this segment
// has no record count and the guard is therefore disabled. Passing it into
// intersectLookups lets the guard fire while the set is being built.
func costGuardLimit(seg *storage.SegmentMeta) int {
	if seg.Records == 0 {
		return 0
	}
	return int(costGuardFraction * float64(seg.Records))
}

// SegmentPlanReason explains why a segment took the path it did.
const (
	ReasonNotIndexed   = "not_indexed"   // active or crash-recovered segment
	ReasonNoPushdown   = "no_pushdown"   // no predicate could be pushed here
	ReasonIndexMissing = "index_missing" // a sidecar was absent or corrupt
	ReasonCostGuard    = "cost_guard"    // candidate set too large; sequential scan is cheaper
	ReasonIndexed      = "indexed"       // pushdown used
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
	Mode      string // "index" or "scan"
	// Reason names WHY this mode was chosen. Previously a cost-guard trip and a missing
	// index both reported Mode:"scan" with Candidates:0, indistinguishably — so a query
	// that silently stopped using the index looked identical to one that never could.
	Reason     string
	Candidates int // candidate offsets when Mode=="index"
}

// Explain reports, per time-pruned segment, whether the query would use the index or
// scan, and how many candidates the index yields (design §9.3's index-vs-scan
// explainability). It runs the index lookups but not the fetch/re-verify.
func (e *Engine) Explain(q Query) []SegmentPlan {
	end := q.End
	if end <= 0 {
		end = maxInt64
	}
	var plans []SegmentPlan
	for _, sh := range e.shards {
		for _, seg := range sh.Manifest().Filter(q.Start, end) {
			lookups, _ := e.plan(seg, q.Preds)
			if lookups == nil {
				reason := ReasonNoPushdown
				if !seg.Indexed {
					reason = ReasonNotIndexed
				}
				plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "scan", Reason: reason})
				continue
			}
			segBase := strings.TrimSuffix(seg.Path, ".log")
			offsets, tripped, ok := intersectLookups(segBase, lookups, costGuardLimit(seg))
			if !ok {
				plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "scan", Reason: ReasonIndexMissing})
				continue
			}
			if tripped || costGuardTrips(seg, len(offsets)) {
				plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "scan", Reason: ReasonCostGuard})
				continue
			}
			plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "index", Reason: ReasonIndexed, Candidates: len(offsets)})
		}
	}
	return plans
}

const maxInt64 = int64(^uint64(0) >> 1)
