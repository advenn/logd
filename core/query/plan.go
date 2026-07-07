package query

import (
	"sort"
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
func (e *Engine) plan(seg *storage.SegmentMeta, preds []Predicate) []plannedLookup {
	if !seg.Indexed {
		return nil
	}
	var lookups []plannedLookup
	for _, p := range preds {
		switch pred := p.(type) {
		case TypedCompare:
			kind, ok := schemaKind(seg, pred.Field)
			// Push only when: not != (not selective); the field is indexed in this
			// segment; and the query value's Kind MATCHES the field's indexed kind. A
			// cross-kind (mistyped) predicate must NOT push — the key would be encoded in
			// the wrong kind-space. Left as a residual, the scan path handles it.
			if pred.Op == OpNe || !ok || pred.Value.Kind != kind {
				continue
			}
			field, run := pred.Field, typedRun(pred)
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := index.OpenReader(index.TidxPath(segBase, field))
				if err != nil {
					return nil, false
				}
				return run(r), true
			}})
		case LineContains:
			if e.ex == nil {
				continue
			}
			field, ok := e.ex.LiteralField(pred.Sub)
			if !ok || !schemaHas(seg, field) {
				continue
			}
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := index.OpenReader(index.TidxPath(segBase, field))
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
			if model.IsReservedLabelKey(pred.Key) || pred.Value == "" || !e.labelAllowed(pred.Key) || segCapped(seg, pred.Key) {
				continue
			}
			key, value := pred.Key, pred.Value
			lookups = append(lookups, plannedLookup{run: func(segBase string) ([]uint64, bool) {
				r, err := label.OpenReader(label.IndexPath(segBase))
				if err != nil {
					return nil, false
				}
				return r.LabelOffsets(key, value), true
			}})
		}
	}
	if len(lookups) == 0 {
		return nil
	}
	return lookups
}

// typedRun builds the reader-level lookup for a typed comparison, with bounds widened
// inclusively so a lossy str key equal to the bound is never dropped (re-verify filters
// > vs >=).
func typedRun(pred TypedCompare) func(r *index.Reader) []uint64 {
	key := index.EncodeKey(pred.Value)
	switch pred.Op {
	case OpEq:
		return func(r *index.Reader) []uint64 { return r.LookupEqual(key) }
	case OpGt, OpGe:
		return func(r *index.Reader) []uint64 { return r.LookupRange(key, maxKey, true, true) }
	case OpLt, OpLe:
		return func(r *index.Reader) []uint64 { return r.LookupRange(zeroKey, key, true, true) }
	default:
		return func(*index.Reader) []uint64 { return nil }
	}
}

func (e *Engine) labelAllowed(key string) bool {
	_, ok := e.labelKeys[key]
	return ok
}

// intersectLookups runs each lookup against the segment and intersects the candidate
// offset sets (predicates are ANDed). Returns ok=false if any lookup's index can't be
// opened, so the caller degrades to a full scan rather than a partial set.
func intersectLookups(segBase string, lookups []plannedLookup) ([]uint64, bool) {
	var result map[uint64]struct{}
	for _, l := range lookups {
		offs, ok := l.run(segBase)
		if !ok {
			return nil, false
		}
		if result == nil {
			result = make(map[uint64]struct{}, len(offs))
			for _, o := range offs {
				result[o] = struct{}{}
			}
			continue
		}
		next := make(map[uint64]struct{})
		for _, o := range offs {
			if _, ok := result[o]; ok {
				next[o] = struct{}{}
			}
		}
		result = next
		if len(result) == 0 {
			break
		}
	}
	out := make([]uint64, 0, len(result))
	for o := range result {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, true
}

// costGuardFraction: when candidate offsets exceed this fraction of a segment's records,
// a sequential scan beats random-access fetches (constant; configurable later).
const costGuardFraction = 0.5

// costGuardTrips reports whether an index candidate set is too large to be worth fetching
// by offset versus scanning the segment.
func costGuardTrips(seg *storage.SegmentMeta, candidates int) bool {
	return seg.Records > 0 && uint64(candidates) > uint64(costGuardFraction*float64(seg.Records))
}

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

// schemaKind returns the indexed value kind of a field in the segment's schema. ok is
// false if the field isn't indexed here or its recorded type is unrecognized (either
// way → don't push, scan instead).
func schemaKind(seg *storage.SegmentMeta, field string) (index.ValueKind, bool) {
	for _, f := range seg.Schema {
		if f.Name != field {
			continue
		}
		switch f.Type {
		case "int":
			return index.KindInt, true
		case "float":
			return index.KindFloat, true
		case "str":
			return index.KindStr, true
		case "uuid":
			return index.KindUUID, true
		default:
			return 0, false
		}
	}
	return 0, false
}

// SegmentPlan is how Explain reports the chosen access path per segment.
type SegmentPlan struct {
	SegmentID  string
	Mode       string // "index" or "scan"
	Candidates int    // candidate offsets when Mode=="index"
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
			lookups := e.plan(seg, q.Preds)
			if lookups == nil {
				plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "scan"})
				continue
			}
			segBase := strings.TrimSuffix(seg.Path, ".log")
			offsets, ok := intersectLookups(segBase, lookups)
			if !ok || costGuardTrips(seg, len(offsets)) {
				// A missing .tidx or a cost-guard trip both make the executor scan.
				plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "scan"})
				continue
			}
			plans = append(plans, SegmentPlan{SegmentID: seg.ID, Mode: "index", Candidates: len(offsets)})
		}
	}
	return plans
}

const maxInt64 = int64(^uint64(0) >> 1)
