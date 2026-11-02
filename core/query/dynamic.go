package query

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/advenn/logd/core/model"
)

// DynamicCompare is the explicit dynamic predicate of design §9.3: decode the record, obtain
// a value from it at query time, attempt the conversion the comparison needs, and filter. It
// is how a query reaches a value no template declares (§9.1: "range on an undeclared field:
// scan + extract at query time + filter. Supported, O(n)"), and how a compat layer whose
// query language converts per line reproduces that behaviour without the core knowing the
// language.
//
// The core does not know where the value comes from: Extract is supplied by the caller (a
// LogQL parser stage, for compat/loki). The predicate is never pushed into an index — the
// planner has no case for it, so it is always a residual — and it is evaluated the same way
// on the scan path, on the re-verify of index candidates and in tail. That is what keeps
// Execute equal to ExecuteScan for every query that uses it.
//
// The native typed predicate stays strict (TypedCompare: a declared field, a typed bound, an
// operator the type supports, else a plan-time error); this one is the opt-in alternative,
// and its cost is a scan of the time-pruned range.
type DynamicCompare struct {
	// Name is the value's name, for errors and plans. Extract alone decides what is read.
	Name string
	// Extract returns the value for one record, or false when the record has none. It must
	// be pure: it runs concurrently (tail evaluates on every shard's writer goroutine).
	Extract func(e model.LogEntry, r Resolver) (string, bool)
	// Op and Value compare the extracted value with a bound. Re, when set, replaces them:
	// the record matches when the value matches Re (Op OpEq) or does not (Op OpNe).
	Op    Op
	Value string
	Re    *regexp.Regexp
	// Numeric compares as float64: both the value and Value are converted with
	// strconv.ParseFloat, and a record whose value is missing or does not convert does not
	// match, whatever the operator. Otherwise the comparison is on the strings, and a missing
	// value is the empty string (the empty-label semantics of LabelEqual and LabelRegex).
	Numeric bool
}

func (p DynamicCompare) Match(e model.LogEntry, r Resolver) bool {
	if p.Extract == nil {
		return false // rejected by validate; tail does not validate, so never call it
	}
	v, ok := p.Extract(e, r)
	if p.Re != nil {
		if p.Op == OpNe {
			return !p.Re.MatchString(v)
		}
		return p.Re.MatchString(v)
	}
	if !p.Numeric {
		return satisfies(strings.Compare(v, p.Value), p.Op)
	}
	if !ok {
		return false
	}
	fv, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	fb, _ := strconv.ParseFloat(p.Value, 64) // checked by validate
	// Go's float operators, not satisfies(cmp): a NaN value (ParseFloat accepts "NaN") would
	// otherwise compare as "equal" to every bound. Here it satisfies only !=, as in Go.
	switch p.Op {
	case OpEq:
		return fv == fb
	case OpNe:
		return fv != fb
	case OpLt:
		return fv < fb
	case OpLe:
		return fv <= fb
	case OpGt:
		return fv > fb
	case OpGe:
		return fv >= fb
	default:
		return false
	}
}

// validate is DynamicCompare's part of validatePreds: a query it cannot answer is an error.
func (p DynamicCompare) validate() error {
	if p.Extract == nil {
		return fmt.Errorf("%w: dynamic comparison on %q has no extractor", ErrInvalidPredicate, p.Name)
	}
	if p.Re != nil {
		if p.Op != OpEq && p.Op != OpNe {
			return fmt.Errorf("%w: a regex on %q takes = or !=", ErrInvalidPredicate, p.Name)
		}
		return nil
	}
	if p.Numeric {
		// A bound that is not a number would make every record fail, and NaN or ±Inf has no
		// meaningful place in the order (see validatePreds): both are malformed queries.
		f, err := strconv.ParseFloat(p.Value, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%w: %q compared with %q, which is not a finite number", ErrInvalidPredicate, p.Name, p.Value)
		}
	}
	return nil
}
