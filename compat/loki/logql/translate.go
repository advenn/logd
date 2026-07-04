package logql

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/query"
)

// Translate converts a parsed LogQL query into the native predicate set. Selector
// matchers and `| label op value` filters become label predicates, EXCEPT a `| field op
// value` where field is a declared template capture (per the extract engine) — that
// becomes a native TypedCompare, which the planner pushes into the typed-range index.
// This is how a Grafana LogQL query transparently lights up logd's differentiator
// (design §12). Time range / limit / direction come from the HTTP params, not here.
//
// ex may be nil (no template fields; every `| field op value` is a label filter).
func Translate(q *Query, ex *extract.Engine) ([]query.Predicate, error) {
	var preds []query.Predicate

	for _, m := range q.Selector.Matchers {
		p, err := matcherPredicate(m.Label, m.Op, m.Value)
		if err != nil {
			return nil, err
		}
		preds = append(preds, p)
	}

	for _, stage := range q.Pipeline {
		switch s := stage.(type) {
		case *LineFilter:
			p, err := lineFilterPredicate(s)
			if err != nil {
				return nil, err
			}
			preds = append(preds, p)
		case *LabelFilter:
			p, err := labelFilterPredicate(s, ex)
			if err != nil {
				return nil, err
			}
			preds = append(preds, p)
		case *UnwrapStage:
			// unwrap is a metric-only annotation (consumed by the range aggregation),
			// not a log-filter predicate.
		case *ParserStage:
			return nil, fmt.Errorf("logql: parser stage `| %s` is not supported yet", s.Parser)
		default:
			return nil, fmt.Errorf("logql: unsupported pipeline stage")
		}
	}
	return preds, nil
}

// TranslateMetric converts a parsed LogQL metric expression into a native MetricQuery.
// Start/End/Step are set by the caller (from HTTP params), not the LogQL string.
func TranslateMetric(m *MetricExpr, ex *extract.Engine) (query.MetricQuery, error) {
	preds, err := Translate(m.Log, ex) // inner log filters (unwrap stage skipped)
	if err != nil {
		return query.MetricQuery{}, err
	}
	rng, err := parseLokiDuration(m.Range)
	if err != nil {
		return query.MetricQuery{}, err
	}
	rangeOp, needsUnwrap, err := mapRangeOp(m.RangeOp)
	if err != nil {
		return query.MetricQuery{}, err
	}
	// `rate` has two forms: over an unwrap it is a value-rate (sum(values)/range), not a
	// count-rate. Reclassify so the unwrap is applied.
	if m.RangeOp == "rate" && m.Unwrap != "" {
		rangeOp, needsUnwrap = query.RangeRateSum, true
	}
	if needsUnwrap && m.Unwrap == "" {
		return query.MetricQuery{}, fmt.Errorf("logql: %s requires `| unwrap <field>`", m.RangeOp)
	}
	vecOp, err := mapVectorOp(m.VectorOp)
	if err != nil {
		return query.MetricQuery{}, err
	}
	q := query.MetricQuery{
		Preds:    preds,
		Range:    rng.Nanoseconds(),
		RangeOp:  rangeOp,
		VectorOp: vecOp,
		Grouping: m.Grouping,
		Without:  m.Without,
	}
	if needsUnwrap {
		q.Unwrap = m.Unwrap
	}
	return q, nil
}

func mapRangeOp(op string) (rangeOp query.RangeOp, needsUnwrap bool, err error) {
	switch op {
	case "count_over_time":
		return query.RangeCount, false, nil
	case "rate":
		return query.RangeRate, false, nil
	case "sum_over_time":
		return query.RangeSum, true, nil
	case "avg_over_time":
		return query.RangeAvg, true, nil
	case "max_over_time":
		return query.RangeMax, true, nil
	case "min_over_time":
		return query.RangeMin, true, nil
	default:
		return 0, false, fmt.Errorf("logql: unsupported range aggregation %q", op)
	}
}

func mapVectorOp(op string) (query.VectorOp, error) {
	switch op {
	case "":
		return query.VecNone, nil
	case "sum":
		return query.VecSum, nil
	case "avg":
		return query.VecAvg, nil
	case "max":
		return query.VecMax, nil
	case "min":
		return query.VecMin, nil
	case "count":
		return query.VecCount, nil
	default:
		return 0, fmt.Errorf("logql: unsupported vector aggregation %q", op)
	}
}

// parseLokiDuration parses a Loki range-selector duration. Go's time.ParseDuration covers
// ns/us/ms/s/m/h; Loki also allows d (days) and w (weeks), handled here.
func parseLokiDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("logql: empty duration")
	}
	var d time.Duration
	if n := len(s); n >= 2 && (s[n-1] == 'd' || s[n-1] == 'w') {
		v, err := strconv.ParseFloat(s[:n-1], 64)
		if err != nil {
			return 0, fmt.Errorf("logql: bad duration %q", s)
		}
		unit := float64(24 * time.Hour)
		if s[n-1] == 'w' {
			unit = float64(7 * 24 * time.Hour)
		}
		d = time.Duration(v * unit)
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("logql: bad duration %q: %w", s, err)
		}
	}
	// Reject non-positive (incl. an int64 overflow from a huge d/w value, which wraps to a
	// negative Duration) so it can't be silently coerced to the default window.
	if d <= 0 {
		return 0, fmt.Errorf("logql: duration must be positive, got %q", s)
	}
	return d, nil
}

// matcherPredicate maps a selector matcher ({k=..}) to a label predicate. Selector
// regexes are fully anchored (Loki semantics).
func matcherPredicate(label string, op Op, value string) (query.Predicate, error) {
	switch op {
	case OpEq:
		return query.LabelEqual{Key: label, Value: value}, nil
	case OpNotEq:
		return query.LabelNotEqual{Key: label, Value: value}, nil
	case OpRe:
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("logql: bad regex %q: %w", value, err)
		}
		return query.LabelRegex{Key: label, Re: re}, nil
	case OpNotRe:
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("logql: bad regex %q: %w", value, err)
		}
		return query.LabelNotRegex{Key: label, Re: re}, nil
	default:
		return nil, fmt.Errorf("logql: invalid selector operator %s", op)
	}
}

// lineFilterPredicate maps a line filter (|=, !=, |~, !~) to a native line predicate.
// Line regexes are unanchored (substring match), matching Loki.
func lineFilterPredicate(s *LineFilter) (query.Predicate, error) {
	switch s.Op {
	case OpPipeEq:
		return query.LineContains{Sub: s.Value}, nil
	case OpPipeNotEq:
		return query.LineNotContains{Sub: s.Value}, nil
	case OpPipeRe:
		re, err := regexp.Compile(s.Value)
		if err != nil {
			return nil, fmt.Errorf("logql: bad line regex %q: %w", s.Value, err)
		}
		return query.LineRegex{Re: re}, nil
	case OpPipeNotRe:
		re, err := regexp.Compile(s.Value)
		if err != nil {
			return nil, fmt.Errorf("logql: bad line regex %q: %w", s.Value, err)
		}
		return query.LineNotRegex{Re: re}, nil
	default:
		return nil, fmt.Errorf("logql: invalid line filter operator %s", s.Op)
	}
}

// labelFilterPredicate maps `| field op value`. If field is a template capture, it
// becomes a typed comparison (index pushdown); otherwise a label filter.
func labelFilterPredicate(s *LabelFilter, ex *extract.Engine) (query.Predicate, error) {
	if ex != nil {
		if kind, ok := ex.FieldKind(s.Label); ok {
			return typedPredicate(s.Label, s.Op, s.Value, kind)
		}
	}
	switch s.Op {
	case OpEq:
		return query.LabelEqual{Key: s.Label, Value: s.Value}, nil
	case OpNotEq:
		return query.LabelNotEqual{Key: s.Label, Value: s.Value}, nil
	case OpRe:
		re, err := regexp.Compile("^(?:" + s.Value + ")$")
		if err != nil {
			return nil, fmt.Errorf("logql: bad regex %q: %w", s.Value, err)
		}
		return query.LabelRegex{Key: s.Label, Re: re}, nil
	case OpNotRe:
		re, err := regexp.Compile("^(?:" + s.Value + ")$")
		if err != nil {
			return nil, fmt.Errorf("logql: bad regex %q: %w", s.Value, err)
		}
		return query.LabelNotRegex{Key: s.Label, Re: re}, nil
	case OpGT, OpGTE, OpLT, OpLTE:
		return query.LabelCompare{Key: s.Label, Op: nativeOrderedOp(s.Op), Value: s.Value}, nil
	default:
		return nil, fmt.Errorf("logql: invalid label filter operator %s", s.Op)
	}
}

// typedPredicate builds a native TypedCompare for a template field.
func typedPredicate(field string, op Op, raw string, kind index.ValueKind) (query.Predicate, error) {
	qop, ok := nativeCompareOp(op)
	if !ok {
		return nil, fmt.Errorf("logql: operator %s is not supported on typed field %q", op, field)
	}
	val, err := parseTypedValue(raw, kind)
	if err != nil {
		return nil, fmt.Errorf("logql: value %q for field %q: %w", raw, field, err)
	}
	return query.TypedCompare{Field: field, Op: qop, Value: val}, nil
}

func parseTypedValue(raw string, kind index.ValueKind) (index.Value, error) {
	switch kind {
	case index.KindInt:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return index.Value{}, fmt.Errorf("not an int")
		}
		return index.Value{Kind: index.KindInt, Int: n}, nil
	case index.KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return index.Value{}, fmt.Errorf("not a float")
		}
		return index.Value{Kind: index.KindFloat, Float: f}, nil
	case index.KindStr:
		return index.Value{Kind: index.KindStr, Str: raw}, nil
	case index.KindUUID:
		u, ok := parseUUID(raw)
		if !ok {
			return index.Value{}, fmt.Errorf("not a uuid")
		}
		return index.Value{Kind: index.KindUUID, UUID: u}, nil
	default:
		return index.Value{}, fmt.Errorf("unsupported typed-filter kind")
	}
}

// parseUUID parses the canonical 8-4-4-4-12 hex form into 16 raw bytes.
func parseUUID(s string) (out [16]byte, ok bool) {
	if len(s) != 36 {
		return out, false
	}
	oi := 0
	for pos := 0; pos < 36; {
		if pos == 8 || pos == 13 || pos == 18 || pos == 23 {
			if s[pos] != '-' {
				return out, false
			}
			pos++
			continue
		}
		hi, ok1 := hexNibble(s[pos])
		lo, ok2 := hexNibble(s[pos+1])
		if !ok1 || !ok2 {
			return out, false
		}
		out[oi] = hi<<4 | lo
		oi++
		pos += 2
	}
	return out, true
}

func hexNibble(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

// nativeCompareOp maps a LogQL op to a native query.Op for typed comparison.
func nativeCompareOp(op Op) (query.Op, bool) {
	switch op {
	case OpEq:
		return query.OpEq, true
	case OpNotEq:
		return query.OpNe, true
	case OpGT:
		return query.OpGt, true
	case OpGTE:
		return query.OpGe, true
	case OpLT:
		return query.OpLt, true
	case OpLTE:
		return query.OpLe, true
	default:
		return 0, false // regex ops aren't typed comparisons
	}
}

func nativeOrderedOp(op Op) query.Op {
	switch op {
	case OpGT:
		return query.OpGt
	case OpGTE:
		return query.OpGe
	case OpLT:
		return query.OpLt
	default:
		return query.OpLe
	}
}
