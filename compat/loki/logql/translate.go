package logql

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
)

// Translate converts a parsed LogQL log query into the native predicate set; see
// TranslateLog, which also returns what the compat layer needs to present results.
//
// ex may be nil (no template fields; every `| field op value` is a label filter).
func Translate(q *Query, ex *extract.Engine) ([]query.Predicate, error) {
	t, err := TranslateLog(q, ex)
	if err != nil {
		return nil, err
	}
	return t.Preds, nil
}

// Translation is a translated LogQL log query.
type Translation struct {
	// Preds are the native predicates, ANDed.
	Preds []query.Predicate
	// Mappings say how each parser stage, and each label filter after one, was translated,
	// and why.
	Mappings []Mapping

	prog   *program // the parser and drop __error__ stages; nil when the query has no parser
	unwrap func(model.LogEntry, query.Resolver) (float64, bool)
}

// Mapping is how one pipeline stage was translated.
type Mapping struct {
	Stage string `json:"stage"`
	// Kind is, for a parser stage, "dynamic": its labels are extracted per line at query
	// time, and filters on them scan. For a label filter it is "typed" (a TypedCompare on a
	// declared template field, which the planner pushes into the index), "dynamic" (a
	// DynamicCompare, evaluated on scan and on re-verify), or "label" (a filter on the
	// record's own labels, as without a parser stage).
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

// Parses reports whether the query has a parser stage, so that a result's labels depend on
// its line.
func (t *Translation) Parses() bool { return t != nil && t.prog != nil }

// Labels returns the labels a result entry carries in a Loki response. Like Loki, a query
// with parser stages adds every label they extract from the entry's line (and __error__ when
// one is set), so a result stream is keyed by the stream labels plus the extracted ones; a
// name the entry already carries is extracted as name_extracted. They are recomputed here
// from the returned line, with Loki's semantics, whatever predicates selected the entry.
func (t *Translation) Labels(e model.LogEntry) map[string]string {
	if !t.Parses() {
		m := model.ParseExtraLabels(e.Extra)
		if m == nil {
			m = map[string]string{}
		}
		return m
	}
	return t.prog.labels(e)
}

// TranslateLog converts a parsed LogQL log query into native predicates.
//
// Selector matchers and filters before any parser stage translate as they always have: line
// filters to line predicates, `| label op value` to label predicates, EXCEPT a filter on a
// declared template field (per the extract engine), which becomes a native TypedCompare that
// the planner pushes into the typed-range index (design §12).
//
// After a parser stage, a filter on a name the stages may extract becomes a
// query.DynamicCompare that re-extracts it per line and converts as Loki does (§9.3: the
// Loki layer maps comparisons on untyped fields to the dynamic predicate). A filter on a
// name no preceding stage can extract translates as if there were no parser stage. Line filters
// never depend on the stages (logd has no line_format), so they stay native wherever they
// are.
func TranslateLog(q *Query, ex *extract.Engine) (*Translation, error) {
	t := &Translation{prog: &program{}}
	for _, m := range q.Selector.Matchers {
		p, err := matcherPredicate(m.Label, m.Op, m.Value)
		if err != nil {
			return nil, err
		}
		t.Preds = append(t.Preds, p)
	}

	parsers := map[int]stageParser{}
	for i, stage := range q.Pipeline {
		ps, ok := stage.(*ParserStage)
		if !ok {
			continue
		}
		sp, err := compileParser(ps)
		if err != nil {
			return nil, err
		}
		parsers[i] = sp
	}

	for i, stage := range q.Pipeline {
		switch s := stage.(type) {
		case *LineFilter:
			p, err := lineFilterPredicate(s)
			if err != nil {
				return nil, err
			}
			t.Preds = append(t.Preds, p)
		case *ParserStage:
			t.prog.steps = append(t.prog.steps, step{parser: parsers[i]})
			m := Mapping{Stage: stageString(s), Kind: "dynamic", Reason: "its labels are extracted from each line at query time"}
			t.Mappings = append(t.Mappings, m)
		case *LabelFilter:
			p, m, err := t.labelFilter(s, ex)
			if err != nil {
				return nil, err
			}
			t.Preds = append(t.Preds, p)
			if m != nil {
				t.Mappings = append(t.Mappings, *m)
			}
		case *UnwrapStage:
			// unwrap is a metric-only annotation (consumed by the range aggregation), not a
			// log-filter predicate. Its value is read where the stage stands in the pipeline.
			n := len(t.prog.steps)
			if t.prog.hasParser(n) && t.prog.mayProduce(n, s.Field) {
				t.unwrap = t.prog.unwrapValue(n, s.Field)
			}
		case *LabelsStage:
			if err := checkLabelsStage(s); err != nil {
				return nil, err
			}
			for _, m := range s.Labels {
				if m.Label == errorLabel {
					drop, err := errorDrop(m)
					if err != nil {
						return nil, err
					}
					t.prog.steps = append(t.prog.steps, step{dropErr: drop})
				}
			}
		default:
			return nil, fmt.Errorf("logql: unsupported pipeline stage")
		}
	}
	if !t.prog.hasParser(len(t.prog.steps)) {
		t.prog = nil
	}
	return t, nil
}

// labelFilter translates one `| name op value` stage, returning the Mapping Explain shows
// for it when it follows a parser stage.
func (t *Translation) labelFilter(s *LabelFilter, ex *extract.Engine) (query.Predicate, *Mapping, error) {
	n := len(t.prog.steps)
	if !t.prog.hasParser(n) {
		p, err := labelFilterPredicate(s, ex)
		return p, nil, err
	}
	m := &Mapping{Stage: stageString(s)}
	switch {
	case t.prog.mayProduce(n, s.Label):
		p, err := t.prog.dynamicCompare(n, s)
		if err != nil {
			return nil, nil, err
		}
		m.Kind = "dynamic"
		m.Reason = fmt.Sprintf("%s is extracted from each line at query time: a DynamicCompare, evaluated by scan", s.Label)
		return p, m, nil
	default:
		p, err := labelFilterPredicate(s, ex)
		if err != nil {
			return nil, nil, err
		}
		m.Kind = "label"
		m.Reason = fmt.Sprintf("no preceding stage extracts %s, so it translates as without a parser stage", s.Label)
		if _, ok := p.(query.TypedCompare); ok {
			m.Kind = "typed"
			m.Reason += " (a declared template field: a TypedCompare)"
		}
		return p, m, nil
	}
}

// compileParser builds a parser stage.
func compileParser(s *ParserStage) (stageParser, error) {
	switch s.Parser {
	case "pattern":
		return newPatternStage(s.Expr)
	case "regexp":
		return newRegexpStage(s.Expr)
	case "logfmt":
		return logfmtStage{}, nil
	case "json":
		return jsonStage{}, nil
	default:
		return nil, fmt.Errorf("logql: parser stage `| %s` is not supported", s.Parser)
	}
}

// errorDrop compiles one item of `| drop __error__…`: a bare name drops any error, a matcher
// drops the error only when its value matches (Loki's DropLabels).
func errorDrop(m LabelMatcher) (func(string) bool, error) {
	switch m.Op {
	case OpNone:
		return func(string) bool { return true }, nil
	case OpEq:
		return func(v string) bool { return v == m.Value }, nil
	case OpNotEq:
		return func(v string) bool { return v != m.Value }, nil
	case OpRe, OpNotRe:
		re, err := anchoredRegex(m.Value)
		if err != nil {
			return nil, err
		}
		want := m.Op == OpRe
		return func(v string) bool { return re.MatchString(v) == want }, nil
	default:
		return nil, errInvalidLabelOp(m.Op)
	}
}

// checkLabelsStage accepts the one drop/keep logd can honour: dropping __error__ and
// __error_details__, which Grafana appends to its log-volume query. __error__ is set only by
// a `| json` that cannot parse its line, and the drop removes it; logd never sets
// __error_details__, so dropping it is a no-op. Anything else would change the labels results
// carry in ways not implemented, so it is refused rather than ignored.
func checkLabelsStage(s *LabelsStage) error {
	if s.Keep {
		return fmt.Errorf("logql: `%s` is not supported", stageString(s))
	}
	for _, m := range s.Labels {
		if m.Label != "__error__" && m.Label != "__error_details__" {
			return fmt.Errorf("logql: `%s` is not supported: only __error__ and __error_details__ can be dropped", stageString(s))
		}
	}
	return nil
}

// TranslateMetric converts a parsed LogQL metric expression into a native MetricQuery.
// Start/End/Step are set by the caller (from HTTP params), not the LogQL string.
func TranslateMetric(m *MetricExpr, ex *extract.Engine) (query.MetricQuery, error) {
	t, err := TranslateLog(m.Log, ex) // inner log filters (unwrap stage skipped)
	if err != nil {
		return query.MetricQuery{}, err
	}
	preds := t.Preds
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
		q.UnwrapValue = t.unwrap // nil unless a parser stage extracts it
	}
	if t.Parses() {
		// Series are keyed by the extracted labels too, as in Loki, which also leaves the
		// unwrapped label out of a series' labels (LabelExtractorWithStages adds it to the
		// without-set). A query without parser stages keeps the built-in labels.
		unwrapped := q.Unwrap
		q.SeriesLabels = func(e model.LogEntry, _ query.Resolver) map[string]string {
			l := t.prog.labels(e)
			if unwrapped != "" {
				delete(l, unwrapped)
			}
			return l
		}
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

// parseLokiDuration parses a Loki range-selector duration (see ParseDuration), which must
// be positive.
func parseLokiDuration(s string) (time.Duration, error) {
	d, err := ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("logql: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("logql: duration must be positive, got %q", s)
	}
	return d, nil
}

// anchoredRegex compiles a label-matcher regex, which Loki anchors at both ends.
func anchoredRegex(expr string) (*regexp.Regexp, error) {
	re, err := regexp.Compile("^(?:" + expr + ")$")
	if err != nil {
		return nil, fmt.Errorf("logql: bad regex %q: %w", expr, err)
	}
	return re, nil
}

func errInvalidLabelOp(op Op) error {
	return fmt.Errorf("logql: invalid label filter operator %s", op)
}

func errNotANumber(f *LabelFilter) error {
	return fmt.Errorf("logql: `%s` compares %s with %q, which is not a number", stageString(f), f.Label, f.Value)
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
		// ParseFloat accepts "NaN", "Inf" and "Infinity". No extracted value is ever
		// non-finite, and NaN is unordered, so such a bound is a malformed query (400), not
		// a comparison to answer; the core engine rejects it too.
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return index.Value{}, fmt.Errorf("not a float")
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return index.Value{}, fmt.Errorf("not a finite float")
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
