package query

import (
	"regexp"
	"strconv"

	"github.com/advenn/logd/internal/logql"
	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
)

// ParseLogQL parses a LogQL query string into a QueryFilter.
//
// Planning rules:
//   - `label = value` selector matchers go into LabelMatch for the fast scan path.
//   - Any filter on an indexed template field (selector matcher or pipeline label
//     filter, any operator) is handled by a templateFieldPredicate that
//     re-extracts the field from each candidate entry. Equality filters
//     additionally populate FieldFilters to drive index pushdown in the reader.
//   - Everything else (!=, =~, !~, line filters, parser stages, non-template
//     label filters) is compiled into the logql pipeline predicate.
//
// tmpl may be nil (no templates configured); template-field handling is skipped.
func ParseLogQL(query string, tmpl *template.Engine) (*storage.QueryFilter, error) {
	ast, err := logql.Parse(query)
	if err != nil {
		return nil, err
	}

	filter := &storage.QueryFilter{
		LabelMatch:   make(map[string]string),
		FieldFilters: make(map[string]string),
	}

	isIndexed := func(name string) bool {
		return tmpl != nil && tmpl.IsIndexedField(name)
	}

	var conds []templateFieldCond

	// Selector matchers: indexed fields → template predicate (+ index pushdown
	// for `=`); plain `=` → LabelMatch; advanced matchers → logql pipeline.
	var advancedMatchers []logql.LabelMatcher
	for _, m := range ast.Selector.Matchers {
		switch {
		case isIndexed(m.Label):
			c, err := makeCond(m.Label, m.Op, m.Value)
			if err != nil {
				return nil, err
			}
			conds = append(conds, c)
			if m.Op == logql.OpEq {
				filter.FieldFilters[m.Label] = m.Value
			}
		case m.Op == logql.OpEq:
			filter.LabelMatch[m.Label] = m.Value
		default:
			advancedMatchers = append(advancedMatchers, m)
		}
	}

	// Pipeline stages: indexed-field label filters → template predicate; rest stays.
	var pipelineStages []logql.PipelineStage
	for _, stage := range ast.Pipeline {
		if lf, ok := stage.(*logql.LabelFilter); ok && isIndexed(lf.Label) {
			c, err := makeCond(lf.Label, lf.Op, lf.Value)
			if err != nil {
				return nil, err
			}
			conds = append(conds, c)
			if lf.Op == logql.OpEq {
				filter.FieldFilters[lf.Label] = lf.Value
			}
			continue
		}
		pipelineStages = append(pipelineStages, stage)
	}

	// Prepend advanced selector matchers as label-filter stages.
	var stages []logql.PipelineStage
	for _, m := range advancedMatchers {
		stages = append(stages, &logql.LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
	}
	stages = append(stages, pipelineStages...)

	var preds []storage.EntryPredicate
	if len(stages) > 0 {
		pipeline, err := logql.CompilePipeline(&logql.Query{Pipeline: stages})
		if err != nil {
			return nil, err
		}
		preds = append(preds, pipeline)
	}
	if len(conds) > 0 {
		preds = append(preds, &templateFieldPredicate{tmpl: tmpl, conds: conds})
	}

	filter.Predicate = combinePredicates(preds)
	return filter, nil
}

// templateFieldCond is one filter on an indexed template field, pre-compiled.
type templateFieldCond struct {
	field string
	op    logql.Op
	value string
	num   float64
	isNum bool
	regex *regexp.Regexp
}

func makeCond(field string, op logql.Op, value string) (templateFieldCond, error) {
	c := templateFieldCond{field: field, op: op, value: value}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		c.num = f
		c.isNum = true
	}
	if op.IsRegex() {
		re, err := regexp.Compile(value)
		if err != nil {
			return c, &logql.ParseError{Msg: "invalid regex: " + err.Error()}
		}
		c.regex = re
	}
	return c, nil
}

func (c templateFieldCond) eval(v string) bool {
	switch c.op {
	case logql.OpEq:
		return v == c.value
	case logql.OpNotEq:
		return v != c.value
	case logql.OpRe:
		return c.regex.MatchString(v)
	case logql.OpNotRe:
		return !c.regex.MatchString(v)
	case logql.OpGT, logql.OpGTE, logql.OpLT, logql.OpLTE:
		return c.compareOrdered(v)
	}
	return false
}

// compareOrdered does numeric comparison when both sides parse as numbers,
// falling back to lexicographic comparison (Loki semantics).
func (c templateFieldCond) compareOrdered(v string) bool {
	if c.isNum {
		if fv, err := strconv.ParseFloat(v, 64); err == nil {
			switch c.op {
			case logql.OpGT:
				return fv > c.num
			case logql.OpGTE:
				return fv >= c.num
			case logql.OpLT:
				return fv < c.num
			case logql.OpLTE:
				return fv <= c.num
			}
		}
	}
	switch c.op {
	case logql.OpGT:
		return v > c.value
	case logql.OpGTE:
		return v >= c.value
	case logql.OpLT:
		return v < c.value
	case logql.OpLTE:
		return v <= c.value
	}
	return false
}

// templateFieldPredicate re-runs template extraction on an entry's message and
// checks every condition — the post-decode verification the index design
// requires (indexes may yield false positives), and the only place range/!=
// filters on template fields are evaluated.
type templateFieldPredicate struct {
	tmpl  *template.Engine
	conds []templateFieldCond
}

func (p *templateFieldPredicate) Match(entry storage.LogEntry) bool {
	got := make(map[string]string)
	for _, m := range p.tmpl.Match(entry.Message) {
		got[m.FieldName] = m.Value
	}
	for _, c := range p.conds {
		v, ok := got[c.field]
		if !ok {
			// Missing field behaves like an empty label value in Loki: only
			// != and !~ are satisfied by absence.
			if c.op == logql.OpNotEq || c.op == logql.OpNotRe {
				continue
			}
			return false
		}
		if !c.eval(v) {
			return false
		}
	}
	return true
}

// andPredicate passes only entries that satisfy every sub-predicate.
type andPredicate struct {
	preds []storage.EntryPredicate
}

func (a *andPredicate) Match(entry storage.LogEntry) bool {
	for _, p := range a.preds {
		if !p.Match(entry) {
			return false
		}
	}
	return true
}

func combinePredicates(preds []storage.EntryPredicate) storage.EntryPredicate {
	switch len(preds) {
	case 0:
		return nil
	case 1:
		return preds[0]
	default:
		return &andPredicate{preds: preds}
	}
}
