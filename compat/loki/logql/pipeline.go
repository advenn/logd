package logql

import (
	"strconv"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
)

// program is the label-producing part of a LogQL pipeline: its parser stages and its
// `drop __error__` stages, in order. Line filters and label filters only filter, so the
// labels and the __error__ a record carries at any point of the pipeline are a function of
// the record and of the steps before that point. That is what lets each label filter after
// a parser become an independent native predicate (a query.DynamicCompare) that recomputes
// what it needs, ANDed with the others exactly as the pipeline is: the result does not
// depend on evaluation order, so the core may evaluate them in any order, on any path.
//
// It is immutable once built, so the closures handed to the core may run concurrently.
type program struct {
	steps []step
}

// step is a parser stage, or a drop of __error__ (dropErr non-nil).
type step struct {
	parser  stageParser
	dropErr func(errValue string) bool // reports whether this drop removes that error
}

// hasParser reports whether the program has a parser stage among its first n steps.
func (p *program) hasParser(n int) bool {
	for _, s := range p.steps[:n] {
		if s.parser != nil {
			return true
		}
	}
	return false
}

// mayProduce reports whether the first n steps can give a record a label of this name, under
// Loki's naming: a stage producing k sets k, or k_extracted on a record that already has k.
func (p *program) mayProduce(n int, name string) bool {
	for _, s := range p.steps[:n] {
		if s.parser == nil {
			continue
		}
		if name == errorLabel { // never a key a stage extracts: only an error sets it
			if s.parser.mayFail() {
				return true
			}
			continue
		}
		if s.parser.produces(name) {
			return true
		}
		if len(name) > len(duplicateSuffix) && name[len(name)-len(duplicateSuffix):] == duplicateSuffix &&
			s.parser.produces(name[:len(name)-len(duplicateSuffix)]) {
			return true
		}
	}
	return false
}

// value is what a label filter placed after the first n steps reads for name, following
// Loki's LabelsBuilder.Get:
//
//   - __error__ is the pipeline error at that point;
//   - a label the record carries itself (its stream labels and structured metadata, both in
//     Extra) comes first — a parser that extracts the same name sets name_extracted
//     instead, so there is never a choice between the two;
//   - then the first value a parser stage set under the name;
//   - then the engine's own resolution (level and service fall back to the record's fields,
//     as they do for a filter before any parser stage).
func (p *program) value(n int, e model.LogEntry, r query.Resolver, name string) (string, bool) {
	if name == errorLabel {
		err := p.errorAfter(n, e)
		return err, err != ""
	}
	if v, ok := model.ExtraLabel(e.Extra, name); ok {
		return v, true
	}
	if v, ok := p.extracted(n, e, name); ok {
		return v, true
	}
	if r == nil {
		return "", false
	}
	return r.Label(e, name)
}

// extracted returns the first value the parser stages among the first n steps set under
// name. Loki keeps the first: once a parsed label is set, later stages skip that name
// (ParserHints.Extracted, recorded by LabelsBuilder.Set).
func (p *program) extracted(n int, e model.LogEntry, name string) (string, bool) {
	var val string
	found := false
	for _, s := range p.steps[:n] {
		if s.parser == nil {
			continue
		}
		s.parser.each(e.Message, func(key, v string) bool {
			if finalNameIs(e, key, name) {
				val, found = v, true
				return false
			}
			return true
		})
		if found {
			return val, true
		}
	}
	return "", false
}

// finalNameIs reports whether a label a stage extracted as key is set under name on this
// record: key itself, or key_extracted when the record already carries key.
func finalNameIs(e model.LogEntry, key, name string) bool {
	switch {
	case key == name:
		_, own := model.ExtraLabel(e.Extra, key)
		return !own
	case len(name) == len(key)+len(duplicateSuffix) && name[:len(key)] == key && name[len(key):] == duplicateSuffix:
		_, own := model.ExtraLabel(e.Extra, key)
		return own
	}
	return false
}

// errorAfter is the __error__ value after the first n steps: the last error a stage set,
// unless a later `drop __error__` removed it (Loki's DropLabels resets the error).
func (p *program) errorAfter(n int, e model.LogEntry) string {
	err := ""
	for _, s := range p.steps[:n] {
		switch {
		case s.parser == nil:
			if err != "" && s.dropErr(err) {
				err = ""
			}
		case s.parser.mayFail():
			if se := s.parser.each(e.Message, func(string, string) bool { return true }); se != "" {
				err = se
			}
		}
	}
	return err
}

// labels returns every label a record carries at the end of the pipeline, as a Loki
// response shows them: its own labels, every label a parser stage extracted (first value
// wins, a name it already carries suffixed with _extracted), and __error__ when one is set.
// An extracted label whose value is empty is left out: in Prometheus' label model an empty
// value is an absent label.
func (p *program) labels(e model.LogEntry) map[string]string {
	own := model.ParseExtraLabels(e.Extra)
	out := make(map[string]string, len(own)+4)
	for k, v := range own {
		out[k] = v
	}
	set := map[string]bool{}
	err := ""
	for _, s := range p.steps {
		if s.parser == nil {
			if err != "" && s.dropErr(err) {
				err = ""
			}
			continue
		}
		se := s.parser.each(e.Message, func(key, v string) bool {
			name := key
			if _, ok := own[key]; ok {
				name += duplicateSuffix
			}
			if !set[name] {
				set[name] = true
				if v != "" {
					out[name] = v
				}
			}
			return true
		})
		if se != "" {
			err = se
		}
	}
	if err != "" {
		out[errorLabel] = err
	}
	return out
}

// dynamicCompare builds the native predicate for a label filter on a name the first n steps
// may extract: the value is recomputed per record, the comparison converts per line.
//
// Loki's grammar decides the comparison: a number literal (`| x > 200`, `| x = 200`) makes
// a numeric filter, a string a string filter, and the ordering operators are numeric with
// either (Loki's grammar has no string ordering; logd accepts `| x > "200"` as it always
// has). A numeric filter on a value that is missing or does not convert does not match —
// see the README's LogQL section for how that differs from Loki, which keeps such a line and
// flags it with __error__=LabelFilterErr.
func (p *program) dynamicCompare(n int, f *LabelFilter) (query.Predicate, error) {
	name := f.Label
	dc := query.DynamicCompare{
		Name:    name,
		Extract: func(e model.LogEntry, r query.Resolver) (string, bool) { return p.value(n, e, r, name) },
		Value:   f.Value,
	}
	switch f.Op {
	case OpRe, OpNotRe:
		re, err := anchoredRegex(f.Value)
		if err != nil {
			return nil, err
		}
		dc.Re, dc.Op = re, query.OpEq
		if f.Op == OpNotRe {
			dc.Op = query.OpNe
		}
		return dc, nil
	case OpEq, OpNotEq:
		dc.Op = query.OpEq
		if f.Op == OpNotEq {
			dc.Op = query.OpNe
		}
		dc.Numeric = f.Numeric
	case OpGT, OpGTE, OpLT, OpLTE:
		dc.Op, dc.Numeric = nativeOrderedOp(f.Op), true
	default:
		return nil, errInvalidLabelOp(f.Op)
	}
	if dc.Numeric {
		if _, err := strconv.ParseFloat(f.Value, 64); err != nil {
			return nil, errNotANumber(f)
		}
	}
	return dc, nil
}

// unwrapValue reads an unwrapped label the first n steps extract, converted as Loki's sample
// extractor does (strconv.ParseFloat). An entry whose value is missing or does not convert
// is dropped, as logd drops it for every unwrap (core/query/metric.go).
func (p *program) unwrapValue(n int, name string) func(model.LogEntry, query.Resolver) (float64, bool) {
	return func(e model.LogEntry, r query.Resolver) (float64, bool) {
		v, ok := p.value(n, e, r, name)
		if !ok {
			return 0, false
		}
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
}
