package extract

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/index"
)

// literalFieldPrefix namespaces existence-index fields for bare literals so they can
// never collide with a template's "<template>_<capture>" field name. A literal has no
// value, so it is indexed under the zero str key (EncodeKey of an empty string), and
// "does record R contain literal L" is LookupEqual(L's field, that zero key).
const literalFieldPrefix = "__lit_"

// Engine is a compiled, immutable extractor. Extract is safe for concurrent use from
// many handler goroutines (design §7.4): all state read during Extract is read-only
// except the per-template failure counters, which are atomic.
type Engine struct {
	templates      []template
	bareLiterals   []string
	bareLitField   map[string]string // literal -> its existence field name
	ac             *ahoCorasick
	patternStrings []string          // AC pattern index -> literal string
	firstLitTmpls  map[string][]int  // first-literal string -> indices of templates starting with it
	schema         []index.FieldType // every indexed field (template captures + literal existence)
	failures       map[string]*atomic.Int64
}

// Compile validates the config and builds an Engine. It enforces the §7.2 rules:
// unique template/field/literal names, a leading literal + a ≥3-byte anchor per
// template, no adjacent captures, and no two identical template patterns.
func Compile(cfg config.IndexConfig) (*Engine, error) {
	e := &Engine{
		bareLitField:  map[string]string{},
		firstLitTmpls: map[string][]int{},
		failures:      map[string]*atomic.Int64{},
	}

	tmplNames := map[string]bool{}
	fieldNames := map[string]bool{}
	shapes := map[string]string{}          // normalized shape -> template name (overlap detection)
	litSet := map[string]struct{}{}        // distinct literal strings for the automaton

	for _, tc := range cfg.Templates {
		if tc.Name == "" {
			return nil, fmt.Errorf("template with empty name")
		}
		if tmplNames[tc.Name] {
			return nil, fmt.Errorf("duplicate template name %q", tc.Name)
		}
		tmplNames[tc.Name] = true

		frags, err := parsePattern(tc.Name, tc.Pattern)
		if err != nil {
			return nil, err
		}
		t := template{name: tc.Name, fragments: frags}
		if err := validateTemplate(&t); err != nil {
			return nil, err
		}
		t.firstLiteral = frags[0].literal

		shape := normalizedShape(&t)
		if other, ok := shapes[shape]; ok {
			return nil, fmt.Errorf("template %q overlaps (identical pattern to) %q", tc.Name, other)
		}
		shapes[shape] = tc.Name

		for _, f := range frags {
			if f.isCapture {
				if fieldNames[f.field] {
					return nil, fmt.Errorf("duplicate queryable field %q (template %q)", f.field, tc.Name)
				}
				fieldNames[f.field] = true
				e.schema = append(e.schema, index.FieldType{Name: f.field, Kind: f.kind})
			} else {
				litSet[f.literal] = struct{}{}
			}
		}

		idx := len(e.templates)
		e.templates = append(e.templates, t)
		e.firstLitTmpls[t.firstLiteral] = append(e.firstLitTmpls[t.firstLiteral], idx)
		e.failures[tc.Name] = new(atomic.Int64)
	}

	seenLit := map[string]bool{}
	for _, lit := range cfg.Literals {
		if lit == "" {
			return nil, fmt.Errorf("empty literal in config")
		}
		if seenLit[lit] {
			return nil, fmt.Errorf("duplicate literal %q", lit)
		}
		seenLit[lit] = true
		field := literalFieldPrefix + lit
		if fieldNames[field] {
			return nil, fmt.Errorf("literal %q collides with an existing field", lit)
		}
		fieldNames[field] = true
		e.bareLiterals = append(e.bareLiterals, lit)
		e.bareLitField[lit] = field
		e.schema = append(e.schema, index.FieldType{Name: field, Kind: index.KindStr})
		litSet[lit] = struct{}{}
	}

	// Build the automaton over every distinct literal string.
	for lit := range litSet {
		e.patternStrings = append(e.patternStrings, lit)
	}
	e.ac = newAhoCorasick(e.patternStrings)
	return e, nil
}

// IndexedFields returns the schema (field name + kind) the writer records per segment.
func (e *Engine) IndexedFields() []index.FieldType { return e.schema }

// LiteralField reports whether sub is exactly a configured bare literal and, if so, the
// existence-index field it is stored under. Phase 5 uses this to push a LineContains
// whose substring equals a literal into that literal's existence .tidx.
func (e *Engine) LiteralField(sub string) (string, bool) {
	f, ok := e.bareLitField[sub]
	return f, ok
}

// FieldKind reports whether name is a queryable TEMPLATE CAPTURE field (e.g.
// "latency_ms") and its value kind. Literal existence fields are excluded — they are
// not user-facing typed fields. The LogQL translator uses this to decide whether
// `| name op value` is a typed comparison (pushdown) or a label filter.
func (e *Engine) FieldKind(name string) (index.ValueKind, bool) {
	if strings.HasPrefix(name, literalFieldPrefix) {
		return 0, false
	}
	for _, f := range e.schema {
		if f.Name == name {
			return f.Kind, true
		}
	}
	return 0, false
}

// FailureCount returns how many times a template's anchor matched but a capture failed
// to parse — a real signal that a template is mis-specified for the data.
func (e *Engine) FailureCount(template string) int64 {
	if c, ok := e.failures[template]; ok {
		return c.Load()
	}
	return 0
}

type matchStatus uint8

const (
	noMatch   matchStatus = iota // a literal fragment didn't align: not this template here
	parseFail                    // literals aligned but a capture didn't parse: a failure
	matchOK
)

type captureVal struct {
	field string
	value index.Value
}

// FieldValue is one extracted, typed value (before key encoding) and the queryable
// field it belongs to. The scan/re-verify path (Phase 4/5) compares these EXACTLY;
// the index path encodes them into keys. Sharing one extractor means the two paths
// can never disagree about what a record contains.
type FieldValue struct {
	Field string
	Value index.Value
}

// ExtractValues runs the automaton once over message and returns every template
// capture's typed value, plus a zero-valued str entry per matched bare literal
// (existence). This is the shared core of extraction.
func (e *Engine) ExtractValues(message string) []FieldValue {
	hits := e.ac.search(message)
	if len(hits) == 0 {
		return nil
	}
	posByLit := make(map[string][]int, len(hits))
	for _, h := range hits {
		lit := e.patternStrings[h.pattern]
		posByLit[lit] = append(posByLit[lit], h.start)
	}

	var out []FieldValue
	for ti := range e.templates {
		t := &e.templates[ti]
		for _, start := range posByLit[t.firstLiteral] {
			vals, status := e.matchAt(message, start, t)
			switch status {
			case matchOK:
				for _, cv := range vals {
					out = append(out, FieldValue{Field: cv.field, Value: cv.value})
				}
			case parseFail:
				e.failures[t.name].Add(1)
			}
		}
	}
	for _, lit := range e.bareLiterals {
		if len(posByLit[lit]) > 0 {
			out = append(out, FieldValue{Field: e.bareLitField[lit], Value: index.Value{Kind: index.KindStr}})
		}
	}
	return out
}

// Extract returns the encoded (field, key) pairs to index for a record, de-duplicated
// on (field, key) so the same value indexed twice in one line produces one entry.
func (e *Engine) Extract(message string) []index.KeyedValue {
	vals := e.ExtractValues(message)
	if len(vals) == 0 {
		return nil
	}
	out := make([]index.KeyedValue, 0, len(vals))
	seen := map[dedupKey]struct{}{}
	for _, fv := range vals {
		key := index.EncodeKey(fv.Value)
		dk := dedupKey{field: fv.Field, key: key}
		if _, ok := seen[dk]; ok {
			continue
		}
		seen[dk] = struct{}{}
		out = append(out, index.KeyedValue{Field: fv.Field, Key: key})
	}
	return out
}

type dedupKey struct {
	field string
	key   [index.KeySize]byte
}

// matchAt attempts to match template t against message starting at start.
func (e *Engine) matchAt(message string, start int, t *template) ([]captureVal, matchStatus) {
	cursor := start
	var vals []captureVal
	for i, f := range t.fragments {
		if !f.isCapture {
			if !hasPrefixAt(message, cursor, f.literal) {
				return nil, noMatch
			}
			cursor += len(f.literal)
			continue
		}
		nextLit := ""
		if i+1 < len(t.fragments) { // guaranteed a literal (no adjacent captures)
			nextLit = t.fragments[i+1].literal
		}
		val, consumed, ok := parseCapture(message[cursor:], f.kind, nextLit)
		if !ok {
			return nil, parseFail
		}
		cursor += consumed
		vals = append(vals, captureVal{field: f.field, value: val})
	}
	return vals, matchOK
}

func parseCapture(s string, kind index.ValueKind, nextLit string) (index.Value, int, bool) {
	switch kind {
	case index.KindInt:
		return parseInt(s)
	case index.KindFloat:
		return parseFloat(s)
	case index.KindStr:
		return parseStr(s, nextLit)
	case index.KindUUID:
		return parseUUID(s)
	default:
		return index.Value{}, 0, false
	}
}
