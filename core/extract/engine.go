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

	// Per-template extraction counters. All three are needed together: a failure count
	// on its own has no denominator ("50 failures" of how many?). candidates counts
	// attempts in which the template's anchor aligned, matches counts full successes,
	// failures counts anchor-aligned-but-unparseable (see classifyMiss). candidates >
	// matches+failures means the anchor fires but the rest of the pattern doesn't align —
	// a different mis-specification than failures > 0 (which means it aligns but the
	// declared type is wrong). Only records the writer accepted are counted (see Tally).
	candidates map[string]*atomic.Int64
	matches    map[string]*atomic.Int64
	failures   map[string]*atomic.Int64
}

// Compile validates the config and builds an Engine. It enforces the §7.2 rules:
// unique template/field/literal names, a leading literal + a ≥3-byte anchor per
// template, no adjacent captures, and no two identical template patterns.
func Compile(cfg config.IndexConfig) (*Engine, error) {
	e := &Engine{
		bareLitField:  map[string]string{},
		firstLitTmpls: map[string][]int{},
		candidates:    map[string]*atomic.Int64{},
		matches:       map[string]*atomic.Int64{},
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
		t := template{name: tc.Name, pattern: tc.Pattern, fragments: frags}
		if err := validateTemplate(&t); err != nil {
			return nil, err
		}
		t.firstLiteral = frags[0].literal
		t.anchor = anchorFragment(frags)

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
				e.schema = append(e.schema, index.FieldType{Name: f.field, Kind: f.kind, Pattern: tc.Pattern})
			} else {
				litSet[f.literal] = struct{}{}
			}
		}

		idx := len(e.templates)
		e.templates = append(e.templates, t)
		e.firstLitTmpls[t.firstLiteral] = append(e.firstLitTmpls[t.firstLiteral], idx)
		e.candidates[tc.Name] = new(atomic.Int64)
		e.matches[tc.Name] = new(atomic.Int64)
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
		e.schema = append(e.schema, index.FieldType{Name: field, Kind: index.KindStr, Pattern: lit})
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

// FieldPattern reports the pattern that currently defines an indexed field — the template
// pattern for a capture field, the literal for a literal field — or false if this engine
// does not extract the field at all. The planner compares it with the pattern recorded in a
// segment's schema: an index built from a different pattern cannot answer for this one.
func (e *Engine) FieldPattern(name string) (string, bool) {
	for _, f := range e.schema {
		if f.Name == name {
			return f.Pattern, true
		}
	}
	return "", false
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
// to parse — a real signal that a template is mis-specified for the data. Prefer Stats,
// which returns this alongside the denominators that make it interpretable.
func (e *Engine) FailureCount(template string) int64 {
	if c, ok := e.failures[template]; ok {
		return c.Load()
	}
	return 0
}

// TemplateStat is one template's live extraction accounting. It answers the question the
// index config actually poses — "is this pattern earning its place?" — which a raw
// failure count cannot:
//
//	Candidates == 0             the anchor never appears; the pattern is for other data
//	Matches < Candidates        the anchor fires but the pattern doesn't align after it
//	Failures > 0                it aligns but a capture won't parse as the declared type
//	Matches ≈ Candidates        healthy
type TemplateStat struct {
	Name       string   `json:"name"`
	Pattern    string   `json:"pattern"`
	Fields     []string `json:"fields"`
	Candidates int64    `json:"candidates"`
	Matches    int64    `json:"matches"`
	Failures   int64    `json:"failures"`
}

// Stats snapshots every template's counters. The three counters are read independently,
// so a concurrent Extract can land between them; the numbers are diagnostic, not
// transactional, and the skew is at most one record.
func (e *Engine) Stats() []TemplateStat {
	out := make([]TemplateStat, 0, len(e.templates))
	for i := range e.templates {
		t := &e.templates[i]
		var fields []string
		for _, f := range t.fragments {
			if f.isCapture {
				fields = append(fields, f.field)
			}
		}
		out = append(out, TemplateStat{
			Name:       t.name,
			Pattern:    t.pattern,
			Fields:     fields,
			Candidates: e.candidates[t.name].Load(),
			Matches:    e.matches[t.name].Load(),
			Failures:   e.failures[t.name].Load(),
		})
	}
	return out
}

// Literals returns the configured bare existence-indexed literals, for config reporting.
func (e *Engine) Literals() []string {
	out := make([]string, len(e.bareLiterals))
	copy(out, e.bareLiterals)
	return out
}

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
// It does NOT update the per-template counters. This method is on the query path too
// (the scan/re-verify path re-extracts every candidate record to compare exact values),
// and counting there would make the stats meaningless: they would report how many times
// the matcher ran, which is dominated by query volume, rather than how much of the
// INGESTED data each template actually matched — the only question the index config poses.
// Extract and ExtractTally, the ingest entry points, are where counting happens.
func (e *Engine) ExtractValues(message string) []FieldValue {
	return e.extractValues(message, nil)
}

// extractValues is the shared core. With a non-nil tally it also classifies each attempt
// for the counters; that changes nothing about what is extracted.
func (e *Engine) extractValues(message string, tally *Tally) []FieldValue {
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
		c := templateTally{tmpl: ti}
		for _, start := range posByLit[t.firstLiteral] {
			vals, ok := e.matchAt(message, start, t)
			if ok {
				for _, cv := range vals {
					out = append(out, FieldValue{Field: cv.field, Value: cv.value})
				}
			}
			if tally == nil {
				continue
			}
			if ok {
				c.candidates++
				c.matches++
				continue
			}
			anchored, failed := t.classifyMiss(message, start)
			if anchored {
				c.candidates++
			}
			if failed {
				c.failures++
			}
		}
		if tally != nil && c.candidates > 0 {
			tally.add(c)
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
//
// It updates the per-template counters at once: one increment per record, independent of
// how often that record is later read. A caller that may still reject the record uses
// ExtractTally instead.
func (e *Engine) Extract(message string) []index.KeyedValue {
	keys, tally := e.ExtractTally(message)
	e.AddTally(tally)
	return keys
}

// ExtractTally returns what Extract returns, plus the record's contribution to the
// per-template counters, which it does NOT apply. The ingest path applies it with AddTally
// once the writer has accepted the record. Extract counted before the writer had seen it,
// so a record rejected as too large for a page (a 400 to the pushing client) still showed
// up in /logd/api/v1/index_stats as matched data.
func (e *Engine) ExtractTally(message string) ([]index.KeyedValue, Tally) {
	var tally Tally
	keys := encodeKeys(e.extractValues(message, &tally))
	return keys, tally
}

// AddTally applies a record's contribution from ExtractTally to the counters. Call it once
// per accepted record.
func (e *Engine) AddTally(t Tally) {
	for i := 0; i < t.n; i++ {
		e.addTemplateTally(t.inline[i])
	}
	for _, c := range t.more {
		e.addTemplateTally(c)
	}
}

func (e *Engine) addTemplateTally(c templateTally) {
	name := e.templates[c.tmpl].name
	e.candidates[name].Add(c.candidates)
	e.matches[name].Add(c.matches)
	e.failures[name].Add(c.failures)
}

// Tally is one record's contribution to the per-template counters, as ExtractTally returns
// it. Room for a few templates is inline, so a record costs no allocation on the ingest
// path unless it hits more templates than that.
type Tally struct {
	n      int
	inline [4]templateTally
	more   []templateTally
}

type templateTally struct {
	tmpl                          int // index into Engine.templates
	candidates, matches, failures int64
}

func (t *Tally) add(c templateTally) {
	if t.n < len(t.inline) {
		t.inline[t.n] = c
		t.n++
		return
	}
	t.more = append(t.more, c)
}

// ReExtract returns exactly what Extract returns but leaves the per-template counters
// untouched. It is for re-deriving keys from records that were already ingested — crash
// recovery rebuilding a segment's index — which Extract would count a second time, inflating
// the candidates/matches/failures that /logd/api/v1/index_stats reports.
func (e *Engine) ReExtract(message string) []index.KeyedValue {
	return encodeKeys(e.extractValues(message, nil))
}

// encodeKeys turns extracted values into index keys, de-duplicated on (field, key).
func encodeKeys(vals []FieldValue) []index.KeyedValue {
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

// matchAt attempts to match template t against message starting at start. How a miss is
// counted is not decided here but by classifyMiss.
func (e *Engine) matchAt(message string, start int, t *template) ([]captureVal, bool) {
	cursor := start
	var vals []captureVal
	for i, f := range t.fragments {
		if !f.isCapture {
			if !hasPrefixAt(message, cursor, f.literal) {
				return nil, false
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
			return nil, false
		}
		cursor += consumed
		vals = append(vals, captureVal{field: f.field, value: val})
	}
	return vals, true
}

// classifyMiss decides how the counters see an attempt at start that matchAt did not match.
// It walks the template again, but where a capture stops the strict match it looks for the
// following literal further along the same token (realign) instead of giving up:
//
//   - anchored: the walk reached the template's anchor — its first literal of at least 3
//     bytes — and aligned it. Only such an attempt is a candidate. Matching has to start at
//     the first literal, but a short first literal ("a" in "a{x:int} took") occurs
//     everywhere, and counting each occurrence made every "a" of "banana" a candidate and a
//     failure. When the first literal is itself the anchor, every attempt is a candidate,
//     as before.
//   - failed: anchored, and a capture's text is not a value of its type. Either it does not
//     parse, or it parses only partway and the following literal aligns later in the same
//     token: "12.5" under {ms:int} in "took 12.5ms", which matchAt sees as "ms" failing to
//     align after "12". A complete value followed by something else ("took 12 seconds") is
//     the pattern not aligning — a candidate, but not a failure.
//
// It runs only when counting and only for misses, so it changes what the counters report
// and nothing that is extracted.
func (t *template) classifyMiss(message string, start int) (anchored, failed bool) {
	cursor := start
	for i, f := range t.fragments {
		if !f.isCapture {
			if !hasPrefixAt(message, cursor, f.literal) {
				return anchored, false
			}
			cursor += len(f.literal)
			if i == t.anchor {
				anchored = true
				if failed { // a capture before the anchor did not parse, yet the template aligned around it
					return true, true
				}
			}
			continue
		}
		nextLit := ""
		if i+1 < len(t.fragments) {
			nextLit = t.fragments[i+1].literal
		}
		_, consumed, ok := parseCapture(message[cursor:], f.kind, nextLit)
		if ok && (nextLit == "" || hasPrefixAt(message, cursor+consumed, nextLit)) {
			cursor += consumed
			continue
		}
		if anchored && !ok {
			return true, true // the anchor aligned and the value does not parse
		}
		if nextLit == "" {
			return anchored, false
		}
		// The value parsed only partway, or (before the anchor) not at all: it is a failure
		// only if the template realigns on the following literal within the same token.
		j, found := realign(message, cursor, cursor+consumed, nextLit)
		if !found {
			return anchored, false
		}
		if anchored {
			return true, true
		}
		failed, cursor = true, j
	}
	return anchored, failed
}

// realign reports where lit next aligns within the token a capture starts at capStart:
// at or after from, before the next str delimiter, and within maxStrCapture bytes. Alignment
// is checked before the delimiter so a literal that starts with one (" ms") still realigns.
func realign(message string, capStart, from int, lit string) (int, bool) {
	for j := from; j <= len(message) && j-capStart <= maxStrCapture; j++ {
		if hasPrefixAt(message, j, lit) {
			return j, true
		}
		if j == len(message) || isStrDelimiter(message[j]) {
			return 0, false
		}
	}
	return 0, false
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
