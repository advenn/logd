package logql

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
)

// The structural matcher of design §9.3: when is a LogQL `| pattern` the same thing as a
// declared template, so that a filter on one of its captures may become the template's
// TypedCompare — typed, and answered from the index?
//
// RULE. Normalize both sides to (literal fragments, capture names). The pattern must start
// with <_>, which is removed; a trailing <_> is removed as well. What remains must be the
// template's fragments exactly: the literals byte-identical, a capture wherever the template
// has one, and each capture named as the template's queryable field — "<template>_<capture>",
// the name the native syntax (`| latency_ms > 200`) and the results already use. Types come
// from the config; the query carries none. Two further conditions keep the argument below
// true: the template must end with a literal, and the literal after an int, float or uuid
// capture must not start with a byte such a value can contain at a place the literal could
// begin ('+' or '-'; for uuid also a hex digit). Any deviation — whitespace, an extra or
// missing fragment, a renamed capture — is no match, and the query scans with Loki's
// semantics: a false match would answer with the wrong index, a false no-match only costs
// a scan.
//
// WHY THE ENDS. Let T = l0 c1 l1 … ck lk be a template (every li a non-empty literal) and
// P = <_> l0 <n1> l1 … <nk> lk [<_>] the pattern, ni the field of ci. Loki
// (pattern.go, Matcher.Matches; ported in patternStage.each) anchors a pattern at the start
// of the line only when it starts with a literal, so the leading <_> is what makes P, like a
// template, look anywhere in the line — without it, P matches only lines that begin with
// l0, which no template means. Loki never anchors the end: after lk the loop simply stops,
// and a trailing <_> is a final unnamed capture that also stops it. P with and without the
// trailing <_> therefore extracts the same labels from every line, and removing it is exact.
// A template that ends with a capture has no such equivalent: Loki's last capture runs to
// the end of the line, a template's stops where its typed value does.
//
// WHERE THEY AGREE. Loki starts at the FIRST occurrence s of l0; each vi (Loki's value of ni)
// is the text from the cursor to the first occurrence of li after it. logd tries T at EVERY
// occurrence of l0: each ci's typed scan must succeed and li must follow it immediately, and
// a TypedCompare holds when SOME occurrence's value satisfies it. Call a line conforming when
// either l0 does not occur in it, or T matches at s and at no later occurrence. On a
// conforming line every filter the matcher maps (mappableFilter: =, <, <=, >, >= with a
// number on an int or float field; = with a non-empty string on a str field) gives the same
// answer in both:
//
//   - l0 absent: Loki extracts nothing, and a numeric filter or `= "x"` on a missing label
//     fails; the template has no value, and TypedCompare fails for those operators too.
//   - T matches at s only: logd's one value comes from the text ti its scanner consumed
//     for ci. Loki's vi ends at the first occurrence of li after the cursor, which is right
//     after ti, because li cannot begin earlier inside ti: a str scan stops at the first
//     occurrence of the next literal (parseStr), and an int, float or uuid text cannot hold
//     the first byte of li where an occurrence could start (the config rejects a digit, and
//     '.', 'e', 'E' after a float; this matcher rejects the rest). By induction over i the
//     cursors coincide and vi = ti. Then for int, Loki compares float64(ti) with float64(b)
//     and logd int64(ti) with int64(b), which order alike while both are within ±2^53
//     (float64 is exact there); for float both parse the same text with strconv.ParseFloat
//     and compare the same float64s (extraction rejects NaN and ±Inf, translation a
//     non-finite bound); for str both compare the same bytes.
//
// WHERE THEY DIFFER, and why the documented rule decides. On a line that does not conform
// the answers can differ, and no rewrite of the query can close the gap, because the index
// holds every occurrence of every conforming value and nothing else:
//
//   - several occurrences ("took=5ms … took=500ms"): Loki compares only the first, 5; the
//     template matches `> 200` through 500.
//   - a first occurrence whose value is not of the declared type ("took=12.5ms" under int,
//     "took=abcms"): Loki converts 12.5 and compares it, and keeps "abc" with
//     __error__=LabelFilterErr; the template has no value, so the line fails.
//   - a literal missing or displaced after the value ("took=7" at the end of the line,
//     "took=5 ms"): Loki's capture runs to the end of the line or the next "ms"; the template
//     does not match.
//   - ints beyond ±2^53, which Loki rounds to float64 and logd compares exactly.
//
// So a structural match uses the TEMPLATE's semantics — the same rows the native
// `| latency_ms > 200` returns — which is the documented rule of design §9.3, and
// TestStructuralMatchDiffersFromLokiOnlyOnNonConformingLines pins exactly these cases. The
// labels a result carries are still Loki's: they are re-extracted with Loki's semantics.

// templateMatch is a pattern that structurally equals a template.
type templateMatch struct {
	shape  extract.Shape
	fields map[string]index.ValueKind // the template's fields, by name
}

// matchTemplate returns the template a pattern equals under the rule above, or nil and why
// not, in words for Explain.
func matchTemplate(nodes []patternNode, shapes []extract.Shape) (*templateMatch, string) {
	if len(shapes) == 0 {
		return nil, "no templates are configured"
	}
	if !nodes[0].capture || nodes[0].text != "_" {
		return nil, "the pattern does not start with <_>, so Loki anchors it at the start of the line; a template matches anywhere in it"
	}
	nodes = nodes[1:]
	if n := len(nodes); n > 0 && nodes[n-1].capture && nodes[n-1].text == "_" {
		nodes = nodes[:n-1]
	}
	if n := len(nodes); n == 0 || nodes[n-1].capture {
		last := "<_>"
		if n > 0 {
			last = "<" + nodes[n-1].text + ">"
		}
		return nil, fmt.Sprintf("the pattern ends with the capture %s, which Loki extends to the end of the line; a template's capture ends where its value does", last)
	}
	for _, n := range nodes {
		if n.capture && n.text == "_" {
			return nil, "the pattern skips text with <_> between its literals, which no template does"
		}
	}
	var literals []string
	for _, n := range nodes {
		if !n.capture {
			literals = append(literals, n.text)
		}
	}
	nearMiss := ""
	for _, sh := range shapes {
		if !sameStructure(nodes, sh.Fragments) {
			continue
		}
		if names, fields := captureNames(nodes), shapeFields(sh); names != fields {
			nearMiss = fmt.Sprintf("template %q has the same literals, but its captures are %s, not %s", sh.Name, fields, names)
			continue
		}
		if reason := literalAfterValue(sh); reason != "" {
			return nil, fmt.Sprintf("template %q: %s", sh.Name, reason)
		}
		m := &templateMatch{shape: sh, fields: map[string]index.ValueKind{}}
		for _, f := range sh.Fragments {
			if f.Field != "" {
				m.fields[f.Field] = f.Kind
			}
		}
		return m, ""
	}
	if nearMiss != "" {
		return nil, nearMiss
	}
	return nil, fmt.Sprintf("no template has exactly the literal fragments %s and captures between them", quoteAll(literals))
}

// sameStructure reports whether normalized pattern nodes and template fragments are the
// same sequence of literals and captures, the literals byte for byte.
func sameStructure(nodes []patternNode, frags []extract.Fragment) bool {
	if len(nodes) != len(frags) {
		return false
	}
	for i, n := range nodes {
		f := frags[i]
		if n.capture != (f.Field != "") {
			return false
		}
		if !n.capture && n.text != f.Literal {
			return false
		}
	}
	return true
}

func captureNames(nodes []patternNode) string {
	var names []string
	for _, n := range nodes {
		if n.capture {
			names = append(names, "<"+n.text+">")
		}
	}
	return strings.Join(names, ", ")
}

func shapeFields(sh extract.Shape) string {
	var names []string
	for _, f := range sh.Fragments {
		if f.Field != "" {
			names = append(names, "<"+f.Field+">")
		}
	}
	return strings.Join(names, ", ")
}

func quoteAll(ss []string) string {
	q := make([]string, len(ss))
	for i, s := range ss {
		q[i] = strconv.Quote(s)
	}
	return strings.Join(q, ", ")
}

// literalAfterValue checks the condition that makes Loki's capture end where the template's
// value does: the literal after an int, float or uuid capture cannot start inside the value.
func literalAfterValue(sh extract.Shape) string {
	for i, f := range sh.Fragments {
		if f.Field == "" || i+1 >= len(sh.Fragments) {
			continue
		}
		c := sh.Fragments[i+1].Literal[0]
		bad := false
		switch f.Kind {
		case index.KindInt, index.KindFloat:
			bad = c == '+' || c == '-'
		case index.KindUUID:
			bad = c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		}
		if bad {
			return fmt.Sprintf("the literal after capture %s starts with %q, which a value of its type can contain, so Loki's capture could end inside the value", f.Field, string(c))
		}
	}
	return ""
}

// mappableFilter reports why a label filter on a field of a matched template cannot be the
// template's TypedCompare, or "" when it can. The cases are those the agreement argument
// above covers; for the others Loki's semantics and the template's differ even on conforming
// lines, so the match is given up rather than answered by a different rule.
func mappableFilter(f *LabelFilter, kind index.ValueKind) string {
	switch f.Op {
	case OpRe, OpNotRe:
		return "a regex is not a typed comparison"
	case OpNotEq:
		return "!= is never answered from an index, and the template's != also matches lines without the field, which Loki's numeric != drops"
	}
	switch kind {
	case index.KindInt:
		if f.Op == OpEq && !f.Numeric {
			return fmt.Sprintf("= %q is a string comparison in Loki, of the captured text", f.Value)
		}
		if _, err := strconv.ParseInt(f.Value, 10, 64); err != nil {
			return fmt.Sprintf("%s is not an int, the type of the template field", f.Value)
		}
	case index.KindFloat:
		if f.Op == OpEq && !f.Numeric {
			return fmt.Sprintf("= %q is a string comparison in Loki, of the captured text", f.Value)
		}
		if v, err := strconv.ParseFloat(f.Value, 64); err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Sprintf("%s is not a finite float, the type of the template field", f.Value)
		}
	case index.KindStr:
		if f.Op != OpEq || f.Numeric {
			return "a str field is compared only with = and a string"
		}
		if f.Value == "" {
			return `= "" matches a line without the label in Loki, and no line without the field in the template`
		}
	default:
		return "a uuid field's comparison differs from Loki's string comparison (case of the hex digits)"
	}
	return ""
}
