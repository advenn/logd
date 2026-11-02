package extract

import "github.com/advenn/logd/core/index"

// Fragment is one piece of a compiled template as Shapes reports it: a literal run, or a
// typed capture (Field != "") together with the queryable field it fills.
type Fragment struct {
	Literal string          // a literal run, when Field is empty
	Field   string          // a capture's queryable field name, "<template>_<capture>"
	Kind    index.ValueKind // a capture's type
}

// Shape is one template's compiled structure: the fragments matching walks, in order.
type Shape struct {
	Name      string
	Pattern   string
	Fragments []Fragment
}

// Shapes returns every template's compiled structure, in config order. Extraction never
// needs it; it is for a caller that compares a template with an expression in another
// pattern language — compat/loki's structural matcher, which decides whether a LogQL
// `| pattern` is the same thing as a declared template (design §9.3). Reporting the parsed
// fragments, rather than the pattern string, keeps that comparison on the one parse the
// extractor itself uses.
func (e *Engine) Shapes() []Shape {
	out := make([]Shape, 0, len(e.templates))
	for i := range e.templates {
		t := &e.templates[i]
		s := Shape{Name: t.name, Pattern: t.pattern, Fragments: make([]Fragment, len(t.fragments))}
		for j, f := range t.fragments {
			if f.isCapture {
				s.Fragments[j] = Fragment{Field: f.field, Kind: f.kind}
			} else {
				s.Fragments[j] = Fragment{Literal: f.literal}
			}
		}
		out = append(out, s)
	}
	return out
}
