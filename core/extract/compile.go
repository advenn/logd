// Package extract turns index config into a running extraction engine: it compiles
// each template pattern into literal fragments and typed captures, builds one
// Aho-Corasick automaton over all literals to find candidate positions in a message
// in a single pass, and runs hand-written typed byte-scanners at those positions to
// pull out values. Its output is []index.KeyedValue (field name + order-preserving
// key) handed to the writer. It imports core/index (for value/key types) and
// core/config; it is never imported by core/storage (extraction runs before the
// writer, design §7.4).
package extract

import (
	"fmt"
	"strings"

	"github.com/advenn/logd/core/index"
)

const anchorMinLen = 3 // every template needs a literal fragment at least this long (design §7.2)

// fragment is one piece of a compiled pattern: either a literal run or a typed capture.
type fragment struct {
	isCapture bool
	literal   string          // when !isCapture
	field     string          // when isCapture: queryable field = "<template>_<capture>"
	capName   string          // when isCapture
	kind      index.ValueKind // when isCapture
}

// template is a compiled pattern. firstLiteral is fragments[0].literal, which validation
// guarantees is a literal — its occurrences in a message are the candidate start
// positions for matching.
type template struct {
	name         string
	pattern      string // the raw configured pattern, kept only so Stats can report it
	fragments    []fragment
	firstLiteral string
}

// numericExtender reports whether byte b, appearing as the first byte of the literal
// after a numeric capture, could be greedily consumed by that kind's scanner.
func numericExtender(b byte, kind index.ValueKind) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	if kind == index.KindFloat && (b == '.' || b == 'e' || b == 'E') {
		return true
	}
	return false
}

// kindFromToken maps a pattern type token to a value kind.
func kindFromToken(tok string) (index.ValueKind, bool) {
	switch tok {
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

// parsePattern splits a pattern like "took {ms:int}ms" into fragments.
func parsePattern(name, pattern string) ([]fragment, error) {
	var frags []fragment
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			frags = append(frags, fragment{literal: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(pattern); {
		if pattern[i] == '{' {
			flush()
			rel := strings.IndexByte(pattern[i:], '}')
			if rel < 0 {
				return nil, fmt.Errorf("template %q: unclosed '{' in pattern", name)
			}
			inner := pattern[i+1 : i+rel] // "name:type"
			colon := strings.IndexByte(inner, ':')
			if colon <= 0 || colon == len(inner)-1 {
				return nil, fmt.Errorf("template %q: capture must be {name:type}, got {%s}", name, inner)
			}
			capName, tok := inner[:colon], inner[colon+1:]
			kind, ok := kindFromToken(tok)
			if !ok {
				return nil, fmt.Errorf("template %q: unknown capture type %q (want int|float|str|uuid)", name, tok)
			}
			frags = append(frags, fragment{isCapture: true, capName: capName, kind: kind, field: name + "_" + capName})
			i += rel + 1
			continue
		}
		lit.WriteByte(pattern[i])
		i++
	}
	flush()
	return frags, nil
}

// validateTemplate enforces the §7.2 structural rules on a compiled template.
func validateTemplate(t *template) error {
	if len(t.fragments) == 0 {
		return fmt.Errorf("template %q: empty pattern", t.name)
	}
	// Must start with a literal so its occurrences give candidate start positions.
	if t.fragments[0].isCapture {
		return fmt.Errorf("template %q: pattern must start with a literal fragment (v1 limitation)", t.name)
	}
	// No adjacent captures — ambiguous without a separator (design §7.2).
	for i := 1; i < len(t.fragments); i++ {
		if t.fragments[i].isCapture && t.fragments[i-1].isCapture {
			return fmt.Errorf("template %q: adjacent captures need a literal separator", t.name)
		}
	}
	// A numeric capture must not be immediately followed by a literal beginning with a
	// byte the number scanner would greedily consume (a digit for int; also '.', 'e',
	// 'E' for float). Otherwise the scanner swallows the anchor's first byte, the
	// following literal fails to align, and the match is silently lost. Reject the
	// ambiguity at config load rather than drop values at runtime.
	for i, f := range t.fragments {
		if !f.isCapture || (f.kind != index.KindInt && f.kind != index.KindFloat) {
			continue
		}
		if i+1 < len(t.fragments) {
			next := t.fragments[i+1] // a literal (no adjacent captures)
			if len(next.literal) > 0 && numericExtender(next.literal[0], f.kind) {
				return fmt.Errorf("template %q: numeric capture %q is followed by a literal starting with %q, which is ambiguous", t.name, f.capName, string(next.literal[0]))
			}
		}
	}
	// At least one literal fragment >= anchorMinLen (the Aho-Corasick anchor).
	hasAnchor := false
	capNames := map[string]bool{}
	for _, f := range t.fragments {
		if f.isCapture {
			if capNames[f.capName] {
				return fmt.Errorf("template %q: duplicate capture name %q", t.name, f.capName)
			}
			capNames[f.capName] = true
			continue
		}
		if len(f.literal) >= anchorMinLen {
			hasAnchor = true
		}
	}
	if !hasAnchor {
		return fmt.Errorf("template %q: needs a literal fragment of at least %d bytes as an anchor", t.name, anchorMinLen)
	}
	return nil
}

// normalizedShape returns a canonical string for overlap detection: literal fragments
// verbatim plus a placeholder per capture type. Two templates with the same shape are
// "identical" (design §7.2 overlap — identical patterns rejected; subsumption is a
// future refinement).
func normalizedShape(t *template) string {
	var b strings.Builder
	for _, f := range t.fragments {
		if f.isCapture {
			fmt.Fprintf(&b, "\x00cap:%d\x00", f.kind)
		} else {
			b.WriteString(f.literal)
		}
	}
	return b.String()
}
