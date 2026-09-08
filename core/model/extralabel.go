package model

import (
	"encoding/json"
	"unicode/utf8"
)

// A hand-written scanner for a single label out of the Extra blob.
//
// This exists because the obvious implementations are not faster. Measured on a
// representative 6-key blob (BenchmarkParseExtraLabels and friends):
//
//	ParseExtraLabels + map index   3295 ns/op   33 allocs   (the status quo)
//	json.Decoder.Token() walk      5582 ns/op   86 allocs   (2x SLOWER — Token allocates)
//	map[string]json.RawMessage     3033 ns/op   28 allocs   (8% — not worth the code)
//	this scanner                   see BenchmarkExtraLabel
//
// encoding/json's reflection-based map decode is the cost, so avoiding it entirely is the
// only route to a real win — and label resolution is 28.7% of query CPU.
//
// The contract is strict: ExtraLabel(extra, key) must equal ParseExtraLabels(extra)[key]
// for EVERY input, because the label index is built with the latter at ingest and queries
// resolve with the former. A divergence would make the index and the scan disagree about a
// record, which is the one failure the Extra format's doc comment warns about. That
// equivalence is enforced by a differential table test and a fuzz target
// (extra_test.go) — do not modify this file without running both.
//
// Two consequences of the equivalence that drive the design:
//
//   - ParseExtraLabels returns nil for ANY malformed input, so this scanner must validate
//     the WHOLE object, not just stop once it finds the key. A blob that is valid up to the
//     target key but garbage afterwards must report nothing.
//   - Duplicate keys resolve last-wins (a map assignment), so the scan cannot return early
//     on the first match.
//
// Trailing bytes after the closing brace are accepted, matching json.Decoder.Decode, which
// reads one value and ignores the rest.

// ExtraLabel returns the value of a single label from the Extra JSON blob without building
// the whole map. It is observationally identical to ParseExtraLabels(extra)[key].
func ExtraLabel(extra, key string) (string, bool) {
	if extra == "" {
		return "", false
	}
	i := skipWS(extra, 0)
	if i >= len(extra) || extra[i] != '{' {
		return "", false // only a top-level object carries labels
	}
	i++

	var val string
	var found bool

	i = skipWS(extra, i)
	if i < len(extra) && extra[i] == '}' {
		return "", false // empty object
	}

	for {
		// key
		i = skipWS(extra, i)
		if i >= len(extra) || extra[i] != '"' {
			return "", false
		}
		kStart := i
		kEnd, ok := scanString(extra, i)
		if !ok {
			return "", false
		}
		i = kEnd

		// colon
		i = skipWS(extra, i)
		if i >= len(extra) || extra[i] != ':' {
			return "", false
		}
		i++

		// value
		i = skipWS(extra, i)
		vStart := i
		vEnd, kind, ok := scanValue(extra, i)
		if !ok {
			return "", false
		}
		i = vEnd

		if keyMatches(extra[kStart:kEnd], key) {
			// Last occurrence wins, exactly as assigning into a map does — including the
			// case where a later non-scalar erases an earlier scalar.
			switch kind {
			case valString:
				s, ok := unquote(extra[vStart:vEnd])
				if !ok {
					return "", false
				}
				val, found = s, true
			case valNumber:
				val, found = extra[vStart:vEnd], true // raw text == json.Number.String()
			case valTrue:
				val, found = "true", true
			case valFalse:
				val, found = "false", true
			default: // null, object, array — omitted by scalarString
				val, found = "", false
			}
		}

		// separator
		i = skipWS(extra, i)
		if i >= len(extra) {
			return "", false
		}
		if extra[i] == ',' {
			i++
			continue
		}
		if extra[i] == '}' {
			break
		}
		return "", false
	}
	return val, found
}

type valKind uint8

const (
	valString valKind = iota
	valNumber
	valTrue
	valFalse
	valNull
	valObject
	valArray
)

func skipWS(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// keyMatches compares a raw (still-quoted, still-escaped) key against a plain key.
//
// The direct slice compare needs the same two guards as unquote: no escapes, and valid
// UTF-8. encoding/json rewrites invalid UTF-8 to U+FFFD when decoding a key, so a raw
// compare would match a key the map path stores under a different name — found by the fuzz
// target on `{"\xf2":""}`.
func keyMatches(raw string, key string) bool {
	if len(raw) < 2 {
		return false
	}
	inner := raw[1 : len(raw)-1]
	if !hasEscape(inner) && utf8.ValidString(inner) {
		return inner == key
	}
	s, ok := unquote(raw)
	return ok && s == key
}

func hasEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			return true
		}
	}
	return false
}

// unquote decodes a raw JSON string token (including its surrounding quotes).
//
// The fast path returns the inner bytes directly when there is nothing to decode. It
// requires BOTH no escapes and valid UTF-8: encoding/json silently replaces invalid UTF-8
// with U+FFFD when decoding into a string, so handing back the raw slice would diverge from
// the map path on exactly those inputs. (The fuzz target finds this if the check is
// dropped.) Otherwise fall back to encoding/json so escapes, surrogate pairs and
// replacement behave identically by construction.
func unquote(raw string) (string, bool) {
	if len(raw) >= 2 {
		if inner := raw[1 : len(raw)-1]; !hasEscape(inner) && utf8.ValidString(inner) {
			return inner, true
		}
	}
	var s string
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return "", false
	}
	return s, true
}

// scanString returns the index just past the closing quote of the string starting at i
// (which must be the opening quote), validating escapes as it goes.
func scanString(s string, i int) (int, bool) {
	i++ // opening quote
	for i < len(s) {
		c := s[i]
		switch {
		case c == '"':
			return i + 1, true
		case c == '\\':
			i++
			if i >= len(s) {
				return 0, false
			}
			switch s[i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i++
			case 'u':
				if i+4 >= len(s) {
					return 0, false
				}
				for j := 1; j <= 4; j++ {
					if !isHex(s[i+j]) {
						return 0, false
					}
				}
				i += 5
			default:
				return 0, false
			}
		case c < 0x20:
			return 0, false // unescaped control character
		default:
			i++
		}
	}
	return 0, false
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// scanValue returns the index just past the value starting at i, and its kind.
func scanValue(s string, i int) (int, valKind, bool) { return scanValueDepth(s, i, 0) }

func scanValueDepth(s string, i, depth int) (int, valKind, bool) {
	if i >= len(s) {
		return 0, 0, false
	}
	switch c := s[i]; {
	case c == '"':
		end, ok := scanString(s, i)
		return end, valString, ok
	case c == '{' || c == '[':
		end, ok := scanComposite(s, i, depth)
		k := valObject
		if c == '[' {
			k = valArray
		}
		return end, k, ok
	case c == 't':
		if len(s)-i >= 4 && s[i:i+4] == "true" {
			return i + 4, valTrue, true
		}
		return 0, 0, false
	case c == 'f':
		if len(s)-i >= 5 && s[i:i+5] == "false" {
			return i + 5, valFalse, true
		}
		return 0, 0, false
	case c == 'n':
		if len(s)-i >= 4 && s[i:i+4] == "null" {
			return i + 4, valNull, true
		}
		return 0, 0, false
	default:
		end, ok := scanNumber(s, i)
		return end, valNumber, ok
	}
}

// scanNumber validates the JSON number grammar strictly — leading zeros and lone
// signs are errors, and rejecting them is what keeps malformed blobs returning nothing
// here just as they do through the map path.
func scanNumber(s string, i int) (int, bool) {
	start := i
	if i < len(s) && s[i] == '-' {
		i++
	}
	// int part
	if i >= len(s) {
		return 0, false
	}
	if s[i] == '0' {
		i++
	} else if s[i] >= '1' && s[i] <= '9' {
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	} else {
		return 0, false
	}
	// frac
	if i < len(s) && s[i] == '.' {
		i++
		if i >= len(s) || s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	// exp
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if i >= len(s) || s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if i == start {
		return 0, false
	}
	return i, true
}

// maxNestingDepth mirrors encoding/json's limit, so a deeply nested blob is rejected by
// both paths rather than overflowing this scanner's stack.
const maxNestingDepth = 10000

// scanComposite validates and skips a nested object or array starting at its opening
// delimiter.
//
// This must be a REAL recursive-descent validator, not a brace counter. A brace counter
// accepts `{""}` — an object with a key and no value — which is invalid JSON that the map
// path rejects outright, so the two would disagree on any blob containing it. The fuzz
// target found exactly that within a second of running.
func scanComposite(s string, i, depth int) (int, bool) {
	if depth > maxNestingDepth {
		return 0, false
	}
	switch s[i] {
	case '{':
		i++
		i = skipWS(s, i)
		if i < len(s) && s[i] == '}' {
			return i + 1, true
		}
		for {
			i = skipWS(s, i)
			if i >= len(s) || s[i] != '"' {
				return 0, false
			}
			end, ok := scanString(s, i)
			if !ok {
				return 0, false
			}
			i = skipWS(s, end)
			if i >= len(s) || s[i] != ':' {
				return 0, false
			}
			i = skipWS(s, i+1)
			end, _, ok = scanValueDepth(s, i, depth+1)
			if !ok {
				return 0, false
			}
			i = skipWS(s, end)
			if i >= len(s) {
				return 0, false
			}
			if s[i] == ',' {
				i++
				continue
			}
			if s[i] == '}' {
				return i + 1, true
			}
			return 0, false
		}
	case '[':
		i++
		i = skipWS(s, i)
		if i < len(s) && s[i] == ']' {
			return i + 1, true
		}
		for {
			i = skipWS(s, i)
			end, _, ok := scanValueDepth(s, i, depth+1)
			if !ok {
				return 0, false
			}
			i = skipWS(s, end)
			if i >= len(s) {
				return 0, false
			}
			if s[i] == ',' {
				i++
				continue
			}
			if s[i] == ']' {
				return i + 1, true
			}
			return 0, false
		}
	}
	return 0, false
}
