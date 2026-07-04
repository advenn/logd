package extract

import (
	"math"
	"strconv"

	"github.com/advenn/logd/core/index"
)

// Typed capture scanners. Each takes the message text starting at the capture and the
// literal that follows the capture in the template (empty if the capture is the last
// fragment), and returns the parsed value, how many bytes it consumed, and whether it
// succeeded. A failure is not an error: the caller records a per-template failure and
// stores the line unindexed (design §7.2).

const maxStrCapture = 256 // a str capture is bounded; the key encoder then takes ≤16 bytes

// isStrDelimiter reports the bytes that terminate a greedy str capture (design §3.2).
func isStrDelimiter(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '"', ',', '}', ']':
		return true
	default:
		return false
	}
}

// parseInt scans an optional sign ('+' or '-') then decimal digits, bounded to int64.
func parseInt(s string) (index.Value, int, bool) {
	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	start := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == start { // no digits
		return index.Value{}, 0, false
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil { // overflow
		return index.Value{}, 0, false
	}
	return index.Value{Kind: index.KindInt, Int: n}, i, true
}

// parseFloat scans a float64 grammar: optional '-', integer digits, optional
// '.'digits, optional exponent. NaN/±Inf can never be produced (only digit/./e/±/-
// bytes are scanned) but are re-checked and rejected defensively.
func parseFloat(s string) (index.Value, int, bool) {
	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	digitsStart := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == digitsStart { // require at least one integer digit
		return index.Value{}, 0, false
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		expDigits := j
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j > expDigits { // only accept the exponent if it has digits
			i = j
		}
	}
	f, err := strconv.ParseFloat(s[:i], 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return index.Value{}, 0, false
	}
	return index.Value{Kind: index.KindFloat, Float: f}, i, true
}

// parseStr scans a greedy run of non-delimiter bytes, also stopping at the start of the
// following literal fragment so a str capture never swallows its trailing anchor.
func parseStr(s, nextLit string) (index.Value, int, bool) {
	i := 0
	for i < len(s) && i < maxStrCapture {
		if isStrDelimiter(s[i]) {
			break
		}
		if nextLit != "" && hasPrefixAt(s, i, nextLit) {
			break
		}
		i++
	}
	if i == 0 {
		return index.Value{}, 0, false
	}
	return index.Value{Kind: index.KindStr, Str: s[:i]}, i, true
}

// parseUUID scans the canonical 8-4-4-4-12 hex form (36 bytes) into 16 raw bytes.
func parseUUID(s string) (index.Value, int, bool) {
	const uuidLen = 36
	if len(s) < uuidLen {
		return index.Value{}, 0, false
	}
	var out [index.KeySize]byte
	oi := 0
	for pos := 0; pos < uuidLen; {
		if pos == 8 || pos == 13 || pos == 18 || pos == 23 {
			if s[pos] != '-' {
				return index.Value{}, 0, false
			}
			pos++
			continue
		}
		hi, ok1 := hexVal(s[pos])
		lo, ok2 := hexVal(s[pos+1])
		if !ok1 || !ok2 {
			return index.Value{}, 0, false
		}
		out[oi] = hi<<4 | lo
		oi++
		pos += 2
	}
	return index.Value{Kind: index.KindUUID, UUID: out}, uuidLen, true
}

func hexVal(b byte) (byte, bool) {
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

// hasPrefixAt reports whether s[at:] begins with lit, without allocating a substring.
func hasPrefixAt(s string, at int, lit string) bool {
	if at+len(lit) > len(s) {
		return false
	}
	for k := 0; k < len(lit); k++ {
		if s[at+k] != lit[k] {
			return false
		}
	}
	return true
}
