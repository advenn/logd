package index

import (
	"bytes"
	"strings"
)

// Compare orders two values of the SAME Kind, returning -1, 0, or +1. Unlike the
// key encoding (which is lossy for long strings so the index can use fixed-width
// keys), Compare works on the full values — the scan path (Phase 4) uses it to
// evaluate typed predicates EXACTLY, which is what makes it the correctness oracle
// the lossy index must re-verify against.
//
// Comparing different Kinds is meaningless (a field is single-typed), but it must
// never return 0 — otherwise a mistyped query value (e.g. an Int predicate against a
// Str field) would spuriously satisfy an equality test. It returns a deterministic
// non-zero ordering by Kind instead, so cross-kind comparisons never read as equal.
func Compare(a, b Value) int {
	if a.Kind != b.Kind {
		switch {
		case a.Kind < b.Kind:
			return -1
		default:
			return 1
		}
	}
	switch a.Kind {
	case KindInt:
		switch {
		case a.Int < b.Int:
			return -1
		case a.Int > b.Int:
			return 1
		default:
			return 0
		}
	case KindFloat:
		switch {
		case a.Float < b.Float:
			return -1
		case a.Float > b.Float:
			return 1
		default:
			return 0
		}
	case KindStr:
		return strings.Compare(a.Str, b.Str)
	case KindUUID:
		return bytes.Compare(a.UUID[:], b.UUID[:])
	default:
		return 0
	}
}
