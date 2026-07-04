// Package query is logd's native, protocol-agnostic query engine. A Query is a time
// window plus an AND-combined set of predicates; results are the matching records,
// time-ordered and limited. This package implements the SCAN PATH (design §13 Phase 4):
// every predicate is evaluated on the fully decoded record using exact values — no
// index, no lossy keys. It is the correctness oracle the Phase-5 index pushdown must
// reproduce exactly. Live tail (§11) is the same predicates applied to records as they
// are written, via the writer's subscriber fan-out.
package query

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
)

// Direction controls result ordering. Backward (newest-first) is the default, matching
// Loki/Grafana's usual log view.
type Direction uint8

const (
	Backward Direction = 0
	Forward  Direction = 1
)

// Query is a native query: a time window, predicates (ANDed), a limit, and a direction.
type Query struct {
	Start, End int64 // event-time window [Start, End] in unix nanos; End<=0 means open-ended
	Preds      []Predicate
	Limit      int // 0 → default 100
	Direction  Direction
}

// Resolver gives predicates the two things they need beyond the raw record: label
// resolution (level/service/Extra) and typed field extraction. The engine supplies a
// concrete implementation so predicates stay pure and testable.
type Resolver interface {
	// Label resolves a label key to its value on this record; ok is false if absent.
	Label(e model.LogEntry, key string) (value string, ok bool)
	// FieldValues returns every typed value extracted for a template field on this
	// record (empty if the field doesn't occur).
	FieldValues(e model.LogEntry, field string) []index.Value
	// AllLabels returns the record's full label set (Extra labels plus the first-class
	// level/service), for metric-query grouping.
	AllLabels(e model.LogEntry) map[string]string
}

// Predicate is one test applied to a decoded record.
type Predicate interface {
	Match(e model.LogEntry, r Resolver) bool
}

func matchAll(preds []Predicate, e model.LogEntry, r Resolver) bool {
	for _, p := range preds {
		if !p.Match(e, r) {
			return false
		}
	}
	return true
}

// --- line filters (on Message) ---

type LineContains struct{ Sub string }

func (p LineContains) Match(e model.LogEntry, _ Resolver) bool {
	return strings.Contains(e.Message, p.Sub)
}

type LineNotContains struct{ Sub string }

func (p LineNotContains) Match(e model.LogEntry, _ Resolver) bool {
	return !strings.Contains(e.Message, p.Sub)
}

type LineRegex struct{ Re *regexp.Regexp }

func (p LineRegex) Match(e model.LogEntry, _ Resolver) bool {
	return p.Re.MatchString(e.Message)
}

type LineNotRegex struct{ Re *regexp.Regexp }

func (p LineNotRegex) Match(e model.LogEntry, _ Resolver) bool {
	return !p.Re.MatchString(e.Message)
}

// --- label filters ---

// A missing label is treated as the empty string (Loki semantics), so {k="x"} excludes
// records without k, {k=""} matches them, {k!="x"} includes them (for x != ""), and
// {k!=""} excludes them.
func labelOrEmpty(e model.LogEntry, r Resolver, key string) string {
	v, _ := r.Label(e, key)
	return v // r.Label returns "" when absent
}

type LabelEqual struct{ Key, Value string }

func (p LabelEqual) Match(e model.LogEntry, r Resolver) bool {
	return labelOrEmpty(e, r, p.Key) == p.Value
}

type LabelNotEqual struct{ Key, Value string }

func (p LabelNotEqual) Match(e model.LogEntry, r Resolver) bool {
	return labelOrEmpty(e, r, p.Key) != p.Value
}

// LabelRegex matches when the label is present and its value matches the (anchored)
// regex. LabelNotRegex matches when the label is absent or doesn't match (Loki
// empty-label semantics). These are scan-only (no index pushdown).
type LabelRegex struct {
	Key string
	Re  *regexp.Regexp
}

func (p LabelRegex) Match(e model.LogEntry, r Resolver) bool {
	return p.Re.MatchString(labelOrEmpty(e, r, p.Key))
}

type LabelNotRegex struct {
	Key string
	Re  *regexp.Regexp
}

func (p LabelNotRegex) Match(e model.LogEntry, r Resolver) bool {
	return !p.Re.MatchString(labelOrEmpty(e, r, p.Key))
}

// LabelCompare is Loki's NUMERIC label filter (`| status >= 500` on a plain label):
// both the label value and the bound must parse as numbers, else the record does NOT
// match (a non-numeric label value is excluded, matching Loki — never a lexicographic
// fallback). Scan-only.
type LabelCompare struct {
	Key   string
	Op    Op // one of OpLt/OpLe/OpGt/OpGe
	Value string
}

func (p LabelCompare) Match(e model.LogEntry, r Resolver) bool {
	v, ok := r.Label(e, p.Key)
	if !ok {
		return false
	}
	fv, ev := strconv.ParseFloat(v, 64)
	fb, eb := strconv.ParseFloat(p.Value, 64)
	if ev != nil || eb != nil {
		return false // numeric comparison requires both sides numeric
	}
	switch {
	case fv < fb:
		return satisfies(-1, p.Op)
	case fv > fb:
		return satisfies(1, p.Op)
	default:
		return satisfies(0, p.Op)
	}
}

// --- typed value comparison (re-extract from Message, compare exactly) ---

// Op is a comparison operator for TypedCompare.
type Op uint8

const (
	OpEq Op = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

// TypedCompare matches records where the named template field has (at least one)
// occurrence satisfying "value <Op> Value". A record lacking the field satisfies only
// OpNe (empty-label semantics), matching the §9 re-verify behavior.
type TypedCompare struct {
	Field string
	Op    Op
	Value index.Value
}

func (p TypedCompare) Match(e model.LogEntry, r Resolver) bool {
	vals := r.FieldValues(e, p.Field)
	if len(vals) == 0 {
		return p.Op == OpNe
	}
	for _, v := range vals {
		if satisfies(index.Compare(v, p.Value), p.Op) {
			return true // OR over occurrences: any qualifying value matches the record
		}
	}
	return false
}

func satisfies(cmp int, op Op) bool {
	switch op {
	case OpEq:
		return cmp == 0
	case OpNe:
		return cmp != 0
	case OpLt:
		return cmp < 0
	case OpLe:
		return cmp <= 0
	case OpGt:
		return cmp > 0
	case OpGe:
		return cmp >= 0
	default:
		return false
	}
}
