package index

import (
	"bytes"
	"math"
	"math/rand"
	"strings"
	"testing"
)

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}

func intKey(i int64) [KeySize]byte   { return EncodeKey(Value{Kind: KindInt, Int: i}) }
func floatKey(f float64) [KeySize]byte { return EncodeKey(Value{Kind: KindFloat, Float: f}) }
func strKey(s string) [KeySize]byte  { return EncodeKey(Value{Kind: KindStr, Str: s}) }

// The core property: for two values of a kind, the byte-order of their keys matches
// the values' natural order.

func TestEncodeIntOrderPreserving(t *testing.T) {
	edges := []int64{math.MinInt64, math.MinInt64 + 1, -1 << 40, -1, 0, 1, 1 << 40, math.MaxInt64 - 1, math.MaxInt64}
	r := rand.New(rand.NewSource(1))
	var vals []int64
	vals = append(vals, edges...)
	for i := 0; i < 2000; i++ {
		vals = append(vals, int64(r.Uint64()))
	}
	for _, a := range vals {
		for _, b := range vals {
			ka, kb := intKey(a), intKey(b)
			want := sign(cmpInt64(a, b))
			got := sign(bytes.Compare(ka[:], kb[:]))
			if got != want {
				t.Fatalf("int order: a=%d b=%d key cmp=%d want %d", a, b, got, want)
			}
		}
	}
}

func TestEncodeFloatOrderPreserving(t *testing.T) {
	edges := []float64{
		math.Inf(-1) + 0, -math.MaxFloat64, -1e300, -1.5, -math.SmallestNonzeroFloat64,
		0, math.SmallestNonzeroFloat64, 1.5, 1e300, math.MaxFloat64,
	}
	// Drop the -Inf sentinel (extraction rejects it); keep finite edges only.
	edges = edges[1:]
	r := rand.New(rand.NewSource(2))
	vals := append([]float64{}, edges...)
	for i := 0; i < 2000; i++ {
		f := math.Float64frombits(r.Uint64())
		if math.IsNaN(f) || math.IsInf(f, 0) {
			continue // never indexed
		}
		vals = append(vals, f)
	}
	for _, a := range vals {
		for _, b := range vals {
			ka, kb := floatKey(a), floatKey(b)
			want := sign(cmpFloat64(a, b))
			got := sign(bytes.Compare(ka[:], kb[:]))
			if got != want {
				t.Fatalf("float order: a=%v b=%v key cmp=%d want %d", a, b, got, want)
			}
		}
	}
}

func TestEncodeFloatNegZeroEqualsPosZero(t *testing.T) {
	pos := floatKey(0.0)
	neg := floatKey(math.Copysign(0, -1))
	if pos != neg {
		t.Fatalf("−0.0 and +0.0 must encode identically: +0=%x −0=%x", pos, neg)
	}
}

func TestEncodeStrOrderPreservingShort(t *testing.T) {
	// For strings that fit in 16 bytes (and contain no 0x00, the pad byte), byte
	// order of keys must match string order exactly.
	words := []string{"", "a", "ab", "abc", "abd", "b", "order-1", "order-10", "order-2", "z", "took", "took", "日本", "日本語"}
	for _, a := range words {
		for _, b := range words {
			if len(a) > KeySize || len(b) > KeySize {
				continue
			}
			ka, kb := strKey(a), strKey(b)
			want := sign(strings.Compare(a, b))
			got := sign(bytes.Compare(ka[:], kb[:]))
			if got != want {
				t.Fatalf("str order: a=%q b=%q key cmp=%d want %d", a, b, got, want)
			}
		}
	}
}

// Regression for the rune-backup false negative: a >16-byte value whose 16-byte cut
// splits a multibyte rune must still sort correctly (plain truncation is monotonic;
// backing up to a rune boundary was not). The key may CONTRADICT natural order only by
// being equal (a lossy candidate re-verified on decode) — never by having the wrong sign.
func TestEncodeStrLongMonotonic(t *testing.T) {
	cases := [][2]string{
		{strings.Repeat("A", 15) + "é", strings.Repeat("A", 16)}, // a>b naturally; the old code sorted a<b
		{strings.Repeat("A", 16), strings.Repeat("A", 15) + "é"}, // b>a
		{strings.Repeat("A", 20), strings.Repeat("A", 16) + "Z"}, // share 16-byte prefix → keys equal (candidate)
		{"order-000000001x", "order-000000002x"},                 // differ within 16
	}
	for _, c := range cases {
		ka, kb := strKey(c[0]), strKey(c[1])
		nat := sign(strings.Compare(c[0], c[1]))
		key := sign(bytes.Compare(ka[:], kb[:]))
		if key != 0 && key != nat {
			t.Fatalf("a=%q b=%q: key cmp %d contradicts natural order %d (false negative risk)", c[0], c[1], key, nat)
		}
	}
}

func TestEncodeStrLossyPrefixCollision(t *testing.T) {
	// Two strings that share their first 16 bytes collide (lossy) — this is expected
	// and is why re-verify exists. Document it as a test so the property is explicit.
	a := "0123456789abcdef-alpha"
	b := "0123456789abcdef-beta"
	if strKey(a) != strKey(b) {
		t.Skip("prefixes differ within 16 bytes; not a collision case")
	}
	// Same key, different strings → the index would return both as candidates.
}

func TestEncodeUUIDOrderIsByteOrder(t *testing.T) {
	var a, b [KeySize]byte
	a[0], b[0] = 1, 2
	ka := EncodeKey(Value{Kind: KindUUID, UUID: a})
	kb := EncodeKey(Value{Kind: KindUUID, UUID: b})
	if bytes.Compare(ka[:], kb[:]) >= 0 {
		t.Fatal("uuid key order must equal raw byte order")
	}
}

// Compare must never report two different-Kind values as equal — otherwise a mistyped
// query value would spuriously satisfy an equality predicate.
func TestCompareCrossKindNeverEqual(t *testing.T) {
	pairs := [][2]Value{
		{{Kind: KindInt, Int: 5}, {Kind: KindStr, Str: "5"}},
		{{Kind: KindFloat, Float: 1.0}, {Kind: KindInt, Int: 1}},
		{{Kind: KindStr, Str: "x"}, {Kind: KindUUID}},
	}
	for _, p := range pairs {
		if Compare(p[0], p[1]) == 0 {
			t.Fatalf("Compare(%v, %v) == 0 across kinds", p[0].Kind, p[1].Kind)
		}
	}
	// Same-kind equality still works.
	if Compare(Value{Kind: KindInt, Int: 7}, Value{Kind: KindInt, Int: 7}) != 0 {
		t.Fatal("same-kind equal values must compare 0")
	}
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
