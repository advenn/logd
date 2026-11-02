package extract

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// Classic Aho-Corasick correctness check with overlapping/nested patterns.
func TestAhoCorasickOverlapping(t *testing.T) {
	pats := []string{"he", "she", "his", "hers"}
	ac := newAhoCorasick(pats)

	var got []string
	for _, h := range ac.search("ushers") {
		got = append(got, fmt.Sprintf("%s@%d", pats[h.pattern], h.start))
	}
	sort.Strings(got)

	want := []string{"he@2", "hers@2", "she@1"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestAhoCorasickMultipleOccurrences(t *testing.T) {
	ac := newAhoCorasick([]string{"took "})
	hits := ac.search("took 1ms took 2ms")
	if len(hits) != 2 {
		t.Fatalf("want 2 occurrences of 'took ', got %d", len(hits))
	}
	if hits[0].start != 0 || hits[1].start != 9 {
		t.Fatalf("occurrence starts: got %d,%d want 0,9", hits[0].start, hits[1].start)
	}
}

func TestAhoCorasickNoFalsePositive(t *testing.T) {
	ac := newAhoCorasick([]string{"panic:"})
	if hits := ac.search("all calm here"); len(hits) != 0 {
		t.Fatalf("unexpected hits: %v", hits)
	}
}

// naiveHits is the reference: every occurrence of every pattern, by brute force.
func naiveHits(pats []string, text string) []string {
	var out []string
	for pi, p := range pats {
		for i := 0; i+len(p) <= len(text); i++ {
			if text[i:i+len(p)] == p {
				out = append(out, fmt.Sprintf("%d@%d", pi, i))
			}
		}
	}
	sort.Strings(out)
	return out
}

func hitStrings(hits []acHit) []string {
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, fmt.Sprintf("%d@%d", h.pattern, h.start))
	}
	sort.Strings(out)
	return out
}

// TestAhoCorasickDFAMatchesNaive: the search runs on a transition table compiled from the trie
// (fail-link walks folded in, bytes grouped into classes). A small alphabet makes overlaps,
// nesting and long fail chains common; every occurrence must still be reported exactly once.
func TestAhoCorasickDFAMatchesNaive(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	alpha := "abcab=x "
	randStr := func(maxLen int) string {
		var b strings.Builder
		for n := 1 + r.Intn(maxLen); n > 0; n-- {
			b.WriteByte(alpha[r.Intn(len(alpha))])
		}
		return b.String()
	}
	for iter := 0; iter < 3000; iter++ {
		seen := map[string]bool{}
		var pats []string
		for n := 1 + r.Intn(6); n > 0; n-- {
			if p := randStr(5); !seen[p] {
				seen[p] = true
				pats = append(pats, p)
			}
		}
		ac := newAhoCorasick(pats)
		for k := 0; k < 5; k++ {
			text := randStr(60) + "\x00\xff" + randStr(20) // bytes outside every pattern too
			got, want := hitStrings(ac.search(text)), naiveHits(pats, text)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("patterns %q, text %q:\n got  %v\n want %v", pats, text, got, want)
			}
		}
	}
}

// TestAhoCorasickEveryByteValue: when the patterns use all 256 byte values no class is left for
// "a byte no pattern contains", and the table falls back to one class per byte value.
func TestAhoCorasickEveryByteValue(t *testing.T) {
	var all []byte
	for b := 0; b < 256; b++ {
		all = append(all, byte(b))
	}
	pats := []string{string(all[:128]), string(all[128:]), "\x00\x01", "\xfe\xff"}
	ac := newAhoCorasick(pats)
	if ac.numClasses != 256 {
		t.Fatalf("numClasses = %d, want the 256-class fallback", ac.numClasses)
	}
	text := string(all) + string(all)
	got, want := hitStrings(ac.search(text)), naiveHits(pats, text)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}
