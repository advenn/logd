package extract

import (
	"fmt"
	"sort"
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
