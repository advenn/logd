package query

import (
	"math"
	"testing"

	"github.com/advenn/logd/core/storage"
)

// offsetsIn returns one record offset in each of the given logical pages.
func offsetsIn(pages ...uint64) []uint64 {
	out := make([]uint64, len(pages))
	for i, p := range pages {
		out[i] = p*storage.PageSize + storage.PageHeaderSize
	}
	return out
}

// repeat returns offs n times over: the over-fetch term counts entries, coverage counts units.
func repeat(offs []uint64, n int) []uint64 {
	var out []uint64
	for i := 0; i < n; i++ {
		out = append(out, offs...)
	}
	return out
}

// cycled returns n offsets, one per page, cycling through the pages [1, pages].
func cycled(n int, pages uint64) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = (1+uint64(i)%pages)*storage.PageSize + storage.PageHeaderSize
	}
	return out
}

// TestCostGuardUnits pins how the guard prices a candidate set: the over-fetch threshold is
// guardOverFetch·√(limit·records), and the coverage is counted in the segment's own recorded
// I/O units — blocks that tile the logical pages from the placeholder page 0, or data pages
// for a raw segment. A segment that recorded no layout has no coverage term.
func TestCostGuardUnits(t *testing.T) {
	compressed := &storage.SegmentMeta{Pages: 79, BlockPages: 8, Records: 2400} // logical pages 0..79: 10 blocks
	raw := &storage.SegmentMeta{Pages: 80, Records: 2400}                       // data pages 1..80
	legacy := &storage.SegmentMeta{Records: 2400}                               // sealed before layouts were recorded

	if g := newCostGuard(compressed, 1); g.totalUnits != 10 || g.unitPages != 8 {
		t.Fatalf("compressed: %d units of %d pages, want 10 of 8", g.totalUnits, g.unitPages)
	}
	if g := newCostGuard(raw, 1); g.totalUnits != 80 || g.unitPages != 1 {
		t.Fatalf("raw: %d units of %d pages, want 80 of 1", g.totalUnits, g.unitPages)
	}
	g4 := newCostGuard(compressed, 4)
	if want := int(guardOverFetch * math.Sqrt(4*2400)); g4.maxFetch != want {
		t.Fatalf("limit 4 over 2400 records: fetches up to %d, want %d", g4.maxFetch, want)
	}
	// The threshold is exact: covering every block, maxFetch candidates fetch, one more scans.
	if g4.trips(cycled(g4.maxFetch, 79)) || !g4.trips(cycled(g4.maxFetch+1, 79)) {
		t.Fatalf("limit 4: the threshold is not at %d candidates", g4.maxFetch)
	}

	over := 4 * newCostGuard(compressed, 1).maxFetch // comfortably past the limit=1 threshold
	for _, c := range []struct {
		name  string
		seg   *storage.SegmentMeta
		limit int
		offs  []uint64
		want  bool
	}{
		{"compressed, every block, over-fetching", compressed, 1, cycled(over, 79), true},
		{"compressed, two blocks, over-fetching", compressed, 1, cycled(over, 15), false},
		{"compressed, every block, limit wants them all", compressed, over, cycled(over, 79), false},
		{"compressed, every block, unbounded", compressed, 0, cycled(over, 79), false},
		{"raw, every page, over-fetching", raw, 1, cycled(over, 80), true},
		{"raw, first half of its pages", raw, 1, cycled(over, 40), false},
		{"no recorded layout", legacy, 1, cycled(over, 79), false},
		{"no recorded record count", &storage.SegmentMeta{Pages: 80}, 1, cycled(over, 80), false},
		// Coverage counts distinct units: one page's records repeated cannot trip it.
		{"one page, repeated", raw, 1, repeat(offsetsIn(5), 100*over), false},
	} {
		if got := newCostGuard(c.seg, c.limit).trips(c.offs); got != c.want {
			t.Errorf("%s: trips=%v, want %v", c.name, got, c.want)
		}
	}
}

// TestRowBound: only a bounded collector prices the guard; the metric path's and collect's
// keep every match and never trip it.
func TestRowBound(t *testing.T) {
	if got := rowBound(newTopK(37, Backward)); got != 37 {
		t.Fatalf("top-K: %d, want 37", got)
	}
	if got := rowBound(&sliceCollector{}); got != 0 {
		t.Fatalf("collect-all: %d, want 0", got)
	}
	if got := rowBound(&metricCollector{}); got != 0 {
		t.Fatalf("metric: %d, want 0", got)
	}
}
