package query

import (
	"math"

	"github.com/advenn/logd/core/storage"
)

// The cost guard decides, per segment, between fetching the candidate records by offset and
// scanning the segment sequentially. It is correctness-neutral — both paths return the same
// rows, which the differential tests check — so it only ever chooses a speed.
//
// It used to scan whenever the candidates exceeded half the segment's records. Measured,
// that made the queries it fired on 2-4x slower. FetchRecords reads each page or block once,
// in order, so there is no random-I/O penalty for the scan to save; what the scan does pay
// for is decoding, and re-extracting, every record it reads, which the index path does only
// for its candidates (and not at all for a settled lossless predicate).
//
// The one thing the scan has that the fetch does not is an early exit. A bounded query's
// scan walks pages in the query's direction and, once its top-K holds `limit` rows, skips
// decoding every page that cannot beat them; the fetch decodes every candidate whatever the
// limit. With the candidates spread evenly over n records, the scan decodes about
// limit·n/candidates records before its top-K fills, the fetch decodes `candidates`, and the
// two cross where candidates = √(limit·n). Hence the two terms, ANDed:
//
//   - over-fetch: candidates > guardOverFetch · √(limit·n). The candidates over-fetch the
//     limit by a ratio that grows with the segment and shrinks with the limit — not by a
//     fixed one, which the sweep refutes: the crossover sat near 40x the limit at
//     limit=100 but near 7-20x at limit=1000. An unbounded query (limit 0: the metric path,
//     and collect) has no early exit at all and never trips; the sweep has the index ahead
//     in every unbounded cell, by 2.6x to 74x.
//   - coverage: the candidates touch more than guardCoverage of the segment's I/O units
//     (compressed blocks, or pages when uncompressed). That is the condition under which
//     "spread evenly" holds, and under which the fetch reads what the scan reads anyway. A
//     set confined to part of the segment is cheap to fetch however large: clustered at the
//     old end, at 50% coverage and 1000x the limit, the index was still 2.3-2.5x faster,
//     because a backward scan has to decode the whole newer half before its first match.
//
// The weakness, kept rather than papered over: the model assumes the scan meets matches at
// an even rate. Matches clustered at the end the scan starts from (an incident in the last
// minutes, queried newest-first) fill its top-K at once; the guard then keeps the index and
// pays the full candidate decode — what every query paid before any guard, never a wrong row.
// Placing the limit-th candidate in scan order would price that case too; not built.
//
// A segment that recorded no layout (sealed before storage.SegmentMeta.Pages existed) never
// trips. Falling back to the over-fetch term alone would scan the clustered case above, a
// loss up to the 74x the sweep measured, to win at most the 1.2-14x of the scattered one;
// such segments age out with retention.
//
// The constants come from TestCostGuardSelectivitySweep, runs 1 and 2 of 2026-11-02 in
// testbed/results/costguard-sweep-2026-11-02.txt (200k records per segment; run 3 there is
// this rule re-measured). Where the scattered corpus crossed from index to scan, in
// candidates:
//
//	                 limit=100     limit=1000      √(limit·n)
//	compressed       ~5k-10k       ~10k-20k        4.5k / 14k
//	uncompressed     ~4k-10k       ~20k            4.5k / 14k
//
// The crossover moved ~2-3x for a 10x limit, as √ predicts (3.2x) and a fixed ratio does
// not (10x). guardOverFetch is the model's own coefficient, 1: it lies inside the measured
// band at both limits in both layouts, and the cells nearest it measured scan/index 0.8-1.25
// over the two runs, so a decision on either side of it there costs little. guardCoverage is
// "nearly all": the scattered sets that scan profitably covered 100% of their units, the
// clustered ones that never should covered at most 50%.
const (
	guardOverFetch = 1.0
	guardCoverage  = 0.9
)

// costGuard holds one (query, segment) pair's inputs to that decision.
type costGuard struct {
	limit      int    // rows the query keeps; 0 = it keeps every match, and the guard never trips
	maxFetch   int    // the largest candidate count still fetched: guardOverFetch·√(limit·n)
	unitPages  uint64 // logical pages per I/O unit: a compressed block's, or 1 when read by page
	totalUnits uint64 // I/O units a scan reads; 0 = the segment recorded no layout
}

// newCostGuard prices a candidate set against the segment's own recorded layout (see
// storage.SegmentMeta.Pages), for a query that keeps limit rows (0 = all of them).
func newCostGuard(seg *storage.SegmentMeta, limit int) costGuard {
	g := costGuard{limit: limit, unitPages: 1}
	// Never below the limit itself: a scan whose top-K cannot fill has no early exit either.
	g.maxFetch = max(limit, int(guardOverFetch*math.Sqrt(float64(limit)*float64(seg.Records))))
	switch {
	case seg.Pages == 0 || seg.Records == 0:
		// Sealed before the layout was recorded: no coverage term, so no trip (see above).
	case seg.BlockPages > 0:
		// Blocks tile the logical pages from 0, the placeholder included, so the last one
		// may be partial; a scan still has to inflate it.
		g.unitPages = uint64(seg.BlockPages)
		g.totalUnits = (seg.Pages + 1 + g.unitPages - 1) / g.unitPages
	default:
		g.totalUnits = seg.Pages
	}
	return g
}

// unit is the I/O unit that holds the record at off, numbered from 0, and false for an
// offset outside the data pages (which no candidate has).
func (g costGuard) unit(off uint64) (uint64, bool) {
	page := off / storage.PageSize
	if g.unitPages > 1 {
		return page / g.unitPages, true
	}
	if page == 0 {
		return 0, false // the placeholder page holds no records
	}
	return page - 1, true
}

// trips reports whether this candidate set is better read by scanning the segment.
//
// offsets may hold duplicates (one record matching a template twice yields two entries).
// That can only make the over-fetch term hold sooner, and the coverage term counts distinct
// units, so a duplicate cannot trip the guard by itself.
func (g costGuard) trips(offsets []uint64) bool {
	if g.limit <= 0 || g.totalUnits == 0 || len(offsets) <= g.maxFetch {
		return false
	}
	need := uint64(guardCoverage * float64(g.totalUnits))
	seen := make([]uint64, (g.totalUnits+63)/64)
	var covered uint64
	for _, off := range offsets {
		u, ok := g.unit(off)
		if !ok || u >= g.totalUnits {
			continue
		}
		if bit := uint64(1) << (u % 64); seen[u/64]&bit == 0 {
			seen[u/64] |= bit
			if covered++; covered > need {
				return true
			}
		}
	}
	return false
}

// rowBound is how many rows acc keeps: a log query's limit, or 0 for a collector that keeps
// every match — the metric path's, and collect's — whose segments the guard never scans.
func rowBound(acc collector) int {
	if t, ok := acc.(*topKCollector); ok {
		return t.limit
	}
	return 0
}
