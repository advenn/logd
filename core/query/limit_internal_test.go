package query

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// This file lives in package `query` (not query_test) so it can reach executeCollectAll —
// a reimplementation of the ORIGINAL semantics: collect every match, sort globally, then
// truncate. Execute now keeps a bounded top-K and prunes whole segments, which is a real
// behaviour change to the most-tested path in the project, so the two are diffed directly
// the same way ExecuteScan is diffed against the index.
//
// The existing differential battery could not catch a regression here on its own: of ~60
// cases only two carry a small limit, and several set Limit:100000 specifically to disable
// truncation.

// executeCollectAll is the pre-optimization implementation, kept only as a test oracle.
func (e *Engine) executeCollectAll(q Query) ([]model.LogEntry, error) {
	start, end := q.Start, q.End
	if end <= 0 {
		end = 1<<63 - 1
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	out, err := e.collect(q.Preds, start, end, false)
	if err != nil {
		return out, err
	}
	sort.SliceStable(out, func(i, j int) bool { return entryLess(out[i], out[j], q.Direction) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// buildLimitFixture writes a corpus with heavy timestamp ties and out-of-order arrivals
// across MANY small segments — the shape where an early stop can go wrong. Segments are
// deliberately tiny so the segment-level pruning path is exercised.
func buildLimitFixture(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 3 * storage.PageSize, // many small segments
		FlushInterval:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	base := int64(1700000000)
	regions := []string{"eu", "us", "ap"}
	for i := 0; i < 400; i++ {
		// i%50 gives 8 records per timestamp: limits land inside tie groups. Arrival order
		// is not time order, so the sort is load-bearing.
		e := model.LogEntry{
			TS:      time.Unix(base+int64(i%50), 0).UTC(),
			Level:   model.LogLevel(i % 4),
			Extra:   fmt.Sprintf(`{"region":%q}`, regions[i%3]),
			Message: fmt.Sprintf("req %03d took %dms", i, (i*7)%1000),
		}
		e.ServiceID = w.InternService(fmt.Sprintf("svc%d", i%3))
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	return NewEngineWithLabels(w, eng, []string{"region"})
}

func sameEntries(t *testing.T, label string, got, want []model.LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows, want %d", label, len(got), len(want))
		return
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.TS.UnixNano() != w.TS.UnixNano() || g.Message != w.Message ||
			g.Extra != w.Extra || g.Level != w.Level || g.ServiceID != w.ServiceID {
			t.Errorf("%s: row %d differs\n got  ts=%d %q lvl=%d svc=%d\n want ts=%d %q lvl=%d svc=%d",
				label, i, g.TS.UnixNano(), g.Message, g.Level, g.ServiceID,
				w.TS.UnixNano(), w.Message, w.Level, w.ServiceID)
			return
		}
	}
}

// TestLimitPushdownEqualsCollectAll is the load-bearing test for the early-stop change: for
// every limit and direction, the bounded/pruning path must return the SAME ORDERED rows as
// collecting everything and sorting.
func TestLimitPushdownEqualsCollectAll(t *testing.T) {
	e := buildLimitFixture(t)
	preds := []Predicate{LabelEqual{Key: "region", Value: "eu"}}
	typed := []Predicate{
		LabelEqual{Key: "region", Value: "eu"},
		TypedCompare{Field: "latency_ms", Op: OpGt, Value: index.Value{Kind: index.KindInt, Int: 300}},
	}

	for _, dir := range []Direction{Backward, Forward} {
		for _, limit := range []int{1, 2, 5, 7, 40, 99, 100, 101, 133, 1000} {
			for name, ps := range map[string][]Predicate{"label": preds, "typed": typed, "none": nil} {
				q := Query{Start: 0, End: 1 << 62, Preds: ps, Limit: limit, Direction: dir}
				want, err := e.executeCollectAll(q)
				if err != nil {
					t.Fatal(err)
				}
				got, err := e.Execute(q)
				if err != nil {
					t.Fatal(err)
				}
				sameEntries(t, fmt.Sprintf("%s dir=%d limit=%d", name, dir, limit), got, want)

				// And the index path must still equal the forced-scan path under a limit —
				// the existing oracle, now exercised at limit boundaries.
				scan, err := e.ExecuteScan(q)
				if err != nil {
					t.Fatal(err)
				}
				sameEntries(t, fmt.Sprintf("%s dir=%d limit=%d (scan)", name, dir, limit), scan, want)
			}
		}
	}
}

// TestLimitBackwardTakesNewest pins the direction semantics an early stop is most likely to
// invert: with out-of-order arrivals in a single page, Backward+Limit must return the
// NEWEST records, not the first ones encountered.
func TestLimitBackwardTakesNewest(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, err := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)
	base := int64(1700000000)
	for _, sec := range []int64{5, 1, 9, 3} { // arrival order is not time order
		if err := ig.Ingest(model.LogEntry{
			TS:      time.Unix(base+sec, 0).UTC(),
			Message: fmt.Sprintf("rec-%d", sec),
		}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := NewEngine(w, eng)

	got, err := e.Execute(Query{Start: 0, End: 1 << 62, Limit: 2, Direction: Backward})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Message != "rec-9" || got[1].Message != "rec-5" {
		t.Fatalf("backward limit 2 over arrivals {5,1,9,3}: got %v, want [rec-9 rec-5]", messages(got))
	}

	fwd, err := e.Execute(Query{Start: 0, End: 1 << 62, Limit: 2, Direction: Forward})
	if err != nil {
		t.Fatal(err)
	}
	if len(fwd) != 2 || fwd[0].Message != "rec-1" || fwd[1].Message != "rec-3" {
		t.Fatalf("forward limit 2: got %v, want [rec-1 rec-3]", messages(fwd))
	}
}

func messages(recs []model.LogEntry) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Message
	}
	return out
}

// TestTopKCollectorOrdering exercises the accumulator directly, including that a full
// collector's cutoff is the WORST kept record (the pruning bound) and that ties are broken
// by content rather than insertion order.
func TestTopKCollectorOrdering(t *testing.T) {
	mk := func(sec int64, msg string) model.LogEntry {
		return model.LogEntry{TS: time.Unix(sec, 0).UTC(), Message: msg}
	}
	c := newTopK(3, Backward)
	if _, ok := c.cutoff(); ok {
		t.Error("an unfilled collector must not prune")
	}
	for _, e := range []model.LogEntry{mk(1, "a"), mk(5, "b"), mk(3, "c"), mk(9, "d"), mk(2, "e")} {
		c.add(e)
	}
	got := messages(c.results())
	if len(got) != 3 || got[0] != "d" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("backward top-3: got %v, want [d b c]", got)
	}
	cut, ok := c.cutoff()
	if !ok || cut != time.Unix(3, 0).UnixNano() {
		t.Errorf("cutoff should be the worst kept (ts=3), got %d ok=%v", cut, ok)
	}
}

// TestTopKTiesBrokenByContent: when timestamps tie, the kept set must be decided by the
// content tiebreaker, not by which record happened to arrive first. This is what makes a
// limit boundary identical across shard counts.
func TestTopKTiesBrokenByContent(t *testing.T) {
	mk := func(msg string) model.LogEntry {
		return model.LogEntry{TS: time.Unix(100, 0).UTC(), Message: msg}
	}
	forward := newTopK(2, Backward)
	for _, m := range []string{"zeta", "alpha", "mid"} {
		forward.add(mk(m))
	}
	reversed := newTopK(2, Backward)
	for _, m := range []string{"mid", "alpha", "zeta"} { // same set, different arrival order
		reversed.add(mk(m))
	}
	a, b := messages(forward.results()), messages(reversed.results())
	if len(a) != 2 || a[0] != "alpha" || a[1] != "mid" {
		t.Errorf("got %v, want [alpha mid]", a)
	}
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Errorf("arrival order changed the result: %v vs %v", a, b)
	}
}
