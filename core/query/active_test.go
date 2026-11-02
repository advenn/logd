package query_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// The active segment is answered from its writer's in-memory index (design §9.2). Every
// test here holds that to the same contract as a sealed segment's sidecars: Execute equals
// ExecuteScan, with the index only ever pruning.

var activeCfg = config.IndexConfig{
	Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
		{Name: "trip", Pattern: "trip:{id:str}"},
	},
	Literals: []string{"panic:"},
}

// activeRecord is record i of the active-segment fixtures: out-of-order timestamps with
// ties, a latency, a lossy string id (16-byte shared prefix), a literal on every 13th, and
// two labels.
func activeRecord(i int) model.LogEntry {
	msg := fmt.Sprintf("req %d took %dms trip:PREFIX0123456789-%d", i, (i*7)%1000, i%40)
	if i%13 == 0 {
		msg = "panic: " + msg
	}
	return model.LogEntry{
		TS:      time.Unix(t0+int64((i*37)%500), 0).UTC(),
		Level:   model.LogLevel(i % 4),
		Extra:   fmt.Sprintf(`{"region":"r%d","env":"%s"}`, i%3, []string{"prod", "dev"}[i%2]),
		Message: msg,
	}
}

type activeFixture struct {
	dir string
	w   *storage.Writer
	ig  *ingest.Ingester
	eng *extract.Engine
	e   *qr.Engine
	n   int
}

// newActiveFixture builds a shard holding records [0, n). Records [0, sealed) are written
// through 8-page segments by a writer that is then closed, so they end up in several sealed
// segments; a second writer on the same directory takes [sealed, n) into one active segment
// and stays open, every page of it on disk. sealed == n seals everything. mutate adjusts the
// options of both writers (the first keeps its 8-page segments).
//
// Two writers, rather than one with small segments left open, make the layout exact: a
// single writer may rotate on the very last page and leave the tail in an empty segment.
func newActiveFixture(t *testing.T, n, sealed int, mutate func(*storage.Options)) *activeFixture {
	t.Helper()
	eng := compileOrFail(t, activeCfg)
	opts := storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    2 * time.Millisecond,
		LabelKeys:        []string{"region"},
	}
	if mutate != nil {
		mutate(&opts)
	}
	f := &activeFixture{dir: t.TempDir(), eng: eng, n: n}
	open := func(o storage.Options) {
		w, err := storage.NewWriter(f.dir, o)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Start(nil); err != nil {
			t.Fatal(err)
		}
		f.w, f.ig = w, ingest.NewWithLabels(eng, w, []string{"region"})
		f.e = qr.NewEngineWithLabels(w, eng, []string{"region"})
	}
	if sealed > 0 {
		small := opts
		small.SegmentSizeBytes = 8 * storage.PageSize
		open(small)
		f.put(0, sealed)
		if err := f.w.Close(); err != nil {
			t.Fatal(err)
		}
		if sealed == n {
			return f
		}
	}
	open(opts)
	t.Cleanup(func() { f.w.Close() })
	f.put(sealed, n)
	f.settle(t, n)
	return f
}

func (f *activeFixture) put(from, to int) {
	for i := from; i < to; i++ {
		if err := f.ig.IngestCtx(context.Background(), activeRecord(i)); err != nil {
			panic(err)
		}
	}
}

// settle waits until the scan sees all n records: the writer has drained its queue and the
// tick has written the last page, so both paths read the same pages.
func (f *activeFixture) settle(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := f.e.ExecuteScan(qr.Query{Start: ts(-1), End: ts(1000), Limit: n + 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records became readable", len(got), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// activeBattery is the predicate battery every layout must answer identically by index and
// by scan: typed (lossless and lossy, every operator), literal, label, conjunctions.
func activeBattery() []struct {
	name  string
	preds []qr.Predicate
} {
	tc := func(f string, op qr.Op, v index.Value) qr.Predicate {
		return qr.TypedCompare{Field: f, Op: op, Value: v}
	}
	return []struct {
		name  string
		preds []qr.Predicate
	}{
		{"latency > 500", []qr.Predicate{tc("latency_ms", qr.OpGt, intV(500))}},
		{"latency >= 500", []qr.Predicate{tc("latency_ms", qr.OpGe, intV(500))}},
		{"latency < 100", []qr.Predicate{tc("latency_ms", qr.OpLt, intV(100))}},
		{"latency <= 100", []qr.Predicate{tc("latency_ms", qr.OpLe, intV(100))}},
		{"latency == 700", []qr.Predicate{tc("latency_ms", qr.OpEq, intV(700))}},
		{"latency == absent", []qr.Predicate{tc("latency_ms", qr.OpEq, intV(999999))}},
		{"latency != 0", []qr.Predicate{tc("latency_ms", qr.OpNe, intV(0))}},
		{"trip > prefix-5 (lossy)", []qr.Predicate{tc("trip_id", qr.OpGt, strV("PREFIX0123456789-5"))}},
		{"trip == prefix-7 (lossy)", []qr.Predicate{tc("trip_id", qr.OpEq, strV("PREFIX0123456789-7"))}},
		{"literal panic:", []qr.Predicate{qr.LineContains{Sub: "panic:"}}},
		{"region=r1", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r1"}}},
		{"region=absent", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r9"}}},
		{"region=\"\"", []qr.Predicate{qr.LabelEqual{Key: "region", Value: ""}}},
		{"env=prod (not allowlisted)", []qr.Predicate{qr.LabelEqual{Key: "env", Value: "prod"}}},
		{"region=r2 AND latency in (100,900)", []qr.Predicate{
			qr.LabelEqual{Key: "region", Value: "r2"}, tc("latency_ms", qr.OpGt, intV(100)), tc("latency_ms", qr.OpLt, intV(900)),
		}},
		{"panic: AND latency > 300", []qr.Predicate{qr.LineContains{Sub: "panic:"}, tc("latency_ms", qr.OpGt, intV(300))}},
	}
}

func runBattery(t *testing.T, e *qr.Engine, layout string) {
	t.Helper()
	for _, c := range activeBattery() {
		for _, lim := range []int{0, 7, 100000} {
			for _, dir := range []qr.Direction{qr.Backward, qr.Forward} {
				for _, win := range [][2]int{{0, 1000}, {120, 300}} {
					q := qr.Query{Start: ts(win[0]), End: ts(win[1]), Preds: c.preds, Limit: lim, Direction: dir}
					idx, err := e.Execute(q)
					if err != nil {
						t.Fatal(err)
					}
					scan, err := e.ExecuteScan(q)
					if err != nil {
						t.Fatal(err)
					}
					equalResults(t, fmt.Sprintf("%s: %s limit=%d dir=%d window=%v", layout, c.name, lim, dir, win), idx, scan)
				}
			}
		}
	}
}

// explainByState counts the plans per (state, reason) for one query.
func explainByState(e *qr.Engine, q qr.Query) map[string]int {
	out := map[string]int{}
	for _, p := range e.Explain(q) {
		out[p.State+"/"+p.Reason]++
	}
	return out
}

// TestActiveSegmentPushdownEqualsScan: the battery agrees across three layouts — the whole
// corpus in the active segment, sealed segments plus an active one, all sealed — and the
// active segment really is answered from its in-memory index, not quietly scanned.
func TestActiveSegmentPushdownEqualsScan(t *testing.T) {
	const n = 1500
	for _, c := range []struct {
		name   string
		sealed int // records written to sealed segments; the rest stay active
	}{
		{"all active", 0},
		{"sealed and active", n * 3 / 4}, // T4's layout: a quarter unsealed
		{"all sealed", n},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newActiveFixture(t, n, c.sealed, nil)
			runBattery(t, f.e, c.name)

			typed := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(990)}}}
			got := explainByState(f.e, typed)
			if c.sealed == n {
				if got["active/indexed_in_memory"] != 0 || got["sealed/indexed"] == 0 {
					t.Fatalf("all sealed: plans %v", got)
				}
				return
			}
			if got["active/indexed_in_memory"] != 1 {
				t.Fatalf("the active segment was not answered from its in-memory index: plans %v", got)
			}
			if c.sealed > 0 && got["sealed/indexed"] == 0 {
				t.Fatalf("sealed segments not indexed: plans %v", got)
			}
			// A third of the records under the default limit of 100 covers every page: the cost
			// guard would scan it. Wanting them all, it pushes down.
			labelOnly := qr.Query{Start: ts(0), End: ts(1000), Limit: n, Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r1"}}}
			if got := explainByState(f.e, labelOnly); got["active/indexed_in_memory"] != 1 {
				t.Fatalf("a label predicate was not pushed into the active segment: plans %v", got)
			}
			// The metric path reads the active segment the same way.
			mq := qr.MetricQuery{Preds: typed.Preds, RangeOp: qr.RangeCount, Range: 2000 * 1e9, Start: ts(1000), End: ts(1000), Step: 1e9}
			series, err := f.e.ExecuteMetric(mq)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := f.e.ExecuteScan(qr.Query{Start: ts(0), End: ts(1000), Limit: n, Preds: typed.Preds})
			total := 0
			for _, s := range series {
				for _, p := range s.Points {
					total += int(p.Value)
				}
			}
			if total != len(want) {
				t.Fatalf("count_over_time over the active segment: %d, scan finds %d", total, len(want))
			}
		})
	}
}

// TestActiveSegmentReadabilityBoundary: records still in the page the writer is assembling
// are invisible to both paths alike, and the in-memory index counts only the records on
// disk among its candidates.
func TestActiveSegmentReadabilityBoundary(t *testing.T) {
	eng := compileOrFail(t, activeCfg)
	w, err := storage.NewWriter(t.TempDir(), storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	defer w.Close()
	ig := ingest.New(eng, w)
	e := qr.NewEngine(w, eng)
	// ~40 records per page: 100 records fill two pages; the third is still in RAM.
	for i := 0; i < 100; i++ {
		if err := ig.IngestCtx(context.Background(), model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req %03d took %dms padding-padding-padding", i, i)}); err != nil {
			t.Fatal(err)
		}
	}
	all := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000}
	deadline := time.Now().Add(5 * time.Second)
	var onDisk []model.LogEntry
	for {
		onDisk, _ = e.ExecuteScan(all)
		if len(onDisk) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(onDisk) == 0 || len(onDisk) >= 100 {
		t.Fatalf("want some but not all records on disk, the scan sees %d", len(onDisk))
	}
	q := all
	q.Preds = []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(0)}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "records in RAM", idx, scan)
	plans := e.Explain(q)
	if len(plans) != 1 || plans[0].Reason != qr.ReasonIndexedInMemory || plans[0].Candidates != len(scan) {
		t.Fatalf("plans %+v: want one in-memory plan with %d candidates (the records on disk)", plans, len(scan))
	}
}

// TestActiveSegmentConfigDrift: the active segment applies the same identity rules as a
// sealed one. A typed field is pushed only if the query engine defines it with the pattern
// the writer extracts it with (F-03); a label key only if the writer derives label sets with
// it (F-02) and it has not hit its value cap in this segment.
func TestActiveSegmentConfigDrift(t *testing.T) {
	t.Run("template pattern changed", func(t *testing.T) {
		f := newActiveFixture(t, 600, 0, nil)
		// Same field name, different pattern: "took=" matches none of these records.
		edited := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took={ms:int}ms"}}})
		e := qr.NewEngineWithLabels(f.w, edited, []string{"region"})
		q := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000, Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(0)}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, "edited pattern", idx, scan)
		if len(scan) != 0 {
			t.Fatalf("the edited pattern matches %d records; the test needs it to match none", len(scan))
		}
		assertAllScan(t, "edited pattern", e, q)
		// The unedited engine still pushes, and finds them.
		if got := explainByState(f.e, q); got["active/indexed_in_memory"] != 1 {
			t.Fatalf("live pattern = built pattern should push: %v", got)
		}
	})

	t.Run("allowlist grown", func(t *testing.T) {
		for _, recorded := range []bool{true, false} {
			f := newActiveFixture(t, 600, 0, func(o *storage.Options) {
				if !recorded {
					o.LabelKeys = nil
				}
			})
			e := qr.NewEngineWithLabels(f.w, f.eng, []string{"region", "env"}) // env added since
			envQ := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000, Preds: []qr.Predicate{qr.LabelEqual{Key: "env", Value: "prod"}}}
			idx, _ := e.Execute(envQ)
			scan, _ := e.ExecuteScan(envQ)
			equalResults(t, "env=prod", idx, scan)
			if len(scan) != 300 {
				t.Fatalf("env=prod: %d records, want 300", len(scan))
			}
			assertAllScan(t, fmt.Sprintf("env=prod, allowlist recorded=%v", recorded), e, envQ)
			regionQ := envQ
			regionQ.Preds = []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r1"}}
			if got := explainByState(e, regionQ); got["active/indexed_in_memory"] != 1 {
				t.Fatalf("region (indexed) should still push, allowlist recorded=%v: %v", recorded, got)
			}
		}
	})

	t.Run("label value cap", func(t *testing.T) {
		f := newActiveFixture(t, 600, 0, func(o *storage.Options) { o.MaxLabelCardinality = 2 }) // r0, r1 indexed; r2 over the cap
		for _, v := range []string{"r1", "r2"} {
			q := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000, Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: v}}}
			idx, _ := f.e.Execute(q)
			scan, _ := f.e.ExecuteScan(q)
			equalResults(t, "capped region="+v, idx, scan)
			if len(scan) != 200 {
				t.Fatalf("region=%s: %d records, want 200", v, len(scan))
			}
			assertAllScan(t, "capped region="+v, f.e, q)
		}
	})
}

// TestRecoveredActiveSegment: a crash-recovered active segment is answered from memory once
// the Reindex hook has rebuilt its index from its records, and scanned without one.
func TestRecoveredActiveSegment(t *testing.T) {
	for _, hook := range []bool{true, false} {
		t.Run(fmt.Sprintf("reindex=%v", hook), func(t *testing.T) {
			f := newActiveFixture(t, 500, 0, nil)
			// A copy of a running shard is what a crash leaves: the manifest names an active
			// segment, its pages are on disk, and nothing was sealed.
			dir := t.TempDir()
			copyDir(t, f.dir, dir)
			eng := f.eng
			opts := storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: 2 * time.Millisecond, LabelKeys: []string{"region"}}

			if hook {
				opts.Reindex = func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
					return eng.ReExtract(e.Message), ingest.DeriveLabels(e.Extra, []string{"region"})
				}
			}
			w2, err := storage.NewWriter(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			w2.Start(nil)
			defer w2.Close()
			e := qr.NewEngineWithLabels(w2, eng, []string{"region"})
			runBattery(t, e, fmt.Sprintf("recovered, reindex=%v", hook))
			want := "active/not_indexed"
			if hook {
				want = "active/indexed_in_memory"
			}
			q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}}
			if got := explainByState(e, q); got[want] != 1 {
				t.Fatalf("recovered segment: plans %v, want %s", got, want)
			}
		})
	}
}

// sealingShard is a writer whose active segment seals while a query is being planned: the
// first ActiveIndex call runs seal, either before handing out a view (so the query finds
// none and must go back to the manifest) or after (so it fetches through a view of a
// segment that has meanwhile sealed and is being compressed).
type sealingShard struct {
	*storage.Writer
	seal      func()
	viewFirst bool
	once      sync.Once
}

func (s *sealingShard) ActiveIndex(id string) (*storage.ActiveIndex, bool) {
	if s.viewFirst {
		v, ok := s.Writer.ActiveIndex(id)
		s.once.Do(s.seal)
		return v, ok
	}
	s.once.Do(s.seal)
	return s.Writer.ActiveIndex(id)
}

// TestSealDuringQuery: the active segment seals between the planner listing it as active
// and reading its index. The result must be what a scan of the same data returns.
func TestSealDuringQuery(t *testing.T) {
	for _, viewFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("view_taken_first=%v", viewFirst), func(t *testing.T) {
			const n = 400
			f := newActiveFixture(t, n, 0, func(o *storage.Options) { o.SegmentSizeBytes = 24 * storage.PageSize })
			sh := &sealingShard{Writer: f.w, viewFirst: viewFirst}
			sh.seal = func() {
				active := f.w.Manifest().Active().ID
				// Later records, outside the queried window, push the segment over its size.
				for i := n; f.w.Manifest().Get(active).State == storage.SegmentActive; i++ {
					e := activeRecord(i)
					e.TS = time.Unix(t0+5000+int64(i), 0).UTC()
					if err := f.ig.IngestCtx(context.Background(), e); err != nil {
						t.Error(err)
						return
					}
					if i > n+20000 {
						t.Error("the active segment never sealed")
						return
					}
				}
			}
			e := qr.NewShardedEngine([]qr.Shard{sh}, f.eng, []string{"region"})
			q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(300)}}}
			idx, err := e.Execute(q)
			if err != nil {
				t.Fatal(err)
			}
			scan, err := f.e.ExecuteScan(q)
			if err != nil {
				t.Fatal(err)
			}
			equalResults(t, "seal during query", idx, scan)
			if len(scan) == 0 {
				t.Fatal("the window matched nothing")
			}
		})
	}
}

// TestQueryDuringIngest: queries of every access path run while the writer appends, flushes,
// rotates and seals (run under -race). Writes are sequential, so every query must see a
// prefix of them: if it returns record k, it returns every matching record before k. A hole
// would be an index answering for a page whose records it does not hold.
func TestQueryDuringIngest(t *testing.T) {
	eng := compileOrFail(t, activeCfg)
	w, err := storage.NewWriter(t.TempDir(), storage.Options{
		Schema: eng.IndexedFields(), SegmentSizeBytes: 16 * storage.PageSize, FlushInterval: time.Millisecond, LabelKeys: []string{"region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	defer w.Close()
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	e := qr.NewEngineWithLabels(w, eng, []string{"region"})

	const n = 6000
	rec := func(i int) model.LogEntry {
		msg := fmt.Sprintf("seq %05d took %dms", i, (i*7919)%1000)
		if i%11 == 0 {
			msg = "panic: " + msg
		}
		return model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":"r%d"}`, i%3), Message: msg}
	}
	shapes := []struct {
		name  string
		preds []qr.Predicate
		match func(i int) bool
	}{
		{"typed", []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(800)}}, func(i int) bool { return (i*7919)%1000 > 800 }},
		{"label", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r1"}}, func(i int) bool { return i%3 == 1 }},
		{"literal", []qr.Predicate{qr.LineContains{Sub: "panic:"}}, func(i int) bool { return i%11 == 0 }},
		{"label+typed", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r2"}, qr.TypedCompare{Field: "latency_ms", Op: qr.OpLt, Value: intV(300)}},
			func(i int) bool { return i%3 == 2 && (i*7919)%1000 < 300 }},
	}
	check := func(name string, got []model.LogEntry, match func(int) bool) error {
		seen := map[int]bool{}
		last := -1
		for _, r := range got {
			var k int
			fmt.Sscanf(r.Message[len("seq "):], "%d", &k)
			if r.Message[0] == 'p' {
				fmt.Sscanf(r.Message[len("panic: seq "):], "%d", &k)
			}
			if !match(k) {
				return fmt.Errorf("%s: returned non-matching record %d", name, k)
			}
			seen[k] = true
			last = max(last, k)
		}
		for k := 0; k <= last; k++ {
			if match(k) && !seen[k] {
				return fmt.Errorf("%s: returned record %d but not the earlier matching record %d", name, last, k)
			}
		}
		return nil
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				s := shapes[(g+i)%len(shapes)]
				q := qr.Query{Start: ts(0), End: ts(n + 1), Limit: n + 1, Preds: s.preds, Direction: qr.Forward}
				run := e.Execute
				if i%3 == 2 {
					run = e.ExecuteScan
				}
				got, err := run(q)
				if err != nil {
					t.Error(err)
					return
				}
				if err := check(s.name, got, s.match); err != nil {
					t.Error(err)
					return
				}
				_ = e.Explain(q)
			}
		}(g)
	}
	for i := 0; i < n; i++ {
		if err := ig.IngestCtx(context.Background(), rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Wait until every record is readable, then stop the queries.
	deadline := time.Now().Add(10 * time.Second)
	for {
		all, _ := e.ExecuteScan(qr.Query{Start: ts(0), End: ts(n + 1), Limit: n + 1})
		if len(all) == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records became readable", len(all), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(done)
	wg.Wait()
	if len(w.Manifest().All()) < 3 {
		t.Fatalf("the test needs rotations during ingest, got %d segments", len(w.Manifest().All()))
	}
	for _, s := range shapes {
		q := qr.Query{Start: ts(0), End: ts(n + 1), Limit: n + 1, Preds: s.preds}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, "after ingest: "+s.name, idx, scan)
	}
}

// copyDir copies a shard directory tree, files only.
func copyDir(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.Walk(from, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		src, err := os.Open(p)
		if err != nil {
			return err
		}
		defer src.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, src)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
