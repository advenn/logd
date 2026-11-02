package query_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	q "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// TestCostGuardSelectivitySweep measures where, if anywhere, scanning a sealed segment beats
// fetching its candidates by offset. The cost guard's thresholds (costguard.go) are read off
// its output rather than guessed.
//
// Per corpus and selectivity it prints the exact share of pages and of I/O units (compressed
// blocks, or pages when uncompressed) the candidate set touches, what the guard decides at
// limit=100 and at limit=1000, and the median time of six queries: the index path with the
// guard switched off and the forced scan, each bounded at those two limits (backward, the
// Grafana default) and unbounded (every row: the shape of the metric path, and of the
// testbed's limit=200000). Two limits, because where the crossover sits relative to the
// limit is exactly what a fixed over-fetch ratio assumes and a cost model may not.
//
// Four corpora: compressed and uncompressed, each with the matching records scattered over
// the whole segment (a hashed value — the shape of every selective query in the evaluation)
// and clustered at its oldest end (contiguous — the case a coverage-blind guard gets wrong,
// because a bounded backward scan must decode everything newer before it finds a match).
//
// Run it on a real disk; /tmp is tmpfs here:
//
//	LOGD_SWEEP=1 LOGD_BENCH_DIR=$HOME/.cache/logd-bench \
//	  go test ./core/query/ -run TestCostGuardSelectivitySweep -v -timeout 60m
//
// It is env-guarded because it builds four 200k-record segments and times ~1500 queries.
func TestCostGuardSelectivitySweep(t *testing.T) {
	if os.Getenv("LOGD_SWEEP") == "" {
		t.Skip("set LOGD_SWEEP=1 to run the cost-guard selectivity sweep")
	}
	load, _ := os.ReadFile("/proc/loadavg")
	t.Logf("load at start: %s", strings.TrimSpace(string(load)))
	for _, c := range []struct {
		name       string
		blockPages int
		value      func(i int) int
	}{
		{"compressed (8-page blocks), scattered", 0, sweepScattered},
		{"compressed (8-page blocks), clustered", 0, sweepClustered},
		{"uncompressed, scattered", -1, sweepScattered},
		{"uncompressed, clustered", -1, sweepClustered},
	} {
		runSweep(t, c.name, c.blockPages, c.value)
	}
	load, _ = os.ReadFile("/proc/loadavg")
	t.Logf("load at end: %s", strings.TrimSpace(string(load)))
}

const sweepN = 200000

// sweepScattered is a deterministic value in [0,10000) for record i, scattered by Knuth's
// multiplicative hash: `> 10000·(1-s)` selects a fraction s spread over every page.
func sweepScattered(i int) int { return int((uint32(i) * 2654435761) % 10000) }

// sweepClustered falls from 9999 to 0 across the corpus, so the same threshold selects the
// OLDEST fraction s of the records, contiguous at the start of the segment.
func sweepClustered(i int) int { return 9999 - i*10000/sweepN }

func sweepDir(t *testing.T) string {
	root := os.Getenv("LOGD_BENCH_DIR")
	if root == "" {
		return t.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := os.MkdirTemp(root, "sweep")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func runSweep(t *testing.T, name string, blockPages int, value func(int) int) {
	dir := sweepDir(t)
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 1 << 40, // one segment, so coverage is unambiguous
		FlushInterval:    time.Hour,
		SyncInterval:     50 * time.Millisecond,
		BlockPages:       blockPages,
		LabelKeys:        []string{"app"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	ig := ingest.NewWithLabels(eng, w, []string{"app"})
	for i := 0; i < sweepN; i++ {
		e := model.LogEntry{
			TS:      time.Unix(t0+int64(i), 0).UTC(),
			Level:   model.LogLevelInfo,
			Extra:   `{"app":"bench"}`,
			Message: fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace_id=%08x%08x", i, value(i), i, i*7),
		}
		// IngestCtx, not Ingest: the bounded queue DROPS when full, which would silently
		// truncate the corpus and invalidate every coverage figure below.
		if err := ig.IngestCtx(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil { // flush, seal, compress (Close waits for it)
		t.Fatal(err)
	}
	segs := w.Manifest().All()
	if len(segs) != 1 {
		t.Fatalf("want one segment, got %d", len(segs))
	}
	seg := segs[0]
	e := q.NewEngineWithLabels(w, eng, []string{"app"})
	unguarded := q.NewEngineWithLabels(w, eng, []string{"app"})
	q.DisableCostGuard(unguarded)

	// Exact coverage, from the stored offsets rather than a model of them.
	recs, err := storage.ReadAllRecords(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != sweepN {
		t.Fatalf("read back %d records, want %d", len(recs), sweepN)
	}
	unitPages := uint64(1)
	if seg.BlockPages > 0 {
		unitPages = uint64(seg.BlockPages)
	}
	unitOf := func(off uint64) uint64 { return off / storage.PageSize / unitPages }
	allUnits := map[uint64]bool{}
	for _, r := range recs {
		allUnits[unitOf(r.Offset)] = true
	}
	t.Logf("")
	t.Logf("=== %s: %d records, %d data pages, block_pages=%d, %d I/O units", name, len(recs), seg.Pages, seg.BlockPages, len(allUnits))
	t.Logf("%6s %7s %6s %6s | %-10s %8s %8s %5s | %-10s %8s %8s %5s | %8s %8s %5s",
		"", "", "", "", "limit=100", "", "", "", "limit=1000", "", "", "", "all rows", "", "")
	t.Logf("%6s %7s %6s %6s | %-10s %8s %8s %5s | %-10s %8s %8s %5s | %8s %8s %5s",
		"sel%", "rows", "pages%", "units%", "guard", "index", "scan", "s/i", "guard", "index", "scan", "s/i", "index", "scan", "s/i")

	for _, sel := range []float64{50, 30, 20, 10, 5, 2, 1, 0.5, 0.2, 0.1} {
		thr := int64(10000 * (1 - sel/100))
		preds := []q.Predicate{
			q.LabelEqual{Key: "app", Value: "bench"},
			q.TypedCompare{Field: "latency_ms", Op: q.OpGt, Value: intV(thr)},
		}
		pages, units, rows := map[uint64]bool{}, map[uint64]bool{}, 0
		for _, r := range recs {
			if int64(value(recOrdinal(r.Entry.Message))) > thr {
				pages[r.Offset/storage.PageSize] = true
				units[unitOf(r.Offset)] = true
				rows++
			}
		}
		decision := func(limit int) string {
			for _, p := range e.Explain(q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: limit, Direction: q.Backward}) {
				if p.Mode != "index" {
					return p.Reason
				}
			}
			return "index"
		}
		var cols []any
		for _, limit := range []int{100, 1000} {
			i := sweepTime(t, unguarded, preds, limit, false)
			s := sweepTime(t, e, preds, limit, true)
			cols = append(cols, decision(limit), ms(i), ms(s), float64(s)/float64(i))
		}
		iAll := sweepTime(t, unguarded, preds, sweepN, false)
		sAll := sweepTime(t, e, preds, sweepN, true)
		cols = append(cols, ms(iAll), ms(sAll), float64(sAll)/float64(iAll))
		t.Logf("%6.1f %7d %6.1f %6.1f | %-10s %8s %8s %5.2f | %-10s %8s %8s %5.2f | %8s %8s %5.2f",
			append([]any{sel, rows,
				100 * float64(len(pages)) / float64(seg.Pages),
				100 * float64(len(units)) / float64(len(allUnits))}, cols...)...)
	}
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d)/1e6) }

// sweepTime returns the median of 5 runs after one warm-up.
func sweepTime(t *testing.T, e *q.Engine, preds []q.Predicate, limit int, forceScan bool) time.Duration {
	t.Helper()
	qq := q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: limit, Direction: q.Backward}
	run := func() time.Duration {
		start := time.Now()
		var err error
		if forceScan {
			_, err = e.ExecuteScan(qq)
		} else {
			_, err = e.Execute(qq)
		}
		if err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	run() // warm the sidecar cache and the page cache
	ds := make([]time.Duration, 5)
	for i := range ds {
		ds[i] = run()
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[len(ds)/2]
}

// recOrdinal pulls the record ordinal back out of "GET /api/v1/orders/<i> ...", so the
// expected value can be recomputed without a parallel manifest.
func recOrdinal(msg string) int {
	const prefix = "GET /api/v1/orders/"
	n := 0
	for i := len(prefix); i < len(msg) && msg[i] >= '0' && msg[i] <= '9'; i++ {
		n = n*10 + int(msg[i]-'0')
	}
	return n
}
