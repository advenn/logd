package query_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// guardFixture is one sealed, compressed segment of n records whose latency is scattered
// (a hashed value) or clustered (falling with the record number, so a high threshold selects
// the oldest records, contiguous at the start of the segment).
func guardFixture(t *testing.T, n int, clustered bool) *qr.Engine {
	t.Helper()
	dir := t.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)
	for i := 0; i < n; i++ {
		ms := int((uint32(i) * 2654435761) % 1000)
		if clustered {
			ms = 999 - i*1000/n
		}
		e := model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req %05d took %dms", i, ms)}
		if err := ig.IngestCtx(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs := w.Manifest().All()
	if len(segs) != 1 || segs[0].Pages == 0 || segs[0].BlockPages == 0 {
		t.Fatalf("fixture: want one compressed segment with a recorded layout, got %+v", segs)
	}
	return qr.NewEngine(w, eng)
}

func planMode(t *testing.T, e *qr.Engine, q qr.Query) (mode, reason string) {
	t.Helper()
	plans := e.Explain(q)
	if len(plans) != 1 {
		t.Fatalf("want one segment planned, got %d", len(plans))
	}
	return plans[0].Mode, plans[0].Reason
}

// The cost guard scans a segment only when the candidates both over-fetch the query's limit
// many times over AND cover nearly every I/O unit (compressed block) of the segment. Either
// term alone is not enough: a scattered broad set under a limit that wants it all has no
// over-fetch, and a clustered one in a few blocks is cheap to fetch however far it overshoots
// the limit. Every case must still equal ExecuteScan.
func TestCostGuard(t *testing.T) {
	const n = 6000
	full := qr.Query{Start: ts(0), End: ts(n + 1)}
	gt := func(v int64) []qr.Predicate {
		return []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(v)}}
	}

	scattered := guardFixture(t, n, false)
	for _, c := range []struct {
		name       string
		preds      []qr.Predicate
		limit      int
		wantMode   string
		wantReason string
	}{
		// ~3000 candidates over every block, for 10 rows: the scan reaches 10 rows sooner.
		{"broad, limit 10", gt(499), 10, "scan", qr.ReasonCostGuard},
		// The same set wanted whole: nothing for the scan's early exit to win.
		{"broad, limit 100000", gt(499), 100000, "index", qr.ReasonIndexed},
		// ~6 candidates: fewer than the limit, so no over-fetch whatever the ratio.
		{"selective, limit 10", gt(998), 10, "index", qr.ReasonIndexed},
	} {
		q := full
		q.Preds, q.Limit = c.preds, c.limit
		if mode, reason := planMode(t, scattered, q); mode != c.wantMode || reason != c.wantReason {
			t.Errorf("scattered %s: planned %s/%s, want %s/%s", c.name, mode, reason, c.wantMode, c.wantReason)
		}
		assertEqScan(t, scattered, q, "scattered "+c.name)
	}

	// Clustered: the same ~3000-row over-fetch at limit 10, but in the oldest half of the
	// segment's blocks only. A backward scan would decode the whole newer half first.
	clustered := guardFixture(t, n, true)
	q := full
	q.Preds, q.Limit = gt(499), 10
	if mode, reason := planMode(t, clustered, q); mode != "index" {
		t.Errorf("clustered broad, limit 10: planned %s/%s, want the index", mode, reason)
	}
	assertEqScan(t, clustered, q, "clustered broad, limit 10")

	// The metric path keeps every match, so the broad set that tripped the guard above is
	// fetched by offset there, and must count the same as a scan would.
	mq := qr.MetricQuery{Preds: gt(499), RangeOp: qr.RangeCount, Range: int64(n+1) * 1e9, Start: ts(n), End: ts(n), Step: 1e9}
	got, err := scattered.ExecuteMetric(mq)
	if err != nil {
		t.Fatal(err)
	}
	broad, err := scattered.ExecuteScan(qr.Query{Start: ts(0), End: ts(n + 1), Limit: n, Preds: gt(499)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Points) != 1 || int(got[0].Points[0].Value) != len(broad) {
		t.Fatalf("count_over_time of the broad set: got %+v, want %d", got, len(broad))
	}
}

// Label discovery must show the active segment's labels BEFORE any seal (Grafana
// dropdowns shouldn't be empty on a fresh daemon).
func TestLiveLabelsBeforeSeal(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	defer w.Close()
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	e := qr.NewEngineWithLabels(w, eng, []string{"region"})

	// Ingest labeled records but do NOT close/seal.
	for i, region := range []string{"us", "eu", "us"} {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, region), Message: "m"}); err != nil {
			t.Fatal(err)
		}
	}
	// Give the writer goroutine a moment to process the async writes.
	time.Sleep(50 * time.Millisecond)

	found := false
	for _, k := range e.Labels() {
		if k == "region" {
			found = true
		}
	}
	if !found {
		t.Fatalf("live label 'region' not discovered before seal: %v", e.Labels())
	}
	vals := map[string]bool{}
	for _, v := range e.LabelValues("region") {
		vals[v] = true
	}
	if !vals["us"] || !vals["eu"] {
		t.Fatalf("live label values incomplete: %v", e.LabelValues("region"))
	}
	if len(e.Series()) == 0 {
		t.Fatal("live streams not reported in /series before seal")
	}
}

// A retention race — a segment's files deleted between planning and reading — degrades to
// skipping that segment, not a query error.
func TestRetentionRaceDegrades(t *testing.T) {
	e, dir := buildMultiSegment(t)
	// High limit so the deleted segment's absence is visible (not masked by the default 100).
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{qr.LineContains{Sub: "took"}}}
	before, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}

	// Delete one whole segment (as retention would), then query again.
	logs, _ := filepath.Glob(filepath.Join(dir, "segments", "*.log"))
	if len(logs) < 2 {
		t.Skip("need multiple segments")
	}
	base := logs[0][:len(logs[0])-len(".log")]
	sidecars, _ := filepath.Glob(base + ".*")
	for _, f := range sidecars {
		os.Remove(f)
	}

	after, err := e.Execute(q)
	if err != nil {
		t.Fatalf("query errored on a retention-deleted segment instead of degrading: %v", err)
	}
	if len(after) == 0 || len(after) >= len(before) {
		t.Fatalf("expected fewer (but some) results after a segment was deleted: before=%d after=%d", len(before), len(after))
	}
}

func mustLoad(t *testing.T, dir string) *storage.Manifest {
	t.Helper()
	m, err := storage.LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func assertEqScan(t *testing.T, e *qr.Engine, q qr.Query, label string) {
	t.Helper()
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, label, idx, scan)
}
