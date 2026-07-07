package query_test

import (
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

// The cost guard: a broad candidate set (covering >50% of a segment) falls back to scan;
// a selective one uses the index. Either way the result equals ExecuteScan.
func TestCostGuard(t *testing.T) {
	e, dir := buildMultiSegment(t)

	// Records are recorded in the sealed manifest (the guard needs them).
	for _, s := range mustLoad(t, dir).All() {
		if s.Records == 0 {
			t.Fatalf("segment %s has no recorded record count", s.ID)
		}
	}

	full := qr.Query{Start: ts(0), End: ts(1000)}

	// Broad: latency >= 0 matches every latency record → guard trips → scan everywhere.
	broad := full
	broad.Preds = []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(0)}}
	for _, p := range e.Explain(broad) {
		if p.Mode != "scan" {
			t.Fatalf("broad query should hit the cost guard (scan), got %q (candidates %d)", p.Mode, p.Candidates)
		}
	}
	assertEqScan(t, e, broad, "broad latency>=0")

	// Selective: a single value → index.
	sel := full
	sel.Preds = []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpEq, Value: intV(700)}}
	usedIndex := false
	for _, p := range e.Explain(sel) {
		if p.Mode == "index" {
			usedIndex = true
		}
	}
	if !usedIndex {
		t.Fatal("a highly selective query should use the index, not scan")
	}
	assertEqScan(t, e, sel, "selective latency==700")
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
