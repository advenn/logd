package query_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Once a label key breaches its value-cardinality cap, its over-cap values are dropped
// from the index but remain in Extra: queries must still find them (via scan), the key is
// recorded as capped, the planner scans it, and index == scan throughout.
func TestCardinalityCapKeepsResultsCorrect(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, err := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, MaxLabelCardinality: 3})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})

	// 5 distinct region values, cap 3 → eu,us,ap indexed; de,fr over-cap (Extra only).
	regions := []string{"eu", "us", "ap", "de", "fr"}
	for i, r := range regions {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, r), Message: "m-" + r}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := qr.NewEngineWithLabels(w, eng, []string{"region"})

	// The breached key is recorded in the segment manifest.
	capped := false
	for _, s := range mustLoad(t, dir).All() {
		for _, k := range s.CappedKeys {
			if k == "region" {
				capped = true
			}
		}
	}
	if !capped {
		t.Fatal("region should be recorded as a capped key after breaching the cap")
	}

	// Every value — including the over-cap de and fr — is found, and index == scan.
	for _, r := range regions {
		q := qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: r}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, "region="+r, idx, scan)
		if len(idx) != 1 {
			t.Fatalf("region=%s: got %d results, want 1 (over-cap values must still be found via scan)", r, len(idx))
		}
	}

	// The capped key scans the segment — even for an under-cap value like eu — because
	// the key's index is incomplete.
	for _, p := range e.Explain(qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}}}) {
		if p.Mode != "scan" {
			t.Fatalf("a breached (capped) key must scan, got %q", p.Mode)
		}
	}
}

// The cap across MANY segments (a value under-cap in one segment can be over-cap in
// another) must still satisfy index == scan, including intersected with a typed pushdown.
func TestCardinalityCapMultiSegmentEqualsScan(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 2 * storage.PageSize, FlushInterval: time.Hour, MaxLabelCardinality: 3})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	for i := 0; i < 300; i++ {
		ig.Ingest(model.LogEntry{
			TS: time.Unix(t0+int64(i%100), 0).UTC(), Level: model.LogLevelInfo,
			Extra:   fmt.Sprintf(`{"region":"r%d"}`, i%10), // 10 distinct values, cap 3/segment
			Message: fmt.Sprintf("took %dms", (i*7)%1000),
		})
	}
	w.Close()
	e := qr.NewEngineWithLabels(w, eng, []string{"region"})

	// Sanity: many segments, at least one with a breached key.
	segs := mustLoad(t, dir).All()
	anyCapped := false
	for _, s := range segs {
		if len(s.CappedKeys) > 0 {
			anyCapped = true
		}
	}
	if len(segs) < 3 || !anyCapped {
		t.Fatalf("expected multiple segments with a breached cap, got %d segments (capped=%v)", len(segs), anyCapped)
	}

	// Every region value equals scan; and intersected with a typed pushdown.
	for v := 0; v < 10; v++ {
		q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: fmt.Sprintf("r%d", v)}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, fmt.Sprintf("region=r%d", v), idx, scan)
	}
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{
		qr.LabelEqual{Key: "region", Value: "r5"},
		qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)},
	}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "region=r5 AND latency>500", idx, scan)
}

// A key that stays UNDER the cap still pushes down normally (the cap only affects breached
// keys), and index == scan.
func TestCardinalityUnderCapStillPushes(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, MaxLabelCardinality: 100})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	for i, r := range []string{"eu", "us", "eu", "us"} {
		ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, r), Message: "m"})
	}
	w.Close()
	e := qr.NewEngineWithLabels(w, eng, []string{"region"})

	for _, s := range mustLoad(t, dir).All() {
		if len(s.CappedKeys) != 0 {
			t.Fatalf("no key should be capped under the limit, got %v", s.CappedKeys)
		}
	}
	q := qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "region=eu", idx, scan)
	used := false
	for _, p := range e.Explain(q) {
		if p.Mode == "index" {
			used = true
		}
	}
	if !used {
		t.Fatal("an under-cap key should still push down to the index")
	}
}
