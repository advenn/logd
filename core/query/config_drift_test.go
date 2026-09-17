package query_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Configuration drift: the index config (label allowlist, templates) changes between the
// time a segment is sealed and the time it is queried. Indexes are built once, at seal, so
// every pushdown decision has to be made against the configuration the SEGMENT was built
// with. Each test here failed before segments recorded that configuration: the index path
// and the scan path returned different rows.

func compileOrFail(t *testing.T, cfg config.IndexConfig) *extract.Engine {
	t.Helper()
	eng, err := extract.Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func assertAllScan(t *testing.T, label string, e *qr.Engine, q qr.Query) {
	t.Helper()
	plans := e.Explain(q)
	if len(plans) == 0 {
		t.Fatalf("%s: no segments planned", label)
	}
	for _, p := range plans {
		if p.Mode != "scan" {
			t.Fatalf("%s: segment %s planned %q (%s), want scan", label, p.SegmentID, p.Mode, p.Reason)
		}
	}
}

// A label key added to the allowlist after data was sealed is absent from those segments'
// label indexes. Pushing it returned 0 rows for segments full of matches.
func TestAllowlistGrownAfterSealEqualsScan(t *testing.T) {
	for _, recorded := range []bool{true, false} {
		t.Run(fmt.Sprintf("allowlist_recorded=%v", recorded), func(t *testing.T) {
			dir := t.TempDir()
			eng := compileOrFail(t, config.IndexConfig{})
			opts := storage.Options{SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour}
			if recorded {
				opts.LabelKeys = []string{"region"}
			}
			w, err := storage.NewWriter(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			w.Start(nil)
			ig := ingest.NewWithLabels(eng, w, []string{"region"}) // env NOT allowlisted at ingest
			const n = 200
			for i := 0; i < n; i++ {
				e := model.LogEntry{
					TS:      time.Unix(t0+int64(i), 0).UTC(),
					Level:   model.LogLevelInfo,
					Extra:   fmt.Sprintf(`{"region":"r%d","env":"prod"}`, i%2),
					Message: fmt.Sprintf("m%03d", i),
				}
				if err := ig.Ingest(e); err != nil {
					t.Fatal(err)
				}
			}
			w.Close()
			if len(w.Manifest().All()) < 2 {
				t.Fatal("test setup: expected several sealed segments")
			}

			// The allowlist has since grown to include env.
			e := qr.NewEngineWithLabels(w, eng, []string{"region", "env"})
			envOnly := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{qr.LabelEqual{Key: "env", Value: "prod"}}}
			idx, err := e.Execute(envOnly)
			if err != nil {
				t.Fatal(err)
			}
			scan, err := e.ExecuteScan(envOnly)
			if err != nil {
				t.Fatal(err)
			}
			equalResults(t, `{env="prod"}`, idx, scan)
			if len(idx) != n {
				t.Fatalf(`{env="prod"} returned %d rows, want all %d`, len(idx), n)
			}
			assertAllScan(t, `{env="prod"}`, e, envOnly)

			// A key the segments DID index keeps pushing down alongside the new one.
			both := envOnly
			both.Preds = []qr.Predicate{qr.LabelEqual{Key: "env", Value: "prod"}, qr.LabelEqual{Key: "region", Value: "r1"}}
			idx, _ = e.Execute(both)
			scan, _ = e.ExecuteScan(both)
			equalResults(t, `{env="prod", region="r1"}`, idx, scan)
			if len(idx) != n/2 {
				t.Fatalf(`{env="prod", region="r1"} returned %d rows, want %d`, len(idx), n/2)
			}
			indexed := false
			for _, p := range e.Explain(both) {
				if p.Mode == "index" {
					indexed = true
				}
			}
			if !indexed {
				t.Fatal("region, which the segments did index, should still push down")
			}
		})
	}
}

// When the segment recorded its allowlist, a key that was allowlisted but never appeared in
// the segment is known to be absent: the lookup still runs and prunes the segment to nothing.
// The fix must not turn that into a scan.
func TestAllowlistedLabelAbsentFromSegmentStillPrunes(t *testing.T) {
	dir := t.TempDir()
	eng := compileOrFail(t, config.IndexConfig{})
	allow := []string{"region", "env"}
	w, err := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour, LabelKeys: allow})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, allow)
	for i := 0; i < 200; i++ {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: `{"region":"eu"}`, Message: fmt.Sprintf("m%03d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	e := qr.NewEngineWithLabels(w, eng, allow)
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{qr.LabelEqual{Key: "env", Value: "prod"}}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, `{env="prod"}`, idx, scan)
	for _, p := range e.Explain(q) {
		if p.Mode != "index" || p.Candidates != 0 {
			t.Fatalf("segment %s: planned %q with %d candidates, want index with 0", p.SegmentID, p.Mode, p.Candidates)
		}
	}
}

// Editing a template's pattern while keeping its name keeps the field name. An index built
// from the old pattern then answered for a different definition of latency_ms than the scan,
// which extracts with the new one: the two paths returned disjoint rows.
func TestTemplateEditedAfterSealEqualsScan(t *testing.T) {
	dir := t.TempDir()
	old := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took={ms:int}ms"}}})
	w, err := storage.NewWriter(dir, storage.Options{Schema: old.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(old, w)
	for i := 0; i < 300; i++ {
		msg := fmt.Sprintf("req %03d took=%dms", i, (i*7)%1000)
		if i%2 == 1 {
			msg = fmt.Sprintf("req %03d took %dms", i, (i*7)%1000)
		}
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: msg}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	edited := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	e := qr.NewEngine(w, edited)
	for _, op := range []qr.Op{qr.OpGt, qr.OpLt, qr.OpEq, qr.OpGe} {
		q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{
			qr.TypedCompare{Field: "latency_ms", Op: op, Value: index.Value{Kind: index.KindInt, Int: 497}},
		}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, fmt.Sprintf("edited template op=%d", op), idx, scan)
		assertAllScan(t, fmt.Sprintf("edited template op=%d", op), e, q)
	}
	nonEmpty, _ := e.ExecuteScan(qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{
		qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: index.Value{Kind: index.KindInt, Int: 497}},
	}})
	if len(nonEmpty) == 0 {
		t.Fatal("test setup: the edited template should match some records")
	}
}

// A template removed from the config: the query engine no longer extracts the field, so the
// scan finds no values, but the old segments still hold a .tidx for it.
func TestTemplateRemovedAfterSealEqualsScan(t *testing.T) {
	dir := t.TempDir()
	old := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, err := storage.NewWriter(dir, storage.Options{Schema: old.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(old, w)
	for i := 0; i < 200; i++ {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req took %dms", (i*7)%1000)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{
		qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: index.Value{Kind: index.KindInt, Int: 500}},
	}}
	for name, e := range map[string]*qr.Engine{
		"engine without the template": qr.NewEngine(w, compileOrFail(t, config.IndexConfig{})),
		"no extraction engine":        qr.NewEngine(w, nil),
	} {
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, name, idx, scan)
		assertAllScan(t, name, e, q)
	}
}

// Segments sealed before patterns were recorded carry an empty pattern. Nothing proves their
// index agrees with the current template, so typed predicates scan them.
func TestSegmentWithoutRecordedPatternScans(t *testing.T) {
	dir := t.TempDir()
	eng := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	legacySchema := []index.FieldType{{Name: "latency_ms", Kind: index.KindInt}} // no Pattern, as before
	w, err := storage.NewWriter(dir, storage.Options{Schema: legacySchema, SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)
	for i := 0; i < 200; i++ {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req took %dms", (i*7)%1000)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	e := qr.NewEngine(w, eng)
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Preds: []qr.Predicate{
		qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: index.Value{Kind: index.KindInt, Int: 500}},
	}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "legacy schema", idx, scan)
	if len(idx) == 0 {
		t.Fatal("test setup: expected matches")
	}
	assertAllScan(t, "legacy schema", e, q)
}
