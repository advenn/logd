package query_test

import (
	"fmt"
	"os"
	"path/filepath"
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

// buildMultiSegment ingests a varied dataset spanning several sealed segments and
// returns the engine plus the data dir (for the degrade test).
func buildMultiSegment(t *testing.T) (*qr.Engine, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.IndexConfig{
		Templates: []config.Template{
			{Name: "latency", Pattern: "took {ms:int}ms"},
			{Name: "trip", Pattern: "trip:{id:str}"},
		},
		Literals: []string{"panic:"},
	}
	eng, err := extract.Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Small segments force rotation → multiple sealed segments, each with its own .tidx.
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)

	levels := []model.LogLevel{model.LogLevelDebug, model.LogLevelInfo, model.LogLevelWarn, model.LogLevelError}
	svcs := []string{"api", "worker", "db"}
	for i := 0; i < 400; i++ {
		lat := (i * 7) % 1000
		msg := fmt.Sprintf("req took %dms trip:T%d", lat, i%40)
		if i%13 == 0 {
			msg = "panic: " + msg
		}
		e := model.LogEntry{
			TS:      time.Unix(t0+int64(i%100), 0).UTC(), // ties + out-of-order
			Level:   levels[i%4],
			Extra:   fmt.Sprintf(`{"region":"r%d","code":%d}`, i%3, 200+(i%3)*100),
			Message: msg,
		}
		e.ServiceID = w.InternService(svcs[i%3])
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	return qr.NewEngine(w, eng), dir
}

func equalResults(t *testing.T, label string, idx, scan []model.LogEntry) {
	t.Helper()
	if len(idx) != len(scan) {
		t.Fatalf("%s: index returned %d records, scan returned %d", label, len(idx), len(scan))
	}
	for i := range idx {
		a, b := idx[i], scan[i]
		if a.TS.UnixNano() != b.TS.UnixNano() || a.Message != b.Message || a.ServiceID != b.ServiceID || a.Level != b.Level || a.Extra != b.Extra {
			t.Fatalf("%s: record %d differs:\n index=%+v\n scan =%+v", label, i, a, b)
		}
	}
}

func intV(n int64) index.Value  { return index.Value{Kind: index.KindInt, Int: n} }
func strV(s string) index.Value { return index.Value{Kind: index.KindStr, Str: s} }

func withPreds(base qr.Query, p ...qr.Predicate) qr.Query { base.Preds = p; return base }

// TestPushdownStrRangeAndNegatives covers the two cases the main battery under-covers:
// lossy STRING RANGES (values >16 bytes sharing a 16-byte key prefix, so the index key
// collides and only re-verify can distinguish them) and NEGATIVE integer bounds. For
// each, pushdown must still equal scan.
func TestPushdownStrRangeAndNegatives(t *testing.T) {
	dir := t.TempDir()
	cfg := config.IndexConfig{Templates: []config.Template{
		{Name: "trip", Pattern: "trip:{id:str}"},
		{Name: "delta", Pattern: "delta {n:int} end"},
	}}
	eng, err := extract.Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.New(eng, w)

	const prefix = "PREFIX0123456789" // exactly 16 bytes → ids below share a full key prefix
	for i := 0; i < 250; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i) // 18+ bytes, 16-byte key collides across all of them
		if i%4 == 0 {
			id = fmt.Sprintf("short%d", i) // some short, distinct ids too
		}
		delta := (i%20)*10 - 100 // range -100..90, includes negatives and zero
		msg := fmt.Sprintf("trip:%s delta %d end", id, delta)
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i%50), 0).UTC(), Level: model.LogLevelInfo, Message: msg}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := qr.NewEngine(w, eng)
	full := qr.Query{Start: ts(0), End: ts(1000)}

	cases := []qr.Query{
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpGt, Value: intV(0)}),
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpLt, Value: intV(0)}),
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpGe, Value: intV(-100)}),
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpLe, Value: intV(-50)}),
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpEq, Value: intV(-100)}),
		withPreds(full, qr.TypedCompare{Field: "delta", Op: qr.OpGt, Value: intV(-1000)}), // below all → all
		withPreds(full, qr.TypedCompare{Field: "trip_id", Op: qr.OpGt, Value: strV(prefix + "-5")}),
		withPreds(full, qr.TypedCompare{Field: "trip_id", Op: qr.OpLt, Value: strV(prefix + "-5")}),
		withPreds(full, qr.TypedCompare{Field: "trip_id", Op: qr.OpGe, Value: strV(prefix + "-100")}),
		withPreds(full, qr.TypedCompare{Field: "trip_id", Op: qr.OpEq, Value: strV(prefix + "-42")}),
	}
	for i, qc := range cases {
		idx, err := e.Execute(qc)
		if err != nil {
			t.Fatalf("case %d Execute: %v", i, err)
		}
		scan, err := e.ExecuteScan(qc)
		if err != nil {
			t.Fatalf("case %d ExecuteScan: %v", i, err)
		}
		equalResults(t, fmt.Sprintf("strneg case %d", i), idx, scan)
	}
}

// TestPushdownEqualsScan is THE Phase-5 deliverable: for a battery of queries over a
// multi-segment dataset, the index-pushdown result must equal the scan-path result
// exactly (same ordered records).
func TestPushdownEqualsScan(t *testing.T) {
	e, _ := buildMultiSegment(t)
	full := qr.Query{Start: ts(0), End: ts(1000)}

	battery := []struct {
		name  string
		preds []qr.Predicate
		limit int
		dir   qr.Direction
	}{
		{name: "no preds"},
		{name: "latency > 500", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}},
		{name: "latency >= 500", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(500)}}},
		{name: "latency < 100", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpLt, Value: intV(100)}}},
		{name: "latency <= 100", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpLe, Value: intV(100)}}},
		{name: "latency == 700", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpEq, Value: intV(700)}}},
		{name: "latency == 999999(none)", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpEq, Value: intV(999999)}}},
		{name: "latency != 0 (residual)", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpNe, Value: intV(0)}}},
		{name: "trip_id == T5", preds: []qr.Predicate{qr.TypedCompare{Field: "trip_id", Op: qr.OpEq, Value: strV("T5")}}},
		{name: "line literal panic:", preds: []qr.Predicate{qr.LineContains{Sub: "panic:"}}},
		{name: "line non-literal took (scan)", preds: []qr.Predicate{qr.LineContains{Sub: "took"}}},
		{name: "label level=ERROR", preds: []qr.Predicate{qr.LabelEqual{Key: "level", Value: "ERROR"}}},
		{name: "label region=r1", preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "r1"}}},
		{name: "label code=300(numeric)", preds: []qr.Predicate{qr.LabelEqual{Key: "code", Value: "300"}}},
		{name: "label region!=r0", preds: []qr.Predicate{qr.LabelNotEqual{Key: "region", Value: "r0"}}},
		{name: "latency in (100,900) intersect", preds: []qr.Predicate{
			qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(100)},
			qr.TypedCompare{Field: "latency_ms", Op: qr.OpLt, Value: intV(900)},
		}},
		{name: "latency>=200 AND level=ERROR", preds: []qr.Predicate{
			qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(200)},
			qr.LabelEqual{Key: "level", Value: "ERROR"},
		}},
		{name: "panic: AND latency>500", preds: []qr.Predicate{
			qr.LineContains{Sub: "panic:"},
			qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)},
		}},
		{name: "latency>300 limit5 fwd", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(300)}}, limit: 5, dir: qr.Forward},
		{name: "latency>300 limit5 back", preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(300)}}, limit: 5, dir: qr.Backward},
	}

	for _, c := range battery {
		q := full
		q.Preds, q.Limit, q.Direction = c.preds, c.limit, c.dir
		idx, err := e.Execute(q)
		if err != nil {
			t.Fatalf("%s: Execute: %v", c.name, err)
		}
		scan, err := e.ExecuteScan(q)
		if err != nil {
			t.Fatalf("%s: ExecuteScan: %v", c.name, err)
		}
		equalResults(t, c.name, idx, scan)
	}

	// Time-window subset must also agree.
	q := qr.Query{Start: ts(20), End: ts(60), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(400)}}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "time window + latency>400", idx, scan)
}

// A mistyped predicate (query value's Kind differs from the field's indexed kind) must
// NOT push down — it would encode the key in the wrong kind-space and search the wrong
// index, diverging from the scan oracle. It must fall back to scan (residual).
func TestPushdownCrossKindDoesNotDiverge(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "score", Pattern: "score {v:float} pts"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.New(eng, w)
	for i, s := range []string{"score -12.5 pts", "score 0 pts", "score 7.25 pts", "score 900 pts"} {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: s}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := qr.NewEngine(w, eng)

	// Int value against a float field (a query-builder mistype) for every operator.
	for _, op := range []qr.Op{qr.OpGt, qr.OpLt, qr.OpGe, qr.OpLe, qr.OpEq} {
		q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "score_v", Op: op, Value: intV(0)}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, fmt.Sprintf("cross-kind op=%d", op), idx, scan)
	}
	// And it must be reported as a scan, not a (wrong) index push.
	for _, p := range e.Explain(qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "score_v", Op: qr.OpGt, Value: intV(0)}}}) {
		if p.Mode != "scan" {
			t.Fatalf("cross-kind predicate should not push down, Explain said %q", p.Mode)
		}
	}
}

// Two literals whose names differ only in a character the old sanitizer folded ('.'
// vs '_') must map to DISTINCT .tidx files, so each existence query returns the right
// records (not the other literal's).
func TestLiteralPathCollisionResolved(t *testing.T) {
	dir := t.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Literals: []string{"a.b", "a_b"}})
	if err != nil {
		t.Fatal(err)
	}
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.New(eng, w)
	ig.Ingest(model.LogEntry{TS: time.Unix(t0, 0).UTC(), Level: model.LogLevelInfo, Message: "x a.b y"})   // contains "a.b" only
	ig.Ingest(model.LogEntry{TS: time.Unix(t0+1, 0).UTC(), Level: model.LogLevelInfo, Message: "z a_b w"}) // contains "a_b" only
	w.Close()
	e := qr.NewEngine(w, eng)

	for _, tc := range []struct {
		sub    string
		wantTS int64
	}{{"a.b", ts(0)}, {"a_b", ts(1)}} {
		q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.LineContains{Sub: tc.sub}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, "literal-collision "+tc.sub, idx, scan)
		if len(idx) != 1 || idx[0].TS.UnixNano() != tc.wantTS {
			t.Fatalf("LineContains %q: got %d records, want exactly the one containing it", tc.sub, len(idx))
		}
	}
}

// A field that is NOT in a sealed segment's schema (e.g. a template added AFTER the
// segment was sealed) must cause that segment to be SCANNED and re-extracted at query
// time — never skipped — or the pushdown result would drop records the scan keeps.
func TestConfigChangeFieldScannedNotSkipped(t *testing.T) {
	dir := t.TempDir()
	// Seal segments with an engine that does NOT know the latency template.
	eng1, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "trip", Pattern: "trip:{id:str}"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng1.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.New(eng1, w)
	for i := 0; i < 200; i++ {
		// Messages DO contain "took Nms", but latency was not a template when sealed.
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i%50), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req took %dms trip:T%d", (i*7)%1000, i)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// "Config change": query with an engine that now DOES know latency.
	eng2, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "trip", Pattern: "trip:{id:str}"},
		{Name: "latency", Pattern: "took {ms:int}ms"},
	}})
	e := qr.NewEngine(w, eng2)
	q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}}

	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "config-change latency>500", idx, scan)
	if len(idx) == 0 {
		t.Fatal("old segments were not re-extracted for the newly-added field (records lost)")
	}
	// The sealed segments lack latency_ms in schema, so the plan must SCAN them.
	for _, p := range e.Explain(q) {
		if p.Mode != "scan" {
			t.Fatalf("segment with latency_ms absent from schema should scan, got %q", p.Mode)
		}
	}
}

// Explain must report that a typed query actually pushes down (index), and that a
// non-pushable query (label-only) scans.
func TestExplainReportsPushdown(t *testing.T) {
	e, _ := buildMultiSegment(t)

	idxPlans := e.Explain(qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}})
	usedIndex := false
	for _, p := range idxPlans {
		if p.Mode == "index" {
			usedIndex = true
		}
	}
	if !usedIndex {
		t.Fatal("typed query did not push down on any sealed segment")
	}

	scanPlans := e.Explain(qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.LabelEqual{Key: "level", Value: "INFO"}}})
	for _, p := range scanPlans {
		if p.Mode != "scan" {
			t.Fatalf("label-only query should scan, got %q", p.Mode)
		}
	}
}

// Deleting the .tidx files makes the segments degrade to scan — the result is unchanged.
func TestDegradeToScanWhenIndexMissing(t *testing.T) {
	e, dir := buildMultiSegment(t)
	q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}}

	want, _ := e.Execute(q) // with index

	matches, _ := filepath.Glob(filepath.Join(dir, "segments", "*.latency_ms.tidx"))
	if len(matches) == 0 {
		t.Fatal("no latency_ms .tidx files found to remove")
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			t.Fatal(err)
		}
	}
	got, err := e.Execute(q) // must degrade to scan
	if err != nil {
		t.Fatal(err)
	}
	equalResults(t, "degrade-to-scan", got, want)
}
