package query_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	q "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

const t0 = int64(1700000000)

func ts(sec int) int64 { return (t0 + int64(sec)) * 1e9 }

type rec struct {
	sec   int
	level model.LogLevel
	svc   string
	extra string
	msg   string
}

func testCfg() config.IndexConfig {
	return config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
		{Name: "trip", Pattern: "trip:{id:str}"},
	}}
}

// buildAndClose ingests records through the real pipeline and closes (flush+seal) so
// Execute reads them from disk. The returned engine still references the (closed)
// writer, which is fine for read-only queries.
func buildAndClose(t *testing.T, recs []rec) *q.Engine {
	t.Helper()
	dir := t.TempDir()
	eng, err := extract.Compile(testCfg())
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)
	for _, r := range recs {
		e := model.LogEntry{TS: time.Unix(t0+int64(r.sec), 0).UTC(), Level: r.level, Extra: r.extra, Message: r.msg}
		e.ServiceID = w.InternService(r.svc)
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	return q.NewEngine(w, eng)
}

func sampleRecs() []rec {
	return []rec{
		{0, model.LogLevelInfo, "api", `{"region":"us"}`, "request took 50ms"},
		{1, model.LogLevelError, "api", `{"region":"eu"}`, "request took 250ms failed"},
		{2, model.LogLevelInfo, "worker", "", "trip:ABC done"},
		{3, model.LogLevelWarn, "api", `{"region":"us"}`, "slow took 900ms"},
		{4, model.LogLevelInfo, "worker", "", "trip:XYZ done"},
	}
}

func run(t *testing.T, e *q.Engine, query q.Query) []model.LogEntry {
	t.Helper()
	out, err := e.Execute(query)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestExecuteTimeWindow(t *testing.T) {
	e := buildAndClose(t, sampleRecs())
	got := run(t, e, q.Query{Start: ts(1), End: ts(3)})
	if len(got) != 3 { // secs 1,2,3
		t.Fatalf("time window [1,3]: got %d want 3", len(got))
	}
}

func TestExecuteOrderingAndLimitAndDirection(t *testing.T) {
	e := buildAndClose(t, sampleRecs())

	back := run(t, e, q.Query{Start: ts(0), End: ts(4), Direction: q.Backward})
	if back[0].TS.UnixNano() != ts(4) || back[4].TS.UnixNano() != ts(0) {
		t.Fatalf("backward not newest-first: %d..%d", back[0].TS.Unix(), back[4].TS.Unix())
	}
	fwd := run(t, e, q.Query{Start: ts(0), End: ts(4), Direction: q.Forward})
	if fwd[0].TS.UnixNano() != ts(0) || fwd[4].TS.UnixNano() != ts(4) {
		t.Fatalf("forward not oldest-first")
	}
	lim := run(t, e, q.Query{Start: ts(0), End: ts(4), Direction: q.Forward, Limit: 2})
	if len(lim) != 2 || lim[0].TS.UnixNano() != ts(0) || lim[1].TS.UnixNano() != ts(1) {
		t.Fatalf("limit after sort wrong: %d", len(lim))
	}
}

func TestExecuteSortsOutOfOrderArrivals(t *testing.T) {
	// Ingest in NON-time order; Execute must still return time-sorted.
	e := buildAndClose(t, []rec{
		{5, model.LogLevelInfo, "s", "", "e took 1ms"},
		{1, model.LogLevelInfo, "s", "", "a took 1ms"},
		{9, model.LogLevelInfo, "s", "", "i took 1ms"},
		{3, model.LogLevelInfo, "s", "", "c took 1ms"},
	})
	got := run(t, e, q.Query{Start: ts(0), End: ts(100), Direction: q.Forward})
	last := int64(-1)
	for _, r := range got {
		if r.TS.UnixNano() <= last {
			t.Fatalf("not ascending: %d after %d", r.TS.UnixNano(), last)
		}
		last = r.TS.UnixNano()
	}
}

func TestExecuteLineFilters(t *testing.T) {
	e := buildAndClose(t, sampleRecs())
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LineContains{Sub: "trip:"}}}); len(got) != 2 {
		t.Fatalf("contains trip: got %d want 2", len(got))
	}
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LineNotContains{Sub: "took"}}}); len(got) != 2 {
		t.Fatalf("not-contains took: got %d want 2", len(got)) // the two trip records
	}
	re := regexp.MustCompile(`took \d{3}ms`)
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LineRegex{Re: re}}}); len(got) != 2 {
		t.Fatalf("regex 3-digit took: got %d want 2", len(got)) // 250, 900
	}
}

func TestExecuteLabelFilters(t *testing.T) {
	e := buildAndClose(t, sampleRecs())
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelEqual{Key: "level", Value: "INFO"}}}); len(got) != 3 {
		t.Fatalf("level=INFO: got %d want 3", len(got))
	}
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelEqual{Key: "service", Value: "api"}}}); len(got) != 3 {
		t.Fatalf("service=api: got %d want 3", len(got))
	}
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelEqual{Key: "region", Value: "us"}}}); len(got) != 2 {
		t.Fatalf("region=us (Extra): got %d want 2", len(got))
	}
	// region!=us includes records with region=eu AND records with no region label.
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelNotEqual{Key: "region", Value: "us"}}}); len(got) != 3 {
		t.Fatalf("region!=us (empty-label semantics): got %d want 3", len(got))
	}
}

func TestExecuteTypedCompare(t *testing.T) {
	e := buildAndClose(t, sampleRecs()) // latency values: 50, 250, 900
	intVal := func(n int64) index.Value { return index.Value{Kind: index.KindInt, Int: n} }

	cases := []struct {
		op   q.Op
		v    int64
		want int
	}{
		{q.OpGt, 200, 2}, // 250, 900
		{q.OpGe, 250, 2}, // 250, 900
		{q.OpLt, 250, 1}, // 50
		{q.OpEq, 50, 1},  // 50
		{q.OpNe, 50, 4},  // 250,900 present-and-≠, plus the two trip records (no latency → absent satisfies !=)
	}
	for _, c := range cases {
		got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.TypedCompare{Field: "latency_ms", Op: c.op, Value: intVal(c.v)}}})
		if len(got) != c.want {
			t.Fatalf("latency_ms op=%d val=%d: got %d want %d", c.op, c.v, len(got), c.want)
		}
	}
}

// Non-string JSON label values (numbers, bools) must resolve to their exact textual
// form so equality/inequality label filters work, rather than reading as absent.
func TestExecuteNumericAndBoolLabels(t *testing.T) {
	e := buildAndClose(t, []rec{
		{0, model.LogLevelInfo, "s", `{"code":500,"ok":false}`, "boom"},
		{1, model.LogLevelInfo, "s", `{"code":200,"ok":true}`, "fine"},
	})
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelEqual{Key: "code", Value: "500"}}}); len(got) != 1 || got[0].TS.UnixNano() != ts(0) {
		t.Fatalf("code=500 numeric label: got %d want the boom record", len(got))
	}
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelEqual{Key: "ok", Value: "true"}}}); len(got) != 1 || got[0].TS.UnixNano() != ts(1) {
		t.Fatalf("ok=true bool label: got %d want the fine record", len(got))
	}
	// The record whose code IS 500 must NOT be included by code!="500" (was a false
	// positive when numeric values resolved as absent).
	if got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{q.LabelNotEqual{Key: "code", Value: "500"}}}); len(got) != 1 || got[0].TS.UnixNano() != ts(1) {
		t.Fatalf("code!=500: got %d want only the code=200 record", len(got))
	}
}

// The scan path compares FULL string values, so two ids sharing a 16-byte key prefix
// (a lossy-key collision) are still distinguished exactly.
func TestExecuteTypedStrExactBeyond16Bytes(t *testing.T) {
	e := buildAndClose(t, []rec{
		{0, model.LogLevelInfo, "s", "", "trip:AAAAAAAAAAAAAAAA1 done"}, // 17-char id ...A1
		{1, model.LogLevelInfo, "s", "", "trip:AAAAAAAAAAAAAAAA2 done"}, // ...A2, same 16-byte key prefix
	})
	got := run(t, e, q.Query{Start: ts(0), End: ts(9), Preds: []q.Predicate{
		q.TypedCompare{Field: "trip_id", Op: q.OpEq, Value: index.Value{Kind: index.KindStr, Str: "AAAAAAAAAAAAAAAA1"}},
	}})
	if len(got) != 1 || got[0].TS.UnixNano() != ts(0) {
		t.Fatalf("exact str beyond 16 bytes: got %d records, want exactly the ...A1 record", len(got))
	}
}
