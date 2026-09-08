package query_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	q "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Benchmarks for the read path, written to answer a specific question: where does the time
// go on a query that returns only a handful of rows out of a large segment?
//
// The motivating measurement, against a live 500k-record daemon, was that
// `{app="x"}` with limit=1 took ~1.6s while Loki answered the same query in ~15ms.
// Execute collects every matching record, sorts all of them, and only then truncates to
// the limit (see engine.go), so a "last N lines" query pays for the whole corpus.
//
// Run:
//
//	go test ./core/query/ -run xxx -bench . -benchtime 3x -cpuprofile /tmp/cpu.out
//	go tool pprof -top -nodecount=25 /tmp/cpu.out

func benchCorpus(b *testing.B, n int) *q.Engine {
	b.Helper()
	dir := b.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	ig := ingest.NewWithLabels(eng, w, []string{"app"})
	for i := 0; i < n; i++ {
		// Latency distribution chosen so a ">2000" predicate is highly selective
		// (~1%) while ">0" matches everything, letting one corpus serve both shapes.
		ms := 10 + (i % 200)
		if i%100 == 0 {
			ms = 2000 + (i % 500)
		}
		e := model.LogEntry{
			TS:      time.Unix(t0+int64(i), 0).UTC(),
			Level:   model.LogLevelInfo,
			Extra:   `{"app":"bench"}`,
			Message: fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace=%08x", i, ms, i),
		}
		e.ServiceID = w.InternService("api")
		if err := ig.Ingest(e); err != nil {
			b.Fatal(err)
		}
	}
	w.Close() // flush + seal, so the segment carries a real .tidx
	return q.NewEngineWithLabels(w, eng, []string{"app"})
}

const benchN = 200000

// BenchmarkLabelOnlyLimit100 is the shape every Grafana Explore session opens with:
// "show me the last 100 lines". There is no typed predicate to push down, so the engine
// scans - but it should not have to materialize and sort the entire corpus to return 100
// rows. This benchmark exists to measure exactly that cost.
func BenchmarkLabelOnlyLimit100(b *testing.B) {
	e := benchCorpus(b, benchN)
	preds := []q.Predicate{q.LabelEqual{Key: "app", Value: "bench"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := e.Execute(q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: 100, Direction: q.Backward})
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != 100 {
			b.Fatalf("got %d rows, want 100", len(out))
		}
	}
}

// BenchmarkLabelOnlyLimit1 isolates the fixed cost still further: one row out of 200k.
// Any gap between this and Limit100 is result-size cost; everything else is work the
// engine does regardless of how few rows the caller asked for.
func BenchmarkLabelOnlyLimit1(b *testing.B) {
	e := benchCorpus(b, benchN)
	preds := []q.Predicate{q.LabelEqual{Key: "app", Value: "bench"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Execute(q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: 1, Direction: q.Backward}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTypedSelective is the index path on a highly selective predicate - the shape
// logd is built to win. Compare per-record cost here against the scan benchmarks above.
func BenchmarkTypedSelective(b *testing.B) {
	e := benchCorpus(b, benchN)
	preds := []q.Predicate{
		q.LabelEqual{Key: "app", Value: "bench"},
		q.TypedCompare{Field: "latency_ms", Op: q.OpGt, Value: intV(2000)},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Execute(q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: 100, Direction: q.Backward}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTypedBroad is the same index path on a predicate that matches almost
// everything, where the planner's cost guard should hand back to a sequential scan.
func BenchmarkTypedBroad(b *testing.B) {
	e := benchCorpus(b, benchN)
	preds := []q.Predicate{
		q.LabelEqual{Key: "app", Value: "bench"},
		q.TypedCompare{Field: "latency_ms", Op: q.OpGt, Value: intV(0)},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Execute(q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: 100, Direction: q.Backward}); err != nil {
			b.Fatal(err)
		}
	}
}
