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

// Read benchmarks over the ACTIVE segment: the layout of T4 in the evaluation, where a
// quarter of the corpus was still unsealed and logd's advantage disappeared because that
// segment was always scanned.
//
// The corpus stays in one unsealed segment (huge size limit, writer left open), with every
// page flushed by the tick before the clock starts. Latencies are hashed, so the matching
// records are scattered across the segment rather than clustered — the shape the selective
// columns of T1 have.
//
//	go test ./core/query/ -run '^$' -bench ActiveSegment -benchtime 10x

const activeBenchN = 200000

// activeMS is a deterministic value in [0, 100000) for record i, scattered by Knuth's
// multiplicative hash so `> 95000` selects ~5% of records spread over every page.
func activeMS(i int) int { return int((uint32(i) * 2654435761) % 100000) }

func activeCorpus(b *testing.B) *q.Engine {
	b.Helper()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	w, err := storage.NewWriter(b.TempDir(), storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    10 * time.Millisecond,
		SyncInterval:     50 * time.Millisecond,
		LabelKeys:        []string{"app"},
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { w.Close() })
	ig := ingest.NewWithLabels(eng, w, []string{"app"})
	for i := 0; i < activeBenchN; i++ {
		e := model.LogEntry{
			TS:      time.Unix(t0+int64(i), 0).UTC(),
			Level:   model.LogLevelInfo,
			Extra:   `{"app":"bench"}`,
			Message: fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace=%08x", i, activeMS(i), i),
		}
		for ig.Ingest(e) != nil { // wait out a full queue rather than lose records
			time.Sleep(50 * time.Microsecond)
		}
	}
	// Let the tick write the last partial page, so both paths see every record.
	time.Sleep(200 * time.Millisecond)
	if segs := w.Manifest().All(); len(segs) != 1 || segs[0].State != storage.SegmentActive {
		b.Fatalf("want one active segment, got %d segments", len(segs))
	}
	return q.NewEngineWithLabels(w, eng, []string{"app"})
}

func BenchmarkActiveSegment(b *testing.B) {
	e := activeCorpus(b)
	for _, sel := range []struct {
		name string
		thr  int64
	}{{"5pct", 95000}, {"1pct", 99000}, {"0.1pct", 99900}} {
		preds := []q.Predicate{
			q.LabelEqual{Key: "app", Value: "bench"},
			q.TypedCompare{Field: "latency_ms", Op: q.OpGt, Value: intV(sel.thr)},
		}
		for _, lim := range []struct {
			name  string
			limit int
		}{{"limit100", 100}, {"all", activeBenchN}} {
			qq := q.Query{Start: 0, End: 1 << 62, Preds: preds, Limit: lim.limit, Direction: q.Backward}
			for _, path := range []struct {
				name string
				run  func(q.Query) ([]model.LogEntry, error)
			}{{"index", e.Execute}, {"scan", e.ExecuteScan}} {
				b.Run(sel.name+"/"+lim.name+"/"+path.name, func(b *testing.B) {
					rows := 0
					for i := 0; i < b.N; i++ {
						out, err := path.run(qq)
						if err != nil {
							b.Fatal(err)
						}
						rows = len(out)
					}
					b.ReportMetric(float64(rows), "rows")
				})
			}
		}
	}
}
