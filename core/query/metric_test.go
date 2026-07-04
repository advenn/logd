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

// buildMetricData ingests records at (region, sec, latency) and returns a query engine.
func buildMetricData(t *testing.T, recs []struct {
	region  string
	sec     int
	latency int
}) *qr.Engine {
	t.Helper()
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	for _, r := range recs {
		ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(r.sec), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, r.region), Message: fmt.Sprintf("took %dms", r.latency)})
	}
	w.Close()
	return qr.NewEngineWithLabels(w, eng, []string{"region"})
}

func nanoAt(sec int) int64 { return (t0 + int64(sec)) * 1e9 }

// seriesFor finds the matrix series whose region label matches.
func seriesFor(m []qr.MatrixSeries, region string) (qr.MatrixSeries, bool) {
	for _, s := range m {
		if s.Labels["region"] == region {
			return s, true
		}
	}
	return qr.MatrixSeries{}, false
}

func pointAt(s qr.MatrixSeries, sec int) (float64, bool) {
	for _, p := range s.Points {
		if p.TS == nanoAt(sec) {
			return p.Value, true
		}
	}
	return 0, false
}

func TestMetricCountOverTime(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{
		{"eu", 5, 100}, {"eu", 15, 200}, {"eu", 25, 300},
		{"us", 5, 50}, {"us", 35, 900},
	})

	m, err := e.ExecuteMetric(qr.MetricQuery{
		Preds:   nil,
		Range:   10 * int64(time.Second),
		RangeOp: qr.RangeCount,
		Start:   nanoAt(0), End: nanoAt(40), Step: 10 * int64(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	eu, ok := seriesFor(m, "eu")
	if !ok {
		t.Fatal("no eu series")
	}
	for _, sec := range []int{10, 20, 30} { // one eu record lands in each window
		if v, ok := pointAt(eu, sec); !ok || v != 1 {
			t.Fatalf("eu count at +%ds: got %v,%v want 1", sec, v, ok)
		}
	}
	if _, ok := pointAt(eu, 0); ok {
		t.Fatal("eu should have no point at +0s (empty window)")
	}
	us, _ := seriesFor(m, "us")
	if v, ok := pointAt(us, 10); !ok || v != 1 {
		t.Fatalf("us count at +10s: got %v", v)
	}
	if v, ok := pointAt(us, 40); !ok || v != 1 {
		t.Fatalf("us count at +40s: got %v", v)
	}
}

func TestMetricRate(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{{"eu", 5, 1}, {"eu", 6, 1}}) // two records in the +10s window
	m, _ := e.ExecuteMetric(qr.MetricQuery{Range: 10 * int64(time.Second), RangeOp: qr.RangeRate, Start: nanoAt(10), End: nanoAt(10), Step: 10 * int64(time.Second)})
	eu, _ := seriesFor(m, "eu")
	if v, ok := pointAt(eu, 10); !ok || v != 0.2 { // 2 events / 10s
		t.Fatalf("rate: got %v want 0.2", v)
	}
}

func TestMetricSumByRegion(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{
		{"eu", 5, 1}, {"eu", 6, 1}, // 2 eu in +10s window
		{"us", 5, 1}, // 1 us
	})
	// sum by(region)(count_over_time[10s]) at +10s → eu=2, us=1
	m, _ := e.ExecuteMetric(qr.MetricQuery{
		Range: 10 * int64(time.Second), RangeOp: qr.RangeCount,
		VectorOp: qr.VecSum, Grouping: []string{"region"},
		Start: nanoAt(10), End: nanoAt(10), Step: 10 * int64(time.Second),
	})
	eu, _ := seriesFor(m, "eu")
	if v, _ := pointAt(eu, 10); v != 2 {
		t.Fatalf("sum by region eu: got %v want 2", v)
	}
	us, _ := seriesFor(m, "us")
	if v, _ := pointAt(us, 10); v != 1 {
		t.Fatalf("sum by region us: got %v want 1", v)
	}
	// Grouped labels contain ONLY region (level dropped by by()).
	if _, hasLevel := eu.Labels["level"]; hasLevel {
		t.Fatalf("by(region) should drop level: %v", eu.Labels)
	}
}

// rate over an unwrap sums the values / range-seconds (RangeRateSum).
func TestMetricRateSum(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{{"eu", 5, 100}, {"eu", 6, 200}}) // sum 300 over the 10s window
	m, _ := e.ExecuteMetric(qr.MetricQuery{
		Range: 10 * int64(time.Second), RangeOp: qr.RangeRateSum, Unwrap: "latency_ms",
		Start: nanoAt(10), End: nanoAt(10), Step: 10 * int64(time.Second),
	})
	eu, _ := seriesFor(m, "eu")
	if v, _ := pointAt(eu, 10); v != 30 { // 300 / 10s
		t.Fatalf("rate(unwrap): got %v want 30", v)
	}
}

// A resolution that would allocate an unbounded number of steps is rejected (DoS guard).
func TestMetricStepCapRejected(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{{"eu", 5, 1}})
	// 100000s range at a 1ns step → ~1e14 steps, far over the cap.
	_, err := e.ExecuteMetric(qr.MetricQuery{Range: int64(time.Second), RangeOp: qr.RangeCount, Start: nanoAt(0), End: nanoAt(100000), Step: 1})
	if err == nil {
		t.Fatal("expected a step-cap error for an over-fine resolution")
	}
}

func TestMetricSumOverTimeUnwrap(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{{"eu", 5, 100}, {"eu", 6, 250}}) // two latencies in the +10s window
	m, _ := e.ExecuteMetric(qr.MetricQuery{
		Range: 10 * int64(time.Second), RangeOp: qr.RangeSum, Unwrap: "latency_ms",
		Start: nanoAt(10), End: nanoAt(10), Step: 10 * int64(time.Second),
	})
	eu, _ := seriesFor(m, "eu")
	if v, _ := pointAt(eu, 10); v != 350 { // 100 + 250
		t.Fatalf("sum_over_time(unwrap latency_ms): got %v want 350", v)
	}
}
