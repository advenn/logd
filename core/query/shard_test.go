package query_test

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// shardDataset is a deterministic set of labelled, latency-carrying records with many
// timestamp ties (i%100), spread across three regions.
func shardDataset() []model.LogEntry {
	baseT := time.Unix(1700000000, 0).UTC()
	regions := []string{"us", "eu", "ap"}
	var recs []model.LogEntry
	for i := 0; i < 400; i++ {
		recs = append(recs, model.LogEntry{
			TS:      baseT.Add(time.Duration(i%100) * time.Second),
			Level:   model.LogLevelInfo,
			Extra:   fmt.Sprintf(`{"region":%q}`, regions[i%3]),
			Message: fmt.Sprintf("req took %dms", (i*7)%1000),
		})
	}
	return recs
}

// ingestSharded ingests the records across n shard folders (routed) and returns a
// fan-in engine over them. n=1 is the plain single-shard engine.
func ingestSharded(t *testing.T, n int, recs []model.LogEntry) *qr.Engine {
	t.Helper()
	root := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	var writers []*storage.Writer
	var shards []qr.Shard
	for i := 0; i < n; i++ {
		w, err := storage.NewWriter(filepath.Join(root, fmt.Sprintf("shard-%04d", i)),
			storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		w.Start(nil)
		writers = append(writers, w)
		shards = append(shards, w)
	}
	ig := ingest.NewShardedWithLabels(eng, writers, []string{"region"})
	for _, r := range recs {
		if err := ig.Ingest(r); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range writers {
		w.Close()
	}
	return qr.NewShardedEngine(shards, eng, []string{"region"})
}

func logMultiset(t *testing.T, e *qr.Engine, q qr.Query) []string {
	t.Helper()
	recs, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = fmt.Sprintf("%d|%s", r.TS.UnixNano(), r.Message)
	}
	sort.Strings(out) // compare as a multiset (tie order across shards is unspecified)
	return out
}

func metricMap(t *testing.T, e *qr.Engine, mq qr.MetricQuery) map[string]map[int64]float64 {
	t.Helper()
	series, err := e.ExecuteMetric(mq)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[int64]float64{}
	for _, s := range series {
		key := ""
		for _, k := range sortedKeys(s.Labels) {
			key += k + "=" + s.Labels[k] + ","
		}
		pts := map[int64]float64{}
		for _, p := range s.Points {
			pts[p.TS] = p.Value
		}
		out[key] = pts
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// The fan-in oracle: querying N shards must return exactly what a single shard over the
// same data returns — for log queries, metric queries, and label discovery.
func TestShardedFanInEqualsSingle(t *testing.T) {
	recs := shardDataset()
	single := ingestSharded(t, 1, recs)
	sharded := ingestSharded(t, 4, recs)

	// Log query (high limit so nothing is truncated; compare as a multiset).
	lq := qr.Query{
		Start: ts(0), End: ts(1000), Limit: 100000,
		Preds: []qr.Predicate{
			qr.LabelEqual{Key: "region", Value: "eu"},
			qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)},
		},
	}
	if a, b := logMultiset(t, single, lq), logMultiset(t, sharded, lq); !equalStr(a, b) {
		t.Fatalf("log fan-in differs:\n single=%v\n sharded=%v", a, b)
	}
	// Prove the query actually matched something (else the equality is vacuous).
	if got := logMultiset(t, sharded, lq); len(got) == 0 {
		t.Fatal("expected some matches for region=eu AND latency>500")
	}

	// Metric query: sum by(region)(count_over_time[60s]) across the whole window.
	mq := qr.MetricQuery{
		Range: 60 * int64(time.Second), RangeOp: qr.RangeCount,
		VectorOp: qr.VecSum, Grouping: []string{"region"},
		Start: ts(0), End: ts(100), Step: 20 * int64(time.Second),
	}
	sm, dm := metricMap(t, single, mq), metricMap(t, sharded, mq)
	if fmt.Sprint(sm) != fmt.Sprint(dm) {
		t.Fatalf("metric fan-in differs:\n single=%v\n sharded=%v", sm, dm)
	}
	if len(dm) != 3 { // us, eu, ap
		t.Fatalf("expected 3 region series, got %d", len(dm))
	}

	// Discovery.
	if !equalStr(single.Labels(), sharded.Labels()) {
		t.Fatalf("Labels differ: %v vs %v", single.Labels(), sharded.Labels())
	}
	if !equalStr(single.LabelValues("region"), sharded.LabelValues("region")) {
		t.Fatalf("LabelValues differ: %v vs %v", single.LabelValues("region"), sharded.LabelValues("region"))
	}
	if a, b := seriesKeys(single), seriesKeys(sharded); !equalStr(a, b) {
		t.Fatalf("Series differ: %v vs %v", a, b)
	}
}

// Live tail across shards must deliver records regardless of which shard they route to.
func TestShardedTailFanIn(t *testing.T) {
	root := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	var writers []*storage.Writer
	var shards []qr.Shard
	for i := 0; i < 3; i++ {
		w, err := storage.NewWriter(filepath.Join(root, fmt.Sprintf("shard-%04d", i)), storage.Options{FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		w.Start(nil)
		defer w.Close()
		writers = append(writers, w)
		shards = append(shards, w)
	}
	ig := ingest.NewShardedWithLabels(eng, writers, []string{"region"})
	e := qr.NewShardedEngine(shards, eng, []string{"region"})

	ch, cancel := e.Tail(nil, 64) // nil preds → match all
	defer cancel()

	regions := []string{"us", "eu", "ap", "us", "eu"} // hash to (possibly) different shards
	for i, r := range regions {
		if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, r), Message: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(got) < len(regions) {
		select {
		case rec := <-ch:
			got[rec.Message] = true
		case <-timeout:
			t.Fatalf("received only %d/%d tail records across shards: %v", len(got), len(regions), got)
		}
	}
}

// With a deterministic tiebreaker, a limit that cuts through a timestamp-tie group must
// return the EXACT SAME ORDERED result set for a sharded and a single engine (not just
// the same multiset) — the fan-in invariant holds even at the boundary.
func TestShardedFanInLimitDeterministic(t *testing.T) {
	recs := shardDataset() // many i%100 timestamp ties, distinct messages
	single := ingestSharded(t, 1, recs)
	sharded := ingestSharded(t, 5, recs)

	// {region="eu"} matches ~133 records; Limit 40 cuts through tie groups.
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 40, Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}}}
	sr, err := single.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	dr, err := sharded.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(sr) != 40 || len(dr) != 40 {
		t.Fatalf("expected 40 results each, got single=%d sharded=%d", len(sr), len(dr))
	}
	for i := range sr {
		if sr[i].TS != dr[i].TS || sr[i].Message != dr[i].Message {
			t.Fatalf("ordered result differs at %d: single=(%d,%q) sharded=(%d,%q)",
				i, sr[i].TS.UnixNano(), sr[i].Message, dr[i].TS.UnixNano(), dr[i].Message)
		}
	}
}

// A metric grouped by `service` must merge correctly across shards even though each shard
// interns the service name to its OWN ServiceID (AllLabels resolves ID→name per shard).
func TestShardedFanInServiceGrouping(t *testing.T) {
	baseT := time.Unix(1700000000, 0).UTC()
	svcs := []string{"api", "web", "api", "db", "web", "api"}
	var recs []model.LogEntry
	for i, s := range svcs {
		recs = append(recs, model.LogEntry{
			TS: baseT.Add(time.Duration(i) * time.Second), Level: model.LogLevelInfo,
			Extra: fmt.Sprintf(`{"service":%q}`, s), Message: "x",
		})
	}
	single := ingestSharded(t, 1, recs)
	sharded := ingestSharded(t, 4, recs)
	mq := qr.MetricQuery{
		Range: 60 * int64(time.Second), RangeOp: qr.RangeCount,
		VectorOp: qr.VecSum, Grouping: []string{"service"},
		Start: ts(10), End: ts(10), Step: int64(time.Second), // window (t0-50s, t0+10s] covers all 6
	}
	sm, dm := metricMap(t, single, mq), metricMap(t, sharded, mq)
	if fmt.Sprint(sm) != fmt.Sprint(dm) {
		t.Fatalf("service-grouped metric differs across shards:\n single=%v\n sharded=%v", sm, dm)
	}
	if len(dm) != 3 { // api, web, db — merged across shards despite per-shard ServiceIDs
		t.Fatalf("expected 3 service series, got %d: %v", len(dm), dm)
	}
}

func seriesKeys(e *qr.Engine) []string {
	var out []string
	for _, s := range e.Series() {
		key := ""
		for _, k := range sortedKeys(s) {
			key += k + "=" + s[k] + ","
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func equalStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
