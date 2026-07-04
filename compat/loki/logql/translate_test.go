package logql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

func translate(t *testing.T, q string, ex *extract.Engine) []query.Predicate {
	t.Helper()
	ast, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	preds, err := logql.Translate(ast, ex)
	if err != nil {
		t.Fatalf("translate %q: %v", q, err)
	}
	return preds
}

// A `| field op value` on a declared template capture becomes a native TypedCompare
// (which the planner pushes down); a plain label becomes a LabelEqual/compare.
func TestTranslateTemplateFieldVsLabel(t *testing.T) {
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})

	preds := translate(t, `{app="api"} | latency_ms > 200 | region = "eu"`, ex)
	if len(preds) != 3 {
		t.Fatalf("want 3 preds, got %d: %#v", len(preds), preds)
	}
	if le, ok := preds[0].(query.LabelEqual); !ok || le.Key != "app" || le.Value != "api" {
		t.Fatalf("pred[0] not LabelEqual{app,api}: %#v", preds[0])
	}
	tc, ok := preds[1].(query.TypedCompare)
	if !ok || tc.Field != "latency_ms" || tc.Op != query.OpGt || tc.Value.Kind != index.KindInt || tc.Value.Int != 200 {
		t.Fatalf("pred[1] not TypedCompare{latency_ms,>,200}: %#v", preds[1])
	}
	if le, ok := preds[2].(query.LabelEqual); !ok || le.Key != "region" || le.Value != "eu" {
		t.Fatalf("pred[2] not LabelEqual{region,eu}: %#v", preds[2])
	}
}

func TestTranslateLineFiltersAndRegex(t *testing.T) {
	preds := translate(t, `{a="b"} |= "panic:" != "debug" |~ "err.*" | status =~ "5.."`, nil)
	if _, ok := preds[1].(query.LineContains); !ok {
		t.Fatalf("pred[1] not LineContains: %#v", preds[1])
	}
	if _, ok := preds[2].(query.LineNotContains); !ok {
		t.Fatalf("pred[2] not LineNotContains: %#v", preds[2])
	}
	if _, ok := preds[3].(query.LineRegex); !ok {
		t.Fatalf("pred[3] not LineRegex: %#v", preds[3])
	}
	if _, ok := preds[4].(query.LabelRegex); !ok {
		t.Fatalf("pred[4] not LabelRegex: %#v", preds[4])
	}
}

// A LogQL filter on a uuid template field must translate to a TypedCompare (not 400).
func TestTranslateUUIDField(t *testing.T) {
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "order", Pattern: "order-{id:uuid} done"}}})
	preds := translate(t, `{a="b"} | order_id = "550e8400-e29b-41d4-a716-446655440000"`, ex)
	tc, ok := preds[1].(query.TypedCompare)
	if !ok || tc.Field != "order_id" || tc.Value.Kind != index.KindUUID {
		t.Fatalf("uuid field not translated to a uuid TypedCompare: %#v", preds[1])
	}
	if tc.Value.UUID[0] != 0x55 || tc.Value.UUID[15] != 0x00 {
		t.Fatalf("uuid not parsed correctly: %x", tc.Value.UUID)
	}
}

func TestTranslateParserStageUnsupported(t *testing.T) {
	ast, _ := logql.Parse(`{a="b"} | json`)
	if _, err := logql.Translate(ast, nil); err == nil {
		t.Fatal("expected an error for the unsupported | json stage")
	}
}

func TestParseMetricCountOverTime(t *testing.T) {
	ast, err := logql.ParseExpr(`count_over_time({app="api"} |= "err" [5m])`)
	if err != nil {
		t.Fatal(err)
	}
	me, ok := ast.(*logql.MetricExpr)
	if !ok {
		t.Fatalf("not a MetricExpr: %T", ast)
	}
	mq, err := logql.TranslateMetric(me, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mq.RangeOp != query.RangeCount || mq.VectorOp != query.VecNone || mq.Range != int64(5*time.Minute) {
		t.Fatalf("bad count_over_time translation: %+v", mq)
	}
	if len(mq.Preds) != 2 { // {app="api"} + |= "err"
		t.Fatalf("inner log preds: got %d want 2", len(mq.Preds))
	}
}

func TestParseMetricSumByRate(t *testing.T) {
	ast, _ := logql.ParseExpr(`sum by(region) (rate({app="api"}[1m]))`)
	me := ast.(*logql.MetricExpr)
	mq, err := logql.TranslateMetric(me, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mq.RangeOp != query.RangeRate || mq.VectorOp != query.VecSum || mq.Range != int64(time.Minute) {
		t.Fatalf("bad sum(rate) translation: %+v", mq)
	}
	if len(mq.Grouping) != 1 || mq.Grouping[0] != "region" || mq.Without {
		t.Fatalf("bad grouping: %+v", mq)
	}
}

func TestParseMetricUnwrap(t *testing.T) {
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	ast, _ := logql.ParseExpr(`avg_over_time({app="api"} | unwrap latency_ms [5m])`)
	me := ast.(*logql.MetricExpr)
	mq, err := logql.TranslateMetric(me, ex)
	if err != nil {
		t.Fatal(err)
	}
	if mq.RangeOp != query.RangeAvg || mq.Unwrap != "latency_ms" {
		t.Fatalf("bad unwrap translation: %+v", mq)
	}
}

func TestParseMetricErrors(t *testing.T) {
	// sum_over_time requires an unwrap.
	ast, _ := logql.ParseExpr(`sum_over_time({app="api"}[5m])`)
	if _, err := logql.TranslateMetric(ast.(*logql.MetricExpr), nil); err == nil {
		t.Fatal("sum_over_time without unwrap should error")
	}
	// Unknown function.
	if _, err := logql.ParseExpr(`topk(5, rate({a="b"}[5m]))`); err == nil {
		t.Fatal("unknown metric function should error")
	}
	// Bad duration.
	if _, err := logql.ParseExpr(`rate({a="b"}[5x])`); err != nil {
		// parse succeeds (5x is tokens); translate should reject the duration.
		return
	}
	ast, _ = logql.ParseExpr(`rate({a="b"}[5x])`)
	if _, err := logql.TranslateMetric(ast.(*logql.MetricExpr), nil); err == nil {
		t.Fatal("bad duration should error")
	}
}

// rate() over an unwrap is a value-rate (sum/range), reclassified to RangeRateSum; plain
// rate stays a count-rate.
func TestParseMetricRateUnwrap(t *testing.T) {
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	ast, _ := logql.ParseExpr(`rate({app="api"} | unwrap latency_ms [5m])`)
	mq, err := logql.TranslateMetric(ast.(*logql.MetricExpr), ex)
	if err != nil {
		t.Fatal(err)
	}
	if mq.RangeOp != query.RangeRateSum || mq.Unwrap != "latency_ms" {
		t.Fatalf("rate+unwrap should be RangeRateSum with unwrap set: %+v", mq)
	}
	ast2, _ := logql.ParseExpr(`rate({app="api"}[5m])`)
	mq2, _ := logql.TranslateMetric(ast2.(*logql.MetricExpr), nil)
	if mq2.RangeOp != query.RangeRate || mq2.Unwrap != "" {
		t.Fatalf("plain rate should stay RangeRate: %+v", mq2)
	}
}

// Negative, zero, and overflowing durations must be rejected, not silently coerced.
func TestParseMetricBadDurations(t *testing.T) {
	for _, q := range []string{`rate({a="b"}[-5m])`, `rate({a="b"}[0s])`, `rate({a="b"}[999999999999w])`} {
		ast, err := logql.ParseExpr(q)
		if err != nil {
			continue // rejected at parse — fine
		}
		if _, err := logql.TranslateMetric(ast.(*logql.MetricExpr), nil); err == nil {
			t.Fatalf("%s: expected a duration error", q)
		}
	}
}

// Trailing junk after a valid expression must be rejected (not silently dropped).
func TestParseTrailingJunk(t *testing.T) {
	if _, err := logql.ParseExpr(`count_over_time({a="b"}[5m]) @ junk`); err == nil {
		t.Fatal("trailing junk after a metric expr should be rejected")
	}
	if _, err := logql.Parse(`{a="b"} @`); err == nil {
		t.Fatal("trailing junk after a log query should be rejected")
	}
}

// End-to-end: a Grafana-style LogQL query with a typed field filter, run through the
// query engine, must equal the scan oracle AND actually push down.
func TestLogQLPushdownEqualsScan(t *testing.T) {
	dir := t.TempDir()
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: ex.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(ex, w, []string{"region"})
	base := time.Unix(1700000000, 0).UTC()
	for i := 0; i < 400; i++ {
		e := model.LogEntry{TS: base.Add(time.Duration(i%100) * time.Second), Level: model.LogLevelInfo,
			Extra: fmt.Sprintf(`{"region":"%s"}`, []string{"us", "eu", "ap"}[i%3]), Message: fmt.Sprintf("req took %dms", (i*7)%1000)}
		ig.Ingest(e)
	}
	w.Close()
	eng := query.NewEngineWithLabels(w, ex, []string{"region"})

	preds := translate(t, `{region="eu"} | latency_ms > 500`, ex)
	q := query.Query{Start: base.UnixNano(), End: base.Add(1000 * time.Second).UnixNano(), Preds: preds}

	idx, err := eng.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	scan, _ := eng.ExecuteScan(q)
	if len(idx) != len(scan) {
		t.Fatalf("pushdown %d != scan %d", len(idx), len(scan))
	}
	for i := range idx {
		if idx[i].TS.UnixNano() != scan[i].TS.UnixNano() || idx[i].Message != scan[i].Message {
			t.Fatalf("record %d differs", i)
		}
	}
	if len(idx) == 0 {
		t.Fatal("expected some matches for region=eu AND latency>500")
	}
	// The typed field must actually push down (region=eu also pushes; both index).
	sawIndex := false
	for _, p := range eng.Explain(q) {
		if p.Mode == "index" {
			sawIndex = true
		}
	}
	if !sawIndex {
		t.Fatal("LogQL typed-field query did not push down to the index")
	}
}
