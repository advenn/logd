package query_test

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// waitedRe reads the undeclared "waited=<n>ms" value, standing in for whatever a caller's
// query language extracts at query time.
var waitedRe = regexp.MustCompile(`waited=(\S+?)ms`)

func waited(e model.LogEntry, _ qr.Resolver) (string, bool) {
	m := waitedRe.FindStringSubmatch(e.Message)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// The comparison rules, one record at a time: numeric comparisons convert per record and
// fail on a missing or unconvertible value whatever the operator; string comparisons read a
// missing value as ""; a regex matches or must not match.
func TestDynamicCompareMatch(t *testing.T) {
	rec := func(msg string) model.LogEntry { return model.LogEntry{Message: msg} }
	num := func(op qr.Op, v string) qr.DynamicCompare {
		return qr.DynamicCompare{Name: "waited", Extract: waited, Op: op, Value: v, Numeric: true}
	}
	str := func(op qr.Op, v string) qr.DynamicCompare {
		return qr.DynamicCompare{Name: "waited", Extract: waited, Op: op, Value: v}
	}
	for _, c := range []struct {
		name string
		p    qr.DynamicCompare
		msg  string
		want bool
	}{
		{"int > bound", num(qr.OpGt, "200"), "waited=250ms", true},
		{"int = bound is not >", num(qr.OpGt, "200"), "waited=200ms", false},
		{"converted as float", num(qr.OpGe, "12.5"), "waited=12.5ms", true},
		{"float bound on int", num(qr.OpLt, "200.5"), "waited=200ms", true},
		{"numeric equality", num(qr.OpEq, "200"), "waited=200.0ms", true},
		{"exponent converts", num(qr.OpGt, "999"), "waited=1e3ms", true},
		{"missing never matches", num(qr.OpLt, "1e9"), "no value here", false},
		{"missing never matches !=", num(qr.OpNe, "1"), "no value here", false},
		{"unconvertible never matches", num(qr.OpNe, "1"), "waited=fastms", false},
		{"NaN satisfies only !=", num(qr.OpNe, "1"), "waited=NaNms", true},
		{"NaN is not equal", num(qr.OpEq, "1"), "waited=NaNms", false},
		{"NaN is not ordered", num(qr.OpGe, "-1e300"), "waited=NaNms", false},
		{"string equality", str(qr.OpEq, "200.0"), "waited=200.0ms", true},
		{"string is not numeric", str(qr.OpEq, "200"), "waited=200.0ms", false},
		{"missing is empty", str(qr.OpEq, ""), "no value here", true},
		{"missing != x", str(qr.OpNe, "x"), "no value here", true},
		{"regex", qr.DynamicCompare{Name: "waited", Extract: waited, Op: qr.OpEq, Re: regexp.MustCompile(`^2\d\d$`)}, "waited=250ms", true},
		{"negated regex", qr.DynamicCompare{Name: "waited", Extract: waited, Op: qr.OpNe, Re: regexp.MustCompile(`^2\d\d$`)}, "waited=250ms", false},
		{"negated regex, missing", qr.DynamicCompare{Name: "waited", Extract: waited, Op: qr.OpNe, Re: regexp.MustCompile(`^2\d\d$`)}, "none", true},
	} {
		if got := c.p.Match(rec(c.msg), nil); got != c.want {
			t.Errorf("%s: Match(%q) = %v, want %v", c.name, c.msg, got, c.want)
		}
	}
}

// A dynamic comparison the engine cannot answer is refused, as a typed one is (§9.3).
func TestDynamicCompareValidated(t *testing.T) {
	e := buildAndClose(t, sampleRecs())
	for _, p := range []qr.DynamicCompare{
		{Name: "x"}, // no extractor
		{Name: "x", Extract: waited, Op: qr.OpGt, Value: "abc", Numeric: true},
		{Name: "x", Extract: waited, Op: qr.OpGt, Value: "NaN", Numeric: true},
		{Name: "x", Extract: waited, Op: qr.OpGt, Value: "+Inf", Numeric: true},
		{Name: "x", Extract: waited, Op: qr.OpGt, Re: regexp.MustCompile("x")},
	} {
		q := qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{p}}
		if _, err := e.Execute(q); err == nil {
			t.Errorf("Execute accepted %+v", p)
		}
		if _, err := e.ExecuteMetric(qr.MetricQuery{Preds: q.Preds, Start: ts(0), End: ts(10), Step: 1e9, Range: 1e9}); err == nil {
			t.Errorf("ExecuteMetric accepted %+v", p)
		}
	}
}

// buildWaited ingests a corpus where the undeclared waited= value appears in various
// shapes, across several sealed segments plus an active one, and returns the engine (over a
// writer left open, so the active segment is part of every query) and the records.
func buildWaited(t *testing.T) (*qr.Engine, []model.LogEntry) {
	t.Helper()
	dir := t.TempDir()
	ex, err := extract.Compile(testCfg())
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{Schema: ex.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	t.Cleanup(func() { w.Close() })
	ig := ingest.New(ex, w)
	var all []model.LogEntry
	for i := 0; i < 600; i++ {
		var msg string
		switch i % 6 {
		case 0:
			msg = fmt.Sprintf("lock waited=%dms took %dms", (i*37)%1000, i%400)
		case 1:
			msg = fmt.Sprintf("lock waited=%d.5ms", (i*13)%500)
		case 2:
			msg = "lock waited=slowms"
		case 3:
			msg = fmt.Sprintf("plain took %dms", (i*11)%900)
		case 4:
			msg = fmt.Sprintf("lock waited=-%dms", i%50)
		default:
			msg = "no value at all"
		}
		e := model.LogEntry{TS: time.Unix(t0+int64(i%120), 0).UTC(), Level: model.LogLevelInfo, Message: msg}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
		all = append(all, e)
	}
	// The last records stay in the active segment, which is scanned live.
	for i := 0; i < 30; i++ {
		e := model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("active waited=%dms took %dms", i*40, i*9)}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
		all = append(all, e)
	}
	waitVisible(t, w, len(all))
	return qr.NewEngine(w, ex), all
}

// waitVisible waits until every written record is readable (the flush tick has run).
func waitVisible(t *testing.T, w *storage.Writer, n int) {
	t.Helper()
	e := qr.NewEngine(w, nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := e.ExecuteScan(qr.Query{Start: ts(-1), End: ts(1000), Limit: n + 1})
		if err == nil && len(got) == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records became visible", len(got), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// §9.1's range on an undeclared field: scan, extract at query time, filter. The result must
// equal a reference computed here from the corpus with Go's regexp and strconv, and the
// index path must equal the forced scan — also when a pushable typed predicate narrows the
// candidates first and the dynamic one is re-verified on them.
func TestDynamicCompareRangeOnUndeclaredField(t *testing.T) {
	e, all := buildWaited(t)
	ref := func(keep func(model.LogEntry) bool) int {
		n := 0
		for _, r := range all {
			if keep(r) {
				n++
			}
		}
		return n
	}
	waitedOver := func(b float64) func(model.LogEntry) bool {
		return func(r model.LogEntry) bool {
			m := waitedRe.FindStringSubmatch(r.Message)
			if m == nil {
				return false
			}
			f, err := strconv.ParseFloat(m[1], 64)
			return err == nil && f > b
		}
	}
	tookRe := regexp.MustCompile(`took (\d+)ms`)
	tookOver := func(b int) func(model.LogEntry) bool {
		return func(r model.LogEntry) bool {
			for _, m := range tookRe.FindAllStringSubmatch(r.Message, -1) {
				if n, _ := strconv.Atoi(m[1]); n > b {
					return true
				}
			}
			return false
		}
	}
	dyn := qr.DynamicCompare{Name: "waited", Extract: waited, Op: qr.OpGt, Value: "200", Numeric: true}
	typed := qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(300)}
	for _, c := range []struct {
		name  string
		preds []qr.Predicate
		want  int
	}{
		{"waited > 200", []qr.Predicate{dyn}, ref(waitedOver(200))},
		{"waited > 200 and latency_ms > 300", []qr.Predicate{dyn, typed}, ref(func(r model.LogEntry) bool { return waitedOver(200)(r) && tookOver(300)(r) })},
	} {
		q := qr.Query{Start: ts(-1), End: ts(1000), Limit: len(all) + 1, Preds: c.preds}
		idx, err := e.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		scan, err := e.ExecuteScan(q)
		if err != nil {
			t.Fatal(err)
		}
		equalResults(t, c.name, idx, scan)
		if len(idx) != c.want || c.want == 0 {
			t.Fatalf("%s: got %d rows, the reference says %d", c.name, len(idx), c.want)
		}
	}

	// The dynamic predicate alone is never pushed: every segment scans. With the typed
	// predicate beside it, sealed segments use the index and the dynamic one is a residual.
	for _, p := range e.Explain(qr.Query{Start: ts(-1), End: ts(1000), Preds: []qr.Predicate{dyn}}) {
		if p.Mode != "scan" {
			t.Fatalf("a dynamic comparison alone was answered by %s (%s)", p.Mode, p.Reason)
		}
	}
	indexed := 0
	for _, p := range e.Explain(qr.Query{Start: ts(-1), End: ts(1000), Preds: []qr.Predicate{dyn, typed}}) {
		if p.Mode == "index" {
			indexed++
		}
	}
	if indexed == 0 {
		t.Fatal("the typed predicate beside a dynamic one no longer used the index")
	}
}

// Tail applies a dynamic comparison to each record as it is written, like any predicate.
func TestDynamicCompareInTail(t *testing.T) {
	w, e, ig := liveSetup(t, time.Hour)
	defer w.Close()
	ch, cancel := e.Tail([]qr.Predicate{qr.DynamicCompare{Name: "waited", Extract: waited, Op: qr.OpGe, Value: "100", Numeric: true}}, 16)
	defer cancel()
	ingestMsg(t, ig, 0, "lock waited=50ms")
	ingestMsg(t, ig, 1, "lock waited=fastms")
	ingestMsg(t, ig, 2, "lock waited=150ms")
	select {
	case got := <-ch:
		if got.Message != "lock waited=150ms" {
			t.Fatalf("tail delivered %q", got.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the matching record never arrived")
	}
	select {
	case got := <-ch:
		t.Fatalf("tail delivered a second record %q", got.Message)
	case <-time.After(100 * time.Millisecond):
	}
}

// The metric hooks: SeriesLabels splits and groups series by labels the caller derives from
// the line, and UnwrapValue aggregates a value only the caller can read.
func TestMetricSeriesLabelsAndUnwrapHooks(t *testing.T) {
	e := buildMetricData(t, []struct {
		region  string
		sec     int
		latency int
	}{{"eu", 1, 100}, {"eu", 2, 300}, {"us", 3, 500}, {"eu", 4, 700}})
	bucket := func(en model.LogEntry, r qr.Resolver) map[string]string {
		m := r.AllLabels(en)
		var n int
		fmt.Sscanf(en.Message, "took %dms", &n)
		m["slow"] = strconv.FormatBool(n >= 300)
		return m
	}
	series, err := e.ExecuteMetric(qr.MetricQuery{
		RangeOp: qr.RangeCount, VectorOp: qr.VecSum, Grouping: []string{"slow"},
		Range: 10e9, Start: nanoAt(10), End: nanoAt(10), Step: 1e9, SeriesLabels: bucket,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, s := range series {
		got[s.Labels["slow"]] = s.Points[0].Value
	}
	if len(series) != 2 || got["true"] != 3 || got["false"] != 1 {
		t.Fatalf("sum by (slow): %+v", series)
	}

	half := func(en model.LogEntry, _ qr.Resolver) (float64, bool) {
		var n int
		if _, err := fmt.Sscanf(en.Message, "took %dms", &n); err != nil || n == 700 {
			return 0, false // dropped, as an unconvertible value is
		}
		return float64(n) / 2, true
	}
	series, err = e.ExecuteMetric(qr.MetricQuery{
		RangeOp: qr.RangeSum, VectorOp: qr.VecSum, Unwrap: "half",
		Range: 10e9, Start: nanoAt(10), End: nanoAt(10), Step: 1e9, UnwrapValue: half,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || math.Abs(series[0].Points[0].Value-450) > 1e-9 {
		t.Fatalf("sum(sum_over_time(half)): %+v, want 450", series)
	}
}
