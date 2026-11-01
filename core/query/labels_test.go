package query_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Allowlist uses only NON-reserved keys (level/service can resolve from first-class
// fields, so they are never pushed).
var labelAllowlist = []string{"region", "code"}

// buildLabeled ingests a labeled dataset across several sealed segments ("trace" is
// high-cardinality and NOT allowlisted → scan-only; "service" is in Extra but reserved).
func buildLabeled(t *testing.T) (*qr.Engine, string) {
	t.Helper()
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, _ := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, labelAllowlist)

	regions := []string{"us", "eu", "ap"}
	for i := 0; i < 400; i++ {
		extra := fmt.Sprintf(`{"region":"%s","code":%d,"trace":"%d"}`, regions[i%3], 200+(i%3)*100, i)
		e := model.LogEntry{TS: time.Unix(t0+int64(i%100), 0).UTC(), Level: model.LogLevelInfo, Extra: extra, Message: fmt.Sprintf("req took %dms", (i*7)%1000)}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	return qr.NewEngineWithLabels(w, eng, labelAllowlist), dir
}

func TestLabelPushdownEqualsScan(t *testing.T) {
	e, _ := buildLabeled(t)
	full := qr.Query{Start: ts(0), End: ts(1000)}

	battery := []struct {
		name  string
		preds []qr.Predicate
	}{
		{"region=eu", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}}},
		{"code=300 (numeric label)", []qr.Predicate{qr.LabelEqual{Key: "code", Value: "300"}}},
		{"region=none", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "nope"}}},
		{"trace=5 (non-allowlisted → scan)", []qr.Predicate{qr.LabelEqual{Key: "trace", Value: "5"}}},
		{"region!=us (residual → scan)", []qr.Predicate{qr.LabelNotEqual{Key: "region", Value: "us"}}},
		{"region=us AND code=200 (label∩label)", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "us"}, qr.LabelEqual{Key: "code", Value: "200"}}},
		{"region=eu AND latency>500 (label∩typed)", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}, qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(500)}}},
		{"region=ap AND trace=2 (indexed∩scanresidual)", []qr.Predicate{qr.LabelEqual{Key: "region", Value: "ap"}, qr.LabelEqual{Key: "trace", Value: "2"}}},
	}
	for _, c := range battery {
		q := full
		q.Preds = c.preds
		idx, err := e.Execute(q)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		scan, _ := e.ExecuteScan(q)
		equalResults(t, c.name, idx, scan)
	}
}

// A reserved key (service) on the allowlist must still be CORRECT when the two sources
// disagree: Extra's value wins, the interned ServiceID answers only when Extra lacks the
// key, and pushdown must not drop matches — the index holds only the Extra values, so a
// lookup would miss the record that matches via ServiceID.
func TestReservedLabelScansCorrectly(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"service"}) // reserved key on the allowlist

	apiID := w.InternService("api")
	// ServiceID says "api" but Extra says "web" — the two sources disagree on purpose.
	ig.Ingest(model.LogEntry{TS: time.Unix(t0, 0).UTC(), Level: model.LogLevelInfo, ServiceID: apiID, Extra: `{"service":"web"}`, Message: "m"})
	// ServiceID only.
	ig.Ingest(model.LogEntry{TS: time.Unix(t0+1, 0).UTC(), Level: model.LogLevelInfo, ServiceID: apiID, Message: "n"})
	w.Close()
	e := qr.NewEngineWithLabels(w, eng, []string{"service"})

	for value, want := range map[string]string{"api": "n", "web": "m"} {
		q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.LabelEqual{Key: "service", Value: value}}}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, "reserved service="+value, idx, scan)
		if len(idx) != 1 || idx[0].Message != want {
			t.Fatalf("service=%s must match only %q, got %v", value, want, idx)
		}
		// Explain: reserved key must NOT push down.
		for _, p := range e.Explain(q) {
			if p.Mode != "scan" {
				t.Fatalf("reserved label 'service' must scan, got %q", p.Mode)
			}
		}
	}
}

func TestLabelExplain(t *testing.T) {
	e, _ := buildLabeled(t)
	full := qr.Query{Start: ts(0), End: ts(1000)}

	usedIndex := false
	for _, p := range e.Explain(withPreds(full, qr.LabelEqual{Key: "region", Value: "eu"})) {
		if p.Mode == "index" {
			usedIndex = true
		}
	}
	if !usedIndex {
		t.Fatal("allowlisted LabelEqual did not push down")
	}
	for _, p := range e.Explain(withPreds(full, qr.LabelEqual{Key: "trace", Value: "5"})) {
		if p.Mode != "scan" {
			t.Fatalf("non-allowlisted label should scan, got %q", p.Mode)
		}
	}
	for _, p := range e.Explain(withPreds(full, qr.LabelNotEqual{Key: "region", Value: "us"})) {
		if p.Mode != "scan" {
			t.Fatalf("LabelNotEqual should scan, got %q", p.Mode)
		}
	}
}

func TestLabelEnumeration(t *testing.T) {
	e, _ := buildLabeled(t)
	keys := e.Labels()
	want := map[string]bool{"region": true, "code": true}
	for _, k := range keys {
		delete(want, k)
		if k == "trace" || k == "service" {
			t.Fatalf("non-indexed key %q should not appear in Labels()", k)
		}
	}
	if len(want) != 0 {
		t.Fatalf("Labels() missing %v (got %v)", want, keys)
	}
	regions := map[string]bool{}
	for _, v := range e.LabelValues("region") {
		regions[v] = true
	}
	for _, w := range []string{"us", "eu", "ap"} {
		if !regions[w] {
			t.Fatalf("LabelValues(region) missing %q (got %v)", w, e.LabelValues("region"))
		}
	}
}

// StreamID is stamped from the label set: identical label sets in a segment share it.
func TestStreamIDStamped(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	labels := []string{"a", "a", "b", "a"}
	for i, l := range labels {
		ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: fmt.Sprintf(`{"region":%q}`, l), Message: "m"})
	}
	w.Close()

	recs, _ := storage.ReadAllRecords(dir)
	if len(recs) != 4 {
		t.Fatalf("want 4 records, got %d", len(recs))
	}
	if recs[0].Entry.StreamID != recs[1].Entry.StreamID || recs[0].Entry.StreamID != recs[3].Entry.StreamID {
		t.Fatal("records with identical labels must share a StreamID")
	}
	if recs[0].Entry.StreamID == recs[2].Entry.StreamID {
		t.Fatal("records with different labels must have different StreamIDs")
	}
}

func TestLabelDegradeToScan(t *testing.T) {
	e, dir := buildLabeled(t)
	q := qr.Query{Start: ts(0), End: ts(1000), Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: "eu"}}}
	want, _ := e.Execute(q)

	matches, _ := filepath.Glob(filepath.Join(dir, "segments", "*.labels.lidx"))
	if len(matches) == 0 {
		t.Fatal("no .lidx files found to remove")
	}
	for _, m := range matches {
		os.Remove(m)
	}
	got, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	equalResults(t, "label-degrade-to-scan", got, want)
}

// level and service carried as labels (what every Loki push does) must match, group and be
// discovered by the value the record carries — the value responses show. They resolved from
// the first-class fields instead: {level="error"} matched nothing while {level="ERROR"}
// matched the rows displayed as level="error"; a metric grouped a pushed "warning" as
// "WARN"; {service="api"} matched nothing because the push path never sets ServiceID; and
// neither key was ever discoverable. The fields still answer when the label is absent.
func TestReservedLabelsResolveFromRecordLabels(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	allow := []string{"app", "level", "service"}
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, LabelKeys: allow})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, allow)
	worker := w.InternService("worker")
	recs := []model.LogEntry{
		// As push.Decode builds them: Level parsed from the label, the label kept in Extra.
		{Level: model.LogLevelError, Extra: `{"app":"x","level":"error"}`, Message: "pushed error"},
		{Level: model.LogLevelWarn, Extra: `{"app":"x","level":"warning"}`, Message: "pushed warning"},
		{Level: model.LogLevelInfo, Extra: `{"app":"y"}`, Message: "no level label"},
		{Level: model.LogLevelInfo, Extra: `{"app":"z","service":"api"}`, Message: "pushed service"},
		// Native: level and service only as fields.
		{Level: model.LogLevelError, ServiceID: worker, Extra: `{"app":"n"}`, Message: "native"},
	}
	for i, r := range recs {
		r.TS = time.Unix(t0+int64(i), 0).UTC()
		if err := ig.Ingest(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := qr.NewEngineWithLabels(w, eng, allow)

	cases := []struct {
		name  string
		preds []qr.Predicate
		want  []string
	}{
		{"level=error", []qr.Predicate{qr.LabelEqual{Key: "level", Value: "error"}}, []string{"pushed error"}},
		{"level=warning", []qr.Predicate{qr.LabelEqual{Key: "level", Value: "warning"}}, []string{"pushed warning"}},
		{"level=WARN", []qr.Predicate{qr.LabelEqual{Key: "level", Value: "WARN"}}, nil},
		{"level=ERROR (field, label absent)", []qr.Predicate{qr.LabelEqual{Key: "level", Value: "ERROR"}}, []string{"native"}},
		{"service=api", []qr.Predicate{qr.LabelEqual{Key: "service", Value: "api"}}, []string{"pushed service"}},
		{"app=z, service!=api", []qr.Predicate{qr.LabelEqual{Key: "app", Value: "z"}, qr.LabelNotEqual{Key: "service", Value: "api"}}, nil},
		{"service=worker (field, label absent)", []qr.Predicate{qr.LabelEqual{Key: "service", Value: "worker"}}, []string{"native"}},
	}
	for _, c := range cases {
		q := qr.Query{Start: ts(0), End: ts(100), Preds: c.preds, Direction: qr.Forward}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, c.name, idx, scan)
		var got []string
		for _, r := range idx {
			got = append(got, r.Message)
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Fatalf("%s: got %q, want %q", c.name, got, c.want)
		}
	}

	// Grouping shows exactly the labels the record carries, as the log path does: no
	// canonical level or interned service is invented for a record that lacks the label.
	m, err := e.ExecuteMetric(qr.MetricQuery{Range: 100 * int64(time.Second), RangeOp: qr.RangeCount,
		VectorOp: qr.VecSum, Grouping: []string{"level", "service"}, Start: ts(10), End: ts(10), Step: int64(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	groups := map[string]float64{}
	for _, s := range m {
		groups[fmt.Sprint(s.Labels)] = s.Points[0].Value
	}
	want := map[string]float64{"map[level:error]": 1, "map[level:warning]": 1, "map[service:api]": 1, "map[]": 2}
	if fmt.Sprint(groups) != fmt.Sprint(want) {
		t.Fatalf("sum by (level, service): got %v, want %v", groups, want)
	}

	// Discovery lists both keys with the carried values.
	if got := fmt.Sprint(e.Labels()); got != "[app level service]" {
		t.Fatalf("Labels() = %s, want [app level service]", got)
	}
	if got := fmt.Sprint(e.LabelValues("level")); got != "[error warning]" {
		t.Fatalf("LabelValues(level) = %s", got)
	}
	if got := fmt.Sprint(e.LabelValues("service")); got != "[api]" {
		t.Fatalf("LabelValues(service) = %s", got)
	}
}
