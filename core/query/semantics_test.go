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

// buildLabelDataset ingests records, some WITH region and some without, for testing
// empty-label semantics.
func buildLabelDataset(t *testing.T, extras []string) *qr.Engine {
	t.Helper()
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	w, _ := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region", "code"})
	for i, ex := range extras {
		ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: ex, Message: "m"})
	}
	w.Close()
	return qr.NewEngineWithLabels(w, eng, []string{"region", "code"})
}

// A missing label is the empty string (Loki): {k=""} matches records lacking k, {k!=""}
// excludes them, {k!="x"} includes them. And pushdown must equal scan throughout.
func TestEmptyLabelSemantics(t *testing.T) {
	e := buildLabelDataset(t, []string{
		`{"region":"eu"}`, // has region
		`{"other":"x"}`,   // no region
		`{"region":"us"}`, // has region
	})
	full := qr.Query{Start: ts(0), End: ts(100)}

	cases := []struct {
		name string
		pred qr.Predicate
		want int
	}{
		{`region=""`, qr.LabelEqual{Key: "region", Value: ""}, 1},        // only the no-region record
		{`region!=""`, qr.LabelNotEqual{Key: "region", Value: ""}, 2},    // the two with region
		{`region!="us"`, qr.LabelNotEqual{Key: "region", Value: "us"}, 2}, // eu + absent (absent==""!=us)
		{`region="eu"`, qr.LabelEqual{Key: "region", Value: "eu"}, 1},
	}
	for _, c := range cases {
		q := full
		q.Preds = []qr.Predicate{c.pred}
		idx, _ := e.Execute(q)
		scan, _ := e.ExecuteScan(q)
		equalResults(t, c.name, idx, scan)
		if len(idx) != c.want {
			t.Fatalf("%s: got %d, want %d", c.name, len(idx), c.want)
		}
	}
}

// A numeric label filter (| code >= 300) is numeric: a non-numeric label value is
// excluded (never a lexicographic fallback), matching Loki.
func TestNumericLabelCompare(t *testing.T) {
	e := buildLabelDataset(t, []string{
		`{"code":"200"}`,
		`{"code":"300"}`,
		`{"code":"500"}`,
		`{"code":"OK"}`, // non-numeric: must NOT satisfy >= 300
	})
	q := qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{qr.LabelCompare{Key: "code", Op: qr.OpGe, Value: "300"}}}
	idx, _ := e.Execute(q)
	scan, _ := e.ExecuteScan(q)
	equalResults(t, "code>=300", idx, scan)
	if len(idx) != 2 { // 300 and 500 only
		t.Fatalf("code>=300: got %d, want 2 (300,500); a non-numeric value must be excluded", len(idx))
	}
	for _, r := range idx {
		if fmt.Sprintf("%s", r.Extra) == `{"code":"OK"}` {
			t.Fatal("non-numeric code=OK wrongly matched >= 300 (lexicographic fallback)")
		}
	}
}

// {k=""} must NOT push down (the index can't surface records lacking k); it scans.
func TestEmptyLabelEqualDoesNotPush(t *testing.T) {
	e := buildLabelDataset(t, []string{`{"region":"eu"}`, `{"other":"x"}`})
	for _, p := range e.Explain(qr.Query{Start: ts(0), End: ts(100), Preds: []qr.Predicate{qr.LabelEqual{Key: "region", Value: ""}}}) {
		if p.Mode != "scan" {
			t.Fatalf(`{region=""} must scan (index can't return absent records), got %q`, p.Mode)
		}
	}
}
