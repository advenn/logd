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

// buildCovering writes 600 records, all labelled app=web and spread over two regions, into
// 3-page segments. Records 400..599 carry no "took" value at all, so the segments that hold
// only them have zero entries for latency_ms — the shape of the last segment of the HDFS
// replay, which held no `size` value.
func buildCovering(t *testing.T) *qr.Engine {
	t.Helper()
	dir := t.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	if err != nil {
		t.Fatal(err)
	}
	labels := []string{"app", "region"}
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize,
		FlushInterval: time.Hour, BlockPages: -1, LabelKeys: labels})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	ig := ingest.NewWithLabels(eng, w, labels)
	for i := 0; i < 600; i++ {
		msg := fmt.Sprintf("req %d took %dms", i, (i*37)%1000)
		if i >= 400 {
			msg = fmt.Sprintf("req %d done", i)
		}
		extra := fmt.Sprintf(`{"app":"web","region":"%s"}`, []string{"eu", "us"}[i%2])
		e := model.LogEntry{TS: time.Unix(t0+int64(i), 0).UTC(), Level: model.LogLevelInfo, Extra: extra, Message: msg}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return qr.NewEngineWithLabels(w, eng, labels)
}

// TestFieldWithoutValuesPrunesSegment: a segment in which no record yielded a value for a
// declared field used to be sealed without that field's .tidx, so the field was missing from
// its schema and every typed predicate on it scanned the segment, to return nothing. It now
// gets an empty .tidx and is answered — with no candidates — from the index.
func TestFieldWithoutValuesPrunesSegment(t *testing.T) {
	e := buildCovering(t)
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000,
		Preds: []qr.Predicate{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(0)}}}

	idx, err := e.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := e.ExecuteScan(q)
	if err != nil {
		t.Fatal(err)
	}
	equalResults(t, "latency_ms >= 0", idx, scan)
	if len(idx) != 400 {
		t.Fatalf("got %d rows, want the 400 records that carry a value", len(idx))
	}

	empty := 0
	for _, p := range e.Explain(q) {
		if p.State != "sealed" {
			continue
		}
		if p.Mode != "index" {
			t.Fatalf("segment %s scanned (%s): a declared field with no values must still be indexed", p.SegmentID, p.Reason)
		}
		if p.Candidates == 0 {
			empty++
		}
	}
	if empty == 0 {
		t.Fatal("expected at least one sealed segment with no latency_ms values, answered with 0 candidates")
	}
}
