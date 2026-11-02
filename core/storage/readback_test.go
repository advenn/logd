package storage

import (
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// TestFetchRecordsVisitsEachOffsetOnce: a typed lookup returns a record once per indexed
// occurrence ("took 33ms took 20ms" twice for latency_ms > 10). Once the planner stopped
// routing a single lookup's result through a de-duplicating map, those records came back
// twice; FetchRecords now materializes each offset once whatever it is handed.
func TestFetchRecordsVisitsEachOffsetOnce(t *testing.T) {
	dir := t.TempDir()
	w := newSyncWriter(t, dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	for i := 0; i < 5; i++ {
		w.processEntry(record{entry: makeEntry(i, 1)})
	}
	path := w.currSeg.Path
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	recs, err := readSegmentRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 5 {
		t.Fatalf("read %d records, want 5", len(recs))
	}
	dup := []uint64{recs[3].Offset, recs[1].Offset, recs[3].Offset, recs[1].Offset, recs[1].Offset}
	var got []string
	if err := FetchRecords(path, dup, func(e model.LogEntry) bool { got = append(got, e.Message); return true }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "00001" || got[1] != "00003" {
		t.Fatalf("visited %v, want each of the two offsets once, in offset order", got)
	}
}
