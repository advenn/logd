package query

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// The metric entry cap must bound memory, not only the answer. It was checked after a
// shard's every matching record had been collected as a full LogEntry, so on one shard it
// bounded nothing. Refusing must cost a small fraction of what evaluating costs.
func TestMetricEntryCapStopsCollection(t *testing.T) {
	w, err := storage.NewWriter(t.TempDir(), storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	const base, n = int64(1700000000), 20000
	for i := 0; i < n; i++ {
		e := model.LogEntry{TS: time.Unix(base+int64(i%100), 0).UTC(), Level: model.LogLevelInfo,
			Extra: `{"app":"a"}`, Message: fmt.Sprintf("line %05d with a little padding", i)}
		if err := w.WriteExtractedCtx(context.Background(), e, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	e := NewEngine(w, nil)
	mq := MetricQuery{Range: 200 * int64(time.Second), RangeOp: RangeCount,
		Start: (base + 100) * 1e9, End: (base + 100) * 1e9, Step: int64(time.Second)}

	run := func() (uint64, error) {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		_, err := e.ExecuteMetric(mq)
		runtime.ReadMemStats(&m1)
		return m1.TotalAlloc - m0.TotalAlloc, err
	}
	full, err := run()
	if err != nil {
		t.Fatal(err)
	}

	defer func(old int) { maxMetricEntries = old }(maxMetricEntries)
	maxMetricEntries = 100
	refused, err := run()
	if err == nil || !strings.Contains(err.Error(), "too many entries") {
		t.Fatalf("over the cap: err = %v, want a too-many-entries error", err)
	}
	if refused > full/10 {
		t.Fatalf("refusing at %d entries allocated %d KB; evaluating all %d allocated %d KB", maxMetricEntries, refused>>10, n, full>>10)
	}
}
