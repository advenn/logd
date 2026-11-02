package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
)

// BenchmarkProcessEntry measures the writer goroutine's own per-record work — encode, page
// assembly, and the active segment's in-memory index — with the disk taken out of it: the
// group-commit window is an hour, so a page write is a page-cache copy, not an fsync.
//
// The Write* benchmarks cannot see this cost. They are bound by fsync on a real disk
// (~100k lines/s on a loaded box), where a few tens of nanoseconds per record are 0.3% of
// the time; here they are a measurable share of it. Run with a fixed count so every run
// builds the same buffer:
//
//	go test ./core/storage/ -run '^$' -bench ProcessEntry -benchtime 1000000x -count 5
func BenchmarkProcessEntry(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		name := "plain"
		if indexed {
			name = "indexed"
		}
		b.Run(name, func(b *testing.B) {
			schema := []index.FieldType{{Name: "latency_ms", Kind: index.KindInt, Pattern: "took {ms:int}ms"}}
			w, err := NewWriter(b.TempDir(), Options{
				Schema:           schema,
				SegmentSizeBytes: 1 << 40,
				FlushInterval:    time.Hour,
				SyncInterval:     time.Hour,
				BlockPages:       -1,
				LabelKeys:        []string{"app", "env", "region"},
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := w.rotateSegment(); err != nil {
				b.Fatal(err)
			}
			w.resetPage()

			const distinct = 1000
			recs := make([]record, distinct)
			regions := []string{"eu", "us", "ap"}
			for i := range recs {
				recs[i].entry = model.LogEntry{
					Level:   model.LogLevelInfo,
					Extra:   fmt.Sprintf(`{"app":"bench","env":"prod","region":"%s"}`, regions[i%3]),
					Message: fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace=%08x", i, 10+i%900, i),
				}
				if indexed {
					recs[i].keys = []index.KeyedValue{{Field: "latency_ms", Key: index.EncodeKey(index.Value{Kind: index.KindInt, Int: int64(10 + i%900)})}}
					recs[i].labels = label.NewSet(map[string]string{"app": "bench", "env": "prod", "region": regions[i%3]})
				}
			}
			base := time.Unix(1700000000, 0).UTC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := recs[i%distinct]
				r.entry.TS = base.Add(time.Duration(i) * time.Millisecond)
				r.entry.IngestedAt = r.entry.TS
				w.processEntry(r)
			}
			b.StopTimer()
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
