package ingest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// Ingest-level benchmark: extraction + label derivation + enqueue, i.e. everything the
// handler goroutine does before the writer sees a record. With fsync no longer dominating
// the write path, this is where the remaining end-to-end cost should be.
func benchIngest(b *testing.B, labels []string, tmpl []config.Template) {
	eng, err := extract.Compile(config.IndexConfig{Templates: tmpl})
	if err != nil {
		b.Fatal(err)
	}
	w, err := storage.NewWriter(b.TempDir(), storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    time.Hour,
		SyncInterval:     50 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	ig := ingest.NewWithLabels(eng, w, labels)

	base := time.Unix(1700000000, 0).UTC()
	msgs := make([]string, 1000)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace=%08x", i, 10+i%900, i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e := model.LogEntry{
			TS:      base.Add(time.Duration(i) * time.Millisecond),
			Level:   model.LogLevelInfo,
			Extra:   `{"app":"bench","env":"prod","region":"eu"}`,
			Message: msgs[i%len(msgs)],
		}
		for ig.Ingest(e) != nil {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

// BenchmarkIngestFull is the shipped shape: one template, a three-key allowlist.
func BenchmarkIngestFull(b *testing.B) {
	benchIngest(b, []string{"app", "env", "region"}, []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}})
}

// BenchmarkIngestNoExtraction isolates extraction's share: same path, no templates.
func BenchmarkIngestNoExtraction(b *testing.B) {
	benchIngest(b, []string{"app", "env", "region"}, nil)
}

// BenchmarkIngestNoLabels isolates label derivation's share (which JSON-parses Extra).
func BenchmarkIngestNoLabels(b *testing.B) {
	benchIngest(b, nil, []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}})
}
