package storage_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// Write-path benchmarks. Motivating measurement, all three backends fed the identical 500k
// corpus over the same protocol with zero retries:
//
//	VictoriaLogs  2.18s
//	Loki          4.65s
//	logd         59.9s
//
// Reads have been profiled and optimized; the write path never has.
//
// IMPORTANT — these must run on a REAL DISK. b.TempDir() honours $TMPDIR, and on this box
// (and most Linux dev boxes) /tmp is tmpfs, where fsync is a no-op. Benchmarking the write
// path there reports ~1.3M lines/s and tells you nothing, because durability is the thing
// under test. Set LOGD_BENCH_DIR to a path on a real filesystem; the benchmark skips rather
// than silently producing a RAM-disk number.
//
//	LOGD_BENCH_DIR=$HOME/.cache/logd-bench go test ./core/storage/ -run xxx -bench Write -benchtime 3x
const benchRecords = 200000

// benchDir returns a fresh directory on a real filesystem, or skips.
func benchDir(b *testing.B) string {
	b.Helper()
	root := os.Getenv("LOGD_BENCH_DIR")
	if root == "" {
		b.Skip("set LOGD_BENCH_DIR to a path on a real (non-tmpfs) filesystem; /tmp is tmpfs and fsync there is free")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "seg")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func benchEntries(n int) []model.LogEntry {
	out := make([]model.LogEntry, n)
	base := time.Unix(1700000000, 0).UTC()
	for i := range out {
		out[i] = model.LogEntry{
			TS:      base.Add(time.Duration(i) * time.Millisecond),
			Level:   model.LogLevelInfo,
			Extra:   `{"app":"bench","env":"prod","region":"eu"}`,
			Message: fmt.Sprintf("GET /api/v1/orders/%d 200 took %dms trace=%08x", i, 10+i%900, i),
		}
	}
	return out
}

// runWrite drives n entries through a real writer and waits for them to be durable.
func runWrite(b *testing.B, opts storage.Options, entries []model.LogEntry) {
	b.Helper()
	w, err := storage.NewWriter(benchDir(b), opts)
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	for i := range entries {
		// Wait out backpressure rather than dropping, so this measures the writer's real
		// drain rate instead of how fast it can shed load.
		for {
			if err := w.WriteExtracted(entries[i], nil, nil); err == nil {
				break
			}
			time.Sleep(200 * time.Microsecond)
		}
	}
	w.Close() // flush + seal: everything is durable when this returns
}

// BenchmarkWriteDefault is the shipped configuration. Under sustained load every page fills
// and is flushed immediately, so the 500ms flush interval never batches anything and the
// per-page fsync pair runs at full rate.
func BenchmarkWriteDefault(b *testing.B) {
	entries := benchEntries(benchRecords)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(b, storage.Options{
			SegmentSizeBytes: 64 << 20,
			FlushInterval:    500 * time.Millisecond,
		}, entries)
	}
	b.ReportMetric(float64(benchRecords*b.N)/b.Elapsed().Seconds(), "lines/s")
}

// BenchmarkWriteGroupCommit50ms trades a bounded loss window for throughput: pages are
// written as they fill but fsynced at most every 50ms. Compare against WriteDefault to see
// what the per-page fsync actually costs.
func BenchmarkWriteGroupCommit50ms(b *testing.B) {
	entries := benchEntries(benchRecords)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(b, storage.Options{
			SegmentSizeBytes: 64 << 20,
			FlushInterval:    500 * time.Millisecond,
			SyncInterval:     50 * time.Millisecond,
		}, entries)
	}
	b.ReportMetric(float64(benchRecords*b.N)/b.Elapsed().Seconds(), "lines/s")
}

// BenchmarkWriteGroupCommit250ms is the same trade with a wider window.
func BenchmarkWriteGroupCommit250ms(b *testing.B) {
	entries := benchEntries(benchRecords)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(b, storage.Options{
			SegmentSizeBytes: 64 << 20,
			FlushInterval:    500 * time.Millisecond,
			SyncInterval:     250 * time.Millisecond,
		}, entries)
	}
	b.ReportMetric(float64(benchRecords*b.N)/b.Elapsed().Seconds(), "lines/s")
}

// BenchmarkWriteHugeSegment removes segment rotation and sealing (index build, .tidx/.lidx
// write, manifest save) so the per-page cost stands alone.
func BenchmarkWriteHugeSegment(b *testing.B) {
	entries := benchEntries(benchRecords)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(b, storage.Options{
			SegmentSizeBytes: 1 << 40,
			FlushInterval:    time.Hour,
		}, entries)
	}
	b.ReportMetric(float64(benchRecords*b.N)/b.Elapsed().Seconds(), "lines/s")
}
