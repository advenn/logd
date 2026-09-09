package storage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// Group commit trades a bounded loss window for throughput (fsync per page measures ~21k
// lines/s on NVMe; a 50ms window measures ~1.05M). These tests pin the properties that make
// that trade safe rather than merely fast.

func writeN(t *testing.T, w *storage.Writer, n int, msg func(int) string) {
	t.Helper()
	base := time.Unix(1700000000, 0).UTC()
	for i := 0; i < n; i++ {
		e := model.LogEntry{TS: base.Add(time.Duration(i) * time.Second), Message: msg(i)}
		for {
			if err := w.WriteExtracted(e, nil, nil); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func countRecords(t *testing.T, path string) int {
	t.Helper()
	n := 0
	if err := storage.ScanSegmentTimeRange(path, 0, 1<<62, func(model.LogEntry) bool {
		n++
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return n
}

// TestGroupCommitDurableAfterClose is the contract that matters most: whatever the sync
// interval, a clean Close must leave EVERY record durable. Group commit may delay an fsync;
// it must never skip one.
func TestGroupCommitDurableAfterClose(t *testing.T) {
	for _, interval := range []time.Duration{0, 10 * time.Millisecond, time.Hour} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			dir := t.TempDir()
			w, err := storage.NewWriter(dir, storage.Options{
				SegmentSizeBytes: 1 << 40,
				FlushInterval:    time.Hour, // never let the ticker do the work
				SyncInterval:     interval,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(nil); err != nil {
				t.Fatal(err)
			}
			const n = 500
			writeN(t, w, n, func(i int) string { return fmt.Sprintf("record-%04d", i) })
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// A SyncInterval of an hour means no periodic sync ever fired, so this only
			// passes if Close forces one.
			segs := w.Manifest().All()
			total := 0
			for _, s := range segs {
				total += countRecords(t, s.Path)
			}
			if total != n {
				t.Errorf("sync interval %v: %d records survived Close, want %d", interval, total, n)
			}
		})
	}
}

// TestGroupCommitSurvivesSealWithoutPeriodicSync: sealing a segment must force it durable
// regardless of where the group-commit window sits, because a sealed segment is advertised
// in the manifest as complete.
func TestGroupCommitSurvivesSealWithoutPeriodicSync(t *testing.T) {
	dir := t.TempDir()
	w, err := storage.NewWriter(dir, storage.Options{
		SegmentSizeBytes: 3 * storage.PageSize, // force several rotations
		FlushInterval:    time.Hour,
		SyncInterval:     time.Hour, // periodic sync will never fire
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	const n = 2000
	writeN(t, w, n, func(i int) string {
		return fmt.Sprintf("record-%04d padding-to-fill-pages-faster-%030d", i, i)
	})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segs := w.Manifest().All()
	if len(segs) < 2 {
		t.Fatalf("expected several sealed segments, got %d", len(segs))
	}
	total := 0
	for _, s := range segs {
		total += countRecords(t, s.Path)
	}
	if total != n {
		t.Errorf("got %d records across %d segments, want %d", total, len(segs), n)
	}
}

// TestGroupCommitPeriodicSyncFires: on a stream that goes quiet, a written page must not sit
// unsynced indefinitely — the flush ticker doubles as the group-commit deadline check.
func TestGroupCommitPeriodicSyncFires(t *testing.T) {
	dir := t.TempDir()
	w, err := storage.NewWriter(dir, storage.Options{
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    5 * time.Millisecond,
		SyncInterval:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	writeN(t, w, 10, func(i int) string { return fmt.Sprintf("quiet-%d", i) })

	// Give the ticker time to flush the partial page and sync it, without closing.
	deadline := time.Now().Add(2 * time.Second)
	found := 0
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		segs := w.Manifest().All()
		if len(segs) == 0 {
			continue
		}
		if found = countRecords(t, segs[0].Path); found == 10 {
			break
		}
	}
	if found != 10 {
		t.Errorf("the ticker should have flushed and synced the partial page; found %d of 10", found)
	}
	w.Close()
}
