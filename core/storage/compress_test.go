package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// Compressing a sealed segment must be invisible to every reader. The contract that makes
// that non-trivial is the OFFSET one: the .tidx stores segment-relative byte offsets and
// the query path turns one into (page, in-page offset) by division, so the compressed form
// has to preserve logical page numbering exactly while storing variable-length blocks.

// buildRawSegment writes n records through a real writer with compression disabled, then
// returns the segment path and everything read back from it.
func buildRawSegment(t *testing.T, n int) (dir, segPath string, recs []Record) {
	t.Helper()
	dir = t.TempDir()
	w, err := NewWriter(dir, Options{
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    time.Hour,
		BlockPages:       -1, // leave raw
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1700000000, 0).UTC()
	for i := 0; i < n; i++ {
		e := model.LogEntry{
			TS:      base.Add(time.Duration(i) * time.Second),
			Level:   model.LogLevel(i % 4),
			Extra:   `{"app":"bench","region":"eu"}`,
			Message: fmt.Sprintf("record %05d took %dms trace=%08x", i, 10+i%900, i),
		}
		for w.WriteExtracted(e, nil, nil) != nil {
			time.Sleep(time.Millisecond)
		}
	}
	w.Close()

	metas := w.Manifest().All()
	if len(metas) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(metas))
	}
	segPath = metas[0].Path
	recs, err = readSegmentRecords(segPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("read back %d records, want %d", len(recs), n)
	}
	return dir, segPath, recs
}

func sameRecords(t *testing.T, label string, got, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d records, want %d", label, len(got), len(want))
	}
	for i := range got {
		if got[i].Offset != want[i].Offset {
			t.Fatalf("%s: record %d offset %d, want %d — logical offsets must survive compression",
				label, i, got[i].Offset, want[i].Offset)
		}
		if got[i].Entry.Message != want[i].Entry.Message ||
			got[i].Entry.TS.UnixNano() != want[i].Entry.TS.UnixNano() ||
			got[i].Entry.Extra != want[i].Entry.Extra ||
			got[i].Entry.Level != want[i].Entry.Level {
			t.Fatalf("%s: record %d differs: %+v vs %+v", label, i, got[i].Entry, want[i].Entry)
		}
	}
}

// TestCompressPreservesRecordsAndOffsets is the core contract: same records, same
// segment-relative offsets, across every block size.
func TestCompressPreservesRecordsAndOffsets(t *testing.T) {
	for _, blockPages := range []int{1, 2, 8, 64, 0 /* default */} {
		t.Run(fmt.Sprint(blockPages), func(t *testing.T) {
			_, segPath, want := buildRawSegment(t, 4000)

			ok, err := CompressSegment(segPath, blockPages)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected the segment to be compressed")
			}
			if _, err := os.Stat(segPath); !os.IsNotExist(err) {
				t.Error("the raw .log should be gone once the sidecar is durable")
			}
			if _, err := os.Stat(SegzPath(segPath)); err != nil {
				t.Fatalf("compressed sidecar missing: %v", err)
			}

			// readSegmentRecords is given the ORIGINAL .log path: callers never learn that
			// a segment was compressed.
			got, err := readSegmentRecords(segPath)
			if err != nil {
				t.Fatal(err)
			}
			sameRecords(t, "after compression", got, want)
		})
	}
}

// TestCompressedFetchByOffset exercises the path the query planner actually uses: random
// access by the offsets the .tidx stored, which were computed before compression existed.
func TestCompressedFetchByOffset(t *testing.T) {
	_, segPath, want := buildRawSegment(t, 4000)
	if _, err := CompressSegment(segPath, 8); err != nil {
		t.Fatal(err)
	}

	// A scattered subset, deliberately not in order — FetchRecords sorts internally, and
	// the block cache depends on that.
	var offsets []uint64
	wantMsg := map[uint64]string{}
	for i := 0; i < len(want); i += 37 {
		offsets = append(offsets, want[i].Offset)
		wantMsg[want[i].Offset] = want[i].Entry.Message
	}
	for i, j := 0, len(offsets)-1; i < j; i, j = i+1, j-1 {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	}

	seen := 0
	if err := FetchRecords(segPath, offsets, func(e model.LogEntry) bool {
		seen++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(offsets) {
		t.Fatalf("fetched %d records, want %d", seen, len(offsets))
	}
}

// TestCompressedScanBothDirections covers the other reader, including the reverse walk the
// limit pushdown uses.
func TestCompressedScanBothDirections(t *testing.T) {
	_, segPath, want := buildRawSegment(t, 3000)
	if _, err := CompressSegment(segPath, 8); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		n := 0
		err := ScanSegmentPages(segPath, 0, 1<<62, reverse, nil, func(model.LogEntry) bool {
			n++
			return true
		})
		if err != nil {
			t.Fatalf("reverse=%v: %v", reverse, err)
		}
		if n != len(want) {
			t.Errorf("reverse=%v: scanned %d records, want %d", reverse, n, len(want))
		}
	}
}

// TestCompressDisabledLeavesRaw: a negative block size must leave the segment untouched, so
// operators can opt out.
func TestCompressDisabledLeavesRaw(t *testing.T) {
	_, segPath, _ := buildRawSegment(t, 500)
	ok, err := CompressSegment(segPath, -1)
	if err != nil || ok {
		t.Fatalf("disabled compression should be a no-op, got ok=%v err=%v", ok, err)
	}
	if _, err := os.Stat(segPath); err != nil {
		t.Error("the raw segment must remain")
	}
}

// TestCorruptDirectoryDegrades: a damaged block directory must be reported, not used to
// compute file offsets. The engine's standing invariant is that a corrupt structure
// degrades to a scan — it must never yield a wrong answer.
func TestCorruptDirectoryDegrades(t *testing.T) {
	_, segPath, _ := buildRawSegment(t, 2000)
	if _, err := CompressSegment(segPath, 8); err != nil {
		t.Fatal(err)
	}
	zpath := SegzPath(segPath)
	data, err := os.ReadFile(zpath)
	if err != nil {
		t.Fatal(err)
	}
	data[segzHeaderLen+3] ^= 0xFF // flip a bit inside the directory
	if err := os.WriteFile(zpath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openPageSource(segPath); err == nil {
		t.Fatal("a corrupt block directory must be rejected, not trusted")
	}
}

// TestCompressedSegmentIsSmaller is a sanity check that the feature does what it claims on
// realistic log text — not a ratio assertion, just that it is not a pessimization.
func TestCompressedSegmentIsSmaller(t *testing.T) {
	_, segPath, _ := buildRawSegment(t, 8000)
	before, err := os.Stat(segPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CompressSegment(segPath, 8); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(SegzPath(segPath))
	if err != nil {
		t.Fatal(err)
	}
	ratio := float64(before.Size()) / float64(after.Size())
	if ratio < 2 {
		t.Errorf("compression ratio %.2fx on log text is suspiciously low", ratio)
	}
	t.Logf("ratio %.2fx (%d -> %d bytes)", ratio, before.Size(), after.Size())
}

// TestRawSegmentsStillReadable: segments written before compression existed carry no
// sidecar and must keep working unchanged.
func TestRawSegmentsStillReadable(t *testing.T) {
	dir, segPath, want := buildRawSegment(t, 1000)
	if _, err := os.Stat(filepath.Join(dir, "segments")); err != nil {
		t.Fatal(err)
	}
	got, err := readSegmentRecords(segPath)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, "raw segment", got, want)
}
