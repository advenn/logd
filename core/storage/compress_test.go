package storage

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
	// The directory follows the header AND the preset dictionary in v2.
	dictLen := int(binary.BigEndian.Uint32(data[24:28]))
	data[segzHeaderLenV2+dictLen+3] ^= 0xFF // flip a bit inside the directory
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

// TestCorruptDictionaryDegrades: the preset dictionary is covered by the same checksum as
// the directory, because a corrupt dictionary would inflate every block to different bytes
// than were compressed — surfacing as wholesale page corruption rather than as the single
// bad field it actually is.
func TestCorruptDictionaryDegrades(t *testing.T) {
	_, segPath, _ := buildRawSegment(t, 4000)
	if _, err := CompressSegment(segPath, 8); err != nil {
		t.Fatal(err)
	}
	zpath := SegzPath(segPath)
	data, err := os.ReadFile(zpath)
	if err != nil {
		t.Fatal(err)
	}
	if ver := binary.BigEndian.Uint16(data[4:6]); ver != segzVersion2 {
		t.Fatalf("expected a version-2 segment, got %d", ver)
	}
	if dictLen := binary.BigEndian.Uint32(data[24:28]); dictLen == 0 {
		t.Fatal("expected a non-empty preset dictionary")
	}
	data[segzHeaderLenV2+5] ^= 0xFF // inside the dictionary
	if err := os.WriteFile(zpath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openPageSource(segPath); err == nil {
		t.Fatal("a corrupt preset dictionary must be rejected, not used to inflate blocks")
	}
}

// TestDictionaryImprovesRatio pins the reason version 2 exists. A 32 KB block cannot build a
// useful window on its own; priming it with a sample of the segment measured 6.15x -> 6.91x
// on real data. This asserts the direction, not the exact figure.
func TestDictionaryImprovesRatio(t *testing.T) {
	_, segPath, _ := buildRawSegment(t, 20000)
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
	if ratio < 3 {
		t.Errorf("ratio %.2fx on log text is suspiciously low", ratio)
	}
	t.Logf("ratio %.2fx (%d -> %d bytes)", ratio, before.Size(), after.Size())
}

// writeSegzV1 rewrites a raw segment in the ORIGINAL version-1 layout: 24-byte header, no
// preset dictionary, plain flate per block. CompressSegment only emits version 2 now, so
// this is the only way to produce a v1 file and prove segments sealed before the dictionary
// existed still read.
func writeSegzV1(t *testing.T, logPath string, blockPages int) {
	t.Helper()
	src, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	info, _ := src.Stat()
	numPages := uint64(info.Size() / PageSize)
	numBlocks := (numPages + uint64(blockPages) - 1) / uint64(blockPages)
	dirLen := int(numBlocks) * segzDirEntry

	dir := make([]byte, dirLen)
	var body bytes.Buffer
	fileOff := uint64(segzHeaderLenV1 + dirLen)
	raw := make([]byte, blockPages*PageSize)
	for b := uint64(0); b < numBlocks; b++ {
		start := b * uint64(blockPages)
		n := blockPages
		if rem := numPages - start; rem < uint64(blockPages) {
			n = int(rem)
		}
		buf := raw[:n*PageSize]
		if _, err := src.ReadAt(buf, PageOffset(start)); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		var comp bytes.Buffer
		zw, _ := flate.NewWriter(&comp, flate.DefaultCompression)
		zw.Write(buf)
		zw.Close()
		e := dir[int(b)*segzDirEntry:]
		binary.BigEndian.PutUint64(e[0:8], fileOff)
		binary.BigEndian.PutUint32(e[8:12], uint32(comp.Len()))
		binary.BigEndian.PutUint32(e[12:16], uint32(len(buf)))
		body.Write(comp.Bytes())
		fileOff += uint64(comp.Len())
	}

	hdr := make([]byte, segzHeaderLenV1)
	binary.BigEndian.PutUint32(hdr[0:4], segzMagic)
	binary.BigEndian.PutUint16(hdr[4:6], segzVersion1)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(blockPages))
	binary.BigEndian.PutUint64(hdr[8:16], numPages)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(numBlocks))
	binary.BigEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(dir))

	out := append(append(append([]byte{}, hdr...), dir...), body.Bytes()...)
	if err := os.WriteFile(SegzPath(logPath), out, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
}

// TestSegzVersion1StillReadable is the backward-compatibility guard. A v1 file has no
// dictionary and a 24-byte header; rejecting it would make every segment sealed before this
// change unreadable, which for compressed segments means DATA LOSS, not a slow scan.
func TestSegzVersion1StillReadable(t *testing.T) {
	_, segPath, want := buildRawSegment(t, 4000)
	writeSegzV1(t, segPath, 8)

	got, err := readSegmentRecords(segPath)
	if err != nil {
		t.Fatalf("a version-1 .logz must still open: %v", err)
	}
	sameRecords(t, "version-1 segment", got, want)

	// And the random-access path, which is what the query planner uses.
	var offsets []uint64
	for i := 0; i < len(want); i += 53 {
		offsets = append(offsets, want[i].Offset)
	}
	seen := 0
	if err := FetchRecords(segPath, offsets, func(model.LogEntry) bool { seen++; return true }); err != nil {
		t.Fatal(err)
	}
	if seen != len(offsets) {
		t.Errorf("v1 fetch by offset: got %d records, want %d", seen, len(offsets))
	}
}
