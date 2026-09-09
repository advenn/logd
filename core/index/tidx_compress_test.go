package index

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// The .tidx record region is deflated (version 2). These tests cover what that changes:
// the size/truncation guards, backward compatibility with version 1 files, and that the
// corruption contract still holds — .tidx is the one structure whose silent corruption
// would change query ANSWERS rather than crash, so "corrupt => error => degrade to scan"
// has to survive the format change.

// writeTIDXPlain writes a version-1 (uncompressed) file by hand. WriteTIDX only emits
// version 2 now, so producing a v1 file is the only way to test that segments sealed
// before compression existed still read.
func writeTIDXPlain(t *testing.T, path string, kind ValueKind, entries []Entry) {
	t.Helper()
	buf := make([]byte, tidxHeaderSize+len(entries)*tidxRecordSize)
	binary.BigEndian.PutUint32(buf[0:4], tidxMagic)
	binary.BigEndian.PutUint16(buf[4:6], tidxVersionPlain)
	buf[6] = byte(kind)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(entries)))
	off := tidxHeaderSize
	for _, e := range entries {
		copy(buf[off:off+KeySize], e.Key[:])
		binary.BigEndian.PutUint64(buf[off+KeySize:off+tidxRecordSize], e.Offset)
		off += tidxRecordSize
	}
	binary.BigEndian.PutUint32(buf[20:24], crc32.Checksum(buf, tidxCRC))
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

// reCRC recomputes and stores the whole-file checksum, so a test can tamper with a header
// field and still exercise the check it is aiming at rather than tripping the CRC first.
func reCRC(buf []byte) {
	binary.BigEndian.PutUint32(buf[20:24], 0)
	binary.BigEndian.PutUint32(buf[20:24], crc32.Checksum(buf, tidxCRC))
}

// TestTidxVersion1StillReadable is the backward-compatibility guard. Rejecting v1 would be
// correct (the caller degrades to a scan) but would silently make every segment sealed
// before this change scan-only until retention aged it out.
func TestTidxVersion1StillReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	values := []int64{-100, -1, 0, 1, 100, 100}
	entries := make([]Entry, len(values))
	for i, v := range values {
		entries[i] = Entry{Key: intKey(v), Offset: uint64(i)}
	}
	writeTIDXPlain(t, path, KindInt, entries)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("a version-1 .tidx must still open: %v", err)
	}
	if r.Kind() != KindInt {
		t.Errorf("kind: got %v, want KindInt", r.Kind())
	}
	if got := r.Count(); got != len(values) {
		t.Fatalf("count: got %d, want %d", got, len(values))
	}
	if got := r.LookupEqual(intKey(100)); len(got) != 2 {
		t.Errorf("duplicate key lookup: got %v, want 2 offsets", got)
	}
	if got := r.LookupRange(intKey(-1), intKey(1), true, true); len(got) != 3 {
		t.Errorf("range over negatives: got %v, want 3 offsets", got)
	}
}

// TestTidxCompressedRoundTrip pins that the compressed form returns exactly what went in,
// in the same order — the rebuild-and-compare test in core/ingest compares SETS, so
// ordering needs its own assertion.
func TestTidxCompressedRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	var entries []Entry
	for i := int64(0); i < 5000; i++ {
		entries = append(entries, Entry{Key: intKey(i % 400), Offset: uint64(i)})
	}
	if err := WriteTIDX(path, KindInt, 0, entries); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Entries()
	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	// WriteTIDX sorts in place, so `entries` is now in the expected order too.
	for i := range got {
		if got[i].Key != entries[i].Key || got[i].Offset != entries[i].Offset {
			t.Fatalf("entry %d differs: got (%v,%d) want (%v,%d)",
				i, got[i].Key, got[i].Offset, entries[i].Key, entries[i].Offset)
		}
	}
}

// TestTidxIsActuallyCompressed guards against the feature silently regressing to a
// pass-through. Real files compress ~5x; this synthetic one is far more redundant.
func TestTidxIsActuallyCompressed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	var entries []Entry
	for i := int64(0); i < 20000; i++ {
		entries = append(entries, Entry{Key: intKey(i % 100), Offset: uint64(i)})
	}
	if err := WriteTIDX(path, KindInt, 0, entries); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	plain := int64(tidxHeaderSize + len(entries)*tidxRecordSize)
	ratio := float64(plain) / float64(info.Size())
	if ratio < 2 {
		t.Errorf("compression ratio %.2fx (%d -> %d bytes) — suspiciously low", ratio, plain, info.Size())
	}
	t.Logf("ratio %.2fx (%d -> %d bytes)", ratio, plain, info.Size())
}

// TestTidxRejectsTruncatedHeader covers a guard that existed but had no test.
func TestTidxRejectsTruncatedHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	if err := os.WriteFile(path, []byte("TIDX"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); err == nil {
		t.Fatal("a file shorter than the header must be rejected")
	}
}

// TestTidxRejectsLengthCountMismatch covers the replacement for the old
// `len(data) == 32 + count*24` invariant: with a compressed body, only the stored
// UNCOMPRESSED length ties the file back to the record count. The CRC is recomputed after
// tampering so this exercises the length check rather than tripping the checksum first.
func TestTidxRejectsLengthCountMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	entries := []Entry{{Key: intKey(1), Offset: 0}, {Key: intKey(2), Offset: 24}}
	if err := WriteTIDX(path, KindInt, 0, entries); err != nil {
		t.Fatal(err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Claim an uncompressed length that no longer matches count*recordSize.
	binary.BigEndian.PutUint64(buf[24:32], uint64(len(entries)*tidxRecordSize+8))
	reCRC(buf)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); err == nil {
		t.Fatal("an uncompressed length disagreeing with the record count must be rejected")
	}
}

// TestTidxRejectsCorruptCompressedBody: a flipped byte inside the deflate stream must be
// caught. The whole-file CRC is the primary guarantee here — inflate would usually fail
// too, but "usually" is not the contract.
func TestTidxRejectsCorruptCompressedBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	var entries []Entry
	for i := int64(0); i < 1000; i++ {
		entries = append(entries, Entry{Key: intKey(i), Offset: uint64(i * 24)})
	}
	if err := WriteTIDX(path, KindInt, 0, entries); err != nil {
		t.Fatal(err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[tidxHeaderSize+10] ^= 0xFF // inside the compressed region
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); err == nil {
		t.Fatal("corruption inside the compressed body must be rejected")
	}
}

// TestTidxRejectsUnknownVersion: a future version must be refused rather than misread,
// since the caller's response (degrade to scan) is correct while a misread is not.
func TestTidxRejectsUnknownVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	if err := WriteTIDX(path, KindInt, 0, []Entry{{Key: intKey(1), Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(buf[4:6], 99)
	reCRC(buf)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); err == nil {
		t.Fatal("an unknown version must be rejected")
	}
}

// TestTidxSizeBytesReportsDecodedSize: the reader cache budgets HEAP, not disk. If
// SizeBytes started reporting the compressed size the cache would hold ~5x more than it
// thinks it does.
func TestTidxSizeBytesReportsDecodedSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	var entries []Entry
	for i := int64(0); i < 1000; i++ {
		entries = append(entries, Entry{Key: intKey(i % 10), Offset: uint64(i)})
	}
	if err := WriteTIDX(path, KindInt, 0, entries); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	want := int64(len(entries)*tidxRecordSize) + 64
	if got := r.SizeBytes(); got != want {
		t.Errorf("SizeBytes: got %d, want %d (decoded size, not the %d-byte file)", got, want, info.Size())
	}
	if r.SizeBytes() <= info.Size() {
		t.Errorf("decoded size %d should exceed the compressed file size %d", r.SizeBytes(), info.Size())
	}
}
