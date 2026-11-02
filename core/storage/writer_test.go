package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
)

// intKV builds the extracted key for an int value on a field (test helper).
func intKV(field string, v int64) index.KeyedValue {
	return index.KeyedValue{Field: field, Key: index.EncodeKey(index.Value{Kind: index.KindInt, Int: v})}
}

// Entries built by makeEntry encode to exactly 32 bytes (27 prefix + 5-byte
// message + 0 extra), so a page holds (4096-32)/32 = 127 of them. Tests that need
// deterministic page boundaries rely on this.
const entriesPerPage = 127

func makeEntry(i int, svc uint16) model.LogEntry {
	return model.LogEntry{
		TS:        time.Unix(int64(1700000000+i), 0).UTC(),
		Level:     model.LogLevelInfo,
		ServiceID: svc,
		Message:   fmt.Sprintf("%05d", i), // fixed 5 bytes ⇒ 32-byte record
	}
}

// noTickOpts disables the flush ticker (1h) so only page-full flushes occur —
// making the set of durable pages deterministic — and never rotates.
func noTickOpts() Options {
	return Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour}
}

func assertPrefixMatches(t *testing.T, got []model.LogEntry, wantCount int) {
	t.Helper()
	if len(got) != wantCount {
		t.Fatalf("read %d entries, want %d", len(got), wantCount)
	}
	for i, e := range got {
		if e.Message != fmt.Sprintf("%05d", i) {
			t.Fatalf("entry %d: message %q, want %05d", i, e.Message, i)
		}
		if e.TS.UnixNano() != time.Unix(int64(1700000000+i), 0).UnixNano() {
			t.Fatalf("entry %d: TS mismatch", i)
		}
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	svc := w.InternService("checkout")

	const n = 300
	for i := 0; i < n; i++ {
		if err := w.Write(makeEntry(i, svc)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil { // flushes final partial page + seals
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
	if got[0].ServiceID != svc {
		t.Fatalf("ServiceID not preserved: got %d want %d", got[0].ServiceID, svc)
	}
}

func TestRotationAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	// ~1 data page per segment forces multiple segments over 300 entries.
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	const n = 300
	for i := 0; i < n; i++ {
		w.Write(makeEntry(i, 1))
	}
	w.Close()

	m, _ := LoadManifest(dir)
	if len(m.All()) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(m.All()))
	}
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
}

// TestLowRateIngestStillRotates: at a low ingest rate every page is written by the flush tick
// before it fills. The size and index-budget checks used to run only when a record did not
// fit in the page, so such a stream never reached them: the active segment grew past
// SegmentSizeBytes without bound and was never sealed, indexed, compressed or expired.
func TestLowRateIngestStillRotates(t *testing.T) {
	dir := t.TempDir()
	const limit = 3 * PageSize // page 0 + two data pages
	w, err := NewWriter(dir, Options{SegmentSizeBytes: limit, FlushInterval: 5 * time.Millisecond, BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	const n = 12
	for i := 0; i < n; i++ {
		if err := w.Write(makeEntry(i, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond) // several ticks: each record goes out on a page of its own
	}

	// Inspected BEFORE Close, which seals whatever is active and would hide the bug. The
	// on-disk manifest is read so the check does not race the writer goroutine.
	sealed := 0
	for _, s := range mustManifest(t, dir).All() {
		if s.State == SegmentSealed {
			sealed++
		}
		if st, err := os.Stat(s.Path); err == nil && st.Size() > limit {
			t.Errorf("segment %s is %d bytes, past SegmentSizeBytes=%d", s.ID, st.Size(), limit)
		}
	}
	if sealed == 0 {
		t.Errorf("no segment sealed after %d tick-flushed pages with a %d-byte segment limit", n, limit)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
}

// newSyncWriter returns a writer whose goroutine is never started, with its first segment
// open. Tests use it to drive the writer goroutine's own methods (processEntry, flushPage)
// from the test goroutine, so a failure can be injected between two records without racing
// the goroutine. Close still flushes and seals as usual.
func newSyncWriter(t *testing.T, dir string, opts Options) *Writer {
	t.Helper()
	w, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.rotateSegment(); err != nil {
		t.Fatal(err)
	}
	w.resetPage()
	return w
}

// segmentLogs lists the .log files in a shard's segments directory.
func segmentLogs(t *testing.T, dir string) []string {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "segments", "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	return logs
}

// syncWrites returns a func that appends k makeEntry records through processEntry, numbering
// them on from the previous call, each with an int key on field "n" when keyed is set.
func syncWrites(w *Writer, keyed bool) (write func(k int), count func() int) {
	n := 0
	write = func(k int) {
		for j := 0; j < k; j++ {
			r := record{entry: makeEntry(n, 1)}
			if keyed {
				r.keys = []index.KeyedValue{intKV("n", int64(n))}
			}
			w.processEntry(r)
			n++
		}
	}
	return write, func() int { return n }
}

// assertStillOn fails unless the writer is still appending to segment id, the manifest holds
// only that segment (still active), and no other segment file exists.
func assertStillOn(t *testing.T, w *Writer, dir, id, stage string) {
	t.Helper()
	if w.currSeg.ID != id {
		t.Fatalf("%s: writer moved to %s although the rotation failed", stage, w.currSeg.ID)
	}
	if segs := w.manifest.All(); len(segs) != 1 || segs[0].ID != id || segs[0].State != SegmentActive {
		t.Fatalf("%s: manifest should hold only the still-active segment, got %d entries", stage, len(segs))
	}
	if logs := segmentLogs(t, dir); len(logs) != 1 {
		t.Fatalf("%s: the failed attempt left segment files behind: %v", stage, logs)
	}
}

// TestFailedSegmentCreateKeepsWriting: a rotation that cannot create the next segment must
// leave the writer on the segment it was writing, losing nothing. Rotation used to seal (and
// close) the old segment first and only then create the next one, so one failed create —
// EACCES here, ENOSPC in practice — left the writer holding a closed file: every later write
// failed, every acknowledged record was dropped until a restart, and the manifest gained an
// active entry with no file behind it.
func TestFailedSegmentCreateKeepsWriting(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	w := newSyncWriter(t, dir, Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour, BlockPages: -1})
	first := w.currSeg.ID
	write, count := syncWrites(w, false)

	segDir := filepath.Join(dir, "segments")
	if err := os.Chmod(segDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(segDir, 0o755) })
	write(3 * entriesPerPage) // two page boundaries past the size limit
	assertStillOn(t, w, dir, first, "create failed")

	// The disk problem is gone: the next attempt rotates.
	if err := os.Chmod(segDir, 0o755); err != nil {
		t.Fatal(err)
	}
	w.rotateRetryAt = time.Time{} // skip the retry back-off
	write(2 * entriesPerPage)
	if w.currSeg.ID == first {
		t.Fatal("rotation did not resume once the next segment could be created")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, count())
	for _, s := range mustManifest(t, dir).All() {
		if s.State != SegmentSealed {
			t.Fatalf("segment %s is %s after Close; a phantom active entry survived", s.ID, s.State)
		}
	}
}

// TestFailedSealKeepsWriting: when the next segment is created but the current one cannot be
// sealed, the new files are undone and the writer keeps appending to the current segment. The
// record whose page boundary triggered the attempt used to be dropped — after it had been
// acknowledged — once per failed attempt.
func TestFailedSealKeepsWriting(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "n", Kind: index.KindInt}}
	w := newSyncWriter(t, dir, Options{Schema: schema, SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour, BlockPages: -1})
	first := w.currSeg.ID
	write, count := syncWrites(w, true)

	// A directory where the .tidx temp file must go makes the index flush, and so the seal,
	// fail.
	obstacle := index.TidxPath(strings.TrimSuffix(w.currSeg.Path, ".log"), "n") + ".tmp"
	if err := os.Mkdir(obstacle, 0o755); err != nil {
		t.Fatal(err)
	}
	write(3 * entriesPerPage)
	w.rotateRetryAt = time.Time{}
	write(entriesPerPage)
	assertStillOn(t, w, dir, first, "seal failed")

	if err := os.Remove(obstacle); err != nil {
		t.Fatal(err)
	}
	w.rotateRetryAt = time.Time{}
	write(entriesPerPage)
	if w.currSeg.ID == first {
		t.Fatal("rotation did not resume once the segment could be sealed")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, count())
	segs := mustManifest(t, dir).All()
	if len(segs) != 2 {
		t.Fatalf("want the grown first segment and its successor, got %d segments", len(segs))
	}
	for _, s := range segs {
		if s.State != SegmentSealed || !s.Indexed {
			t.Fatalf("segment %s: state %s indexed=%v, want sealed and indexed", s.ID, s.State, s.Indexed)
		}
		r, err := index.OpenReader(index.TidxPath(strings.TrimSuffix(s.Path, ".log"), "n"))
		if err != nil {
			t.Fatal(err)
		}
		if uint64(r.Count()) != s.Records {
			t.Fatalf("segment %s: .tidx holds %d entries for %d records", s.ID, r.Count(), s.Records)
		}
	}
}

// TestFailedSealSaveLeavesSegmentActive: a seal whose manifest save fails must not be visible
// in memory either. It used to be applied first and saved second, so after a failed save the
// live manifest said "sealed, indexed" while the writer kept appending to the segment — and
// the planner pushed predicates into a .tidx missing every later record.
func TestFailedSealSaveLeavesSegmentActive(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "n", Kind: index.KindInt}}
	w := newSyncWriter(t, dir, Options{Schema: schema, SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour, BlockPages: -1})
	first := w.currSeg.ID
	write, count := syncWrites(w, true)

	// A directory where the manifest's temp file must go makes the save fail.
	obstacle := filepath.Join(dir, "manifest.json.tmp")
	if err := os.Mkdir(obstacle, 0o755); err != nil {
		t.Fatal(err)
	}
	write(entriesPerPage + 1) // one page boundary at the size limit: the seal runs, its save fails
	if s := w.manifest.Get(first); s == nil || s.State != SegmentActive || s.Indexed {
		t.Fatalf("after a failed seal save the segment reads as %+v, want active and not indexed", s)
	}
	write(entriesPerPage) // the writer keeps appending to it

	if err := os.Remove(obstacle); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s := mustManifest(t, dir).Get(first)
	if s == nil || s.State != SegmentSealed || s.Records != uint64(count()) {
		t.Fatalf("after Close the segment reads as %+v, want sealed with all %d records", s, count())
	}
	r, err := index.OpenReader(index.TidxPath(strings.TrimSuffix(s.Path, ".log"), "n"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != count() {
		t.Fatalf(".tidx holds %d entries for %d records", r.Count(), count())
	}
}

// lookupN returns the messages of the records the sealed segment's "n" index finds for v.
func lookupN(t *testing.T, segPath string, v int64) []string {
	t.Helper()
	r, err := index.OpenReader(index.TidxPath(strings.TrimSuffix(segPath, ".log"), "n"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	err = FetchRecords(segPath, r.LookupEqual(index.EncodeKey(index.Value{Kind: index.KindInt, Int: v})), func(e model.LogEntry) bool {
		out = append(out, e.Message)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// readOnlyFile opens path read-only: swapped in for a writer's descriptor, every write
// through it fails, standing in for EIO or ENOSPC.
func readOnlyFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestTickFlushFailureKeepsPage: a partial page the flush tick cannot write must stay in RAM.
// The tick used to reset it anyway, losing its records while their index entries stayed
// buffered — pointing at the page number the next page was then written to, so an exact
// index lookup returned a different record.
func TestTickFlushFailureKeepsPage(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "n", Kind: index.KindInt}}
	w := newSyncWriter(t, dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	segPath := w.currSeg.Path
	put := func(i int) {
		w.processEntry(record{entry: makeEntry(i, 1), keys: []index.KeyedValue{intKV("n", int64(i))}})
	}
	for i := 0; i < 5; i++ {
		put(i)
	}
	good := w.currSeg.File
	w.currSeg.File = readOnlyFile(t, segPath)
	w.flushTick(time.Now()) // the page write fails
	w.currSeg.File = good
	for i := 5; i < 10; i++ {
		put(i)
	}
	w.flushTick(time.Now())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, 10)
	for v := int64(0); v < 10; v++ {
		if msgs := lookupN(t, segPath, v); len(msgs) != 1 || msgs[0] != fmt.Sprintf("%05d", v) {
			t.Fatalf("index lookup n == %d returned %v", v, msgs)
		}
	}
}

// TestFullPageWrittenOnceWhenIndexAppendFails: once a page is on disk it must count as
// written, whatever fails after. A failed .idx append (an fsync error behaves the same) used
// to leave the page in RAM with its page number already advanced, so it was written a second
// time as the next page — duplicates in every scan — and the record that triggered the flush
// was dropped.
func TestFullPageWrittenOnceWhenIndexAppendFails(t *testing.T) {
	dir := t.TempDir()
	w := newSyncWriter(t, dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	write, count := syncWrites(w, false)
	write(entriesPerPage) // exactly one full page

	good := w.idxWriter.file
	w.idxWriter.file = readOnlyFile(t, indexPathFor(w.currSeg.Path))
	write(1) // flushes the full page: the page write succeeds, the .idx append fails
	w.idxWriter.file = good
	write(1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, count())
}

// TestFullPageWriteFailureRetries: when the full page cannot be written, the record that did
// not fit — already acknowledged — must wait for it rather than be dropped, and so must every
// record behind it. They used to be dropped one per failed attempt until the write worked.
func TestFullPageWriteFailureRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	// A new service name must be made durable in lookup.bin before the first page that uses
	// it; a read-only shard directory makes that, and so the page flush, fail.
	svc := w.InternService("checkout")
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	const n = 2 * entriesPerPage
	for i := 0; i < n; i++ {
		if err := w.Write(makeEntry(i, svc)); err != nil {
			t.Fatal(err)
		}
	}
	// Wait until the record that does not fit has been taken off the queue, then let a few
	// attempts fail.
	for deadline := time.Now().Add(5 * time.Second); len(w.writeCh) >= n-entriesPerPage; {
		if time.Now().After(deadline) {
			t.Fatal("the writer never reached the full page")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(3 * flushRetryDelay)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
}

// TestCloseReportsFailedSeal: Close used to return nil whatever its final flush and seal did,
// so a daemon whose last seal failed exited as if every record were safe.
func TestCloseReportsFailedSeal(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "n", Kind: index.KindInt}}
	w := newSyncWriter(t, dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	for i := 0; i < 10; i++ {
		w.processEntry(record{entry: makeEntry(i, 1), keys: []index.KeyedValue{intKV("n", int64(i))}})
	}
	obstacle := index.TidxPath(strings.TrimSuffix(w.currSeg.Path, ".log"), "n") + ".tmp"
	if err := os.Mkdir(obstacle, 0o755); err != nil {
		t.Fatal(err)
	}
	err := w.Close()
	if err == nil {
		t.Fatal("Close returned nil although the final seal failed")
	}
	if again := w.Close(); again != err {
		t.Fatalf("a second Close returned %v, want the same %v", again, err)
	}
}

func TestSealWritesSchemaField(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, noTickOpts())
	w.Start(nil)
	w.Write(makeEntry(0, 1))
	w.Close()

	m, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	segs := m.All()
	if len(segs) != 1 {
		t.Fatalf("want 1 segment, got %d", len(segs))
	}
	if segs[0].State != SegmentSealed {
		t.Fatalf("segment not sealed: %s", segs[0].State)
	}
	if segs[0].Schema == nil {
		t.Fatal("schema field missing from sealed manifest (must be present, even if empty)")
	}
}

// TestSealRecordsBuildConfig: a sealed segment records the configuration its indexes were
// built with — each field's pattern and the label allowlist — so the planner can refuse to
// use an index after the configuration has changed.
func TestSealRecordsBuildConfig(t *testing.T) {
	dir := t.TempDir()
	opts := noTickOpts()
	opts.Schema = []index.FieldType{{Name: "latency_ms", Kind: index.KindInt, Pattern: "took {ms:int}ms"}}
	opts.LabelKeys = []string{"region", "env"}
	w, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	if err := w.WriteExtracted(makeEntry(0, 1), []index.KeyedValue{intKV("latency_ms", 250)}, nil); err != nil {
		t.Fatal(err)
	}
	w.Close()

	m, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	seg := m.All()[0]
	if len(seg.Schema) != 1 || seg.Schema[0].Pattern != "took {ms:int}ms" {
		t.Fatalf("schema = %+v, want latency_ms with its pattern", seg.Schema)
	}
	if !seg.LabelKeysRecorded || strings.Join(seg.LabelKeys, ",") != "env,region" {
		t.Fatalf("label keys = %v (recorded=%v), want sorted [env region]", seg.LabelKeys, seg.LabelKeysRecorded)
	}

	// Without Options.LabelKeys nothing is claimed about the allowlist.
	dir2 := t.TempDir()
	w2, err := NewWriter(dir2, noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	w2.Start(nil)
	w2.Write(makeEntry(0, 1))
	w2.Close()
	m2, _ := LoadManifest(dir2)
	if m2.All()[0].LabelKeysRecorded {
		t.Fatal("a writer without Options.LabelKeys must not record an allowlist")
	}
}

// TestWriteRejectsRecordLargerThanPage: a record that cannot fit in one page must be refused
// when it is written, not accepted and then dropped by the writer goroutine, where no caller
// can see the failure. A record of exactly MaxRecordSize must still be stored.
func TestWriteRejectsRecordLargerThanPage(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)

	extra := `{"app":"checkout"}`
	fits := model.LogEntry{
		TS:      time.Unix(1700000000, 0).UTC(),
		Level:   model.LogLevelError,
		Extra:   extra,
		Message: strings.Repeat("a", MaxRecordSize-recordPrefixSize-len(extra)),
	}
	if EncodedSize(fits) != MaxRecordSize {
		t.Fatalf("test setup: record is %d bytes, want exactly %d", EncodedSize(fits), MaxRecordSize)
	}
	tooBig := fits
	tooBig.Message += "b"

	if err := w.Write(fits); err != nil {
		t.Fatalf("a record of exactly MaxRecordSize must be accepted: %v", err)
	}
	if err := w.Write(tooBig); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Write of %d bytes: got %v, want ErrEntryTooLarge", EncodedSize(tooBig), err)
	}
	if err := w.WriteExtractedCtx(context.Background(), tooBig, nil, nil); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("WriteExtractedCtx of %d bytes: got %v, want ErrEntryTooLarge", EncodedSize(tooBig), err)
	}
	w.Close()

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Message != fits.Message {
		t.Fatalf("stored %d records, want only the one that fits", len(got))
	}
}

// TestWriteRejectsInvalidLevel: a record whose Level the decoder rejects must be refused
// when it is written. It used to be stored, and then never came back from any read.
func TestWriteRejectsInvalidLevel(t *testing.T) {
	w, err := NewWriter(t.TempDir(), noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	defer w.Close()
	e := makeEntry(0, 1)
	e.Level = model.LogLevel(9)
	if err := w.Write(e); !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("Write: got %v, want ErrInvalidLevel", err)
	}
	if err := w.WriteExtractedCtx(context.Background(), e, nil, nil); !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("WriteExtractedCtx: got %v, want ErrInvalidLevel", err)
	}
}

// TestUndecodableRecordSkippedAlone: a record that will not decode inside a page that passed
// its CRC — one an older writer stored with an invalid level — must cost that record only,
// and cost it on every read path. The scan stopped at it and lost the rest of its page,
// ReadAll dropped the whole page, and FetchRecords, which decodes each offset on its own,
// still returned the later records: the index and scan paths disagreed.
func TestUndecodableRecordSkippedAlone(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	const n, bad = 20, 5
	for i := 0; i < n; i++ {
		w.Write(makeEntry(i, 1))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := mustManifest(t, dir).All()[0].Path
	recs, err := readSegmentRecords(path)
	if err != nil || len(recs) != n {
		t.Fatalf("setup: read %d records (err %v), want %d", len(recs), err, n)
	}

	// Store record 5 with level 9, page CRC and all, as a writer without the check would.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	pageNum := recs[bad].Offset / PageSize
	page := make([]byte, PageSize)
	if _, err := f.ReadAt(page, PageOffset(pageNum)); err != nil {
		t.Fatal(err)
	}
	page[recs[bad].Offset%PageSize+16] = 9
	FinalizePage(page, decodePageHeader(page[:PageHeaderSize]))
	if _, err := f.WriteAt(page, PageOffset(pageNum)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	want := n - 1
	scanned := 0
	if err := ScanSegmentTimeRange(path, 0, 1<<62, func(model.LogEntry) bool { scanned++; return true }); err != nil {
		t.Fatal(err)
	}
	all, _ := ReadAll(dir)
	withOffsets, _ := readSegmentRecords(path)
	var offsets []uint64
	for _, r := range recs {
		offsets = append(offsets, r.Offset)
	}
	fetched := 0
	if err := FetchRecords(path, offsets, func(model.LogEntry) bool { fetched++; return true }); err != nil {
		t.Fatal(err)
	}
	if scanned != want || len(all) != want || len(withOffsets) != want || fetched != want {
		t.Fatalf("with one undecodable record of %d: scan=%d ReadAll=%d records=%d fetch=%d, want %d each",
			n, scanned, len(all), len(withOffsets), fetched, want)
	}
	for _, e := range all {
		if e.Message == fmt.Sprintf("%05d", bad) {
			t.Fatal("the undecodable record was returned")
		}
	}
}

// TestDataDirRelocatable: moving a shard directory must not lose its data. The manifest used
// to store full paths built from the configured data_dir, so after a move every sealed segment
// was "not found" — which the query path reads as a retention race, i.e. an empty result with
// no error.
func TestDataDirRelocatable(t *testing.T) {
	root := t.TempDir()
	oldDir, newDir := filepath.Join(root, "old"), filepath.Join(root, "moved", "shard")
	w, err := NewWriter(oldDir, Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	const n = 300
	for i := 0; i < n; i++ {
		w.Write(makeEntry(i, 1))
	}
	w.Close() // seals, and waits for the background compressions

	raw, err := os.ReadFile(filepath.Join(oldDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), oldDir) {
		t.Fatalf("manifest.json still stores absolute paths under %s", oldDir)
	}

	if err := os.MkdirAll(filepath.Dir(newDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldDir, newDir); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(newDir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)

	// A writer reopened at the new location sees every segment where it now is.
	w2, err := NewWriter(newDir, noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	for _, s := range w2.Manifest().All() {
		if !strings.HasPrefix(s.Path, newDir) {
			t.Fatalf("segment %s resolved to %s, outside the moved directory", s.ID, s.Path)
		}
	}
}

// TestLegacyManifestPathIgnored: a manifest written before paths became relative stores
// whatever data_dir was configured at the time. Those strings must not be trusted over the
// segment files that are actually present.
func TestLegacyManifestPathIgnored(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	const n = 300
	for i := 0; i < n; i++ {
		w.Write(makeEntry(i, 1))
	}
	w.Close()

	mpath := filepath.Join(dir, "manifest.json")
	raw, err := os.ReadFile(mpath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := strings.ReplaceAll(string(raw), `"path": "segments/`, `"path": "/no/such/old-data-dir/shard-0000/segments/`)
	if legacy == string(raw) {
		t.Fatal("test setup: expected relative segment paths to rewrite")
	}
	if err := os.WriteFile(mpath, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
}

// TestCrashRecoveryRepublishesBounds is the regression for the SIGKILL data-loss
// bug: a crash before seal never persisted the active segment's real time bounds,
// so on restart it loaded with sentinel bounds and dropped out of time-range
// queries. Recovery must rescan the segment and republish correct bounds.
func TestCrashRecoveryRepublishesBounds(t *testing.T) {
	dir := t.TempDir()
	w1, _ := NewWriter(dir, noTickOpts())
	w1.Start(nil)

	const n = 300 // 2 full pages (254 entries) flush; the 3rd partial page is lost on crash
	for i := 0; i < n; i++ {
		w1.Write(makeEntry(i, 1))
	}
	w1.stop() // crash: no final flush, no seal

	const durable = 2 * entriesPerPage // 254

	// Sanity: on-disk manifest still has the active segment with stale (sentinel)
	// bounds — the bug's precondition.
	pre, _ := LoadManifest(dir)
	if a := pre.Active(); a == nil || a.MinTS != emptyMinTS {
		t.Fatalf("precondition: active segment should have stale sentinel bounds, got %+v", a)
	}

	// Recovery happens in NewWriter.
	w2, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer w2.Close()

	active := w2.manifest.Active()
	if active == nil {
		t.Fatal("no active segment after recovery")
	}
	wantMin := time.Unix(1700000000, 0).UnixNano()
	wantMax := time.Unix(int64(1700000000+durable-1), 0).UnixNano()
	if active.MinTS != wantMin || active.MaxTS != wantMax {
		t.Fatalf("bounds not republished: got [%d,%d], want [%d,%d]", active.MinTS, active.MaxTS, wantMin, wantMax)
	}

	// The whole point: a time-range filter must now include the recovered segment.
	if len(w2.manifest.Filter(wantMin, wantMax)) != 1 {
		t.Fatal("recovered active segment excluded from time-range filter")
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, durable) // exactly the flushed pages survive
}

// TestTornPageTruncatedOnRecovery: a crash mid-write can leave a partially-written
// (torn) trailing page. Recovery must detect it via the full-page checksum and
// truncate it, keeping every prior valid entry.
func TestTornPageTruncatedOnRecovery(t *testing.T) {
	dir := t.TempDir()
	w1, _ := NewWriter(dir, noTickOpts())
	w1.Start(nil)
	const n = 300
	for i := 0; i < n; i++ {
		w1.Write(makeEntry(i, 1))
	}
	w1.stop()

	const durable = 2 * entriesPerPage // 254 entries in 2 flushed pages

	// Find the active segment file and append a junk page (a torn trailing write).
	m, _ := LoadManifest(dir)
	path := m.Active().Path
	seg, err := OpenSegment(path) // opens read-write; no torn tail yet
	if err != nil {
		t.Fatal(err)
	}
	tornPageNum := seg.PageNum // one past the last valid page
	junk := make([]byte, PageSize)
	for i := range junk {
		junk[i] = 0xFF // bad magic + bad checksum
	}
	if err := seg.WritePage(tornPageNum, junk); err != nil {
		t.Fatal(err)
	}
	seg.Sync()
	sizeBefore, _ := seg.File.Stat()
	seg.Close()

	// Recovery must truncate the torn page.
	w2, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer w2.Close()

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, durable)

	// File should be back to whole valid pages (torn page removed).
	info, _ := w2.currSeg.File.Stat()
	if info.Size() >= sizeBefore.Size() {
		t.Fatalf("torn page not truncated: size %d (was %d before recovery)", info.Size(), sizeBefore.Size())
	}
	if info.Size() != PageOffset(tornPageNum) {
		t.Fatalf("truncated to %d, want %d (end of last valid page)", info.Size(), PageOffset(tornPageNum))
	}
}

// TestRecoveryKeepsPagesAfterMidSegmentCorruption: recovery truncates a torn TAIL. A corrupt
// page with valid pages after it is damage in the middle, not a torn write, and recovery used
// to treat it as one: everything from it onwards was truncated, though every later page was
// valid and long since fsynced. Only the trailing run of invalid pages may go.
func TestRecoveryKeepsPagesAfterMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	w1, _ := NewWriter(dir, noTickOpts()) // SyncInterval 0: every page fsynced as it is written
	w1.Start(nil)
	const pages = 10
	for i := 0; i < pages*entriesPerPage+1; i++ { // the extra record pushes the 10th page out
		w1.Write(makeEntry(i, 1))
	}
	w1.stop() // crash

	path := mustManifest(t, dir).Active().Path
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, PageOffset(2)+100); err != nil { // bit rot in page 2
		t.Fatal(err)
	}
	junk := make([]byte, PageSize)
	for i := range junk {
		junk[i] = 0xFF
	}
	if _, err := f.WriteAt(junk, PageOffset(pages+1)); err != nil { // and a torn trailing page
		t.Fatal(err)
	}
	f.Close()

	w2, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if info, _ := w2.currSeg.File.Stat(); info.Size() != PageOffset(pages+1) {
		t.Fatalf("recovered segment is %d bytes, want %d: pages 0-%d kept, only the torn tail cut", info.Size(), PageOffset(pages+1), pages)
	}
	w2.Start(nil)
	if err := w2.Write(makeEntry(99999, 1)); err != nil { // appends after the last valid page
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := (pages-1)*entriesPerPage + 1 // all but page 2, plus the record written after recovery
	if len(got) != want {
		t.Fatalf("read %d records after recovery, want %d", len(got), want)
	}
	if last := got[len(got)-2].Message; last != fmt.Sprintf("%05d", pages*entriesPerPage-1) {
		t.Fatalf("last pre-crash record is %q, want the one from page %d", last, pages)
	}
}

// A missing event time is defaulted to arrival time rather than being rejected or
// stored as a wrapped zero-instant.
func TestZeroTSDefaultsToIngestedAt(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, noTickOpts())
	w.Start(nil)
	ingest := time.Unix(1700000000, 0).UTC()
	if err := w.Write(model.LogEntry{IngestedAt: ingest, Message: "x"}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got, _ := ReadAll(dir)
	if len(got) != 1 {
		t.Fatalf("want 1 entry, got %d", len(got))
	}
	if got[0].TS.UnixNano() != ingest.UnixNano() {
		t.Fatalf("zero TS not defaulted to IngestedAt: got %v want %v", got[0].TS, ingest)
	}
}

// TestServiceLookupSurvivesCrash is the regression for the interning-table bug: a
// crash before clean Close used to lose lookup.bin, so ServiceIDs got reused for
// different names and durable entries were misattributed. The table must now be
// durable (persisted on flush) and IDs must not be reused after recovery.
func TestServiceLookupSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	w1, _ := NewWriter(dir, noTickOpts())
	w1.Start(nil)
	checkout := w1.InternService("checkout")
	billing := w1.InternService("billing")

	// Fill at least one full page so a flush (and thus a durable lookup.bin) happens.
	for i := 0; i < 2*entriesPerPage; i++ {
		svc := checkout
		if i%2 == 1 {
			svc = billing
		}
		w1.Write(makeEntry(i, svc))
	}
	w1.stop() // crash — no clean Close

	w2, err := NewWriter(dir, noTickOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if n, ok := w2.serviceLT.GetName(checkout); !ok || n != "checkout" {
		t.Fatalf("checkout id %d resolves to %q,%v after crash", checkout, n, ok)
	}
	if n, ok := w2.serviceLT.GetName(billing); !ok || n != "billing" {
		t.Fatalf("billing id %d resolves to %q,%v after crash", billing, n, ok)
	}
	if got := w2.InternService("analytics"); got == checkout || got == billing {
		t.Fatalf("new service reused an existing ID %d (checkout=%d billing=%d)", got, checkout, billing)
	}
}

// TestCloseIdempotentAndCtxCancel exercises the concurrent-shutdown path: cancelling
// the Start ctx (which triggers the watcher's Close) while an explicit Close() runs,
// plus a redundant third Close. With -race this fails if the post-Wait cleanup is not
// guarded by closeOnce (double flush/seal on shared page state).
func TestCloseIdempotentAndCtxCancel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	w, _ := NewWriter(dir, noTickOpts())
	w.Start(ctx)
	const n = 50 // < one page: graceful shutdown must flush the partial page
	for i := 0; i < n; i++ {
		w.Write(makeEntry(i, 1))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); cancel() }()
	go func() { defer wg.Done(); _ = w.Close() }()
	wg.Wait()
	_ = w.Close() // idempotent

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, n)
}

// Retention deletes sealed segments whose data is entirely older than the window (and
// their sidecar index files), keeping recent segments — and never touches the active one.
func TestRetentionDeletesSealedSegments(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour, Retention: 24 * time.Hour}

	// Phase 1: old records (2023 timestamps), sealed into their own segments.
	w1, _ := NewWriter(dir, opts)
	w1.Start(nil)
	for i := 0; i < 200; i++ {
		w1.Write(makeEntry(i, 1)) // makeEntry TS is in 2023 → older than 24h
	}
	w1.Close()

	// Phase 2: recent records, sealed into a separate segment.
	w2, _ := NewWriter(dir, opts)
	w2.Start(nil)
	recent := time.Now().Add(-time.Minute)
	for i := 0; i < 30; i++ {
		w2.Write(model.LogEntry{TS: recent.Add(time.Duration(i) * time.Second), Level: model.LogLevelInfo, Message: fmt.Sprintf("%05d", i)})
	}
	w2.Close()

	beforeSegs := len(mustManifest(t, dir).All())
	if beforeSegs < 3 {
		t.Fatalf("expected several sealed segments, got %d", beforeSegs)
	}

	// Enforce retention on a fresh (unstarted) writer so no goroutine races the Save.
	w3, _ := NewWriter(dir, opts)
	w3.enforceRetention(time.Now())

	// Only the recent records survive; a query reads them without error.
	recs, err := ReadAllRecords(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 30 {
		t.Fatalf("after retention: got %d records, want 30 (the recent ones)", len(recs))
	}
	// Old segments' files are gone. Counted across BOTH extensions: a sealed segment is
	// rewritten to .logz, so counting only .log would pass for the wrong reason (or, as
	// here, fail for one) the moment compression is enabled.
	raw, _ := filepath.Glob(filepath.Join(dir, "segments", "*.log"))
	comp, _ := filepath.Glob(filepath.Join(dir, "segments", "*.logz"))
	if len(raw)+len(comp) != 1 {
		t.Fatalf("expected 1 surviving segment file, got %d raw + %d compressed", len(raw), len(comp))
	}
	// A segment must never be left as both: the compressed rewrite unlinks the raw file
	// only after the sidecar is durable, so seeing both means that sequence broke.
	for _, c := range comp {
		if _, err := os.Stat(strings.TrimSuffix(c, ".logz") + ".log"); err == nil {
			t.Errorf("%s has both a raw and a compressed file", c)
		}
	}
}

// TestRetentionDeletesFilesUnderPatternLikeDir: retention found a segment's files with the glob
// <base>.*, which made the data directory part of the pattern. Under a directory named like
// "logd[prod]" it matched nothing, the delete reported success, and the manifest entry was
// dropped while every file stayed on disk, never to be deleted.
func TestRetentionDeletesFilesUnderPatternLikeDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logd[prod]", "shard-0000")
	opts := Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, Retention: time.Hour}
	w1, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	w1.Start(nil)
	for i := 0; i < 300; i++ {
		w1.Write(makeEntry(i, 1)) // 2023 timestamps: past the retention window
	}
	if err := w1.Close(); err != nil { // seals, compresses, writes the sidecars
		t.Fatal(err)
	}

	w2, err := NewWriter(dir, opts) // unstarted, so no goroutine races the sweep
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	w2.enforceRetention(time.Now())
	if n := len(w2.manifest.All()); n != 0 {
		t.Fatalf("%d segments still in the manifest after retention", n)
	}
	left, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		var names []string
		for _, e := range left {
			names = append(names, e.Name())
		}
		t.Fatalf("retention left the expired segment's files behind: %v", names)
	}
}

func mustManifest(t *testing.T, dir string) *Manifest {
	t.Helper()
	m, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A shard folder's flock prevents a second concurrent writer; after Close another writer
// can take it.
func TestShardLockPreventsSecondWriter(t *testing.T) {
	dir := t.TempDir()
	w1, err := NewWriter(dir, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w1.Start(nil)
	if _, err := NewWriter(dir, Options{FlushInterval: time.Hour}); err == nil {
		w1.Close()
		t.Fatal("a second writer on the same shard folder must fail (flock)")
	}
	w1.Close() // releases the lock

	w2, err := NewWriter(dir, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("reopen after close should succeed: %v", err)
	}
	w2.Close()
}

// The index-memory budget seals segments early even when the size cap is never reached,
// bounding index RAM — and no data is lost across the forced rotations.
func TestIndexMemBudgetForcesRotation(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "latency_ms", Kind: index.KindInt}}
	// Huge size cap so ONLY the index-mem budget can rotate; a 1 KB budget is exceeded
	// within a single page (~127 entries × 24 B), forcing frequent seals.
	w, _ := NewWriter(dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, IndexMemBudget: 1024})
	w.Start(nil)
	const n = 400
	for i := 0; i < n; i++ {
		if err := w.WriteExtracted(makeEntry(i, 1), []index.KeyedValue{intKV("latency_ms", int64(i))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs := mustManifest(t, dir).All()
	if len(segs) < 2 {
		t.Fatalf("index-mem budget should force multiple segments, got %d", len(segs))
	}
	for _, s := range segs {
		if !s.Indexed {
			t.Fatalf("segment %s should be fully indexed", s.ID)
		}
	}
	recs, err := ReadAllRecords(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("got %d records across the forced segments, want %d", len(recs), n)
	}
}

// A normally-sealed segment writes a .tidx for its indexed fields, records them in the
// manifest schema, and the index resolves values to the right record offsets.
func TestTidxWrittenOnSeal(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "latency_ms", Kind: index.KindInt}}
	w, _ := NewWriter(dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	const n = 300
	for i := 0; i < n; i++ {
		if err := w.WriteExtracted(makeEntry(i, 1), []index.KeyedValue{intKV("latency_ms", int64(i))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	m, _ := LoadManifest(dir)
	segs := m.All()
	if len(segs) != 1 || segs[0].State != SegmentSealed {
		t.Fatalf("want 1 sealed segment, got %d", len(segs))
	}
	if len(segs[0].Schema) != 1 || segs[0].Schema[0].Name != "latency_ms" || segs[0].Schema[0].Type != "int" {
		t.Fatalf("schema not recorded correctly: %+v", segs[0].Schema)
	}
	if !segs[0].Indexed {
		t.Fatal("a fully-indexed segment must be marked Indexed=true")
	}

	segBase := strings.TrimSuffix(segs[0].Path, ".log")
	r, err := index.OpenReader(index.TidxPath(segBase, "latency_ms"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != n {
		t.Fatalf(".tidx has %d records, want %d", r.Count(), n)
	}
	offs := r.LookupEqual(index.EncodeKey(index.Value{Kind: index.KindInt, Int: 5}))
	if len(offs) != 1 {
		t.Fatalf("LookupEqual(5): got %v", offs)
	}
	if got := len(r.LookupRange(
		index.EncodeKey(index.Value{Kind: index.KindInt, Int: 10}),
		index.EncodeKey(index.Value{Kind: index.KindInt, Int: 20}), true, true)); got != 11 {
		t.Fatalf("range [10,20]: got %d want 11", got)
	}
}

// After a crash, the recovered active segment must be sealed with an EMPTY schema
// (scan-only) and must NOT get a .tidx — a partial index would be a false negative for
// the pre-crash records that were never buffered.
func TestRecoverySealsAsScanned(t *testing.T) {
	dir := t.TempDir()
	schema := []index.FieldType{{Name: "latency_ms", Kind: index.KindInt}}
	w1, _ := NewWriter(dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w1.Start(nil)
	for i := 0; i < 300; i++ {
		w1.WriteExtracted(makeEntry(i, 1), []index.KeyedValue{intKV("latency_ms", int64(i))}, nil)
	}
	w1.stop() // crash; 254 durable, segment still active on disk

	w2, err := NewWriter(dir, Options{Schema: schema, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w2.Close() // seals the recovered segment (scan-only)

	m, _ := LoadManifest(dir)
	segs := m.All()
	if len(segs) != 1 || segs[0].State != SegmentSealed {
		t.Fatalf("want 1 sealed recovered segment, got %d", len(segs))
	}
	if len(segs[0].Schema) != 0 {
		t.Fatalf("recovered segment must have empty schema (scan-only), got %+v", segs[0].Schema)
	}
	if segs[0].Indexed {
		t.Fatal("recovered segment must be marked Indexed=false (scan-only)")
	}
	segBase := strings.TrimSuffix(segs[0].Path, ".log")
	if _, err := os.Stat(index.TidxPath(segBase, "latency_ms")); !os.IsNotExist(err) {
		t.Fatal("recovered segment must not have a .tidx")
	}
	if recs, _ := ReadAllRecords(dir); len(recs) != 2*entriesPerPage {
		t.Fatalf("recovered records: got %d want %d", len(recs), 2*entriesPerPage)
	}
}

// TestMissingActiveSegmentDoesNotBlockTheShard: a manifest naming an active segment whose
// file is gone made NewWriter fail ("recovering active segment: ... no such file"), so the
// daemon would not start and every SEALED segment of the shard was unavailable too — found by
// the fault matrix (E-6). The unsealed records are lost either way; the sealed ones must not be.
func TestMissingActiveSegmentDoesNotBlockTheShard(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentSizeBytes: 4 * PageSize, FlushInterval: time.Hour, BlockPages: -1}
	w1, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Start(nil); err != nil {
		t.Fatal(err)
	}
	const n = 6 * entriesPerPage // more than one 4-page segment, so at least one seals
	for i := 0; i < n; i++ {
		if err := w1.Write(makeEntry(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(5 * time.Second); len(w1.manifest.All()) < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no segment sealed")
		}
	}
	w1.stop() // crash: the active segment stays active in the manifest

	pre, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	active := pre.Active()
	if active == nil {
		t.Fatal("precondition: the manifest should still name an active segment")
	}
	var sealed int
	for _, s := range pre.All() {
		if s.State == SegmentSealed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Fatal("precondition: expected at least one sealed segment")
	}
	if err := os.Remove(active.Path); err != nil {
		t.Fatal(err)
	}
	var durable []model.LogEntry // what the sealed segments still hold
	for _, s := range pre.All() {
		if s.State != SegmentSealed {
			continue
		}
		entries, err := readSegmentEntries(s.Path)
		if err != nil {
			t.Fatal(err)
		}
		durable = append(durable, entries...)
	}

	w2, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatalf("the shard did not open: %v", err)
	}
	if err := w2.Start(nil); err != nil {
		t.Fatal(err)
	}
	if got := w2.manifest.Get(active.ID); got != nil {
		t.Fatalf("the missing active segment is still listed: %+v", got)
	}
	if err := w2.Write(makeEntry(n, 1)); err != nil {
		t.Fatalf("writing after recovery: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(durable)+1 {
		t.Fatalf("read %d entries after reopening, want the %d sealed ones plus the new one", len(after), len(durable))
	}
}
