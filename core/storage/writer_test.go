package storage

import (
	"context"
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
	// Old segments' files are gone.
	if logs, _ := filepath.Glob(filepath.Join(dir, "segments", "*.log")); len(logs) != 1 {
		t.Fatalf("expected 1 surviving segment file, got %d", len(logs))
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
