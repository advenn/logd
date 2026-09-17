package query_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// A compressed block that fails to inflate must cost the same records on every access path.
// Before the fix a forward scan stopped at the bad block, so the index path (which fetches
// by offset and skips only that block) returned more rows than ExecuteScan, and forward and
// backward scans disagreed with each other.
func TestUndecodableBlockPushdownEqualsScan(t *testing.T) {
	dir := t.TempDir()
	eng, err := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: 8})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(eng, w)
	for i := 0; i < 6000; i++ {
		e := model.LogEntry{TS: time.Unix(t0, int64(i)*int64(time.Millisecond)).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("GET /orders/%d took %dms", i, i%1000)}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close() // seals and waits for the background compression

	segs := w.Manifest().All()
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	corruptMiddleBlock(t, storage.SegzPath(segs[0].Path))

	e := qr.NewEngine(w, eng)
	full := qr.Query{Start: ts(0), End: ts(1000), Limit: 100000}
	for _, dir := range []qr.Direction{qr.Forward, qr.Backward} {
		for _, preds := range [][]qr.Predicate{
			nil,
			{qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: intV(900)}},
			{qr.TypedCompare{Field: "latency_ms", Op: qr.OpEq, Value: intV(42)}},
		} {
			q := full
			q.Direction, q.Preds = dir, preds
			idx, err := e.Execute(q)
			if err != nil {
				t.Fatal(err)
			}
			scan, err := e.ExecuteScan(q)
			if err != nil {
				t.Fatal(err)
			}
			equalResults(t, fmt.Sprintf("dir=%d preds=%v", dir, preds), idx, scan)
		}
	}

	// Both directions must also agree on the total, and it must exclude only the bad block.
	fwd, _ := e.ExecuteScan(qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Direction: qr.Forward})
	back, _ := e.ExecuteScan(qr.Query{Start: ts(0), End: ts(1000), Limit: 100000, Direction: qr.Backward})
	if len(fwd) != len(back) {
		t.Fatalf("forward scan returned %d records, backward %d", len(fwd), len(back))
	}
	if len(fwd) == 0 || len(fwd) >= 6000 {
		t.Fatalf("expected the bad block's records (and only those) to be lost, got %d of 6000", len(fwd))
	}
}

// corruptMiddleBlock overwrites the start of the middle block's deflate stream in a .logz
// file (v2 layout: 32-byte header, dictionary, 16-byte directory entries).
func corruptMiddleBlock(t *testing.T, zpath string) {
	t.Helper()
	data, err := os.ReadFile(zpath)
	if err != nil {
		t.Fatal(err)
	}
	numBlocks := binary.BigEndian.Uint32(data[16:20])
	dictLen := binary.BigEndian.Uint32(data[24:28])
	entry := data[32+int(dictLen)+int(numBlocks/2)*16:]
	off := binary.BigEndian.Uint64(entry[0:8])
	copy(data[off:off+4], []byte{0xFF, 0xFF, 0xFF, 0xFF})
	if err := os.WriteFile(zpath, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
