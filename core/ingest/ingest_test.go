package ingest_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

func sampleCfg() config.IndexConfig {
	return config.IndexConfig{
		Templates: []config.Template{
			{Name: "latency", Pattern: "took {ms:int}ms"},
			{Name: "payment", Pattern: "paid {amt:float} usd"},
		},
		Literals: []string{"panic:"},
	}
}

func messages() []string {
	return []string{
		"request took 10ms",
		"request took 250ms and paid 12.50 usd",
		"nothing interesting here",
		"panic: nil pointer",
		"request took 999ms",
		"paid 0.99 usd for took 5ms combo",
		"took 250ms again (duplicate value, different record)",
	}
}

// TestRebuildAndCompare is the Phase 3 deliverable: the .tidx written at seal must
// contain EXACTLY the (key, offset) pairs that re-extracting each stored record
// produces — index == derived-from-truth.
func TestRebuildAndCompare(t *testing.T) {
	dir := t.TempDir()
	engine, err := extract.Compile(sampleCfg())
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{
		Schema:           engine.IndexedFields(),
		SegmentSizeBytes: 1 << 40,
		FlushInterval:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.New(engine, w)

	base := time.Unix(1700000000, 0).UTC()
	for i, msg := range messages() {
		if err := ig.Ingest(model.LogEntry{TS: base.Add(time.Duration(i) * time.Second), Message: msg}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close() // seals → writes .tidx

	// Rebuild the expected index from the stored records (the source of truth).
	recs, err := storage.ReadAllRecords(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(messages()) {
		t.Fatalf("stored %d records, want %d", len(recs), len(messages()))
	}
	expected := map[string]map[entryKey]bool{}
	for _, r := range recs {
		for _, kv := range engine.Extract(r.Entry.Message) {
			if expected[kv.Field] == nil {
				expected[kv.Field] = map[entryKey]bool{}
			}
			expected[kv.Field][entryKey{kv.Key, r.Offset}] = true
		}
	}

	// Compare each field's .tidx against the rebuilt expectation.
	m, _ := storage.LoadManifest(dir)
	segs := m.All()
	if len(segs) != 1 {
		t.Fatalf("want 1 segment, got %d", len(segs))
	}
	segBase := strings.TrimSuffix(segs[0].Path, ".log")

	for _, field := range engine.IndexedFields() {
		want := expected[field.Name]
		r, err := index.OpenReader(index.TidxPath(segBase, field.Name))
		if err != nil {
			if len(want) == 0 {
				continue // no matches, no .tidx: correct
			}
			t.Fatalf("field %s: expected %d entries but no .tidx: %v", field.Name, len(want), err)
		}
		got := map[entryKey]bool{}
		for _, e := range r.Entries() {
			got[entryKey{e.Key, e.Offset}] = true
		}
		if !sameSet(got, want) {
			t.Fatalf("field %s: .tidx contents differ from rebuilt truth\n got  %s\n want %s",
				field.Name, dump(got), dump(want))
		}
	}
}

// TestRangeQueryEndToEnd walks the whole pipeline and confirms a range query over the
// .tidx returns the offsets of exactly the records whose extracted value is in range.
func TestRangeQueryEndToEnd(t *testing.T) {
	dir := t.TempDir()
	engine, _ := extract.Compile(sampleCfg())
	w, _ := storage.NewWriter(dir, storage.Options{Schema: engine.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	ig := ingest.New(engine, w)

	base := time.Unix(1700000000, 0).UTC()
	for i, msg := range messages() {
		ig.Ingest(model.LogEntry{TS: base.Add(time.Duration(i) * time.Second), Message: msg})
	}
	w.Close()

	// Map offset → the latency value stored there, by re-extraction.
	recs, _ := storage.ReadAllRecords(dir)
	offToMs := map[uint64]int64{}
	for _, r := range recs {
		for _, kv := range engine.Extract(r.Entry.Message) {
			if kv.Field == "latency_ms" {
				// decode value from message for the assertion (took Nms)
				var n int64
				fmt.Sscanf(r.Entry.Message[strings.Index(r.Entry.Message, "took ")+5:], "%dms", &n)
				offToMs[r.Offset] = n
			}
		}
	}

	m, _ := storage.LoadManifest(dir)
	segBase := strings.TrimSuffix(m.All()[0].Path, ".log")
	r, err := index.OpenReader(index.TidxPath(segBase, "latency_ms"))
	if err != nil {
		t.Fatal(err)
	}
	// latency values are 10, 250, 999, 5, 250. In [10,250] (inclusive): 10, 250, 250 →
	// 3 offsets (5 is below the low bound, 999 above the high). The two 250s live at
	// different record offsets, so both must be returned.
	lo := index.EncodeKey(index.Value{Kind: index.KindInt, Int: 10})
	hi := index.EncodeKey(index.Value{Kind: index.KindInt, Int: 250})
	offs := r.LookupRange(lo, hi, true, true)
	if len(offs) != 3 {
		t.Fatalf("range [10,250]: got %d offsets, want 3", len(offs))
	}
	for _, off := range offs {
		if v := offToMs[off]; v < 10 || v > 250 {
			t.Fatalf("offset %d has value %d, outside [10,250]", off, v)
		}
	}
}

type entryKey struct {
	key    [index.KeySize]byte
	offset uint64
}

func sameSet(a, b map[entryKey]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func dump(s map[entryKey]bool) string {
	var parts []string
	for k := range s {
		parts = append(parts, fmt.Sprintf("{%x@%d}", k.key[8:], k.offset))
	}
	sort.Strings(parts)
	return "[" + strings.Join(parts, " ") + "]"
}
