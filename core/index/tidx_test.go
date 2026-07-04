package index

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// buildIntIndex writes a .tidx for KindInt over the given values (offset = value's
// position so tests can map offsets back to values) and returns a reader.
func buildIntIndex(t *testing.T, values []int64) *Reader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	entries := make([]Entry, len(values))
	for i, v := range values {
		entries[i] = Entry{Key: intKey(v), Offset: uint64(i)}
	}
	if err := WriteTIDX(path, KindInt, 7, entries); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestTidxEqualityLookup(t *testing.T) {
	values := []int64{50, 10, 30, 10, 20, 10} // 10 appears at offsets 1,3,5
	r := buildIntIndex(t, values)
	if r.Kind() != KindInt {
		t.Fatalf("kind: got %v", r.Kind())
	}
	got := r.LookupEqual(intKey(10))
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []uint64{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("LookupEqual(10): got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("LookupEqual(10): got %v want %v", got, want)
		}
	}
	if n := r.LookupEqual(intKey(999)); len(n) != 0 {
		t.Fatalf("LookupEqual(absent): got %v", n)
	}
}

func TestTidxRangeLookup(t *testing.T) {
	// values at offsets 0..6
	values := []int64{0, 10, 20, 30, 40, 50, 60}
	r := buildIntIndex(t, values)

	// (10, 40] inclusive-hi, exclusive-lo → 20,30,40 → offsets 2,3,4
	got := r.LookupRange(intKey(10), intKey(40), false, true)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assertOffsets(t, "(10,40]", got, []uint64{2, 3, 4})

	// [10, 40) inclusive-lo, exclusive-hi → 10,20,30 → offsets 1,2,3
	got = r.LookupRange(intKey(10), intKey(40), true, false)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assertOffsets(t, "[10,40)", got, []uint64{1, 2, 3})

	// [0, 60] full range → all 7
	if n := r.LookupRange(intKey(0), intKey(60), true, true); len(n) != 7 {
		t.Fatalf("full range: got %d want 7", len(n))
	}

	// range entirely below data → empty
	if n := r.LookupRange(intKey(-100), intKey(-1), true, true); len(n) != 0 {
		t.Fatalf("below-range: got %v", n)
	}
}

// A negative range boundary must work (sign-flip encoding), catching any regression
// where keys are treated as unsigned values.
func TestTidxRangeWithNegatives(t *testing.T) {
	values := []int64{-100, -1, 0, 1, 100}
	r := buildIntIndex(t, values)
	got := r.LookupRange(intKey(-50), intKey(50), true, true) // → -1,0,1 at offsets 1,2,3
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assertOffsets(t, "[-50,50]", got, []uint64{1, 2, 3})
}

func TestTidxRejectsCorruptHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	if err := WriteTIDX(path, KindInt, 1, []Entry{{Key: intKey(1), Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	data[10] ^= 0xFF // corrupt a header byte (not the CRC field)
	os.WriteFile(path, data, 0o644)
	if _, err := OpenReader(path); err == nil {
		t.Fatal("OpenReader accepted a corrupt-header .tidx (should error so caller scans)")
	}
}

// In-place corruption of the RECORD region (not just the header) must be detected, so
// a corrupt index degrades to scan instead of silently returning wrong/missing offsets.
func TestTidxRejectsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.field.tidx")
	entries := []Entry{{Key: intKey(1), Offset: 10}, {Key: intKey(2), Offset: 20}, {Key: intKey(3), Offset: 30}}
	if err := WriteTIDX(path, KindInt, 1, entries); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF // corrupt the last record's offset byte
	os.WriteFile(path, data, 0o644)
	if _, err := OpenReader(path); err == nil {
		t.Fatal("OpenReader accepted a corrupt-record .tidx (should error so caller scans)")
	}
}

func TestBufferFlushWritesPerFieldFiles(t *testing.T) {
	dir := t.TempDir()
	segBase := filepath.Join(dir, "seg-000001")
	b := NewBuffer([]FieldType{{Name: "latency", Kind: KindInt}, {Name: "amount", Kind: KindFloat}})
	b.Add("latency", intKey(247), 100)
	b.Add("latency", intKey(12), 200)
	b.Add("amount", EncodeKey(Value{Kind: KindFloat, Float: 9.99}), 100)
	b.Add("unknown", intKey(1), 0) // ignored: not in schema
	written, err := b.Flush(segBase, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(written) != 2 { // latency + amount (not unknown)
		t.Fatalf("Flush reported %d written fields, want 2", len(written))
	}

	r, err := OpenReader(TidxPath(segBase, "latency"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != 2 {
		t.Fatalf("latency index count: got %d want 2", r.Count())
	}
	if off := r.LookupEqual(intKey(247)); len(off) != 1 || off[0] != 100 {
		t.Fatalf("latency lookup 247: got %v", off)
	}
	if _, err := os.Stat(TidxPath(segBase, "amount")); err != nil {
		t.Fatalf("amount .tidx not written: %v", err)
	}
	if _, err := os.Stat(TidxPath(segBase, "unknown")); !os.IsNotExist(err) {
		t.Fatal("unknown field should not have produced a .tidx")
	}
}

func assertOffsets(t *testing.T, label string, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v want %v", label, got, want)
		}
	}
}
