package index

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
)

// sortedOffsets returns a sorted copy, so lookups that return the same set in different
// orders (a .tidx walks key order, a FieldView append order) compare equal.
func sortedOffsets(offs []uint64) []uint64 {
	out := slices.Clone(offs)
	slices.Sort(out)
	return out
}

// TestFieldViewMatchesReader: the active segment's linear lookups must return exactly what
// the sealed .tidx built from the same entries returns, for every operator shape the
// planner uses — equality, and ranges with each inclusivity — including keys that collide
// (truncated strings) and duplicate offsets (a record matching a template twice). The
// planner runs one lookup builder against either, so any difference here would be a
// difference between querying a segment before and after it seals.
func TestFieldViewMatchesReader(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for _, kind := range []ValueKind{KindInt, KindStr} {
		var vals []Value
		for i := 0; i < 400; i++ {
			switch kind {
			case KindInt:
				vals = append(vals, Value{Kind: KindInt, Int: int64(r.Intn(60) - 30)})
			default:
				// 16-byte shared prefix: many distinct values, few distinct keys.
				vals = append(vals, Value{Kind: KindStr, Str: fmt.Sprintf("PREFIX0123456789%d", r.Intn(5))})
				if r.Intn(3) == 0 {
					vals[len(vals)-1].Str = fmt.Sprintf("k%d", r.Intn(20))
				}
			}
		}
		b := NewBuffer([]FieldType{{Name: "f", Kind: kind}})
		var off uint64 = 4096
		for i, v := range vals {
			b.Add("f", EncodeKey(v), off)
			if i%5 != 4 { // every fifth value shares its record with the previous one
				off += 40
			}
		}
		path := filepath.Join(t.TempDir(), "f.tidx")
		if err := WriteTIDX(path, kind, 0, slices.Clone(b.byField["f"].entries)); err != nil {
			t.Fatal(err)
		}
		rd, err := OpenReader(path)
		if err != nil {
			t.Fatal(err)
		}
		v := b.View(off + 1)["f"]

		for i := 0; i < 500; i++ {
			lo, hi := EncodeKey(vals[r.Intn(len(vals))]), EncodeKey(vals[r.Intn(len(vals))])
			if r.Intn(4) == 0 {
				lo = [KeySize]byte{}
			}
			if r.Intn(4) == 0 {
				for j := range hi {
					hi[j] = 0xFF
				}
			}
			incLo, incHi := r.Intn(2) == 0, r.Intn(2) == 0
			want := sortedOffsets(rd.LookupRange(lo, hi, incLo, incHi))
			got := sortedOffsets(v.LookupRange(lo, hi, incLo, incHi))
			if !slices.Equal(got, want) {
				t.Fatalf("kind %d range [%x, %x] inc=%v,%v: view %v, .tidx %v", kind, lo, hi, incLo, incHi, got, want)
			}
			if want, got := sortedOffsets(rd.LookupEqual(lo)), sortedOffsets(v.LookupEqual(lo)); !slices.Equal(got, want) {
				t.Fatalf("kind %d equal %x: view %v, .tidx %v", kind, lo, got, want)
			}
		}
	}
}

// TestFieldViewHidesOffsetsPastItsBound: a view answers only for records below its bound —
// the pages already on disk — however many entries the buffer holds past it.
func TestFieldViewHidesOffsetsPastItsBound(t *testing.T) {
	b := NewBuffer([]FieldType{{Name: "f", Kind: KindInt}})
	for i := uint64(0); i < 10; i++ {
		b.Add("f", intKey(7), 4096+i*100)
	}
	v := b.View(4096 + 500)["f"]
	if got := v.LookupEqual(intKey(7)); !slices.Equal(got, []uint64{4096, 4196, 4296, 4396, 4496}) {
		t.Fatalf("equal: %v", got)
	}
	if got := v.LookupRange([KeySize]byte{}, intKey(100), true, true); len(got) != 5 {
		t.Fatalf("range: %v", got)
	}
}

// TestViewUnchangedByLaterAppendsAndFlush: a view taken while the segment was active keeps
// answering the same after the writer appends more — enough to move the entries to a new
// array — and after the seal writes the .tidx. The seal used to sort the buffer in place,
// which pulled entries appended after the view (here, smaller keys) into the slots the view
// covers and dropped its own.
func TestViewUnchangedByLaterAppendsAndFlush(t *testing.T) {
	b := NewBuffer([]FieldType{{Name: "f", Kind: KindInt}})
	for i := uint64(0); i < 100; i++ {
		b.Add("f", intKey(int64(1000+i)), 4096+i*50)
	}
	v := b.View(1 << 40)["f"]
	all := func() []uint64 { return v.LookupRange([KeySize]byte{}, intKey(1<<40), true, true) }
	before := all()

	for i := uint64(0); i < 5000; i++ {
		b.Add("f", intKey(int64(i)), 1<<20+i*50) // smaller keys: they sort first
	}
	if _, err := b.Flush(filepath.Join(t.TempDir(), "seg"), 0); err != nil {
		t.Fatal(err)
	}
	if after := all(); !slices.Equal(after, before) {
		t.Fatalf("the view changed: %d offsets before, %d after (first %v)", len(before), len(after), after[:3])
	}
	// And the buffer itself is still in append order: the file was written from a copy.
	if e := b.byField["f"].entries; e[0].Key != intKey(1000) || e[100].Key != intKey(0) {
		t.Fatal("Flush reordered the buffer")
	}
}
