package label

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSetCanonicalOrderIndependent(t *testing.T) {
	a := NewSet(map[string]string{"z": "1", "a": "2", "m": "3"})
	b := NewSet(map[string]string{"a": "2", "m": "3", "z": "1"})
	if a.Canonical() != b.Canonical() {
		t.Fatal("canonical form must be independent of map iteration order")
	}
	if v, ok := a.Get("m"); !ok || v != "3" {
		t.Fatalf("Get(m): %q,%v", v, ok)
	}
	if _, ok := a.Get("absent"); ok {
		t.Fatal("Get(absent) should be false")
	}
}

// Two DISTINCT label sets must never produce the same Canonical() string — even when a
// value contains bytes an in-band delimiter would use (0x1e/0x1f). A collision would
// merge them to one StreamID and cause pushdown false negatives.
func TestCanonicalNoDelimiterCollision(t *testing.T) {
	a := NewSet(map[string]string{"a": "x", "b": "y"})
	// b packs what would be a's delimiter-joined form into a single value.
	b := NewSet(map[string]string{"a": "x\x1eb\x1fy"})
	if a.Canonical() == b.Canonical() {
		t.Fatalf("distinct sets collided: %q", a.Canonical())
	}
	// Empty-value / empty-key edge cases must also stay distinct.
	c := NewSet(map[string]string{"": "ab"})
	d := NewSet(map[string]string{"a": "b"})
	if c.Canonical() == d.Canonical() {
		t.Fatal("empty-key set collided with a normal set")
	}
}

func TestBuilderInternDedup(t *testing.T) {
	b := NewBuilder()
	id1 := b.Intern(NewSet(map[string]string{"svc": "api", "env": "prod"}))
	id2 := b.Intern(NewSet(map[string]string{"env": "prod", "svc": "api"})) // same set, different order
	id3 := b.Intern(NewSet(map[string]string{"svc": "worker"}))
	if id1 != id2 {
		t.Fatalf("identical label sets must share a StreamID: %d vs %d", id1, id2)
	}
	if id1 == id3 {
		t.Fatal("distinct label sets must get distinct StreamIDs")
	}
}

func TestBuilderFlushReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.labels.lidx")
	b := NewBuilder()
	s1 := b.Intern(NewSet(map[string]string{"svc": "api", "env": "prod"}))
	s2 := b.Intern(NewSet(map[string]string{"svc": "worker", "env": "prod"}))
	// postings added out of order and with a duplicate — encode must sort+dedup.
	b.AddPosting(s1, 500)
	b.AddPosting(s1, 100)
	b.AddPosting(s1, 500)
	b.AddPosting(s2, 200)

	wrote, err := b.Flush(path)
	if err != nil || !wrote {
		t.Fatalf("Flush: wrote=%v err=%v", wrote, err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}

	if got := r.LabelOffsets("svc", "api"); !reflect.DeepEqual(got, []uint64{100, 500}) {
		t.Fatalf("LabelOffsets(svc=api): %v", got)
	}
	// env=prod spans both streams → union of their offsets, sorted.
	if got := r.LabelOffsets("env", "prod"); !reflect.DeepEqual(got, []uint64{100, 200, 500}) {
		t.Fatalf("LabelOffsets(env=prod): %v", got)
	}
	if got := r.LabelOffsets("svc", "nonexistent"); len(got) != 0 {
		t.Fatalf("LabelOffsets(absent): %v", got)
	}
	if got := r.Keys(); !reflect.DeepEqual(got, []string{"env", "svc"}) {
		t.Fatalf("Keys: %v", got)
	}
	if got := r.Values("svc"); !reflect.DeepEqual(got, []string{"api", "worker"}) {
		t.Fatalf("Values(svc): %v", got)
	}
	if got := len(r.Streams()); got != 2 {
		t.Fatalf("Streams: %d want 2", got)
	}
}

func TestFlushSkipsLabelless(t *testing.T) {
	b := NewBuilder()
	b.AddPosting(b.Intern(nil), 10) // label-less record → empty set
	wrote, err := b.Flush(filepath.Join(t.TempDir(), "x.lidx"))
	if err != nil {
		t.Fatal(err)
	}
	if wrote {
		t.Fatal("a label-less segment must not write a .lidx")
	}
}

func TestReaderRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.labels.lidx")
	b := NewBuilder()
	b.AddPosting(b.Intern(NewSet(map[string]string{"k": "v"})), 1)
	b.Flush(path)

	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF // corrupt a posting byte
	os.WriteFile(path, data, 0o644)
	if _, err := OpenReader(path); err == nil {
		t.Fatal("OpenReader accepted a corrupt .lidx (should error so caller scans)")
	}
}
