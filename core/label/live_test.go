package label

import (
	"path/filepath"
	"slices"
	"testing"
)

// TestLivePostingsMatchSealedIndex: label lookups against the active segment's builder must
// return what the sealed .lidx written from the same builder returns, below any bound; and
// nothing at or past the bound, which marks the records not yet on disk.
func TestLivePostingsMatchSealedIndex(t *testing.T) {
	b := NewBuilder()
	sets := []Set{
		NewSet(map[string]string{"region": "eu", "env": "prod"}),
		NewSet(map[string]string{"region": "us", "env": "prod"}),
		NewSet(map[string]string{"region": "eu"}),
		nil, // the {} stream: records without allowlisted labels
	}
	var off uint64 = 4096
	for i := 0; i < 200; i++ {
		s := sets[i%len(sets)]
		id, ok := b.Find(s.Canonical())
		if !ok {
			id = b.InternCanonical(s.Canonical(), s)
		}
		b.AddPosting(id, off)
		off += 37
	}
	path := filepath.Join(t.TempDir(), "seg.labels.lidx")
	if _, err := b.Flush(path); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range [][2]string{{"region", "eu"}, {"env", "prod"}, {"region", "us"}, {"region", "ap"}, {"zone", "a"}} {
		want := r.LabelOffsets(kv[0], kv[1])
		if got := MergePostings(b.Postings(kv[0], kv[1]), off); !slices.Equal(got, want) {
			t.Fatalf("%s=%s: live %v, sealed %v", kv[0], kv[1], got, want)
		}
		// Below a bound: exactly the sealed answer's prefix under it.
		const bound = 4096 + 37*50
		var wantBelow []uint64
		for _, o := range want {
			if o < bound {
				wantBelow = append(wantBelow, o)
			}
		}
		if got := MergePostings(b.Postings(kv[0], kv[1]), bound); !slices.Equal(got, wantBelow) {
			t.Fatalf("%s=%s below %d: live %v, want %v", kv[0], kv[1], bound, got, wantBelow)
		}
	}
	for _, k := range []string{"region", "env"} {
		if !b.HasKey(k) || !r.HasKey(k) {
			t.Fatalf("HasKey(%q) false", k)
		}
	}
	if b.HasKey("zone") {
		t.Fatal(`HasKey("zone") true for a key no stream carries`)
	}
}

// TestCappedKeysWhileApplying: CappedKeys may be read by a query while the writer applies
// the cap (run under -race).
func TestCappedKeysWhileApplying(t *testing.T) {
	c := NewCardinality(2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			c.Apply(NewSet(map[string]string{"pod": string(rune('a' + i%26))}))
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = c.CappedKeys()
	}
	<-done
	if got := c.CappedKeys(); !slices.Equal(got, []string{"pod"}) {
		t.Fatalf("CappedKeys %v, want [pod]", got)
	}
}
