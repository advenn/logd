package query

import (
	"path/filepath"
	"testing"

	"github.com/advenn/logd/core/index"
)

// fakeReader builds a Reader-shaped entry of a known size so budget/eviction behaviour can
// be tested without touching the filesystem.
func entryOfSize(path string, size int64) *cacheEntry {
	return &cacheEntry{path: path, size: size, tidx: &index.Reader{}}
}

func TestReaderCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := newReaderCache(300)
	c.insert(entryOfSize("a", 100))
	c.insert(entryOfSize("b", 100))
	c.insert(entryOfSize("c", 100))

	// Touch "a" so "b" becomes the least recently used.
	if _, ok := c.lookup("a"); !ok {
		t.Fatal("a should be cached")
	}
	c.insert(entryOfSize("d", 100)) // over budget: evicts one

	if _, ok := c.lookup("b"); ok {
		t.Error("b was least recently used and should have been evicted")
	}
	for _, want := range []string{"a", "c", "d"} {
		if _, ok := c.lookup(want); !ok {
			t.Errorf("%s should still be cached", want)
		}
	}
	n, used, budget := c.Stats()
	if n != 3 || used != 300 || budget != 300 {
		t.Errorf("stats: got (%d, %d, %d), want (3, 300, 300)", n, used, budget)
	}
}

// TestReaderCacheRejectsOversizedEntry: an entry bigger than the whole budget would evict
// everything else every time it is touched, so it is simply not cached.
func TestReaderCacheRejectsOversizedEntry(t *testing.T) {
	c := newReaderCache(100)
	c.insert(entryOfSize("huge", 500))
	if _, ok := c.lookup("huge"); ok {
		t.Error("an entry larger than the budget must not be cached")
	}
	if n, used, _ := c.Stats(); n != 0 || used != 0 {
		t.Errorf("cache should be empty, got %d entries / %d bytes", n, used)
	}
}

// TestReaderCacheDisabled pins that a zero budget turns the cache into a no-op rather than
// a broken cache — every lookup misses and the caller falls through to opening the file.
func TestReaderCacheDisabled(t *testing.T) {
	c := newReaderCache(0)
	c.insert(entryOfSize("a", 10))
	if _, ok := c.lookup("a"); ok {
		t.Error("a disabled cache must never report a hit")
	}
}

// TestReaderCacheNilSafe: a zero-value Engine (or one constructed before SetIndexCacheBytes)
// must not panic on the query path.
func TestReaderCacheNilSafe(t *testing.T) {
	var c *readerCache
	if _, ok := c.lookup("a"); ok {
		t.Error("nil cache reported a hit")
	}
	c.insert(entryOfSize("a", 10)) // must not panic
	c.Forget("a")                  // must not panic
	if n, used, budget := c.Stats(); n != 0 || used != 0 || budget != 0 {
		t.Errorf("nil cache stats: got (%d, %d, %d)", n, used, budget)
	}
}

// TestReaderCacheForgetDropsSegmentSidecars: retention deletes a segment's .log and every
// sidecar together, so Forget must drop all of them by shared base path — not just an exact
// match on one file.
func TestReaderCacheForgetDropsSegmentSidecars(t *testing.T) {
	c := newReaderCache(1000)
	c.insert(entryOfSize("/d/seg-1.latency_ms.tidx", 100))
	c.insert(entryOfSize("/d/seg-1.labels.lidx", 100))
	c.insert(entryOfSize("/d/seg-2.latency_ms.tidx", 100))

	c.Forget("/d/seg-1")

	if _, ok := c.lookup("/d/seg-1.latency_ms.tidx"); ok {
		t.Error("seg-1 tidx should have been forgotten")
	}
	if _, ok := c.lookup("/d/seg-1.labels.lidx"); ok {
		t.Error("seg-1 lidx should have been forgotten")
	}
	if _, ok := c.lookup("/d/seg-2.latency_ms.tidx"); !ok {
		t.Error("seg-2 must be untouched")
	}
	if n, used, _ := c.Stats(); n != 1 || used != 100 {
		t.Errorf("after Forget: got %d entries / %d bytes, want 1 / 100", n, used)
	}
}

// TestReaderCacheInsertRaceKeepsOneEntry: loads happen outside the lock, so two goroutines
// can both open the same cold path. The second insert must not double-count the budget.
func TestReaderCacheInsertRaceKeepsOneEntry(t *testing.T) {
	c := newReaderCache(1000)
	c.insert(entryOfSize("a", 100))
	c.insert(entryOfSize("a", 100)) // the loser of the race
	if n, used, _ := c.Stats(); n != 1 || used != 100 {
		t.Errorf("duplicate insert double-counted: %d entries / %d bytes", n, used)
	}
}

// TestReaderCacheReloadsRewrittenSidecar: a path is not a version. A seal that fails after
// writing a segment's sidecars is retried later and rewrites them with the records appended
// since; a reader cached under the path alone kept answering from the first version, so a
// pushed-down predicate missed every record added in between.
func TestReaderCacheReloadsRewrittenSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg-1.n.tidx")
	write := func(n int) {
		t.Helper()
		entries := make([]index.Entry, n)
		for i := range entries {
			entries[i] = index.Entry{Key: index.EncodeKey(index.Value{Kind: index.KindInt, Int: int64(i)}), Offset: uint64(i)}
		}
		if err := index.WriteTIDX(path, index.KindInt, 0, entries); err != nil {
			t.Fatal(err)
		}
	}
	c := newReaderCache(1 << 20)

	write(3)
	first, err := c.tidx(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Count() != 3 {
		t.Fatalf("first load: %d entries, want 3", first.Count())
	}
	write(5) // the retried seal's rewrite (an atomic rename, as every sidecar write is)
	r, err := c.tidx(path)
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != 5 {
		t.Fatalf("served the cached reader for the replaced file: %d entries, want 5", r.Count())
	}
	if n, _, _ := c.Stats(); n != 1 {
		t.Fatalf("the replaced file's reader should have been dropped, cache holds %d entries", n)
	}
	if again, _ := c.tidx(path); again != r {
		t.Fatal("an unchanged file must still be served from the cache")
	}
}
