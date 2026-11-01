package query

import (
	"container/list"
	"os"
	"sync"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
)

// readerCache keeps opened index sidecars in memory across queries.
//
// Without it, every query re-read and re-decoded each sidecar from scratch — per segment,
// PER PREDICATE. index.OpenReader does os.ReadFile of the whole file, a CRC32 over all of
// it, and a decode loop over every entry; label.OpenReader is the same shape. On a real
// deployment a single field's .tidx was 7.2 MB, so a two-predicate query over two segments
// re-read ~29 MB and re-decoded ~1.2M entries before doing any useful work. That is why a
// broad indexed query measured SLOWER than simply scanning the segment.
//
// A sealed segment's sidecars are immutable, but a path is not proof of that: a seal that
// failed after writing them is retried later, rewriting both with the records appended in
// between, and a .lidx left by such an attempt is visible to label discovery while the
// segment is still active. A reader keyed by path alone kept serving the first version
// after the rewrite — a .tidx or .lidx missing every later record, i.e. false negatives on
// pushdown. So each entry remembers the file it was loaded from, and a hit counts only
// while the path still names that same file (see sameFile). The sidecars are always
// replaced by an atomic rename, so a rewrite is a new file; the check costs one stat per
// use.
//
// Retention deleting a segment is NOT a correctness problem here, which is worth stating
// because it looks like one. The query path only visits segments returned by
// Manifest.Filter, and retention removes the manifest entry as part of deleting the files,
// so a stale reader is never consulted — it merely occupies budget until the LRU evicts it.
// Forget exists to reclaim that memory promptly where a caller knows a segment is gone;
// nothing depends on it being called, which is what keeps core/storage from having to know
// about core/query.
//
// Concurrency: the Engine is shared across HTTP handlers. Loads happen OUTSIDE the lock so
// a slow read cannot serialize unrelated queries; two goroutines racing on the same cold
// path may both load it, which wastes one read and is otherwise harmless.
type readerCache struct {
	mu     sync.Mutex
	budget int64 // 0 disables caching entirely
	used   int64
	ll     *list.List               // front = most recently used
	items  map[string]*list.Element // path -> element holding *cacheEntry
}

type cacheEntry struct {
	path string
	info os.FileInfo // the file the reader was loaded from, stat'ed BEFORE reading it
	size int64
	tidx *index.Reader
	lidx *label.Reader
}

// sameFile reports whether the file at path is still the one entry e was loaded from. The
// stat is taken before the load, so a file replaced mid-load is recorded under its old
// identity and is merely reloaded next time — never served stale.
func (e *cacheEntry) sameFile(cur os.FileInfo) bool {
	return e.info != nil && os.SameFile(e.info, cur) &&
		e.info.Size() == cur.Size() && e.info.ModTime().Equal(cur.ModTime())
}

// defaultIndexCacheBytes is the budget when a caller does not set one. Sized so a handful
// of multi-megabyte sidecars stay resident, which is where the win is; the daemon overrides
// it from index_cache_mb.
const defaultIndexCacheBytes = 256 << 20

func newReaderCache(budgetBytes int64) *readerCache {
	return &readerCache{
		budget: budgetBytes,
		ll:     list.New(),
		items:  map[string]*list.Element{},
	}
}

// lookup returns a cached entry and marks it most-recently-used.
func (c *readerCache) lookup(path string) (*cacheEntry, bool) {
	if c == nil || c.budget <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[path]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry), true
}

// insert stores an entry, evicting least-recently-used entries until the budget holds. An
// entry larger than the whole budget is not cached (it would evict everything else on
// every use), but is still returned to the caller.
func (c *readerCache) insert(e *cacheEntry) {
	if c == nil || c.budget <= 0 || e.size > c.budget {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[e.path]; ok {
		old := el.Value.(*cacheEntry)
		if e.info == nil || old.info != nil && os.SameFile(old.info, e.info) {
			c.ll.MoveToFront(el) // lost a load race: keep the existing entry
			return
		}
		// The file was replaced since the cached reader was loaded: this one supersedes it.
		c.ll.Remove(el)
		delete(c.items, old.path)
		c.used -= old.size
	}
	el := c.ll.PushFront(e)
	c.items[e.path] = el
	c.used += e.size
	for c.used > c.budget {
		back := c.ll.Back()
		if back == nil {
			break
		}
		old := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, old.path)
		c.used -= old.size
	}
}

// Forget drops any cached readers whose path begins with segBase. Retention deletes a
// sealed segment and its sidecars together, and a cached reader would otherwise keep
// serving a file that no longer exists — and hold its memory forever.
func (c *readerCache) Forget(segBase string) {
	if c == nil || c.budget <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for path, el := range c.items {
		if len(path) >= len(segBase) && path[:len(segBase)] == segBase {
			c.ll.Remove(el)
			delete(c.items, path)
			c.used -= el.Value.(*cacheEntry).size
		}
	}
}

// tidx returns a typed-range reader for path, from cache when possible.
func (c *readerCache) tidx(path string) (*index.Reader, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if e, ok := c.lookup(path); ok && e.tidx != nil && e.sameFile(info) {
		return e.tidx, nil
	}
	r, err := index.OpenReader(path)
	if err != nil {
		return nil, err
	}
	c.insert(&cacheEntry{path: path, info: info, size: r.SizeBytes(), tidx: r})
	return r, nil
}

// lidx returns a label reader for path, from cache when possible.
func (c *readerCache) lidx(path string) (*label.Reader, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if e, ok := c.lookup(path); ok && e.lidx != nil && e.sameFile(info) {
		return e.lidx, nil
	}
	r, err := label.OpenReader(path)
	if err != nil {
		return nil, err
	}
	c.insert(&cacheEntry{path: path, info: info, size: r.SizeBytes(), lidx: r})
	return r, nil
}

// Stats reports current occupancy, for tests and introspection.
func (c *readerCache) Stats() (entries int, usedBytes, budgetBytes int64) {
	if c == nil {
		return 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items), c.used, c.budget
}
