package label

import (
	"sort"
	"sync"
)

// Cardinality caps the number of distinct VALUES indexed per label key within one segment
// (design §6.2). When a key exceeds the cap, further new values are dropped from the
// indexed label set — they remain in the record's Extra blob and are still found by scan —
// and the breached key is reported so the query planner scans that key for the segment
// (never an index false negative). A max <= 0 disables the cap. One per active segment,
// reset on rotate. Apply and Breaches are the writer goroutine's; CappedKeys may also be
// called by a query planning against the active segment while the writer applies the cap.
type Cardinality struct {
	max      int
	values   map[string]map[string]struct{} // key → distinct values kept (bounded by max)
	breaches map[string]int                 // per-key count of dropped (over-cap) pairs
	drops    *Drops                         // also counts each dropped pair past this segment; nil = not counted
	// capped is the one field a query reads, so it alone is locked. It changes only on a
	// key's first breach, so the lock is off the per-record path.
	mu     sync.Mutex
	capped map[string]struct{} // keys that breached the cap
}

// NewCardinality creates a per-segment value-cardinality cap. max <= 0 disables it.
func NewCardinality(max int) *Cardinality {
	return &Cardinality{
		max:      max,
		values:   map[string]map[string]struct{}{},
		capped:   map[string]struct{}{},
		breaches: map[string]int{},
	}
}

// CountDropsIn makes the cap also count every pair it drops from now on in d, which
// outlives the segment.
func (c *Cardinality) CountDropsIn(d *Drops) { c.drops = d }

// Apply returns the subset of set whose (key,value) pairs are within their key's value
// cap, recording new values and marking/counting any pair that breaches. When nothing is
// dropped it returns set unchanged. The result stays canonical (a subset of a sorted,
// unique-key Set preserves order and uniqueness).
func (c *Cardinality) Apply(set Set) Set {
	if c == nil || c.max <= 0 || len(set) == 0 {
		return set
	}
	dropped := false
	kept := make(Set, 0, len(set))
	for _, p := range set {
		vals := c.values[p.Key]
		if vals == nil {
			vals = make(map[string]struct{})
			c.values[p.Key] = vals
		}
		if _, seen := vals[p.Value]; seen {
			kept = append(kept, p) // already-indexed value
			continue
		}
		if len(vals) < c.max {
			vals[p.Value] = struct{}{} // new value, room remains
			kept = append(kept, p)
			continue
		}
		// New value for a key already at its cap: drop from the index (stays in Extra).
		if c.breaches[p.Key] == 0 {
			c.mu.Lock()
			c.capped[p.Key] = struct{}{}
			c.mu.Unlock()
		}
		c.breaches[p.Key]++
		if c.drops != nil {
			c.drops.add(set, p.Key)
		}
		dropped = true
	}
	if !dropped {
		return set
	}
	return kept
}

// CappedKeys returns the sorted keys that breached the cap in this segment (recorded in
// the segment manifest so the planner scans them).
func (c *Cardinality) CappedKeys() []string {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.capped) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.capped))
	for k := range c.capped {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Breaches returns the per-key count of dropped over-cap pairs (observability).
func (c *Cardinality) Breaches() map[string]int {
	if c == nil || len(c.breaches) == 0 {
		return nil
	}
	out := make(map[string]int, len(c.breaches))
	for k, v := range c.breaches {
		out[k] = v
	}
	return out
}

// Drops counts the label pairs a cardinality cap kept out of the index, per label key, for
// as long as it lives: a Cardinality lasts one segment, a Drops the writer, so the count is
// the "counter increments" of design §6.2 rather than a per-segment tally. Each count is
// kept per value of a scope key in the record's label set — the tenant label, so that a
// tenant can be shown the drops among its own records only. It is written only when a pair
// is dropped, so it costs nothing while every key is under its cap; one goroutine writes it
// and any may read it.
type Drops struct {
	scopeKey string
	mu       sync.Mutex
	n        map[dropKey]int64
}

type dropKey struct{ scope, key string }

// NewDrops counts dropped pairs per label key and per value of scopeKey ("" for a record
// without it).
func NewDrops(scopeKey string) *Drops {
	return &Drops{scopeKey: scopeKey, n: map[dropKey]int64{}}
}

// add counts one dropped pair of key, from a record whose (whole, uncapped) set is set.
func (d *Drops) add(set Set, key string) {
	scope, _ := set.Get(d.scopeKey)
	d.mu.Lock()
	d.n[dropKey{scope, key}]++
	d.mu.Unlock()
}

// Snapshot returns the counts so far: scope value → label key → dropped pairs.
func (d *Drops) Snapshot() map[string]map[string]int64 {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]map[string]int64)
	for k, n := range d.n {
		m := out[k.scope]
		if m == nil {
			m = make(map[string]int64)
			out[k.scope] = m
		}
		m[k.key] = n
	}
	return out
}
