package query

import (
	"container/heap"
	"sort"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// ranked is a collected record together with its shard's service dictionary, which the
// ordering needs for its last tiebreaker (see entryLess).
type ranked struct {
	model.LogEntry
	services *storage.LookupTable // nil when the record's shard is unknown (unit tests)
}

// serviceName is the record's service as a NAME. ServiceID alone is a per-shard interning
// ID, so the same service has different IDs on different shards.
func (r ranked) serviceName() string {
	if r.services == nil {
		return ""
	}
	name, _ := r.services.GetName(r.ServiceID)
	return name
}

// entryLess is THE ordering for query results: event time in the requested direction,
// then a deterministic tiebreaker on client-visible content.
//
// The tiebreaker is not cosmetic. Collection order is shard × segment × page × append
// order, which differs between a 1-shard and an N-shard deployment holding the same data.
// Without a total order, a limit cutting through a group of equal-timestamp records would
// return different members of that group depending on partitioning. Making the comparator
// total on the compared fields is what lets the fan-in invariant (sharded == single-shard)
// hold at a limit boundary.
//
// Level and the service are included even though the previous comparator stopped at Extra.
// equalResults (the differential oracle) compares them, so records tying on
// (TS, Message, Extra) but differing in Level/service were previously ordered by
// collection order alone — latent, since no fixture produced such ties, but early
// termination would have made it live. The service is compared by name, not ServiceID:
// the ID is interned per shard, so "auth" was ID 0 on one shard and "billing" ID 0 on
// another, and a limit=1 over the same two records kept auth on one shard and billing on
// two. The name is looked up only when everything else ties.
func entryLess(a, b ranked, dir Direction) bool {
	ta, tb := a.TS.UnixNano(), b.TS.UnixNano()
	if ta != tb {
		if dir == Forward {
			return ta < tb
		}
		return ta > tb
	}
	if a.Message != b.Message {
		return a.Message < b.Message
	}
	if a.Extra != b.Extra {
		return a.Extra < b.Extra
	}
	if a.Level != b.Level {
		return a.Level < b.Level
	}
	return a.serviceName() < b.serviceName()
}

// collector accumulates matching records. Two implementations: an unbounded one for the
// metric path (which needs every entry in the window or its aggregations under-count), and
// a bounded top-K for log queries, which is what lets the engine stop early.
type collector interface {
	// add offers a matching record with the resolver of the shard it came from.
	add(e model.LogEntry, r *engineResolver)
	// cutoff returns the event time of the worst record currently kept, and whether the
	// collector is full enough for that to prune anything. Callers use it to skip whole
	// segments and pages that cannot contain a better record.
	cutoff() (int64, bool)
	// done reports that the collector will take no more records, so collection stops: the
	// current scan at its next record, and no further segment is read. Only the metric
	// collector ever says so, once it has refused the query for matching too many entries.
	done() bool
	results() []model.LogEntry
}

// sliceCollector keeps everything, preserving the pre-existing collect-all behaviour.
type sliceCollector struct{ recs []model.LogEntry }

func (c *sliceCollector) add(e model.LogEntry, _ *engineResolver) { c.recs = append(c.recs, e) }
func (c *sliceCollector) cutoff() (int64, bool)                   { return 0, false } // never prunes
func (c *sliceCollector) done() bool                              { return false }
func (c *sliceCollector) results() []model.LogEntry               { return c.recs }

// topKCollector keeps only the best `limit` records under entryLess.
//
// It is a max-heap under that ordering, so the root is the WORST record kept — the one a
// newly-seen better record displaces, and the one whose timestamp is the pruning cutoff.
type topKCollector struct {
	limit int
	dir   Direction
	h     entryHeap
}

func newTopK(limit int, dir Direction) *topKCollector {
	return &topKCollector{limit: limit, dir: dir, h: entryHeap{dir: dir}}
}

func (c *topKCollector) add(e model.LogEntry, r *engineResolver) {
	re := ranked{LogEntry: e}
	if r != nil {
		re.services = r.lt
	}
	if len(c.h.recs) < c.limit {
		heap.Push(&c.h, re)
		return
	}
	// Full: replace the worst only if this record is strictly better.
	if entryLess(re, c.h.recs[0], c.dir) {
		c.h.recs[0] = re
		heap.Fix(&c.h, 0)
	}
}

// done is always false: a full top-K still takes better records, and prunes by cutoff.
func (c *topKCollector) done() bool { return false }

func (c *topKCollector) cutoff() (int64, bool) {
	if len(c.h.recs) < c.limit {
		return 0, false // not yet full: any record could still make the cut
	}
	return c.h.recs[0].TS.UnixNano(), true
}

func (c *topKCollector) results() []model.LogEntry {
	sorted := make([]ranked, len(c.h.recs))
	copy(sorted, c.h.recs)
	sort.Slice(sorted, func(i, j int) bool { return entryLess(sorted[i], sorted[j], c.dir) })
	out := make([]model.LogEntry, len(sorted))
	for i := range sorted {
		out[i] = sorted[i].LogEntry
	}
	return out
}

type entryHeap struct {
	recs []ranked
	dir  Direction
}

func (h entryHeap) Len() int { return len(h.recs) }

// Less is INVERTED relative to entryLess so this is a max-heap: the root is the worst
// record kept, which is what must be evicted and what defines the pruning cutoff.
func (h entryHeap) Less(i, j int) bool { return entryLess(h.recs[j], h.recs[i], h.dir) }
func (h entryHeap) Swap(i, j int)      { h.recs[i], h.recs[j] = h.recs[j], h.recs[i] }

func (h *entryHeap) Push(x any) { h.recs = append(h.recs, x.(ranked)) }
func (h *entryHeap) Pop() any {
	old := h.recs
	n := len(old)
	x := old[n-1]
	h.recs = old[:n-1]
	return x
}

// segmentCannotBeat reports whether a segment's time bounds put every record in it strictly
// worse than the current cutoff, so the segment can be skipped entirely.
//
// The comparison is STRICT. On equality the content tiebreaker could still let a record in
// that segment win, so an inclusive test would silently drop rows.
func segmentCannotBeat(minTS, maxTS, cutoff int64, dir Direction) bool {
	if dir == Forward {
		return minTS > cutoff // everything here is later than the worst kept
	}
	return maxTS < cutoff // everything here is earlier than the worst kept
}
