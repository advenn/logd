package query

import (
	"container/heap"
	"sort"

	"github.com/advenn/logd/core/model"
)

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
// Level and ServiceID are included even though the previous comparator stopped at Extra.
// equalResults (the differential oracle) compares them, so records tying on
// (TS, Message, Extra) but differing in Level/ServiceID were previously ordered by
// collection order alone — latent, since no fixture produced such ties, but early
// termination would have made it live.
func entryLess(a, b model.LogEntry, dir Direction) bool {
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
	return a.ServiceID < b.ServiceID
}

// collector accumulates matching records. Two implementations: an unbounded one for the
// metric path (which needs every entry in the window or its aggregations under-count), and
// a bounded top-K for log queries, which is what lets the engine stop early.
type collector interface {
	add(model.LogEntry)
	// cutoff returns the event time of the worst record currently kept, and whether the
	// collector is full enough for that to prune anything. Callers use it to skip whole
	// segments and pages that cannot contain a better record.
	cutoff() (int64, bool)
	results() []model.LogEntry
}

// sliceCollector keeps everything, preserving the pre-existing collect-all behaviour.
type sliceCollector struct{ recs []model.LogEntry }

func (c *sliceCollector) add(e model.LogEntry)     { c.recs = append(c.recs, e) }
func (c *sliceCollector) cutoff() (int64, bool)    { return 0, false } // never prunes
func (c *sliceCollector) results() []model.LogEntry { return c.recs }

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

func (c *topKCollector) add(e model.LogEntry) {
	if len(c.h.recs) < c.limit {
		heap.Push(&c.h, e)
		return
	}
	// Full: replace the worst only if this record is strictly better.
	if entryLess(e, c.h.recs[0], c.dir) {
		c.h.recs[0] = e
		heap.Fix(&c.h, 0)
	}
}

func (c *topKCollector) cutoff() (int64, bool) {
	if len(c.h.recs) < c.limit {
		return 0, false // not yet full: any record could still make the cut
	}
	return c.h.recs[0].TS.UnixNano(), true
}

func (c *topKCollector) results() []model.LogEntry {
	out := make([]model.LogEntry, len(c.h.recs))
	copy(out, c.h.recs)
	sort.Slice(out, func(i, j int) bool { return entryLess(out[i], out[j], c.dir) })
	return out
}

type entryHeap struct {
	recs []model.LogEntry
	dir  Direction
}

func (h entryHeap) Len() int { return len(h.recs) }

// Less is INVERTED relative to entryLess so this is a max-heap: the root is the worst
// record kept, which is what must be evicted and what defines the pruning cutoff.
func (h entryHeap) Less(i, j int) bool { return entryLess(h.recs[j], h.recs[i], h.dir) }
func (h entryHeap) Swap(i, j int)      { h.recs[i], h.recs[j] = h.recs[j], h.recs[i] }

func (h *entryHeap) Push(x any) { h.recs = append(h.recs, x.(model.LogEntry)) }
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
