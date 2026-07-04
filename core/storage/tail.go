package storage

import (
	"sync"

	"github.com/advenn/logd/core/model"
)

// tailRegistry is the live-tail fan-out (design §11). The writer, after appending a
// record, offers it to every subscriber whose predicate matches. Delivery is
// non-blocking: a slow tailer whose buffer is full drops records rather than stalling
// the write path — the writer goroutine must never block on a consumer.
type tailRegistry struct {
	mu     sync.Mutex
	subs   map[*tailSub]struct{}
	closed bool // set once the writer has shut down; no further deliveries will occur
}

type tailSub struct {
	match func(model.LogEntry) bool
	ch    chan model.LogEntry
}

func newTailRegistry() *tailRegistry {
	return &tailRegistry{subs: make(map[*tailSub]struct{})}
}

func (r *tailRegistry) add(match func(model.LogEntry) bool, buffer int) (*tailSub, <-chan model.LogEntry) {
	if buffer < 1 {
		buffer = 1
	}
	s := &tailSub{match: match, ch: make(chan model.LogEntry, buffer)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		// The writer already shut down: no delivery will ever happen, so hand back an
		// already-closed channel (a tailer ranging for EOF returns immediately) rather
		// than a live channel that would leak.
		close(s.ch)
		return s, s.ch
	}
	r.subs[s] = struct{}{}
	return s, s.ch
}

func (r *tailRegistry) remove(s *tailSub) {
	r.mu.Lock()
	if _, ok := r.subs[s]; ok {
		delete(r.subs, s)
		close(s.ch)
	}
	r.mu.Unlock()
}

// offer delivers e to every matching subscriber. It runs on the writer goroutine. The
// lock is held across the non-blocking sends so a concurrent remove cannot close a
// channel mid-send (which would panic); removal waits for the current offer to finish.
func (r *tailRegistry) offer(e model.LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := range r.subs {
		r.deliver(s, e)
	}
}

// deliver evaluates one subscriber's predicate and, if it matches, does a non-blocking
// send. The predicate is caller-supplied (untrusted) and runs on the writer goroutine,
// so a panic in it MUST be contained: a recover here keeps a bad predicate (e.g. a nil
// regex) from crashing the writer and taking durability down with it. (A merely slow
// predicate still stalls the write path — callers should keep tail predicates cheap.)
func (r *tailRegistry) deliver(s *tailSub, e model.LogEntry) {
	defer func() { _ = recover() }()
	if !s.match(e) {
		return
	}
	select {
	case s.ch <- e:
	default: // buffer full: drop, never block the writer
	}
}

// closeAll drops every subscription and marks the registry closed (writer shutdown).
func (r *tailRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for s := range r.subs {
		delete(r.subs, s)
		close(s.ch)
	}
}
