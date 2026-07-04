package storage

import (
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// A panicking tail predicate must be contained: it must not crash the writer goroutine
// (which would take durability down with it). After a delivery that panics, the writer
// keeps accepting and persisting writes.
func TestTailPanickingPredicateDoesNotKillWriter(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)

	// A subscriber whose predicate always panics.
	_, cancel := w.Subscribe(func(model.LogEntry) bool { panic("boom") }, 1)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := w.Write(makeEntry(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	// Give the writer goroutine a moment to process (and survive the panic path).
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Durability intact: the records are all readable.
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("writer lost data after a panicking predicate: got %d want 10", len(got))
	}
}

// Subscribing after the writer has shut down returns an already-closed channel, so a
// tailer ranging for EOF returns immediately instead of leaking.
func TestSubscribeAfterShutdownReturnsClosedChannel(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	w.Start(nil)
	w.Close()

	ch, cancel := w.Subscribe(func(model.LogEntry) bool { return true }, 1)
	defer cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected an already-closed channel after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed for a post-shutdown subscribe")
	}
}
