package storage

import (
	"testing"
	"time"
)

// segmentStates returns the states of a writer's segments in MinTS order, with each sealed
// segment's record count, as the in-memory manifest has them.
func segmentStates(w *Writer) (states []SegmentState, records []uint64) {
	for _, s := range w.manifest.All() {
		states = append(states, s.State)
		records = append(records, s.Records)
	}
	return states, records
}

// TestSegmentSealsByAge: a segment whose first record is older than MaxSegmentAge is sealed
// at the next flush tick, whatever its size — including a tick with nothing to write, which
// is every tick of a stream that has gone quiet. An empty segment is never sealed for age.
func TestSegmentSealsByAge(t *testing.T) {
	dir := t.TempDir()
	w := newSyncWriter(t, dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1, MaxSegmentAge: time.Hour})
	write, _ := syncWrites(w, false)

	write(5)
	w.flushTick(time.Now())
	if st, _ := segmentStates(w); len(st) != 1 || st[0] != SegmentActive {
		t.Fatalf("a fresh segment was sealed: %v", st)
	}
	write(3) // still in the page in RAM when the deadline passes
	later := time.Now().Add(time.Hour + time.Second)
	w.flushTick(later)
	st, recs := segmentStates(w)
	if len(st) != 2 || st[0] != SegmentSealed || st[1] != SegmentActive {
		t.Fatalf("after its age limit: segments %v, want [sealed active]", st)
	}
	if recs[0] != 8 {
		t.Fatalf("the aged segment sealed with %d records, want all 8 (the page in RAM is written first)", recs[0])
	}

	// The new segment is empty: no tick, however late, seals it.
	w.flushTick(later.Add(100 * time.Hour))
	if st, _ := segmentStates(w); len(st) != 2 {
		t.Fatalf("an empty segment was sealed for age: %v", st)
	}
	// Its clock starts at its own first record, not at the previous segment's.
	write(2)
	w.flushTick(time.Now())
	if st, _ := segmentStates(w); len(st) != 2 {
		t.Fatalf("the new segment inherited the old one's age: %v", st)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertPrefixMatches(t, got, 10)
}

// TestSegmentAgeLimitDisabled: MaxSegmentAge 0 leaves rotation to size and the index budget.
func TestSegmentAgeLimitDisabled(t *testing.T) {
	w := newSyncWriter(t, t.TempDir(), Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	defer w.Close()
	write, _ := syncWrites(w, false)
	write(5)
	w.flushTick(time.Now().Add(10000 * time.Hour))
	if st, _ := segmentStates(w); len(st) != 1 || st[0] != SegmentActive {
		t.Fatalf("segments %v with no age limit, want one active", st)
	}
}

// TestRecoveredSegmentKeepsItsAge: a crash-recovered active segment is as old as its first
// record, not as old as the restart. Otherwise a daemon restarted more often than
// MaxSegmentAge — a crash loop, or routine deploys — would never seal it.
func TestRecoveredSegmentKeepsItsAge(t *testing.T) {
	for _, c := range []struct {
		name      string
		written   time.Duration // how long before the restart the records were written
		wantSeal  bool
		wantState SegmentState
	}{
		{"older than the limit", 2 * time.Hour, true, SegmentSealed},
		{"younger than the limit", time.Minute, false, SegmentActive},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: 2 * time.Millisecond, BlockPages: -1})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(nil); err != nil {
				t.Fatal(err)
			}
			at := time.Now().Add(-c.written)
			for i := 0; i < 10; i++ {
				e := makeEntry(i, 1)
				e.IngestedAt = at
				if err := w.Write(e); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(50 * time.Millisecond) // let the tick write the page
			w.stop()                          // crash: no seal

			w2, err := NewWriter(dir, Options{SegmentSizeBytes: 1 << 40, FlushInterval: 2 * time.Millisecond, BlockPages: -1, MaxSegmentAge: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if got := w2.segFirstWrite; !got.Equal(at) {
				t.Fatalf("recovered first-write time %v, want the first record's %v", got, at)
			}
			if err := w2.Start(nil); err != nil {
				t.Fatal(err)
			}
			recovered := w2.manifest.Active().ID
			deadline := time.Now().Add(2 * time.Second)
			for c.wantSeal && w2.manifest.Get(recovered).State != SegmentSealed && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !c.wantSeal {
				time.Sleep(50 * time.Millisecond) // many ticks
			}
			if got := w2.manifest.Get(recovered).State; got != c.wantState {
				t.Fatalf("recovered segment written %v ago is %s, want %s", c.written, got, c.wantState)
			}
			if err := w2.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := ReadAll(dir)
			if err != nil {
				t.Fatal(err)
			}
			assertPrefixMatches(t, got, 10)
		})
	}
}
