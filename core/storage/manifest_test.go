package storage

import (
	"sync"
	"testing"
	"time"
)

// TestManifestReturnsSnapshots: what a manifest accessor returns must not change under its
// holder. The accessors used to hand out the shared entries, which the query path read with
// no lock while the writer rewrote them, so one planning pass could mix fields from before
// and after a seal.
func TestManifestReturnsSnapshots(t *testing.T) {
	m := NewManifest()
	m.Add(&SegmentMeta{ID: "a", State: SegmentActive, MinTS: 10, MaxTS: 20})
	views := map[string]*SegmentMeta{
		"Filter": m.Filter(0, 100)[0],
		"All":    m.All()[0],
		"Active": m.Active(),
		"Get":    m.Get("a"),
	}

	m.SetBounds("a", 5, 50)
	if err := m.SealSegment(t.TempDir(), "a", SealInfo{MinTS: 5, MaxTS: 50, Indexed: true, CappedKeys: []string{"k"}}); err != nil {
		t.Fatal(err)
	}
	for name, v := range views {
		if v.State != SegmentActive || v.Indexed || v.MinTS != 10 || v.MaxTS != 20 || v.CappedKeys != nil {
			t.Errorf("%s: the returned entry changed under its holder: %+v", name, *v)
		}
	}
	if s := m.Get("a"); s.State != SegmentSealed || !s.Indexed || s.MinTS != 5 || len(s.CappedKeys) != 1 {
		t.Errorf("a fresh Get does not see the seal: %+v", *s)
	}
}

// TestManifestReadsDuringWritesAreRaceFree reads every manifest accessor's results while the
// writer flushes, rotates and seals — the query path's access pattern. It only fails under
// the race detector, which the suite is run with.
func TestManifestReadsDuringWritesAreRaceFree(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 3 * PageSize, FlushInterval: time.Millisecond, BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		read := func(s *SegmentMeta) int {
			n := int(s.MaxTS-s.MinTS) + len(s.Path) + len(s.Schema) + len(s.CappedKeys) + int(s.Records)
			if s.Indexed && s.State == SegmentSealed {
				n++
			}
			return n
		}
		sink := 0
		for {
			select {
			case <-stop:
				_ = sink
				return
			default:
			}
			m := w.Manifest()
			for _, s := range m.Filter(0, 1<<62) {
				sink += read(s)
			}
			for _, s := range m.All() {
				sink += read(s)
			}
			if a := m.Active(); a != nil {
				sink += read(a)
				if g := m.Get(a.ID); g != nil {
					sink += read(g)
				}
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		for w.Write(makeEntry(i, 1)) != nil {
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadAll(dir); err != nil || len(got) != 2000 {
		t.Fatalf("read back %d records (err %v), want 2000", len(got), err)
	}
}
