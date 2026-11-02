package storage

import (
	"os"
	"testing"
	"time"
)

// sealedPages counts a segment's data pages from its files, compressed or raw, to check the
// count the manifest recorded at seal against.
func sealedPages(t *testing.T, logPath string) uint64 {
	t.Helper()
	src, err := openPageSource(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	return src.numPages() - 1 // page 0 is the placeholder
}

// TestSealRecordsLayout: every sealed segment records its own I/O layout — its data-page
// count at seal, and the block size it was compressed with once compression has finished —
// and the record survives a restart. The cost guard prices a candidate set against these,
// so they must describe the files, not the configuration.
func TestSealRecordsLayout(t *testing.T) {
	for _, c := range []struct {
		name       string
		blockPages int // Options.BlockPages
		want       int // recorded BlockPages
	}{
		{"default blocks", 0, DefaultBlockPages},
		{"3-page blocks", 3, 3},
		{"uncompressed", -1, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWriter(dir, Options{SegmentSizeBytes: 6 * PageSize, FlushInterval: time.Hour, BlockPages: c.blockPages})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(nil); err != nil {
				t.Fatal(err)
			}
			const n = 20 * entriesPerPage
			for i := 0; i < n; i++ {
				if err := w.Write(makeEntry(i, 1)); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			segs := mustManifest(t, dir).All() // from disk: the record must have been saved
			if len(segs) < 3 {
				t.Fatalf("want several sealed segments, got %d", len(segs))
			}
			for _, s := range segs {
				if s.State != SegmentSealed {
					t.Fatalf("segment %s is %s after Close", s.ID, s.State)
				}
				if got := sealedPages(t, s.Path); s.Pages != got {
					t.Errorf("segment %s recorded %d data pages, its file holds %d", s.ID, s.Pages, got)
				}
				if s.BlockPages != c.want {
					t.Errorf("segment %s recorded block_pages=%d, want %d", s.ID, s.BlockPages, c.want)
				}
			}
		})
	}
}

// TestInterruptedCompressionRecordsLayout: a segment sealed raw and compressed only by a
// later run records the block size that run used, which is not the one in force when it was
// sealed.
func TestInterruptedCompressionRecordsLayout(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 6 * PageSize, FlushInterval: time.Hour, BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12*entriesPerPage; i++ {
		if err := w.Write(makeEntry(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sealedBefore := mustManifest(t, dir).All()
	for _, s := range sealedBefore {
		if s.BlockPages != 0 {
			t.Fatalf("raw segment %s recorded block_pages=%d", s.ID, s.BlockPages)
		}
	}

	// The next run compresses with 4-page blocks: Start finishes the "interrupted" work.
	w2, err := NewWriter(dir, Options{SegmentSizeBytes: 6 * PageSize, FlushInterval: time.Hour, BlockPages: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Start(nil); err != nil {
		t.Fatal(err)
	}
	// Close stops that backlog after the segment in hand, so wait for all of it first: the
	// raw files go once their .logz is in place.
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw := 0
		for _, s := range sealedBefore {
			if _, err := os.Stat(s.Path); err == nil {
				raw++
			}
		}
		if raw == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d segments still uncompressed", raw)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	compressed := 0
	for _, s := range mustManifest(t, dir).All() {
		if s.Pages == 0 {
			continue // the second run's own empty segment
		}
		if s.BlockPages != 4 {
			t.Errorf("segment %s recorded block_pages=%d after being compressed with 4", s.ID, s.BlockPages)
		}
		if got := sealedPages(t, s.Path); s.Pages != got {
			t.Errorf("segment %s recorded %d data pages, its file holds %d", s.ID, s.Pages, got)
		}
		compressed++
	}
	if compressed < 2 {
		t.Fatalf("want several compressed segments, got %d", compressed)
	}
}
