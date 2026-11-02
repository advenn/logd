package storage

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
)

var allKeys = [2][index.KeySize]byte{{}, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}}

// viewN returns the messages of the records the view finds on field "n" (any value),
// fetched from the segment file — so an offset the file cannot serve shows up as missing.
func viewN(t *testing.T, w *Writer, v *ActiveIndex) []string {
	t.Helper()
	out, err := viewNErr(w, v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// viewNErr is viewN for a goroutine other than the test's, which must not call t.Fatal.
func viewNErr(w *Writer, v *ActiveIndex) ([]string, error) {
	f, ok := v.Field("n")
	if !ok {
		return nil, fmt.Errorf(`no field "n" in the view`)
	}
	offs := f.LookupRange(allKeys[0], allKeys[1], true, true)
	var out []string
	if err := FetchRecords(SegmentPath(w.dir, v.SegmentID), offs, func(e model.LogEntry) bool {
		out = append(out, e.Message)
		return true
	}); err != nil {
		return nil, err
	}
	if len(out) != len(offs) {
		return nil, fmt.Errorf("the view returned %d offsets, the file serves %d of them", len(offs), len(out))
	}
	return out, nil
}

func wantMsgs(from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("%05d", i))
	}
	return out
}

// TestActiveIndexReadabilityBoundary: the active segment's index answers for exactly the
// records whose page is on disk. A record still in the page being assembled — or in a page
// whose write failed — is in the buffers already, but a query must not see it there before
// the page is readable (it could not fetch it), and must see it as soon as it is.
func TestActiveIndexReadabilityBoundary(t *testing.T) {
	dir := t.TempDir()
	w := newSyncWriter(t, dir, Options{Schema: []index.FieldType{{Name: "n", Kind: index.KindInt}}, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1, LabelKeys: []string{"app"}})
	put := func(from, to int) {
		for i := from; i < to; i++ {
			w.processEntry(record{entry: makeEntry(i, 1), keys: []index.KeyedValue{intKV("n", int64(i))}, labels: label.NewSet(map[string]string{"app": "a"})})
		}
	}
	view := func() *ActiveIndex {
		t.Helper()
		v, ok := w.ActiveIndex(w.currSeg.ID)
		if !ok {
			t.Fatal("no index for the active segment")
		}
		return v
	}

	put(0, 5) // in the page in RAM
	v0 := view()
	if got := viewN(t, w, v0); len(got) != 0 || v0.Pages != 0 || len(v0.LabelOffsets("app", "a")) != 0 {
		t.Fatalf("records still in RAM were visible: %v (pages %d)", got, v0.Pages)
	}

	if err := w.flushPage(); err != nil {
		t.Fatal(err)
	}
	v1 := view()
	if got := viewN(t, w, v1); !slices.Equal(got, wantMsgs(0, 5)) || v1.Pages != 1 || v1.Records != 5 {
		t.Fatalf("after the page write: %v (pages %d, records %d)", got, v1.Pages, v1.Records)
	}
	if n := len(v1.LabelOffsets("app", "a")); n != 5 {
		t.Fatalf("label lookup after the page write: %d offsets, want 5", n)
	}
	if got := viewN(t, w, v0); len(got) != 0 {
		t.Fatalf("an earlier view grew: %v", got)
	}

	// A page whose write fails stays in RAM, and stays invisible.
	put(5, 8)
	good := w.currSeg.File
	w.currSeg.File = readOnlyFile(t, w.currSeg.Path)
	w.flushTick(time.Now())
	w.currSeg.File = good
	if got := viewN(t, w, view()); !slices.Equal(got, wantMsgs(0, 5)) {
		t.Fatalf("after a failed page write: %v", got)
	}
	w.flushTick(time.Now())
	if got := viewN(t, w, view()); !slices.Equal(got, wantMsgs(0, 8)) {
		t.Fatalf("after the retry: %v", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestActiveIndexReadOrder: a page written between the moment a query reads the boundary
// and the moment it copies the buffers must not leave the view short. Reading the boundary
// first makes such a page fall outside the view (it is seen next time); copying the buffers
// first would put it inside the boundary with its records missing — a false negative.
func TestActiveIndexReadOrder(t *testing.T) {
	w := newSyncWriter(t, t.TempDir(), Options{Schema: []index.FieldType{{Name: "n", Kind: index.KindInt}}, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	defer w.Close()
	write, _ := syncWrites(w, true)
	write(entriesPerPage) // one full page
	if err := w.flushPage(); err != nil {
		t.Fatal(err)
	}
	testHookViewAfterBoundary = func() {
		write(entriesPerPage) // a second page, written inside the window
		if err := w.flushPage(); err != nil {
			t.Error(err)
		}
	}
	v, ok := w.ActiveIndex(w.currSeg.ID)
	testHookViewAfterBoundary = nil
	if !ok {
		t.Fatal("no index for the active segment")
	}
	got := viewN(t, w, v)
	if uint64(len(got)) != v.Records || v.Pages != 1 || !slices.Equal(got, wantMsgs(0, entriesPerPage)) {
		t.Fatalf("view covers %d pages / %d records but finds %d", v.Pages, v.Records, len(got))
	}
	v2, _ := w.ActiveIndex(w.currSeg.ID)
	if got := viewN(t, w, v2); !slices.Equal(got, wantMsgs(0, 2*entriesPerPage)) {
		t.Fatalf("the next view finds %d records, want %d", len(got), 2*entriesPerPage)
	}
}

// TestActiveIndexSegmentIdentity: a view is handed out only for the segment that is active
// now. A query that saw a segment as active before it sealed gets nothing for it, so it can
// go back to the manifest instead of reading one segment's offsets against another's file.
func TestActiveIndexSegmentIdentity(t *testing.T) {
	w := newSyncWriter(t, t.TempDir(), Options{Schema: []index.FieldType{{Name: "n", Kind: index.KindInt}}, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1})
	first := w.currSeg.ID
	if _, ok := w.ActiveIndex(first); !ok {
		t.Fatal("no index for the active segment")
	}
	if _, ok := w.ActiveIndex("01NOTASEGMENT0000000000000"); ok {
		t.Fatal("an index for an unknown segment")
	}
	write, _ := syncWrites(w, true)
	write(10)
	if err := w.flushPage(); err != nil {
		t.Fatal(err)
	}
	held, _ := w.ActiveIndex(first)
	if err := w.rotateSegment(); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.ActiveIndex(first); ok {
		t.Fatal("an index for a segment that has sealed")
	}
	if _, ok := w.ActiveIndex(w.currSeg.ID); !ok {
		t.Fatal("no index for the new active segment")
	}
	// A view taken before the seal still answers for its own segment.
	if got := viewN(t, w, held); !slices.Equal(got, wantMsgs(0, 10)) {
		t.Fatalf("view held across the seal: %v", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.ActiveIndex(w.manifest.All()[1].ID); ok {
		t.Fatal("an index after Close")
	}
}

// TestRecoveredActiveIndex: a crash-recovered segment is queryable through its in-memory
// index only if the Reindex hook rebuilt that index from its records; without it the index
// starts at the restart and misses everything before, so none is offered.
func TestRecoveredActiveIndex(t *testing.T) {
	for _, hook := range []bool{true, false} {
		t.Run(fmt.Sprintf("reindex=%v", hook), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Schema: []index.FieldType{{Name: "n", Kind: index.KindInt}}, SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: -1}
			w := newSyncWriter(t, dir, opts)
			write, _ := syncWrites(w, true)
			write(3*entriesPerPage + 10) // three full pages on disk, ten records in RAM
			w.stop()

			if hook {
				opts.Reindex = func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
					var n int64
					fmt.Sscanf(e.Message, "%d", &n)
					return []index.KeyedValue{intKV("n", n)}, nil
				}
			}
			w2, err := NewWriter(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := w2.Start(nil); err != nil {
				t.Fatal(err)
			}
			defer w2.Close()
			v, ok := w2.ActiveIndex(w2.manifest.Active().ID)
			if ok != hook {
				t.Fatalf("index offered=%v for a recovered segment with reindex=%v", ok, hook)
			}
			if !hook {
				return
			}
			if got := viewN(t, w2, v); !slices.Equal(got, wantMsgs(0, 3*entriesPerPage)) {
				t.Fatalf("recovered view holds %d records, want the %d on disk", len(got), 3*entriesPerPage)
			}
			if v.Pages != 3 || v.Records != 3*entriesPerPage {
				t.Fatalf("recovered view covers %d pages / %d records, want 3 / %d", v.Pages, v.Records, 3*entriesPerPage)
			}
		})
	}
}

// TestActiveIndexDuringIngest: queries take views and fetch through them while the writer
// appends, flushes and grows its buffers (run under -race). Every view must cover a prefix
// of the segment's records — all records below its boundary, none past it — and every
// offset it returns must be fetchable.
func TestActiveIndexDuringIngest(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{Schema: []index.FieldType{{Name: "n", Kind: index.KindInt}}, SegmentSizeBytes: 1 << 40, FlushInterval: time.Millisecond, BlockPages: -1, LabelKeys: []string{"app"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	id := w.manifest.Active().ID
	const n = 20000
	done := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				v, ok := w.ActiveIndex(id)
				if !ok {
					t.Error("no index for the active segment")
					return
				}
				got, err := viewNErr(w, v)
				if err != nil {
					t.Error(err)
					return
				}
				if !slices.Equal(got, wantMsgs(0, len(got))) {
					t.Errorf("a view of %d records is not a prefix of the segment", len(got))
					return
				}
				if uint64(len(got)) > v.Records {
					t.Errorf("a view of %d records counts %d", len(got), v.Records)
					return
				}
				if l := len(v.LabelOffsets("app", "a")); l != len(got) {
					t.Errorf("label lookup found %d records, typed lookup %d", l, len(got))
					return
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		e := makeEntry(i, 1)
		for w.WriteExtracted(e, []index.KeyedValue{intKV("n", int64(i))}, label.NewSet(map[string]string{"app": "a"})) != nil {
			time.Sleep(10 * time.Microsecond)
		}
	}
	// The queue drains and the tick writes the last page in their own time; under -race
	// on a loaded machine that is well past any fixed sleep.
	deadline := time.Now().Add(10 * time.Second)
	for {
		v, _ := w.ActiveIndex(id)
		if got := viewN(t, w, v); len(got) == n {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("after ingest settled the view holds %d records, want %d", len(got), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(done)
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
