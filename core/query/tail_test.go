package query_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	q "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

func liveSetup(t *testing.T, flush time.Duration) (*storage.Writer, *q.Engine, *ingest.Ingester) {
	t.Helper()
	dir := t.TempDir()
	eng, err := extract.Compile(testCfg())
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: flush})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	return w, q.NewEngine(w, eng), ingest.New(eng, w)
}

func ingestMsg(t *testing.T, ig *ingest.Ingester, sec int, msg string) {
	t.Helper()
	if err := ig.Ingest(model.LogEntry{TS: time.Unix(t0+int64(sec), 0).UTC(), Level: model.LogLevelInfo, Message: msg}); err != nil {
		t.Fatal(err)
	}
}

func TestTailDeliversOnlyMatches(t *testing.T) {
	w, e, ig := liveSetup(t, time.Hour)
	defer w.Close()

	ch, cancel := e.Tail([]q.Predicate{q.LineContains{Sub: "took"}}, 16)

	ingestMsg(t, ig, 0, "req took 5ms")
	ingestMsg(t, ig, 1, "no match here")

	// The matching record must arrive.
	select {
	case got := <-ch:
		if !strings.Contains(got.Message, "took") {
			t.Fatalf("delivered a non-matching record: %q", got.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("matching record never delivered")
	}
	// The non-matching record must NOT arrive.
	select {
	case got := <-ch:
		t.Fatalf("delivered a record that shouldn't match: %q", got.Message)
	case <-time.After(150 * time.Millisecond):
	}

	// After cancel, the channel closes and no further records are delivered.
	cancel()
	ingestMsg(t, ig, 2, "more took 9ms")
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed, as expected
			}
			// drain any record buffered before cancel; keep going until closed
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}

// A slow tailer whose buffer is full must drop records, never block the writer.
func TestTailBufferDropDoesNotBlockWriter(t *testing.T) {
	w, e, ig := liveSetup(t, time.Hour)
	defer w.Close()

	_, cancel := e.Tail([]q.Predicate{q.LineContains{Sub: "x"}}, 1) // buffer 1, never drained
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			ingestMsg(t, ig, i, "x")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer appears blocked by a full tail buffer")
	}
}

// Execute and Tail run concurrently with active ingestion (flushing) under -race.
func TestConcurrentQueryTailDuringIngest(t *testing.T) {
	w, e, ig := liveSetup(t, 5*time.Millisecond) // flush during the test → concurrent read of flushed pages
	ch, cancel := e.Tail([]q.Predicate{q.LineContains{Sub: "took"}}, 64)

	drainDone := make(chan struct{})
	go func() {
		for range ch {
		}
		close(drainDone)
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 800; i++ {
			ingestMsg(t, ig, i%50, fmt.Sprintf("req took %dms", i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			if _, err := e.Execute(q.Query{Start: ts(0), End: ts(100), Preds: []q.Predicate{q.LineContains{Sub: "took"}}}); err != nil {
				t.Errorf("Execute during ingest: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	cancel()
	<-drainDone
	w.Close()
}
