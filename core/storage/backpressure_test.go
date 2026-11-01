package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// These tests pin the difference between the two write entry points, because choosing the
// wrong one caused silent data duplication that only a ground-truth corpus revealed:
// pushing 50,000 lines through the Loki endpoint stored 169,692 rows.
//
// The mechanism was not a storage bug — it was WriteExtracted's drop-on-full contract used
// underneath a batch protocol. A push handler ingested entries one at a time; when the
// queue filled midway it returned 503 for the WHOLE request; and since the Loki protocol
// has no way to say "I accepted the first 412 of your 1000", every real shipper retried
// the whole batch and the accepted prefix was stored twice.
//
// The queue only drains once Start is called, so NOT starting the writer fills it
// deterministically — no timing luck, no sleeps.

func fillQueue(t *testing.T, w *storage.Writer) {
	t.Helper()
	// The write channel is 4096 deep; push past that so it is certainly full.
	for i := 0; i < 4096; i++ {
		if err := w.WriteExtracted(model.LogEntry{TS: time.Unix(0, int64(i)), Message: "x"}, nil, nil); err != nil {
			t.Fatalf("queue should accept the first 4096 entries, failed at %d: %v", i, err)
		}
	}
}

// TestWriteExtractedDropsWhenFull documents the existing contract: the non-blocking entry
// point sheds load rather than waiting. This is the right behaviour for a caller that can
// afford to lose a record, and the wrong one underneath a retrying batch client.
func TestWriteExtractedDropsWhenFull(t *testing.T) {
	w, err := storage.NewWriter(t.TempDir(), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately NOT started: nothing drains, so the queue fills and stays full.
	fillQueue(t, w)

	err = w.WriteExtracted(model.LogEntry{TS: time.Unix(0, 9999), Message: "overflow"}, nil, nil)
	if err == nil {
		t.Fatal("expected a drop once the queue is full")
	}
}

// TestWriteExtractedCtxWaitsInsteadOfDropping is the fix's contract: it never silently
// discards a record. With a full queue and no drain it must block until the caller's
// context expires, reporting the deadline — not report a drop it did not have to make.
func TestWriteExtractedCtxWaitsInsteadOfDropping(t *testing.T) {
	w, err := storage.NewWriter(t.TempDir(), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, w)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = w.WriteExtractedCtx(ctx, model.LogEntry{TS: time.Unix(0, 9999), Message: "overflow"}, nil, nil)
	waited := time.Since(start)

	if err == nil {
		t.Fatal("expected the context deadline to end the wait")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a context deadline error, got %v", err)
	}
	// The point of the fix: it WAITED. A drop would have returned immediately.
	if waited < 100*time.Millisecond {
		t.Fatalf("returned after %v — it dropped instead of waiting for queue space", waited)
	}
}

// TestWriteExtractedCtxSucceedsOnceDrained is the normal case: backpressure costs latency,
// not correctness. Once the writer starts draining, a blocked write completes rather than
// failing, so a batch stays indivisible from the client's point of view.
func TestWriteExtractedCtxSucceedsOnceDrained(t *testing.T) {
	w, err := storage.NewWriter(t.TempDir(), storage.Options{FlushInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, w)

	// Start draining shortly after the write blocks.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = w.Start(context.Background())
	}()
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WriteExtractedCtx(ctx, model.LogEntry{TS: time.Unix(0, 9999), Message: "overflow"}, nil, nil); err != nil {
		t.Fatalf("write should succeed once the queue drains, got %v", err)
	}
}

// TestWriteExtractedCtxReportsClosedWriter: a write blocked on a full queue must not deadlock
// forever on a channel nobody will ever drain again: Close wakes it with an error.
func TestWriteExtractedCtxReportsClosedWriter(t *testing.T) {
	w, err := storage.NewWriter(t.TempDir(), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	fillQueue(t, w) // never started, so nothing drains

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	blocked := make(chan error, 1)
	go func() {
		blocked <- w.WriteExtractedCtx(ctx, model.LogEntry{TS: time.Unix(0, 9999), Message: "overflow"}, nil, nil)
	}()
	time.Sleep(20 * time.Millisecond) // let it block on the full queue
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	err = <-blocked
	if err == nil {
		t.Fatal("expected an error writing to a closed writer")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked on the context instead of noticing the writer was closed")
	}
}

// TestWritesAfterCloseAreRefused: once Close has begun, both write methods must say the
// record was not taken. WriteExtracted never checked, and WriteExtractedCtx's select could
// pick the free queue slot over the closed channel: either way the caller got nil for a
// record nobody would ever read.
func TestWritesAfterCloseAreRefused(t *testing.T) {
	dir := t.TempDir()
	w, err := storage.NewWriter(dir, storage.Options{BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		e := model.LogEntry{TS: time.Unix(1700000000, int64(i)), Message: "late"}
		if err := w.WriteExtractedCtx(context.Background(), e, nil, nil); !errors.Is(err, storage.ErrWriterClosed) {
			t.Fatalf("WriteExtractedCtx after Close: got %v, want ErrWriterClosed", err)
		}
		if err := w.WriteExtracted(e, nil, nil); !errors.Is(err, storage.ErrWriterClosed) {
			t.Fatalf("WriteExtracted after Close: got %v, want ErrWriterClosed", err)
		}
	}
}

// TestCloseLosesNoAcceptedWrite: every write that returned nil must be on disk after Close,
// however the writes and the Close interleave. The writers here behave like push handlers
// while the daemon shuts down: they keep writing, before, during and after Close.
func TestCloseLosesNoAcceptedWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := storage.NewWriter(dir, storage.Options{BlockPages: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		t.Fatal(err)
	}
	var accepted, seq atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				i := seq.Add(1)
				e := model.LogEntry{TS: time.Unix(1700000000, i), Message: fmt.Sprintf("line %d", i)}
				if w.WriteExtractedCtx(context.Background(), e, nil, nil) == nil {
					accepted.Add(1)
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // writes keep arriving after Close has returned
	stop.Store(true)
	wg.Wait()

	got, err := storage.ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(got)) != accepted.Load() {
		t.Fatalf("%d writes were accepted, %d records stored", accepted.Load(), len(got))
	}
}
