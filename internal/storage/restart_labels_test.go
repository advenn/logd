package storage

import (
	"context"
	"testing"
	"time"

	"github.com/advenn/logd/internal/config"
)

// Labels (keys + values) must survive a graceful restart: they are persisted on
// Close and reloaded by NewFileStorage. Without this, /labels is empty after a
// restart until new data is ingested.
func TestLabelsPersistAcrossRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 50

	// --- session 1: ingest then close ---
	store, err := NewFileStorage(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	store.Write(LogEntry{
		TS: time.Now(), IngestedAt: time.Now(), Level: LogLevelError,
		Message: "x", Extra: `{"service":"api","region":"us"}`,
	})
	time.Sleep(200 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()

	// --- session 2: reopen, labels should be restored ---
	store2, err := NewFileStorage(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := store2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	labels, _ := store2.Labels()
	want := map[string]bool{"service": true, "region": true, "level": true}
	for _, l := range labels {
		delete(want, l)
	}
	if len(want) > 0 {
		t.Errorf("labels missing after restart: %v (got %v)", want, labels)
	}

	vals, _ := store2.LabelValues("service")
	if len(vals) != 1 || vals[0] != "api" {
		t.Errorf("service values after restart = %v, want [api]", vals)
	}
}
