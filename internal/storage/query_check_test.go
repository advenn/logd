package storage

import (
	"context"
	"github.com/advenn/logd/internal/config"
	"testing"
	"time"
)

func TestQueryIntegration(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 100

	store, err := NewFileStorage(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}

	entry := LogEntry{
		TS:         time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC),
		IngestedAt: time.Now(),
		Level:      LogLevelError,
		Message:    "request timeout after 30 seconds",
		Extra:      `{"service":"api"}`,
	}
	if err := store.Write(entry); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	filter := QueryFilter{
		StartTS: 0,
		EndTS:   time.Now().Add(365 * 24 * time.Hour).UnixNano(),
		Limit:   10,
	}
	results, err := store.Query(filter)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Results: %d", len(results))
	for _, e := range results {
		t.Logf("  ts=%v level=%v msg=%q", e.TS, e.Level, e.Message)
	}

	if len(results) == 0 {
		t.Error("expected at least 1 result, got 0 — query returned empty")
	}

	store.Close()
}
