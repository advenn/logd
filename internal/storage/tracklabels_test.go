package storage

import (
	"context"
	"testing"
	"time"

	"github.com/advenn/logd/internal/config"
)

// Entries with empty Extra and no service (raw container logs) must still track
// "level", and must NOT produce an empty "service" option.
func TestTrackLabelsEmptyExtraAndService(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 50

	store, err := NewFileStorage(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// No Extra, no service.
	store.Write(LogEntry{TS: time.Now(), IngestedAt: time.Now(), Level: LogLevelError, Message: "boom"})
	time.Sleep(150 * time.Millisecond)

	labels, _ := store.Labels()
	hasLevel, hasService := false, false
	for _, l := range labels {
		if l == "level" {
			hasLevel = true
		}
		if l == "service" {
			hasService = true
		}
	}
	if !hasLevel {
		t.Errorf("level not tracked for empty-Extra entry; labels=%v", labels)
	}
	if hasService {
		t.Errorf("empty service should not be a label; labels=%v", labels)
	}

	vals, _ := store.LabelValues("level")
	if len(vals) != 1 || vals[0] != "ERROR" {
		t.Errorf("level values = %v, want [ERROR]", vals)
	}
}
