package storage

import (
	"context"
	"testing"
	"time"

	"github.com/advenn/logd/internal/config"
)

// Reproduces the Grafana symptom: alloy pushes a stream with a `container`
// label (stored in Extra by the ingestion parser). We expect that label to
// be discoverable (Labels/LabelValues) AND attached to the returned log line.
func TestContainerLabelRoundTrip(t *testing.T) {
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
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelInfo,
		Message:    "logd listening on :3100",
		Extra:      `{"container":"logd","service":"api"}`,
	}
	if err := store.Write(entry); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)

	labels, _ := store.Labels()
	t.Logf("labels: %v", labels)
	for _, l := range labels {
		vals, _ := store.LabelValues(l)
		t.Logf("  %s = %v", l, vals)
	}

	res, _ := store.Query(QueryFilter{
		StartTS: 0,
		EndTS:   time.Now().Add(time.Hour).UnixNano(),
		Limit:   10,
	})
	for _, e := range res {
		t.Logf("entry: msg=%q extra=%q level=%v serviceID=%d", e.Message, e.Extra, e.Level, e.ServiceID)
	}

	store.Close()
}
