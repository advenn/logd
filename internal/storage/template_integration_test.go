package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/advenn/logd/internal/config"
	"github.com/advenn/logd/internal/logql"
	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
)

func TestTemplateIndexIntegration(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 100
	cfg.Templates = []config.Template{
		{
			Name:    "order_created",
			Pattern: "order-{order_id:uint32} created",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
			},
		},
	}

	tmplEngine, err := template.NewEngine(cfg.Templates)
	if err != nil {
		t.Fatal(err)
	}

	store, err := storage.NewFileStorage(cfg, tmplEngine)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}

	entries := []storage.LogEntry{
		{
			TS:      time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC),
			Level:   storage.LogLevelInfo,
			Message: "order-1234 created",
			Extra:   `{"service":"api"}`,
		},
		{
			TS:      time.Date(2026, 6, 11, 10, 0, 1, 0, time.UTC),
			Level:   storage.LogLevelInfo,
			Message: "order-5678 created",
			Extra:   `{"service":"api"}`,
		},
		{
			TS:      time.Date(2026, 6, 11, 10, 0, 2, 0, time.UTC),
			Level:   storage.LogLevelInfo,
			Message: "random log message without template match",
			Extra:   `{"service":"api"}`,
		},
	}
	for _, e := range entries {
		if err := store.Write(e); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify .tidx file was created with correct entries.
	ir, err := template.OpenIndexReader(cfg.DataDir+"/index", "order_id")
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}

	refs, err := ir.LookupUint32(1234)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) == 0 {
		t.Error("expected page refs for order_id=1234, got none")
	}
	t.Logf("order_id=1234 refs: %+v", refs)

	refs, err = ir.LookupUint32(5678)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) == 0 {
		t.Error("expected page refs for order_id=5678, got none")
	}

	refs, err = ir.LookupUint32(9999)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs for non-existent value, got %d", len(refs))
	}
}

func TestTemplateIndexQueryAcceleration(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 100
	cfg.Templates = []config.Template{
		{
			Name:    "order",
			Pattern: "order-{order_id:uint32} created",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
			},
		},
	}

	tmplEngine, err := template.NewEngine(cfg.Templates)
	if err != nil {
		t.Fatal(err)
	}

	store, err := storage.NewFileStorage(cfg, tmplEngine)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Write many entries, only one matches the indexed field.
	for i := 0; i < 50; i++ {
		e := storage.LogEntry{
			TS:      time.Date(2026, 6, 11, 10, 0, i, 0, time.UTC),
			Level:   storage.LogLevelInfo,
			Message: "some random log message about other things",
			Extra:   `{"service":"api"}`,
		}
		if i == 25 {
			e.Message = "order-7777 created"
		}
		if err := store.Write(e); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-open storage to query sealed segment with index.
	store2, err := storage.NewFileStorage(cfg, tmplEngine)
	if err != nil {
		t.Fatal(err)
	}

	// Compile a LogQL predicate for per-entry filtering.
	pipeline, err := logql.CompilePipeline(&logql.Query{
		Pipeline: []logql.PipelineStage{
			&logql.LineFilter{Op: logql.OpPipeEq, Value: "7777"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	filter := storage.QueryFilter{
		StartTS:      time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC).UnixNano(),
		EndTS:        time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC).UnixNano(),
		FieldFilters: map[string]string{"order_id": "7777"},
		Predicate:    pipeline,
		Limit:        50,
	}
	results, err := store2.Query(filter)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Message != "order-7777 created" {
		t.Errorf("got message %q, want %q", results[0].Message, "order-7777 created")
	}
	t.Logf("Index-accelerated query returned: %s", results[0].Message)

	store2.Close()
}

func TestTemplateNoMatchFallback(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.FlushIntervalMs = 100
	cfg.Templates = []config.Template{
		{
			Name:    "order",
			Pattern: "order-{order_id:uint32} created",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
			},
		},
	}

	tmplEngine, err := template.NewEngine(cfg.Templates)
	if err != nil {
		t.Fatal(err)
	}

	store, err := storage.NewFileStorage(cfg, tmplEngine)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.Start(ctx)

	for i := 0; i < 5; i++ {
		e := storage.LogEntry{
			TS:      time.Date(2026, 6, 11, 10, 0, i, 0, time.UTC),
			Level:   storage.LogLevelInfo,
			Message: "some random log message",
			Extra:   `{"service":"api"}`,
		}
		store.Write(e)
	}

	time.Sleep(500 * time.Millisecond)
	store.Close()

	store2, _ := storage.NewFileStorage(cfg, tmplEngine)
	defer store2.Close()

	// Query WITHOUT FieldFilters — should fall back to full scan.
	filter := storage.QueryFilter{
		StartTS: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC).UnixNano(),
		EndTS:   time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC).UnixNano(),
		Limit:   10,
	}
	results, err := store2.Query(filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 results from full scan, got %d", len(results))
	}
}
