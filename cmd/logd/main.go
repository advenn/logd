// Command logd is the log-storage daemon. It wires the protocol-agnostic core (extract →
// storage → query) to the Loki-compatible HTTP surface so Grafana can point at it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/advenn/logd/compat/loki"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		log.Fatalf("logd: %v", err)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	// Core: compile extraction, open storage, launch the writer.
	engine, err := extract.Compile(cfg.Index)
	if err != nil {
		return fmt.Errorf("compiling index config: %w", err)
	}
	// With multitenancy on, the tenant label must be indexed (allowlisted) so the
	// tenant-scoping predicate pushes down instead of scanning every query.
	labels := cfg.Labels
	if cfg.Multitenancy {
		labels = append(append([]string{}, labels...), model.TenantLabel)
	}
	opts := storage.Options{
		Schema:              engine.IndexedFields(),
		SegmentSizeBytes:    cfg.SegmentSizeBytes(),
		FlushInterval:       cfg.FlushInterval(),
		Retention:           cfg.Retention(),
		IndexMemBudget:      cfg.IndexMemBudgetBytes(),
		SyncInterval:        cfg.SyncInterval(),
		BlockPages:          cfg.BlockPages(),
		MaxLabelCardinality: cfg.MaxLabelValues(),
		// Rebuild a crash-recovered segment's index by re-extracting its records (§8),
		// using the same extraction + allowlist as live ingest so index == scan.
		Reindex: func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
			var keys []index.KeyedValue
			if engine != nil {
				keys = engine.Extract(e.Message)
			}
			return keys, ingest.DeriveLabels(e.Extra, labels)
		},
	}
	// One shard-writer per data_root/shard-NNNN folder (shared-nothing, §10). The query
	// engine fans in across the same shards.
	writers := make([]*storage.Writer, 0, cfg.ShardCount())
	shards := make([]query.Shard, 0, cfg.ShardCount())
	for i := 0; i < cfg.ShardCount(); i++ {
		shardDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%04d", i))
		w, err := storage.NewWriter(shardDir, opts)
		if err != nil {
			closeWriters(writers)
			return fmt.Errorf("opening shard %d: %w", i, err)
		}
		writers = append(writers, w)
		shards = append(shards, w)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for i, w := range writers {
		if err := w.Start(ctx); err != nil {
			closeWriters(writers)
			return fmt.Errorf("starting shard %d: %w", i, err)
		}
	}

	ig := ingest.NewShardedWithLabels(engine, writers, labels)
	qe := query.NewShardedEngine(shards, engine, labels)
	qe.SetIndexCacheBytes(cfg.IndexCacheBytes())
	lokiSrv := loki.NewServer(ig, qe, engine)
	if cfg.Multitenancy {
		lokiSrv.EnableMultitenancy()
	}
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      lokiSrv.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Serve until a signal arrives, then shut down HTTP and flush+seal storage.
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("logd: listening on :%d (data %s, %d shard(s))", cfg.Port, cfg.DataDir, len(writers))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		closeWriters(writers)
		return err
	case <-ctx.Done():
		log.Print("logd: shutting down")
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("logd: http shutdown: %v", err)
	}
	if err := closeWriters(writers); err != nil { // flush final page, seal, persist
		return fmt.Errorf("closing storage: %w", err)
	}
	return nil
}

// closeWriters closes every shard writer, returning the first error.
func closeWriters(writers []*storage.Writer) error {
	var firstErr error
	for _, w := range writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
