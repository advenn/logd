// logd — lightweight, Loki-compatible log storage daemon.
//
// Single binary. Drop-in replacement for Loki on single-node deployments.
// Target footprint: <50MB RAM.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/advenn/logd/internal/config"
	"github.com/advenn/logd/internal/server"
	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	// Load configuration.
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	log.Printf("logd starting (data dir: %s)", cfg.DataDir)

	// Compile template engine from config.
	tmplEngine, err := template.NewEngine(cfg.Templates)
	if err != nil {
		log.Fatalf("compiling templates: %v", err)
	}
	if len(cfg.Templates) > 0 {
		log.Printf("loaded %d templates with %d indexed fields", len(cfg.Templates), len(tmplEngine.IndexedFields()))
	}

	// Create storage backend.
	store, err := storage.NewFileStorage(cfg, tmplEngine)
	if err != nil {
		log.Fatalf("creating storage: %v", err)
	}

	// Start writer goroutine.
	// Catch SIGTERM too — `docker stop` and most process managers send it, and
	// without it shutdown is non-graceful (manifest/labels never persisted).
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := store.Start(ctx); err != nil {
		log.Fatalf("starting storage: %v", err)
	}

	// Create HTTP server.
	srv := server.New(cfg, store, tmplEngine)

	// Run until signal.
	if err := srv.Run(ctx); err != nil {
		log.Printf("server stopped: %v", err)
	}

	// Shutdown storage.
	if err := store.Close(); err != nil {
		log.Printf("closing storage: %v", err)
	}

	log.Println("logd stopped")
}
