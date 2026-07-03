package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/advenn/logd/internal/config"
	"github.com/advenn/logd/internal/ingestion"
	"github.com/advenn/logd/internal/query"
	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
)

// Server wraps the HTTP server, routing, and lifecycle.
type Server struct {
	cfg     *config.Config
	store   storage.Storage
	httpSrv *http.Server
	mux     *http.ServeMux
}

// New creates a Server with all routes registered. tmpl may be nil when no
// templates are configured.
func New(cfg *config.Config, store storage.Storage, tmpl *template.Engine) *Server {
	mux := http.NewServeMux()

	// Health check. Note: do NOT register "GET /" — in Go 1.22 the path "/" is a
	// subtree wildcard that catches ALL unmatched GET requests (e.g. /loki/api/v1/query
	// would return text/plain "logd ready" instead of JSON, breaking Grafana).
	mux.HandleFunc("GET /ready", handleRoot)
	mux.HandleFunc("GET /loki/api/v1/ready", handleRoot)

	// Create handler sets.
	ingestion.NewHandler(ingestion.NewWriteQueue(store)).RegisterRoutes(mux)
	query.NewHandler(store, tmpl).RegisterRoutes(mux)

	// Add middleware: recovery, logging.
	wrapped := recoveryMiddleware(loggingMiddleware(mux))

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      wrapped,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		cfg:     cfg,
		store:   store,
		httpSrv: srv,
		mux:     mux,
	}
}

// Run starts the HTTP server and blocks until ctx is cancelled, then shuts down
// gracefully.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		log.Printf("logd listening on :%d", s.cfg.Port)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutdownCtx)

	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	}
}

// loggingMiddleware logs each request method, path, and duration.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}

// handleRoot serves health check requests. Grafana's "Save & Test" sends a
// request to the datasource URL and expects a non-error response.
func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("logd ready"))
}

// recoveryMiddleware catches panics in handlers and returns 500.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic: %v", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
