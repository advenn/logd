// Command pgshim puts PostgreSQL behind the one HTTP endpoint the testbed's harness times.
//
// The interleaved harness (scripts/bench-vs.sh) measures every engine the same way: one
// curl per sample, wall-clock from request to the last byte of a Loki-shaped JSON body. A
// PostgreSQL figure taken any other way -- psql's \timing, EXPLAIN ANALYZE -- would leave out
// the network round trip, row transfer and result serialisation that every other engine pays
// inside its number, and would flatter PostgreSQL by exactly the part that dominates broad
// queries. So the shim does what logd's and Loki's HTTP layers do: run the query, group the
// rows into streams, and serialise them.
//
//	GET /loki/api/v1/query_range?query=latency_ms > 200&variant=structured
//	    &start=<ns>&end=<ns>&limit=<n>[&direction=backward|forward][&dataset=app]
//
// `query` is a single whitelisted predicate (see pgdata.ParsePredicate), not LogQL. `variant`
// is required and selects the pre-structured table (generated column + B-tree) or the raw
// one (extraction at query time). Both parameters travel in the URL, because bench-vs.sh can
// only pass engine-specific parameters that way.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pgshim:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":7300", "HTTP listen address")
	dsn := flag.String("dsn", envOr("PGSHIM_DSN", "postgres://logd:logd@postgres:5432/logs"), "PostgreSQL connection string")
	maxConns := flag.Int("max-conns", 4, "connection pool size")
	flag.Parse()

	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		return fmt.Errorf("parsing -dsn: %w", err)
	}
	cfg.MaxConns = int32(*maxConns)
	// Plan every execution with its actual threshold. pgx prepares and caches statements, and
	// after five executions PostgreSQL may switch a prepared statement to a GENERIC plan,
	// which estimates `latency_ms > $1` at a fixed 33% selectivity whatever the threshold is.
	// Whether that switch happens depends on the order the harness happens to run thresholds
	// in, so it would be a hidden variable in the sweep. Custom plans are also what an ad-hoc
	// SQL client sending literal values gets.
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
	cfg.ConnConfig.RuntimeParams["application_name"] = "pgshim"

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating pool: %w", err)
	}
	defer pool.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := newServer(pool, log)
	srv := &http.Server{
		Addr:              *listen,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", *listen)

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
