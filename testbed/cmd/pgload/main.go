// Command pgload loads a testbed manifest into PostgreSQL with COPY and reports what it cost:
// load time, lines/s, WAL written, and table and index sizes, per variant.
//
// It reads the same JSONL manifest the push driver wrote, so PostgreSQL stores exactly the
// lines, timestamps and labels the log stores received, and the same ground truth applies.
//
// Measurement decisions, each of which moves the numbers:
//
//   - Rows go in as COPY batches of -batch lines (default 1000, the push driver's batch), each
//     its own transaction. With synchronous_commit at its default every batch is on disk when
//     COPY returns, so this is the closed-loop "accepted" figure the push driver measures for
//     the log stores, with stricter durability. -batch=0 loads everything in one COPY, the bulk-
//     load best case, which no log shipper produces.
//   - The indexes exist during the load. A one-off import would build them afterwards, but logs
//     arrive continuously and the other engines index as they ingest, so maintaining the B-trees
//     per row is the comparable cost.
//   - The manifest is decoded before the clock starts, so JSON decoding in this process is not
//     charged to PostgreSQL, as the push driver encodes each batch before starting an
//     endpoint's clock. Encoding the binary COPY stream does fall inside the clock: pgx writes
//     it while streaming, so it cannot be separated out.
//   - Variants load one after another, never concurrently, so each gets the whole server.
//   - Each load starts from a CHECKPOINT, so every run pays the same full-page writes instead
//     of whichever checkpoint happened to land mid-load.
//   - VACUUM (ANALYZE) runs after the load and is timed separately. Without it the first
//     queries would set hint bits on every page they touch -- writing during the measured
//     queries -- and the planner would have no statistics on the extracted column. Autovacuum
//     would do the same work later, at a moment nobody chose.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/advenn/logd/testbed/internal/loggen"
	"github.com/advenn/logd/testbed/internal/pgdata"
	"github.com/jackc/pgx/v5"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pgload:", err)
		os.Exit(1)
	}
}

type corpusRow struct {
	tsNano int64
	stream int // index into the distinct label sets
	line   string
}

// Result is one variant's load accounting. Sizes are bytes.
type Result struct {
	Variant       string           `json:"variant"`
	Table         string           `json:"table"`
	Rows          int64            `json:"rows"`
	RowsWithValue int64            `json:"rows_with_value,omitempty"`
	Batches       int              `json:"batches"`
	LoadSec       float64          `json:"load_sec"`
	LinesPerSec   float64          `json:"lines_per_s"`
	VacuumSec     float64          `json:"vacuum_analyze_sec"`
	WALBytes      int64            `json:"wal_bytes"`
	HeapBytes     int64            `json:"heap_bytes"`  // pg_table_size: heap + TOAST + FSM + VM
	IndexBytes    int64            `json:"index_bytes"` // pg_indexes_size
	TotalBytes    int64            `json:"total_bytes"` // pg_total_relation_size
	Indexes       map[string]int64 `json:"indexes"`
}

func run() error {
	var (
		dsn      = flag.String("dsn", envOr("PG_DSN", "postgres://logd:logd@localhost:5432/logs"), "PostgreSQL connection string")
		manifest = flag.String("manifest", "", "JSONL manifest written by loggen (required)")
		dataset  = flag.String("dataset", "app", "dataset the manifest belongs to (selects the tables)")
		variants = flag.String("variants", "structured,unstructured", "comma-separated variants to load")
		batch    = flag.Int("batch", 1000, "rows per COPY, each its own transaction; 0 = one COPY for everything")
		wait     = flag.Duration("wait", 60*time.Second, "how long to wait for the server to accept connections")
		jsonOut  = flag.String("json", "", "also write the results as JSON here")
	)
	flag.Parse()
	if *manifest == "" {
		return errors.New("-manifest is required")
	}
	d, ok := pgdata.Lookup(*dataset)
	if !ok {
		return fmt.Errorf("unknown dataset %q", *dataset)
	}
	var vs []pgdata.Variant
	for _, s := range strings.Split(*variants, ",") {
		v, err := pgdata.ParseVariant(strings.TrimSpace(s))
		if err != nil {
			return err
		}
		vs = append(vs, v)
	}

	rows, labelSets, withValue, err := readManifest(*manifest, d.Field)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "pgload: %d rows, %d streams, %d carrying %s, from %s\n", len(rows), len(labelSets), withValue, d.Field, *manifest)

	ctx := context.Background()
	conn, err := connect(ctx, *dsn, *wait)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	ids, err := ensureStreams(ctx, conn, labelSets)
	if err != nil {
		return err
	}

	var results []Result
	for _, v := range vs {
		r, err := load(ctx, conn, d, v, rows, ids, *batch)
		if err != nil {
			return fmt.Errorf("%s: %w", v, err)
		}
		if v == pgdata.Structured && r.RowsWithValue != withValue {
			return fmt.Errorf("%s: the generated column extracted %s from %d rows, the manifest says %d -- the SQL expression and the ground truth disagree",
				d.Structured, d.Field, r.RowsWithValue, withValue)
		}
		results = append(results, r)
	}

	printResults(os.Stdout, results)
	if *jsonOut != "" {
		raw, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*jsonOut, append(raw, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// readManifest decodes the whole manifest up front, interning label sets, so the timed load
// measures PostgreSQL rather than this process's JSON decoder.
func readManifest(path, field string) ([]corpusRow, []string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	defer f.Close()
	var (
		rows      []corpusRow
		labelSets []string
		withValue int64
		index     = map[string]int{}
	)
	err = loggen.ReadManifest(f, func(r loggen.Record) error {
		if strings.IndexByte(r.Line, 0) >= 0 {
			return fmt.Errorf("record %d: NUL byte in line; PostgreSQL text cannot store it", r.I)
		}
		key, err := json.Marshal(r.Labels) // map keys marshal sorted: one key per label set
		if err != nil {
			return err
		}
		i, ok := index[string(key)]
		if !ok {
			i = len(labelSets)
			index[string(key)] = i
			labelSets = append(labelSets, string(key))
		}
		if _, ok := manifestValue(r, field); ok {
			withValue++
		}
		rows = append(rows, corpusRow{tsNano: r.TSNano, stream: i, line: r.Line})
		return nil
	})
	return rows, labelSets, withValue, err
}

// manifestValue returns the ground-truth value a record carries for field.
func manifestValue(r loggen.Record, field string) (int64, bool) {
	if field == "latency_ms" && r.LatencyMS != nil {
		return int64(*r.LatencyMS), true
	}
	return 0, false
}

func connect(ctx context.Context, dsn string, wait time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(wait)
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			if err = conn.Ping(ctx); err == nil {
				return conn, nil
			}
			conn.Close(ctx)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connecting to %s: %w", redact(dsn), err)
		}
		time.Sleep(time.Second)
	}
}

// ensureStreams returns the database id of every label set, creating missing ones. Streams
// are never truncated: two datasets share the table, and reloading one must not orphan the
// other's rows.
func ensureStreams(ctx context.Context, conn *pgx.Conn, labelSets []string) ([]int32, error) {
	ids := make([]int32, len(labelSets))
	for i, ls := range labelSets {
		if _, err := conn.Exec(ctx, "INSERT INTO streams (labels) VALUES ($1::jsonb) ON CONFLICT (labels) DO NOTHING", ls); err != nil {
			return nil, fmt.Errorf("creating stream %s: %w", ls, err)
		}
		if err := conn.QueryRow(ctx, "SELECT id FROM streams WHERE labels = $1::jsonb", ls).Scan(&ids[i]); err != nil {
			return nil, fmt.Errorf("resolving stream %s: %w", ls, err)
		}
	}
	return ids, nil
}

func load(ctx context.Context, conn *pgx.Conn, d pgdata.Dataset, v pgdata.Variant, rows []corpusRow, ids []int32, batch int) (Result, error) {
	table := d.Table(v)
	ident := pgx.Identifier{table}.Sanitize()
	r := Result{Variant: string(v), Table: table}

	if _, err := conn.Exec(ctx, "TRUNCATE "+ident); err != nil {
		return r, err
	}
	if _, err := conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return r, err
	}
	var wal0 string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&wal0); err != nil {
		return r, err
	}

	if batch <= 0 {
		batch = len(rows)
	}
	cols := []string{"ts_ns", "stream_id", "line"}
	start := time.Now()
	for lo := 0; lo < len(rows); lo += batch {
		chunk := rows[lo:min(lo+batch, len(rows))]
		n, err := conn.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromSlice(len(chunk), func(i int) ([]any, error) {
			x := chunk[i]
			return []any{x.tsNano, ids[x.stream], x.line}, nil
		}))
		if err != nil {
			return r, fmt.Errorf("COPY rows %d..%d: %w", lo, lo+len(chunk), err)
		}
		r.Rows += n
		r.Batches++
	}
	r.LoadSec = time.Since(start).Seconds()
	r.LinesPerSec = float64(r.Rows) / r.LoadSec

	if err := conn.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint", wal0).Scan(&r.WALBytes); err != nil {
		return r, err
	}

	vstart := time.Now()
	if _, err := conn.Exec(ctx, "VACUUM (ANALYZE) "+ident); err != nil {
		return r, err
	}
	r.VacuumSec = time.Since(vstart).Seconds()

	if err := conn.QueryRow(ctx,
		"SELECT pg_table_size($1::regclass), pg_indexes_size($1::regclass), pg_total_relation_size($1::regclass)", table,
	).Scan(&r.HeapBytes, &r.IndexBytes, &r.TotalBytes); err != nil {
		return r, err
	}
	r.Indexes = map[string]int64{}
	irows, err := conn.Query(ctx, "SELECT indexrelname::text, pg_relation_size(indexrelid) FROM pg_stat_user_indexes WHERE relname = $1", table)
	if err != nil {
		return r, err
	}
	for irows.Next() {
		var name string
		var size int64
		if err := irows.Scan(&name, &size); err != nil {
			irows.Close()
			return r, err
		}
		r.Indexes[name] = size
	}
	if err := irows.Err(); err != nil {
		return r, err
	}

	// Read the row count back rather than trusting COPY's: a loader that miscounted would
	// otherwise report a throughput for rows that are not there.
	var stored int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+ident).Scan(&stored); err != nil {
		return r, err
	}
	if stored != int64(len(rows)) {
		return r, fmt.Errorf("%s holds %d rows after loading %d", table, stored, len(rows))
	}
	if v == pgdata.Structured {
		col := pgx.Identifier{d.Field}.Sanitize()
		if err := conn.QueryRow(ctx, "SELECT count("+col+") FROM "+ident).Scan(&r.RowsWithValue); err != nil {
			return r, err
		}
	}
	return r, nil
}

func printResults(w *os.File, results []Result) {
	mb := func(b int64) string { return fmt.Sprintf("%.2f", float64(b)/1e6) }
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "variant\ttable\trows\tload s\tlines/s\tvacuum s\tWAL MB\theap MB\tindex MB\ttotal MB\t")
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%.2f\t%.0f\t%.2f\t%s\t%s\t%s\t%s\t\n",
			r.Variant, r.Table, r.Rows, r.LoadSec, r.LinesPerSec, r.VacuumSec,
			mb(r.WALBytes), mb(r.HeapBytes), mb(r.IndexBytes), mb(r.TotalBytes))
	}
	tw.Flush()
	fmt.Fprintln(w)
	for _, r := range results {
		names := make([]string, 0, len(r.Indexes))
		for name := range r.Indexes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "  index %-24s %8s MB\n", name, mb(r.Indexes[name]))
		}
	}
	fmt.Fprintln(w, "\nMB = 10^6 bytes. heap = pg_table_size (heap + TOAST + FSM + VM), index = pg_indexes_size,")
	fmt.Fprintln(w, "total = pg_total_relation_size; WAL is what the load wrote and is not part of total.")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func redact(dsn string) string {
	if i := strings.Index(dsn, "@"); i >= 0 {
		if j := strings.Index(dsn, "://"); j >= 0 && j < i {
			return dsn[:j+3] + "…" + dsn[i:]
		}
	}
	return dsn
}
