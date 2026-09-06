// Command loggen produces log traffic for the testbed.
//
// Two modes:
//
//	live    — the original behaviour: generate at a rate to stdout/file/syslog forever.
//	          Used with the Alloy compatibility profile.
//	corpus  — generate a FIXED, replayable body of lines with synthetic monotonic
//	          timestamps, write a ground-truth manifest, and optionally push the identical
//	          encoded bytes to several backends at once. Used for parity and benchmarking.
//
// Corpus mode is what makes the head-to-head defensible: because the manifest records what
// every line contains, the expected answer to a query can be computed without asking any
// backend, so "all three agreed" can be distinguished from "all three were right".
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/advenn/logd/testbed/internal/loggen"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loggen:", err)
		os.Exit(1)
	}
}

func run() error {
	// Corpus-mode flags are parsed off a private FlagSet first, because the inherited
	// loggen.Load owns the live-mode flag namespace and would reject unknown flags.
	var (
		corpus   = flag.Bool("corpus", false, "generate a fixed, replayable corpus instead of live traffic")
		count    = flag.Int("count", 100000, "corpus mode: number of lines")
		manifest = flag.String("manifest", "", "corpus mode: write the ground-truth JSONL manifest here")
		baseTS   = flag.String("base-ts", "auto", "corpus mode: first timestamp (RFC3339), or \"auto\" to end just before now")
		step     = flag.Duration("step", time.Millisecond, "corpus mode: spacing between timestamps")
		batch    = flag.Int("batch", 1000, "corpus mode: entries per push request")
		format   = flag.String("format", "app", "line format: app, json, nginx, logfmt, java, syslog, noisy")
		seed     = flag.Int64("seed", 1001, "PRNG seed")
		progress = flag.Int("progress", 100000, "corpus mode: log progress every N lines (0 to disable)")
	)
	var pushURLs stringList
	var labelArgs stringList
	flag.Var(&pushURLs, "push", "corpus mode: push to this Loki-protocol URL (repeatable)")
	flag.Var(&labelArgs, "label", "corpus mode: stream label k=v (repeatable)")
	flag.Parse()

	if !*corpus {
		return runLive()
	}

	labels, err := parseLabels(labelArgs)
	if err != nil {
		return err
	}
	base, err := resolveBase(*baseTS, *count, *step)
	if err != nil {
		return err
	}

	cfg, err := loggen.Load([]string{
		"-format=" + *format,
		fmt.Sprintf("-seed=%d", *seed),
		"-rate=1",
	})
	if err != nil {
		return err
	}

	var mf *os.File
	var mw *bufio.Writer
	if *manifest != "" {
		if mf, err = os.Create(*manifest); err != nil {
			return fmt.Errorf("creating manifest: %w", err)
		}
		defer mf.Close()
		mw = bufio.NewWriterSize(mf, 1<<20)
		defer mw.Flush()
	}

	var sink *loggen.PushSink
	if len(pushURLs) > 0 {
		sink = loggen.NewPushSink(pushURLs, labels, *batch)
	}

	enc := json.NewEncoder(mw)
	start := time.Now()
	var n int

	err = loggen.GenerateCorpus(cfg, loggen.CorpusOpts{
		Count:  *count,
		Base:   base,
		Step:   *step,
		Labels: labels,
	}, func(r loggen.Record) error {
		if mw != nil {
			if err := enc.Encode(r); err != nil {
				return err
			}
		}
		if sink != nil {
			if err := sink.Add(loggen.Entry{TSNano: r.TSNano, Line: r.Line}); err != nil {
				return err
			}
		}
		n++
		if *progress > 0 && n%*progress == 0 {
			fmt.Fprintf(os.Stderr, "loggen: %d/%d lines (%.0f/s)\n", n, *count, float64(n)/time.Since(start).Seconds())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if sink != nil {
		if err := sink.Flush(); err != nil {
			return err
		}
	}
	if mw != nil {
		if err := mw.Flush(); err != nil {
			return err
		}
	}

	return report(n, start, sink)
}

// report prints a machine-readable summary to stdout so the bench harness can consume it
// without scraping human-facing text.
func report(n int, start time.Time, sink *loggen.PushSink) error {
	elapsed := time.Since(start)
	out := map[string]any{
		"lines":       n,
		"elapsed_sec": elapsed.Seconds(),
		"lines_per_s": float64(n) / elapsed.Seconds(),
	}
	if sink != nil {
		eps := map[string]any{}
		for url, st := range sink.Stats() {
			eps[url] = map[string]any{
				"requests":     st.Requests,
				"entries":      st.Entries,
				"bytes":        st.Bytes,
				"failures":     st.Failures,
				"rate_limited": st.RateLimit,
				"retries":      st.Retries,
				"elapsed_sec":  st.Elapsed.Seconds(),
			}
		}
		out["endpoints"] = eps
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(out)
}

// resolveBase picks the corpus's first timestamp.
//
// "auto" places the corpus so it ENDS one minute before now. That is not cosmetic: log
// stores treat recent and historical data completely differently, and a corpus dated in
// the distant past is silently invisible rather than rejected.
//
//   - Loki only consults its ingesters for data within query_ingesters_within (3h by
//     default). Older data must already have been flushed to the object store, which does
//     not happen until chunk_idle_period/max_chunk_age elapse. A corpus dated months back
//     is therefore accepted with HTTP 204, held in memory, and returns ZERO rows — with no
//     error anywhere to explain it.
//   - VictoriaLogs applies its retention window on ingest, so an old corpus needs
//     -retentionPeriod raised or it is dropped outright.
//
// Determinism is unaffected: the LINES come from the seed, and the manifest records every
// record's exact ts_ns, so ground truth stays exact regardless of where the window sits.
// Pass an explicit RFC3339 -base-ts when a fixed window is genuinely wanted.
func resolveBase(spec string, count int, step time.Duration) (time.Time, error) {
	if spec == "" || spec == "auto" {
		return time.Now().Add(-time.Minute).Add(-time.Duration(count) * step), nil
	}
	t, err := time.Parse(time.RFC3339, spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing -base-ts: %w", err)
	}
	return t, nil
}

func parseLabels(args []string) (map[string]string, error) {
	labels := map[string]string{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("bad -label %q, want k=v", a)
		}
		labels[k] = v
	}
	if len(labels) == 0 {
		labels = map[string]string{"app": "checkout", "env": "prod", "region": "eu"}
	}
	return labels, nil
}

// runLive keeps the original generator behaviour for the Alloy compatibility profile:
// stdout stays a pure log stream, diagnostics go to stderr.
func runLive() error {
	cfg, err := loggen.Load(flag.Args())
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return loggen.Run(ctx, cfg, log)
}
