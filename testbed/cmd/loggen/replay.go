package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/advenn/logd/testbed/internal/loggen"
)

type replayArgs struct {
	path       string
	count      int
	valueRegex string
	valueName  string
	baseTS     string
	step       time.Duration
	batch      int
	progress   int
	manifest   string
	labels     []string
	push       []string
}

// runReplay pushes the first -replay-count lines of a dataset to every -push URL, closed-
// loop and in -batch entries per request exactly like corpus mode, and writes a manifest in
// the corpus schema (ts_ns, labels, line, plus value/value_name), so window.sh and the
// expected-count tooling read it unchanged.
func runReplay(a replayArgs) error {
	// Without -label, the app label is the file's base name: HDFS.log -> app=hdfs.
	app := strings.ToLower(strings.TrimSuffix(filepath.Base(a.path), filepath.Ext(a.path)))
	labels, err := parseLabels(a.labels, map[string]string{"app": app})
	if err != nil {
		return err
	}
	var re *regexp.Regexp
	if (a.valueRegex == "") != (a.valueName == "") {
		return fmt.Errorf("-replay-value-regex and -replay-value-name go together")
	}
	if a.valueRegex != "" {
		if re, err = regexp.Compile(a.valueRegex); err != nil {
			return fmt.Errorf("-replay-value-regex: %w", err)
		}
	}

	// "auto" must END the replay one minute before now, as in corpus mode. When the file is
	// shorter than -replay-count, placing the base by the requested count would end the
	// replay early by the difference, so count what will actually be replayed first.
	n := a.count
	if a.baseTS == "" || a.baseTS == "auto" {
		f, err := os.Open(a.path)
		if err != nil {
			return err
		}
		n, err = loggen.CountReplayLines(f, a.count)
		f.Close()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%s has no lines to replay", a.path)
		}
	}
	base, err := resolveBase(a.baseTS, n, a.step)
	if err != nil {
		return err
	}

	f, err := os.Open(a.path)
	if err != nil {
		return err
	}
	defer f.Close()

	var mw *bufio.Writer
	if a.manifest != "" {
		mf, err := os.Create(a.manifest)
		if err != nil {
			return fmt.Errorf("creating manifest: %w", err)
		}
		defer mf.Close()
		mw = bufio.NewWriterSize(mf, 1<<20)
		defer mw.Flush()
	}
	enc := json.NewEncoder(mw)

	var sink *loggen.PushSink
	if len(a.push) > 0 {
		sink = loggen.NewPushSink(a.push, labels, a.batch)
	}
	streams := map[string]string{} // component -> rendered label set, computed once each

	start := time.Now()
	stats, err := loggen.Replay(bufio.NewReaderSize(f, 1<<20), loggen.ReplayOpts{
		Count:      a.count,
		Base:       base,
		Step:       a.step,
		Labels:     labels,
		ValueRegex: re,
		ValueName:  a.valueName,
	}, func(r loggen.Record) error {
		if mw != nil {
			if err := enc.Encode(r); err != nil {
				return err
			}
		}
		if sink != nil {
			comp := r.Labels[loggen.ComponentLabel]
			s, ok := streams[comp]
			if !ok {
				s = loggen.LabelString(r.Labels)
				streams[comp] = s
			}
			if err := sink.Add(loggen.Entry{TSNano: r.TSNano, Line: r.Line, Stream: s}); err != nil {
				return err
			}
		}
		if a.progress > 0 && (r.I+1)%a.progress == 0 {
			fmt.Fprintf(os.Stderr, "loggen: replayed %d/%d lines (%.0f/s)\n", r.I+1, n, float64(r.I+1)/time.Since(start).Seconds())
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
	return report(stats.Lines, start, sink, map[string]any{"replay": stats})
}
