package loggen

import (
	"testing"
	"time"
)

func corpusCfg(t *testing.T) *Config {
	t.Helper()
	cfg, err := Load([]string{"-format=app", "-seed=1001", "-rate=1"})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

func collect(t *testing.T, cfg *Config, o CorpusOpts) []Record {
	t.Helper()
	var out []Record
	if err := GenerateCorpus(cfg, o, func(r Record) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatalf("generate: %v", err)
	}
	return out
}

func defaultOpts(n int) CorpusOpts {
	return CorpusOpts{
		Count:  n,
		Base:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Step:   time.Millisecond,
		Labels: map[string]string{"app": "checkout", "env": "prod", "region": "eu"},
	}
}

// TestCorpusIsReproducible is the foundation of the whole parity story: the expected
// answer to a query is computed from the manifest, so a corpus that differed between the
// generation run and the verification run would silently invalidate every comparison.
func TestCorpusIsReproducible(t *testing.T) {
	a := collect(t, corpusCfg(t), defaultOpts(500))
	b := collect(t, corpusCfg(t), defaultOpts(500))
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Line != b[i].Line || a[i].TSNano != b[i].TSNano {
			t.Fatalf("record %d differs:\n  a=%d %q\n  b=%d %q", i, a[i].TSNano, a[i].Line, b[i].TSNano, b[i].Line)
		}
	}
}

// TestCorpusTimestampsAreStrictlyMonotonic pins the property the parity diff relies on:
// ts_ns is the primary key, so duplicates would make "did this backend return the right
// rows?" unanswerable.
func TestCorpusTimestampsAreStrictlyMonotonic(t *testing.T) {
	recs := collect(t, corpusCfg(t), defaultOpts(2000))
	for i := 1; i < len(recs); i++ {
		if recs[i].TSNano <= recs[i-1].TSNano {
			t.Fatalf("timestamps not strictly increasing at %d: %d then %d", i, recs[i-1].TSNano, recs[i].TSNano)
		}
	}
}

// TestCorpusRejectsClockSkew: skew is a useful feature when observing real agents, but it
// would break monotonicity here. Failing loudly beats emitting a subtly unusable corpus.
func TestCorpusRejectsClockSkew(t *testing.T) {
	cfg := corpusCfg(t)
	cfg.ClockSkew = 5 * time.Second
	err := GenerateCorpus(cfg, defaultOpts(10), func(Record) error { return nil })
	if err == nil {
		t.Fatal("expected clock skew to be rejected in corpus mode")
	}
}

// TestLatencyGroundTruthMatchesRenderedLine is the assertion that caught a real bug.
//
// The app formatter renders durations three different ways: "took=<n>ms" on most
// branches, "waited=<n>ms" on the lock-contention warning, and a bare "<n>ms" with no key
// on one info branch. Ground truth must reflect what the line SAYS, not what the event
// carried — otherwise a range query's expected answer includes lines that do not contain
// an indexable duration at all.
func TestLatencyGroundTruthMatchesRenderedLine(t *testing.T) {
	recs := collect(t, corpusCfg(t), defaultOpts(5000))

	var withTook, withLatency int
	for _, r := range recs {
		hasTook := indexOfStr(r.Line, "took=") >= 0
		if hasTook {
			withTook++
		}
		if r.LatencyMS != nil {
			withLatency++
			if !hasTook {
				t.Fatalf("record %d has ground-truth latency but no took= in line: %q", r.I, r.Line)
			}
		} else if hasTook {
			t.Fatalf("record %d has took= in line but no ground-truth latency: %q", r.I, r.Line)
		}
	}
	if withLatency != withTook {
		t.Fatalf("ground truth (%d) disagrees with rendered lines (%d)", withLatency, withTook)
	}
	// Sanity: the corpus must actually exercise the typed index. A hit rate near 0 would
	// mean the template config and the generator have drifted apart again.
	rate := float64(withLatency) / float64(len(recs))
	if rate < 0.5 || rate > 0.95 {
		t.Errorf("took= hit rate %.1f%% is outside the expected band; the corpus should exercise but not saturate the typed index", rate*100)
	}
	t.Logf("took= hit rate: %.1f%% (%d/%d) — the rest carry waited= or a bare duration", rate*100, withLatency, len(recs))
}

// TestLatencyIsSelectiveEnoughToIndex guards the benchmark's premise. logd's planner falls
// back to a sequential scan when a predicate selects more than half a segment, so a
// differentiator query that matched most lines would measure the scan path and quietly
// prove nothing.
func TestLatencyIsSelectiveEnoughToIndex(t *testing.T) {
	recs := collect(t, corpusCfg(t), defaultOpts(5000))
	var over int
	for _, r := range recs {
		if r.LatencyMS != nil && *r.LatencyMS > 200 {
			over++
		}
	}
	frac := float64(over) / float64(len(recs))
	if frac == 0 {
		t.Fatal("no records exceed 200ms; the differentiator query would return nothing")
	}
	if frac > 0.5 {
		t.Errorf("latency_ms > 200 selects %.1f%% of the corpus, above the planner's cost-guard threshold — the query would degrade to a scan", frac*100)
	}
	t.Logf("latency_ms > 200 selects %.1f%% (%d/%d)", frac*100, over, len(recs))
}

func indexOfStr(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
