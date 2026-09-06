package loggen

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"
)

// A corpus is a fixed, replayable body of log lines plus the ground truth about what is
// in it. It exists because a head-to-head comparison between log stores is only worth
// publishing if you can prove all three returned the *right* answer, not merely the same
// answer — two backends can agree and both be wrong.
//
// Two properties make that possible, and both are why corpus generation does not simply
// reuse Run():
//
//   - Timestamps are SYNTHETIC and strictly monotonic (base + i*step), not time.Now().
//     The timestamp is the primary key used to diff result sets across backends, so
//     collisions would make the diff ambiguous. At a million lines, wall-clock time
//     collides constantly.
//   - Every record is written to a manifest as it is generated, so the expected answer to
//     a query can be computed offline in plain Go, with no backend involved.

// Record is one corpus line plus its ground truth.
type Record struct {
	I      int               `json:"i"`
	TSNano int64             `json:"ts_ns"`
	Labels map[string]string `json:"labels"`
	Line   string            `json:"line"`
	Level  string            `json:"level"`
	// LatencyMS is the duration the RENDERED line actually carries, or nil when the line
	// carries none. See extractLatencyMS for why this is scanned back out of the text
	// rather than read from the event.
	LatencyMS *int `json:"latency_ms,omitempty"`
}

// CorpusOpts controls deterministic corpus generation.
type CorpusOpts struct {
	Count  int               // number of records to emit
	Base   time.Time         // first timestamp; fixed, never time.Now()
	Step   time.Duration     // spacing between consecutive timestamps
	Labels map[string]string // stream labels attached to every record
}

// tookRe matches the duration the app formatter actually renders. Note the '=' — the
// generator emits "took=247ms", not "took 247ms". Getting this wrong is silent: the
// corpus would claim no line has a latency and every range query would expect zero rows.
var tookRe = regexp.MustCompile(`took=([0-9]+)ms`)

// extractLatencyMS pulls the duration back out of the rendered line.
//
// This deliberately does NOT read Event.DurationMS. The event carries a float that only
// *some* formatter branches render, and renders differently: the app formatter emits
// "took=<n>ms" on most branches, "waited=<n>ms" on the lock-contention warning, and a
// bare "<n>ms" with no key on one info branch. Reading the event would encode our belief
// about what the formatter does; scanning the output encodes what it actually did — and
// the branches that legitimately carry no "took=" correctly come back nil.
func extractLatencyMS(line string) *int {
	m := tookRe.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return nil
	}
	return &n
}

// GenerateCorpus emits Count deterministic records, calling emit for each. The same cfg
// seed and the same opts always produce byte-identical output.
func GenerateCorpus(cfg *Config, o CorpusOpts, emit func(Record) error) error {
	if o.Count <= 0 {
		return fmt.Errorf("corpus count must be positive, got %d", o.Count)
	}
	if o.Step <= 0 {
		return fmt.Errorf("corpus step must be positive, got %v", o.Step)
	}
	// Per-service clock skew is a feature when observing real agents, but here it would
	// break the strict monotonicity the parity diff depends on. Refuse rather than
	// silently producing a corpus whose primary key has duplicates.
	if cfg.ClockSkew != 0 {
		return fmt.Errorf("corpus mode requires clock_skew=0 (got %v): skew breaks timestamp monotonicity, which the parity diff uses as its primary key", cfg.ClockSkew)
	}

	g := NewGenerator(cfg)
	f, err := NewFormatter(cfg.Format)
	if err != nil {
		return err
	}

	for i := 0; i < o.Count; i++ {
		ts := o.Base.Add(time.Duration(i) * o.Step)
		ev := g.Next(ts)
		// The generator may apply its own notion of "now"; the corpus timestamp is the
		// authority, so it is stamped back on before formatting.
		ev.TS = ts
		line := f.Format(ev)

		labels := make(map[string]string, len(o.Labels))
		for k, v := range o.Labels {
			labels[k] = v
		}
		if err := emit(Record{
			I:         i,
			TSNano:    ts.UnixNano(),
			Labels:    labels,
			Line:      line,
			Level:     ev.Level,
			LatencyMS: extractLatencyMS(line),
		}); err != nil {
			return err
		}
	}
	return nil
}

// WriteManifest streams a corpus as JSON Lines. One object per line keeps memory flat at
// any corpus size and makes the file greppable.
func WriteManifest(w io.Writer, recs func(func(Record) error) error) error {
	enc := json.NewEncoder(w)
	return recs(func(r Record) error { return enc.Encode(r) })
}

// ReadManifest streams a JSON Lines manifest back, calling visit per record.
func ReadManifest(r io.Reader, visit func(Record) error) error {
	dec := json.NewDecoder(r)
	for dec.More() {
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}
		if err := visit(rec); err != nil {
			return err
		}
	}
	return nil
}
