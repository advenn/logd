package query

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
)

// Metric queries (design §13 Phase 7/8): a range aggregation over the entries of a log
// query, optionally reduced by a vector aggregation, evaluated at each step across the
// time range → a matrix (one time series per output label set). This is the native,
// protocol-agnostic model; the LogQL metric syntax (count_over_time, rate, sum by, …)
// translates to it in compat/loki.

// RangeOp aggregates the entries in a (t-Range, t] window per stream.
type RangeOp uint8

const (
	RangeCount   RangeOp = iota // count_over_time
	RangeRate                   // rate: count / range-seconds
	RangeSum                    // sum_over_time(unwrap)
	RangeAvg                    // avg_over_time(unwrap)
	RangeMax                    // max_over_time(unwrap)
	RangeMin                    // min_over_time(unwrap)
	RangeRateSum                // rate over unwrapped values: sum(values) / range-seconds
)

// VectorOp reduces per-stream values across streams (grouped by/without labels).
type VectorOp uint8

const (
	VecNone VectorOp = iota // no vector aggregation (one series per stream)
	VecSum
	VecAvg
	VecMax
	VecMin
	VecCount
)

// MetricQuery is a native metric query.
type MetricQuery struct {
	Preds            []Predicate // the inner log query
	Range            int64       // window width in nanos ([5m] → 5*60*1e9)
	RangeOp          RangeOp
	Unwrap           string   // field/label to unwrap for sum/avg/max/min (empty for count/rate)
	VectorOp         VectorOp // VecNone → per-stream series
	Grouping         []string // labels for by(...)/without(...)
	Without          bool
	Start, End, Step int64 // matrix time range + step, in nanos

	// SeriesLabels, when set, gives each matching entry's series labels in place of the
	// labels it carries (Resolver.AllLabels). A compat layer whose language derives labels
	// from the line at query time — LogQL parser stages — supplies them, so streams split
	// and group by those labels too. It must be pure; nil keeps the built-in behaviour.
	SeriesLabels func(e model.LogEntry, r Resolver) map[string]string
	// UnwrapValue, when set, replaces the built-in unwrap lookup (a template field's first
	// value, else a numeric label) with the caller's: it returns the value to aggregate, or
	// false to drop the entry. Unwrap must still name the value; this only says how to read
	// it.
	UnwrapValue func(e model.LogEntry, r Resolver) (float64, bool)
}

// MetricPoint is one [timestamp, value] sample.
type MetricPoint struct {
	TS    int64
	Value float64
}

// MatrixSeries is one output time series.
type MatrixSeries struct {
	Labels map[string]string
	Points []MetricPoint
}

const (
	// maxMetricSteps caps the resolution of a matrix (Loki's default is ~11000); a finer
	// step over a wide range is rejected rather than allocating per-step slices to OOM.
	maxMetricSteps = 11000
)

// maxMetricEntries caps how many matching entries a single metric query will load
// (the metric path has no result limit, unlike log queries) — a coarse OOM guard.
var maxMetricEntries = 20_000_000

type annotatedEntry struct {
	ts  int64
	val float64 // unwrap value (0 for count/rate)
}

type streamValues struct {
	labels  map[string]string
	vals    []float64
	present []bool
}

type streamGroup struct {
	labels  map[string]string
	entries []annotatedEntry
}

// ExecuteMetric evaluates a metric query into a matrix.
func (e *Engine) ExecuteMetric(q MetricQuery) ([]MatrixSeries, error) {
	if err := validatePreds(q.Preds); err != nil {
		return nil, err
	}
	step := q.Step
	if step <= 0 {
		step = defaultStep(q.Start, q.End)
	}
	rng := q.Range
	if rng <= 0 {
		rng = step
	}
	if q.End < q.Start {
		return nil, nil
	}
	// Reject a resolution that would allocate an unbounded number of steps (DoS guard). The
	// count is taken in uint64: End-Start overflows int64 for bounds far enough apart (e.g.
	// MinInt64..MaxInt64), and the wrapped value slipped past this guard.
	n := (uint64(q.End)-uint64(q.Start))/uint64(step) + 1
	if n > maxMetricSteps {
		return nil, fmt.Errorf("query resolution too fine: %d steps exceeds the max of %d — widen the step or narrow the range", n, maxMetricSteps)
	}

	// Gather entries that could fall in any window ([Start-Range, End]) PER SHARD, so each
	// entry is labelled/unwrapped with its own shard's resolver (service names resolve per
	// shard) before being merged into cross-shard streams.
	acc := &metricCollector{e: e, unwrap: q.Unwrap, unwrapValue: q.UnwrapValue, seriesLabels: q.SeriesLabels, streams: map[string]*streamGroup{}}
	for _, sh := range e.shards {
		if err := e.collectShardInto(acc, sh, q.Preds, windowStart(q.Start, rng), q.End, false, Backward); err != nil {
			return nil, err
		}
		if acc.done() {
			return nil, fmt.Errorf("metric query matched too many entries (more than %d) — narrow the time range or add filters", maxMetricEntries)
		}
	}
	streams := acc.streams

	steps := stepsBetween(q.Start, step, int(n))
	rangeSecs := float64(rng) / 1e9

	var perStream []streamValues
	for _, s := range streams {
		sort.Slice(s.entries, func(i, j int) bool { return s.entries[i].ts < s.entries[j].ts })
		sv := streamValues{labels: s.labels, vals: make([]float64, len(steps)), present: make([]bool, len(steps))}
		for i, t := range steps {
			window := entriesInWindow(s.entries, windowStart(t, rng), t)
			sv.vals[i], sv.present[i] = rangeAggregate(window, q.RangeOp, rangeSecs)
		}
		perStream = append(perStream, sv)
	}

	var out []MatrixSeries
	if q.VectorOp == VecNone {
		for _, sv := range perStream {
			if pts := points(steps, sv.vals, sv.present); len(pts) > 0 {
				out = append(out, MatrixSeries{Labels: sv.labels, Points: pts})
			}
		}
	} else {
		out = vectorAggregate(perStream, steps, q)
	}
	sort.Slice(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return out, nil
}

// metricCollector is the metric path's collector. It annotates each matching record as it
// arrives — the stream it groups under and, for an unwrap, its value — and keeps only that:
// 16 bytes per entry rather than the whole record. Once more than maxMetricEntries records
// have matched it reports done, which stops the scan there. The cap used to be checked after
// a shard's every matching record had been collected as a full LogEntry, so on a
// single-shard daemon it bounded nothing.
type metricCollector struct {
	e            *Engine
	unwrap       string
	unwrapValue  func(model.LogEntry, Resolver) (float64, bool)   // MetricQuery.UnwrapValue
	seriesLabels func(model.LogEntry, Resolver) map[string]string // MetricQuery.SeriesLabels
	streams      map[string]*streamGroup
	matched      int
}

func (c *metricCollector) add(en model.LogEntry, r *engineResolver) {
	if c.matched++; c.done() {
		return
	}
	var val float64
	if c.unwrap != "" {
		var v float64
		var ok bool
		if c.unwrapValue != nil {
			v, ok = c.unwrapValue(en, r)
		} else {
			v, ok = c.e.unwrapValue(en, r, c.unwrap)
		}
		if !ok {
			return // non-unwrappable entries are dropped (Loki)
		}
		val = v
	}
	var labels map[string]string
	if c.seriesLabels != nil {
		labels = c.seriesLabels(en, r)
	} else {
		labels = r.AllLabels(en)
	}
	key := labelKey(labels)
	s := c.streams[key]
	if s == nil {
		s = &streamGroup{labels: labels}
		c.streams[key] = s
	}
	s.entries = append(s.entries, annotatedEntry{ts: en.TS.UnixNano(), val: val})
}

func (c *metricCollector) cutoff() (int64, bool)     { return 0, false } // every entry counts
func (c *metricCollector) done() bool                { return c.matched > maxMetricEntries }
func (c *metricCollector) results() []model.LogEntry { return nil }

// entriesInWindow returns the annotations with ts in (lo, hi]. entries are sorted by ts.
func entriesInWindow(entries []annotatedEntry, lo, hi int64) []annotatedEntry {
	start := sort.Search(len(entries), func(i int) bool { return entries[i].ts > lo })
	end := sort.Search(len(entries), func(i int) bool { return entries[i].ts > hi })
	return entries[start:end]
}

// rangeAggregate reduces a window of entries; ok=false means "no sample at this step"
// (an empty window yields a gap, matching Prometheus/Loki).
func rangeAggregate(window []annotatedEntry, op RangeOp, rangeSecs float64) (float64, bool) {
	if len(window) == 0 {
		return 0, false
	}
	switch op {
	case RangeCount:
		return float64(len(window)), true
	case RangeRate:
		if rangeSecs <= 0 {
			return 0, false
		}
		return float64(len(window)) / rangeSecs, true
	case RangeRateSum:
		if rangeSecs <= 0 {
			return 0, false
		}
		var s float64
		for _, e := range window {
			s += e.val
		}
		return s / rangeSecs, true
	case RangeSum:
		var s float64
		for _, e := range window {
			s += e.val
		}
		return s, true
	case RangeAvg:
		var s float64
		for _, e := range window {
			s += e.val
		}
		return s / float64(len(window)), true
	case RangeMax:
		m := window[0].val
		for _, e := range window[1:] {
			if e.val > m {
				m = e.val
			}
		}
		return m, true
	case RangeMin:
		m := window[0].val
		for _, e := range window[1:] {
			if e.val < m {
				m = e.val
			}
		}
		return m, true
	default:
		return 0, false
	}
}

// vectorAggregate reduces per-stream series across streams, grouped by the query's
// by/without labels, at each step.
func vectorAggregate(perStream []streamValues, steps []int64, q MetricQuery) []MatrixSeries {
	type acc struct {
		labels map[string]string
		sum    []float64
		count  []int
		max    []float64
		min    []float64
	}
	groups := map[string]*acc{}
	for _, sv := range perStream {
		glabels := reduceLabels(sv.labels, q.Grouping, q.Without)
		key := labelKey(glabels)
		g := groups[key]
		if g == nil {
			g = &acc{labels: glabels, sum: make([]float64, len(steps)), count: make([]int, len(steps)), max: make([]float64, len(steps)), min: make([]float64, len(steps))}
			groups[key] = g
		}
		for i := range steps {
			if !sv.present[i] {
				continue
			}
			v := sv.vals[i]
			if g.count[i] == 0 || v > g.max[i] {
				g.max[i] = v
			}
			if g.count[i] == 0 || v < g.min[i] {
				g.min[i] = v
			}
			g.sum[i] += v
			g.count[i]++
		}
	}
	var out []MatrixSeries
	for _, g := range groups {
		vals := make([]float64, len(steps))
		present := make([]bool, len(steps))
		for i := range steps {
			if g.count[i] == 0 {
				continue
			}
			present[i] = true
			switch q.VectorOp {
			case VecSum:
				vals[i] = g.sum[i]
			case VecAvg:
				vals[i] = g.sum[i] / float64(g.count[i])
			case VecMax:
				vals[i] = g.max[i]
			case VecMin:
				vals[i] = g.min[i]
			case VecCount:
				vals[i] = float64(g.count[i])
			}
		}
		if pts := points(steps, vals, present); len(pts) > 0 {
			out = append(out, MatrixSeries{Labels: g.labels, Points: pts})
		}
	}
	return out
}

// reduceLabels applies by(grouping)/without(grouping) to a label set.
func reduceLabels(labels map[string]string, grouping []string, without bool) map[string]string {
	out := map[string]string{}
	set := map[string]struct{}{}
	for _, g := range grouping {
		set[g] = struct{}{}
	}
	for k, v := range labels {
		_, in := set[k]
		if without {
			if !in {
				out[k] = v
			}
		} else {
			if in {
				out[k] = v
			}
		}
	}
	return out
}

// defaultStep is the resolution of a query that names none: Loki's default (itself
// Prometheus'), about 250 points across the range in whole seconds, and at least 1s. A fixed
// 15s answered a one-week query with the 11,000-step error and a one-minute query with four
// points.
func defaultStep(start, end int64) int64 {
	secs := int64(1)
	if end > start {
		secs = max(secs, int64((uint64(end)-uint64(start))/1e9/250))
	}
	return secs * 1e9
}

// stepsBetween returns the n evaluation times start, start+step, …. It counts by index
// rather than advancing a cursor until it passes the end: a cursor near MaxInt64 wraps
// negative, stays <= end, and the loop never stops. Every value is <= End by construction
// (n was derived from End), so the uint64 arithmetic yields the exact int64 result even
// when the intermediate product would overflow int64.
func stepsBetween(start, step int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(uint64(start) + uint64(i)*uint64(step))
	}
	return out
}

// windowStart is t-rng, saturated at MinInt64 so a window ending near the bottom of the
// range cannot wrap to a huge positive start (which would silently empty it).
func windowStart(t, rng int64) int64 {
	if lo := t - rng; lo <= t {
		return lo
	}
	return math.MinInt64
}

func points(steps []int64, vals []float64, present []bool) []MetricPoint {
	var pts []MetricPoint
	for i, t := range steps {
		if present[i] {
			pts = append(pts, MetricPoint{TS: t, Value: vals[i]})
		}
	}
	return pts
}

// labelKey is a stable, injective key for a label map (for grouping/deduping/sorting).
func labelKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, strconv.Itoa(len(k))...)
		b = append(b, ':')
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, strconv.Itoa(len(m[k]))...)
		b = append(b, ':')
		b = append(b, m[k]...)
	}
	return string(b)
}

// unwrapValue extracts a numeric value to unwrap: a template field's typed value, else a
// numeric label value. ok=false if the field is absent or non-numeric (Loki drops it).
func (e *Engine) unwrapValue(en model.LogEntry, r Resolver, field string) (float64, bool) {
	if vals := r.FieldValues(en, field); len(vals) > 0 {
		return valueToFloat(vals[0])
	}
	if v, ok := r.Label(en, field); ok {
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	return 0, false
}

func valueToFloat(v index.Value) (float64, bool) {
	switch v.Kind {
	case index.KindInt:
		return float64(v.Int), true
	case index.KindFloat:
		return v.Float, true
	default:
		return 0, false // str/uuid are not numeric
	}
}
