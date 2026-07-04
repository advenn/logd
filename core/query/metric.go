package query

import (
	"fmt"
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
	defaultStep = int64(15e9) // 15s
	// maxMetricSteps caps the resolution of a matrix (Loki's default is ~11000); a finer
	// step over a wide range is rejected rather than allocating per-step slices to OOM.
	maxMetricSteps = 11000
	// maxMetricEntries caps how many matching entries a single metric query will load
	// (the metric path has no result limit, unlike log queries) — a coarse OOM guard.
	maxMetricEntries = 20_000_000
)

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
	step := q.Step
	if step <= 0 {
		step = defaultStep
	}
	rng := q.Range
	if rng <= 0 {
		rng = step
	}
	if q.End < q.Start {
		return nil, nil
	}
	// Reject a resolution that would allocate an unbounded number of steps (DoS guard).
	if n := (q.End-q.Start)/step + 1; n > maxMetricSteps {
		return nil, fmt.Errorf("query resolution too fine: %d steps exceeds the max of %d — widen the step or narrow the range", n, maxMetricSteps)
	}

	// Gather entries that could fall in any window ([Start-Range, End]) PER SHARD, so each
	// entry is labelled/unwrapped with its own shard's resolver (service names resolve per
	// shard) before being merged into cross-shard streams.
	streams := map[string]*streamGroup{}
	total := 0
	for _, sh := range e.shards {
		r := e.resolverFor(sh)
		entries, err := e.collectShard(sh, q.Preds, q.Start-rng, q.End, false)
		if err != nil {
			return nil, err
		}
		total += len(entries)
		if total > maxMetricEntries {
			return nil, fmt.Errorf("metric query matched too many entries (%d, max %d) — narrow the time range or add filters", total, maxMetricEntries)
		}
		for _, en := range entries {
			var val float64
			if q.Unwrap != "" {
				v, ok := e.unwrapValue(en, r, q.Unwrap)
				if !ok {
					continue // non-unwrappable entries are dropped (Loki)
				}
				val = v
			}
			labels := r.AllLabels(en)
			key := labelKey(labels)
			s := streams[key]
			if s == nil {
				s = &streamGroup{labels: labels}
				streams[key] = s
			}
			s.entries = append(s.entries, annotatedEntry{ts: en.TS.UnixNano(), val: val})
		}
	}

	steps := stepsBetween(q.Start, q.End, step)
	rangeSecs := float64(rng) / 1e9

	var perStream []streamValues
	for _, s := range streams {
		sort.Slice(s.entries, func(i, j int) bool { return s.entries[i].ts < s.entries[j].ts })
		sv := streamValues{labels: s.labels, vals: make([]float64, len(steps)), present: make([]bool, len(steps))}
		for i, t := range steps {
			window := entriesInWindow(s.entries, t-rng, t)
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

func stepsBetween(start, end, step int64) []int64 {
	var out []int64
	for t := start; t <= end; t += step {
		out = append(out, t)
	}
	return out
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
