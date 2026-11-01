package loki

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/query"
)

// Query-parameter parsing, ported from Loki's pkg/loghttp/params.go so that a request means
// the same thing to logd as to Loki. In particular an unparseable value is a 400, never
// silently replaced by a default: an unparseable start used to mean "the last hour", and an
// answer to a question the client did not ask is worse than an error.

const (
	defaultQueryLimit = 100
	// maxQueryLimit plays the role of Loki's max_entries_limit_per_query (default 5,000 in
	// Loki). It is set high because the testbed's harness reads whole result sets — the
	// cross-engine benchmark asks for 200,000 lines and compare-self.sh for 1,000,000 —
	// while still bounding what one request can make the top-K accumulator hold.
	maxQueryLimit = 1_000_000
	defaultSince  = time.Hour
)

// The range of timestamps a query may name: the Unix epoch up to the start of 2262, just
// short of where int64 nanoseconds end (2262-04-11). Anything outside it is not a time any
// real query means, and inside the engine it is a time that arithmetic can overflow on:
// time=MaxInt64 once made the metric step cursor wrap and a single GET exhaust memory. The
// engine now guards its own arithmetic too; this answers such a request with a 400 instead
// of an empty result.
var (
	minQueryTime = time.Unix(0, 0)
	maxQueryTime = time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
)

// timeRange resolves [start, end] as Loki's determineBounds does: end is `end` (or the
// instant's `time`), defaulting to now; start defaults to `since` (default 1h) before end,
// or before now when end is in the future.
func (s *Server) timeRange(r *http.Request) (start, end int64, err error) {
	now := s.now()
	endName, endStr := "end", r.FormValue("end")
	if endStr == "" {
		endName, endStr = "time", r.FormValue("time")
	}
	endT, err := parseTimestamp(endName, endStr, now)
	if err != nil {
		return 0, 0, err
	}
	since := defaultSince
	if v := r.FormValue("since"); v != "" {
		if since, err = logql.ParseDuration(v); err != nil {
			return 0, 0, fmt.Errorf("could not parse 'since' parameter %q: %v", v, err)
		}
	}
	endOrNow := endT
	if endT.After(now) {
		endOrNow = now
	}
	def := endOrNow.Add(-since)
	if def.Before(minQueryTime) { // a default is not checked, so keep it representable
		def = minQueryTime
	}
	startT, err := parseTimestamp("start", r.FormValue("start"), def)
	if err != nil {
		return 0, 0, err
	}
	return startT.UnixNano(), endT.UnixNano(), nil
}

// parseTimestamp reads a timestamp parameter as Loki's parseTimestamp does: a value with a
// '.' that parses as a float is seconds (fraction rounded to the millisecond); an integer
// is seconds when it has at most 10 digits and nanoseconds otherwise; anything else must
// be RFC3339. An empty value is def. A given value outside [minQueryTime, maxQueryTime] is
// an error.
func parseTimestamp(name, v string, def time.Time) (time.Time, error) {
	if v == "" {
		return def, nil
	}
	t, err := lokiTimestamp(v)
	if err != nil && !errors.Is(err, errOutOfRange) {
		return time.Time{}, fmt.Errorf("could not parse '%s' parameter %q: want unix seconds or nanoseconds, float seconds, or RFC3339", name, v)
	}
	if err != nil || t.Before(minQueryTime) || t.After(maxQueryTime) {
		return time.Time{}, fmt.Errorf("'%s' parameter %q is outside the supported range (%s to %s)", name, v,
			minQueryTime.UTC().Format(time.RFC3339), maxQueryTime.Format(time.RFC3339))
	}
	return t, nil
}

var errOutOfRange = errors.New("out of range")

func lokiTimestamp(v string) (time.Time, error) {
	if strings.Contains(v, ".") {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			// Range-check before converting: a float beyond int64 (or NaN) converts to an
			// arbitrary value.
			if !(f >= float64(minQueryTime.Unix()) && f <= float64(maxQueryTime.Unix())) {
				return time.Time{}, errOutOfRange
			}
			sec, frac := math.Modf(f)
			frac = math.Round(frac*1000) / 1000
			return time.Unix(int64(sec), int64(frac*float64(time.Second))), nil
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Parse(time.RFC3339Nano, v)
	}
	if len(v) <= 10 {
		return time.Unix(n, 0), nil
	}
	return time.Unix(0, n), nil
}

// parseLimit reads `limit` as Loki does: default 100, positive, and here also at most
// maxQueryLimit.
func parseLimit(v string) (int, error) {
	if v == "" {
		return defaultQueryLimit, nil
	}
	l, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse 'limit' parameter %q", v)
	}
	if l <= 0 {
		return 0, errors.New("limit must be a positive value")
	}
	if l > maxQueryLimit {
		return 0, fmt.Errorf("max entries limit per query exceeded, limit > max_entries_limit_per_query (%d > %d)", l, maxQueryLimit)
	}
	return int(l), nil
}

// parseDirection reads `direction`, case-insensitively as Loki does (it upper-cases the
// value before looking it up).
func parseDirection(v string) (query.Direction, error) {
	switch strings.ToLower(v) {
	case "", "backward":
		return query.Backward, nil
	case "forward":
		return query.Forward, nil
	default:
		return 0, fmt.Errorf("invalid direction '%s'", v)
	}
}

// parseStep reads the query resolution as Loki's parseSecondsOrDuration does: float
// seconds (what Grafana sends) or a duration such as "15s". An absent step is 0, which lets
// the metric evaluator choose Loki's default resolution. A zero or negative step is an
// error, as in Loki.
func parseStep(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	var step int64
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		ns := secs * float64(time.Second)
		if math.IsNaN(ns) || ns >= math.MaxInt64 || ns <= math.MinInt64 {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", v)
		}
		step = int64(ns)
	} else if d, err := logql.ParseDuration(v); err == nil {
		step = int64(d)
	} else {
		return 0, fmt.Errorf("cannot parse %q to a valid duration", v)
	}
	if step <= 0 {
		return 0, errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
	}
	return step, nil
}
