package loki

import (
	"net/http"
	"runtime"

	"github.com/advenn/logd/compat/loki/logql"
)

// This file implements the Loki endpoints Grafana calls to *discover what a datasource can
// do*, as opposed to the endpoints that move log data. They matter more than their size
// suggests: Grafana feature-gates the whole datasource on /status/buildinfo, and a 404
// there silently downgrades the UI (no log-volume histogram, a reduced query builder)
// with no error the user can see.

// lokiCompatVersion is the Loki version logd reports to Grafana.
//
// This is a capability claim, not a vanity string. Grafana switches features on by
// comparing against it, so claiming a version whose endpoints logd does not implement is
// strictly worse than claiming a lower one: Grafana would call /detected_fields and
// /patterns for real and surface errors. Raise this only as the corresponding endpoints
// land, and re-verify against the pinned Grafana image when you do.
const lokiCompatVersion = "2.9.4"

type buildInfoResponse struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

func (s *Server) handleBuildInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, buildInfoResponse{
		Version:   lokiCompatVersion,
		Revision:  "logd",
		Branch:    "main",
		BuildUser: "logd",
		BuildDate: "1970-01-01T00:00:00Z",
		GoVersion: runtime.Version(),
	})
}

// indexStatsResponseLoki is Loki's /index/stats shape, which drives Explore's
// "this query will process approximately N bytes" banner before a query runs.
type indexStatsResponseLoki struct {
	Streams uint64 `json:"streams"`
	Chunks  uint64 `json:"chunks"`
	Entries uint64 `json:"entries"`
	Bytes   uint64 `json:"bytes"`
}

// handleIndexStatsLoki estimates how much data a query would touch. It is deliberately an
// ESTIMATE from the manifest and planner rather than a measurement: the point is to answer
// before the query runs, cheaply.
//
// It is also a demo beat in its own right. For a typed range like `| latency_ms > 200`,
// logd's planner knows the index will hand back only the matching offsets, so this reports
// kilobytes where a scan-based store must report the whole time range.
func (s *Server) handleIndexStatsLoki(w http.ResponseWriter, r *http.Request) {
	est := s.estimate(r)
	writeJSON(w, est)
}

func (s *Server) estimate(r *http.Request) indexStatsResponseLoki {
	var out indexStatsResponseLoki
	out.Streams = uint64(len(s.qe.Series()))

	expr := r.FormValue("query")
	if expr == "" {
		return out
	}
	ast, err := logql.ParseExpr(expr)
	if err != nil {
		return out
	}
	lq, ok := ast.(*logql.Query)
	if !ok {
		return out
	}
	q, err := s.logQuery(r, lq)
	if err != nil {
		return out
	}
	for _, p := range s.qe.Explain(q) {
		out.Chunks++
		out.Entries += uint64(p.Candidates)
	}
	// Loki reports bytes of log line content. We have no per-candidate size before
	// reading, so approximate with a nominal line width; the figure is an order-of-
	// magnitude hint for the UI banner, and labelling it as such beats a false precision.
	const nominalLineBytes = 120
	out.Bytes = out.Entries * nominalLineBytes
	return out
}

// handleFormatQuery echoes a query back in its canonical form, which is what Grafana's
// query-builder "prettify" button calls. logql.Query already has a String() that round
// trips, so this is a parse-and-reprint.
func (s *Server) handleFormatQuery(w http.ResponseWriter, r *http.Request) {
	expr := r.FormValue("query")
	ast, err := logql.ParseExpr(expr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Only log queries have a canonical printer today; a metric expression is echoed
	// unchanged rather than reformatted, which is honest and never lossy.
	out := expr
	if lq, ok := ast.(*logql.Query); ok {
		out = lq.String()
	}
	writeJSON(w, formatQueryResponse{Status: "success", Data: out})
}

type formatQueryResponse struct {
	Status string `json:"status"`
	Data   string `json:"data"`
}
