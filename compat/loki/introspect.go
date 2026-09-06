package loki

import (
	"net/http"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/index"
)

// This file is logd's own introspection surface. It deliberately lives under /logd/api/v1/
// rather than /loki/api/v1/ for two reasons: Grafana must never see a route Loki doesn't
// have (it probes the Loki namespace to feature-detect), and a future Loki release can
// then never collide with one of these names.
//
// The three endpoints here expose engine capabilities that already existed and had no
// caller outside tests: query.Engine.Explain, query.Engine.ExecuteScan, and
// extract.Engine's per-template counters.

// schemaField is one declared, queryable typed field. Unlike Loki's detected-fields —
// which are guessed from a sample of recent lines — these are *declared* in config and
// backed by a sorted index, so the kind is authoritative rather than inferred.
type schemaField struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type indexStatsResponse struct {
	Templates []templateStat `json:"templates"`
	Literals  []string       `json:"literals"`
	Schema    []schemaField  `json:"schema"`
	Labels    []string       `json:"labels"`
}

// templateStat mirrors extract.TemplateStat plus the derived rates, so a caller reading
// the JSON doesn't have to recompute the only two numbers anyone actually wants.
type templateStat struct {
	Name       string   `json:"name"`
	Pattern    string   `json:"pattern"`
	Fields     []string `json:"fields"`
	Candidates int64    `json:"candidates"`
	Matches    int64    `json:"matches"`
	Failures   int64    `json:"failures"`
	// MatchRate is Matches/Candidates: of the lines where the anchor appeared, how many
	// did the whole pattern actually align on. A low value means the pattern is wrong for
	// this data even though its anchor is common.
	MatchRate float64 `json:"match_rate"`
}

func kindName(k index.ValueKind) string {
	switch k {
	case index.KindInt:
		return "int"
	case index.KindFloat:
		return "float"
	case index.KindStr:
		return "str"
	case index.KindUUID:
		return "uuid"
	default:
		return "unknown"
	}
}

// handleIndexStats reports what this daemon is configured to extract and how those
// templates are faring against live traffic. It is the feedback signal for tuning the
// index list: without it, choosing templates is guesswork.
func (s *Server) handleIndexStats(w http.ResponseWriter, _ *http.Request) {
	resp := indexStatsResponse{
		Templates: []templateStat{},
		Literals:  []string{},
		Schema:    []schemaField{},
		Labels:    s.qe.Labels(),
	}
	if s.ex != nil {
		for _, st := range s.ex.Stats() {
			ts := templateStat{
				Name:       st.Name,
				Pattern:    st.Pattern,
				Fields:     st.Fields,
				Candidates: st.Candidates,
				Matches:    st.Matches,
				Failures:   st.Failures,
			}
			if st.Candidates > 0 {
				ts.MatchRate = float64(st.Matches) / float64(st.Candidates)
			}
			if ts.Fields == nil {
				ts.Fields = []string{}
			}
			resp.Templates = append(resp.Templates, ts)
		}
		resp.Literals = s.ex.Literals()
		for _, f := range s.ex.IndexedFields() {
			resp.Schema = append(resp.Schema, schemaField{Name: f.Name, Kind: kindName(f.Kind)})
		}
	}
	if resp.Labels == nil {
		resp.Labels = []string{}
	}
	writeJSON(w, resp)
}

type explainResponse struct {
	Query    string             `json:"query"`
	Segments []explainSegment   `json:"segments"`
	Summary  explainSummaryJSON `json:"summary"`
}

type explainSegment struct {
	SegmentID  string `json:"segment_id"`
	Mode       string `json:"mode"`
	Candidates int    `json:"candidates"`
}

type explainSummaryJSON struct {
	SegmentsTotal   int `json:"segments_total"`
	SegmentsIndexed int `json:"segments_indexed"`
	SegmentsScanned int `json:"segments_scanned"`
	Candidates      int `json:"candidates"`
}

// handleExplain reports, per time-pruned segment, whether this query would be answered
// from the typed/label index or by scanning. This is what makes the differentiator
// observable instead of merely fast: it distinguishes "the index worked" from "the data
// happened to be small".
//
// Note it re-runs planning rather than reporting what an execution did, so it costs a
// second planning pass. That is acceptable for a debug route and is why it is not on the
// hot path.
func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	ast, err := logql.ParseExpr(r.FormValue("query"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	lq, ok := ast.(*logql.Query)
	if !ok {
		s.writeError(w, http.StatusBadRequest, "explain accepts a log query, not a metric query")
		return
	}
	q, err := s.logQuery(r, lq)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := explainResponse{Query: r.FormValue("query"), Segments: []explainSegment{}}
	for _, p := range s.qe.Explain(q) {
		resp.Segments = append(resp.Segments, explainSegment{
			SegmentID:  p.SegmentID,
			Mode:       p.Mode,
			Candidates: p.Candidates,
		})
		resp.Summary.SegmentsTotal++
		resp.Summary.Candidates += p.Candidates
		if p.Mode == "index" {
			resp.Summary.SegmentsIndexed++
		} else {
			resp.Summary.SegmentsScanned++
		}
	}
	writeJSON(w, resp)
}

// handleQueryScan answers exactly like /loki/api/v1/query_range but forces every segment
// down the scan path. Same binary, same data, same page cache — the only variable is
// whether the index is consulted.
//
// This is the fairest benchmark logd can offer: an index-vs-scan comparison against
// another product invites "you misconfigured it", but a comparison against itself does
// not. It is also a live assertion of the engine's core contract, since Execute and
// ExecuteScan must return identical results.
func (s *Server) handleQueryScan(w http.ResponseWriter, r *http.Request) {
	s.queryRangeWith(w, r, s.qe.ExecuteScan)
}
