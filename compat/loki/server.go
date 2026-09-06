// Package loki is the Grafana-facing Loki-compatible HTTP surface: it accepts Loki push
// requests into native ingestion, translates LogQL queries into the native query engine
// (so template-field filters transparently push down), and encodes results in the JSON
// shapes Grafana expects. Core stays protocol-agnostic; all Loki assumptions live here.
package loki

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/compat/loki/push"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
)

// Server wires the Loki HTTP endpoints to the native ingest + query engines.
type Server struct {
	ig          *ingest.Ingester
	qe          *query.Engine
	ex          *extract.Engine // for LogQL translation (template-field detection); may be nil
	now         func() time.Time
	multitenant bool // when true, X-Scope-OrgID tags data on push and scopes reads (§12)
}

// NewServer builds a Loki-compat server over an ingester, query engine, and the extract
// engine used to translate LogQL typed-field filters.
func NewServer(ig *ingest.Ingester, qe *query.Engine, ex *extract.Engine) *Server {
	return &Server{ig: ig, qe: qe, ex: ex, now: time.Now}
}

// EnableMultitenancy turns on tenant isolation: pushes are tagged with the requester's
// X-Scope-OrgID (default "default") and reads are scoped to it. The engine and ingester
// MUST include model.TenantLabel in their label allowlist so the scoping predicate pushes
// down; the daemon wires that up.
func (s *Server) EnableMultitenancy() { s.multitenant = true }

// tenant returns the requester's tenant id, or "" when multitenancy is off.
func (s *Server) tenant(r *http.Request) string {
	if !s.multitenant {
		return ""
	}
	if id := strings.TrimSpace(r.Header.Get("X-Scope-OrgID")); id != "" {
		return id
	}
	return "default"
}

// scopedPreds prepends the tenant-isolation predicate to a query's predicates (a no-op
// when multitenancy is off).
func (s *Server) scopedPreds(r *http.Request, preds []query.Predicate) []query.Predicate {
	if t := s.tenant(r); t != "" {
		return append([]query.Predicate{query.LabelEqual{Key: model.TenantLabel, Value: t}}, preds...)
	}
	return preds
}

// Handler returns the HTTP mux with all Loki routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /loki/api/v1/push", s.handlePush)
	mux.HandleFunc("/loki/api/v1/query_range", s.handleQueryRange)
	mux.HandleFunc("/loki/api/v1/query", s.handleQuery)
	mux.HandleFunc("/loki/api/v1/labels", s.handleLabels)
	mux.HandleFunc("/loki/api/v1/label/{name}/values", s.handleLabelValues)
	mux.HandleFunc("/loki/api/v1/series", s.handleSeries)
	mux.HandleFunc("/loki/api/v1/tail", s.handleTail)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/loki/api/v1/ready", s.handleReady)

	// Grafana's capability-discovery calls. Without buildinfo Grafana feature-gates the
	// datasource down and the user sees a degraded UI with no visible error. See
	// discovery.go.
	mux.HandleFunc("/loki/api/v1/status/buildinfo", s.handleBuildInfo)
	mux.HandleFunc("/loki/api/v1/index/stats", s.handleIndexStatsLoki)
	mux.HandleFunc("/loki/api/v1/format_query", s.handleFormatQuery)

	// logd's own introspection surface. Kept out of the /loki/ namespace so Grafana's
	// feature detection never sees a route real Loki lacks. See introspect.go.
	mux.HandleFunc("/logd/api/v1/index_stats", s.handleIndexStats)
	mux.HandleFunc("/logd/api/v1/explain", s.handleExplain)
	mux.HandleFunc("/logd/api/v1/query_scan", s.handleQueryScan)
	return mux
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "ready")
}

// maxPushBody caps the (compressed) request body a push may carry, so a client can't
// stream unbounded data into memory. The decoder additionally caps the decompressed size.
const maxPushBody = 32 << 20 // 32 MB

func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPushBody))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	entries, err := push.Decode(r.Header.Get("Content-Type"), body)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if t := s.tenant(r); t != "" {
		for i := range entries {
			entries[i].Extra = model.WithExtraLabel(entries[i].Extra, model.TenantLabel, t)
		}
	}
	// Wait for queue space rather than dropping. A push is one indivisible batch to the
	// client: there is no way in the Loki protocol to report "I accepted the first 412 of
	// your 1000 entries", so failing partway through leaves the client to retry the whole
	// batch and duplicate everything already stored. Expressing backpressure as latency
	// (which is what Loki and VictoriaLogs do) keeps the request effectively atomic.
	for i := range entries {
		if err := s.ig.IngestCtx(r.Context(), entries[i]); err != nil {
			s.writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	s.queryRangeWith(w, r, s.qe.Execute)
}

// queryRangeWith serves a range query, executing the log branch through exec. The
// executor is a parameter so /loki/api/v1/query_range (index pushdown) and
// /logd/api/v1/query_scan (forced scan) share one code path: the benchmark compares the
// two, so any drift between them would silently corrupt the comparison.
func (s *Server) queryRangeWith(w http.ResponseWriter, r *http.Request, exec func(query.Query) ([]model.LogEntry, error)) {
	ast, err := logql.ParseExpr(r.FormValue("query"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch e := ast.(type) {
	case *logql.Query:
		q, err := s.logQuery(r, e)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		results, err := exec(q)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, newQueryRangeResponse(s.streamsFor(results)))
	case *logql.MetricExpr:
		mq, err := s.metricQuery(r, e)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		series, err := s.qe.ExecuteMetric(mq)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, newMatrixResponse(s.hideTenant(series)))
	default:
		// Without this, an expression type the switch doesn't know returns 200 with an
		// empty body — which reads to a client as "no data" rather than "unsupported".
		s.writeError(w, http.StatusBadRequest, "unsupported query expression")
	}
}

// handleQuery serves instant queries. Grafana's datasource health check
// (vector(1)+vector(1)) gets a scalar vector stub; a selector query returns streams; a
// metric query is evaluated at the instant and returned as a vector.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	expr := strings.TrimSpace(r.FormValue("query"))
	ast, err := logql.ParseExpr(expr)
	if err != nil {
		// Grafana's datasource health check sends `vector(1)+vector(1)`, which isn't a
		// valid log/metric query. Only stub when the parse fails AND it looks like the
		// health check — a real query that merely contains "vector(" still parses and is
		// evaluated normally.
		if strings.Contains(expr, "vector(") {
			writeJSON(w, newScalarVector(s.now(), "1"))
			return
		}
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch e := ast.(type) {
	case *logql.Query:
		q, err := s.logQuery(r, e)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		results, err := s.qe.Execute(q)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, newQueryRangeResponse(s.streamsFor(results)))
	case *logql.MetricExpr:
		mq, err := s.metricQuery(r, e)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		at := s.parseTime(r.FormValue("time"), s.now())
		mq.Start, mq.End = at, at // instant: single step
		series, err := s.qe.ExecuteMetric(mq)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, newVectorFromMatrix(s.hideTenant(series)))
	}
}

// tailBuffer is the per-tailer channel depth; a client slower than this drops entries
// (the writer never blocks) rather than stalling ingestion.
const tailBuffer = 256

// handleTail streams matching entries live over a WebSocket (Loki's /tail). Only log
// queries are tailable (Loki has no metric tail).
func (s *Server) handleTail(w http.ResponseWriter, r *http.Request) {
	ast, err := logql.ParseExpr(r.FormValue("query"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	logAST, ok := ast.(*logql.Query)
	if !ok {
		s.writeError(w, http.StatusBadRequest, "tail supports log queries only")
		return
	}
	preds, err := logql.Translate(logAST, s.ex)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	preds = s.scopedPreds(r, preds)

	ws, err := wsUpgrade(w, r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer ws.Close()

	ch, cancel := s.qe.Tail(preds, tailBuffer)
	defer cancel()

	done := make(chan struct{})
	go ws.readLoop(done) // notices client close / answers pings

	keepalive := time.NewTicker(10 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-done:
			return // client went away
		case <-keepalive.C:
			if err := ws.writePing(); err != nil {
				return
			}
		case e, ok := <-ch:
			if !ok {
				return // writer shut down
			}
			msg, err := json.Marshal(tailResponse{Streams: groupStreams(drainTail(ch, e)), DroppedEntries: []any{}})
			if err != nil {
				return
			}
			if err := ws.writeText(msg); err != nil {
				return
			}
		}
	}
}

// drainTail coalesces the first entry with any others already buffered (non-blocking) into
// one batch, so a burst becomes a single WebSocket message.
func drainTail(ch <-chan model.LogEntry, first model.LogEntry) []model.LogEntry {
	batch := []model.LogEntry{first}
	for len(batch) < 100 {
		select {
		case e, ok := <-ch:
			if !ok {
				return batch
			}
			batch = append(batch, e)
		default:
			return batch
		}
	}
	return batch
}

func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	if t := s.tenant(r); t != "" {
		seen := map[string]struct{}{}
		for _, m := range scopeSeries(s.qe.Series(), t) {
			for k := range m {
				seen[k] = struct{}{}
			}
		}
		writeJSON(w, labelResponse(sortedSetKeys(seen)))
		return
	}
	writeJSON(w, labelResponse(s.qe.Labels()))
}

func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if t := s.tenant(r); t != "" {
		if name == model.TenantLabel { // don't expose the internal tenant label
			writeJSON(w, labelResponse(nil))
			return
		}
		seen := map[string]struct{}{}
		for _, m := range scopeSeries(s.qe.Series(), t) {
			if v, ok := m[name]; ok {
				seen[v] = struct{}{}
			}
		}
		writeJSON(w, labelResponse(sortedSetKeys(seen)))
		return
	}
	writeJSON(w, labelResponse(s.qe.LabelValues(name)))
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	series := s.qe.Series()
	if t := s.tenant(r); t != "" {
		series = scopeSeries(series, t)
	}
	writeJSON(w, map[string]any{"status": "success", "data": series})
}

// streamsFor groups results into Loki streams, stripping the internal tenant label from
// the response so it never surfaces in query stream labels (Grafana would show it).
func (s *Server) streamsFor(results []model.LogEntry) []streamResult {
	streams := groupStreams(results)
	if s.multitenant {
		for i := range streams {
			delete(streams[i].Stream, model.TenantLabel)
		}
	}
	return streams
}

// hideTenant removes the internal tenant label from metric series labels.
func (s *Server) hideTenant(series []query.MatrixSeries) []query.MatrixSeries {
	if s.multitenant {
		for _, ser := range series {
			delete(ser.Labels, model.TenantLabel)
		}
	}
	return series
}

// scopeSeries keeps only the streams belonging to tenant and strips the internal tenant
// label from each, so a tenant sees only its own series without the machinery.
func scopeSeries(series []map[string]string, tenant string) []map[string]string {
	out := make([]map[string]string, 0, len(series))
	for _, m := range series {
		if m[model.TenantLabel] != tenant {
			continue
		}
		reduced := make(map[string]string, len(m))
		for k, v := range m {
			if k != model.TenantLabel {
				reduced[k] = v
			}
		}
		out = append(out, reduced)
	}
	return out
}

func sortedSetKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// logQuery builds a native log Query from a parsed selector + the HTTP time/limit params.
func (s *Server) logQuery(r *http.Request, ast *logql.Query) (query.Query, error) {
	preds, err := logql.Translate(ast, s.ex)
	if err != nil {
		return query.Query{}, err
	}
	preds = s.scopedPreds(r, preds)
	start, end := s.timeRange(r)
	limit := 100
	if l, err := strconv.Atoi(r.FormValue("limit")); err == nil && l > 0 {
		limit = l
	}
	dir := query.Backward
	if r.FormValue("direction") == "forward" {
		dir = query.Forward
	}
	return query.Query{Start: start, End: end, Preds: preds, Limit: limit, Direction: dir}, nil
}

// metricQuery builds a native MetricQuery from a parsed metric expr + the HTTP time/step.
func (s *Server) metricQuery(r *http.Request, ast *logql.MetricExpr) (query.MetricQuery, error) {
	mq, err := logql.TranslateMetric(ast, s.ex)
	if err != nil {
		return query.MetricQuery{}, err
	}
	mq.Preds = s.scopedPreds(r, mq.Preds)
	mq.Start, mq.End = s.timeRange(r)
	mq.Step = s.parseStep(r)
	return mq, nil
}

// timeRange resolves [start, end] from start/end (range) or time (instant), defaulting to
// the last hour.
func (s *Server) timeRange(r *http.Request) (start, end int64) {
	endStr := r.FormValue("end")
	if endStr == "" {
		endStr = r.FormValue("time")
	}
	end = s.parseTime(endStr, s.now())
	start = s.parseTime(r.FormValue("start"), time.Unix(0, end).Add(-time.Hour))
	return start, end
}

// parseStep reads the query resolution: Grafana sends `step` as float seconds; a Go
// duration string is also accepted. 0 lets the metric evaluator pick a default.
func (s *Server) parseStep(r *http.Request) int64 {
	v := r.FormValue("step")
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		return int64(secs * 1e9)
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d.Nanoseconds()
	}
	return 0
}

// parseTime accepts a unix-nanosecond string (what Grafana sends) or RFC3339, else def.
func (s *Server) parseTime(v string, def time.Time) int64 {
	if v == "" {
		return def.UnixNano()
	}
	if nano, err := strconv.ParseInt(v, 10, 64); err == nil {
		return nano
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UnixNano()
	}
	return def.UnixNano()
}

// groupStreams groups result entries into Loki streams keyed by their label set (Extra
// labels plus the first-class level). Values are [unix-nano-string, line].
func groupStreams(entries []model.LogEntry) []streamResult {
	type group struct {
		labels map[string]string
		values [][2]string
	}
	order := []string{}
	groups := map[string]*group{}
	for _, e := range entries {
		// Use the record's own labels verbatim (from Extra) as the stream identity — do
		// NOT synthesize a "level" label, which would overwrite a pushed level value
		// (e.g. "warning"→"WARN") and split one logical stream by level.
		labels := model.ParseExtraLabels(e.Extra)
		if labels == nil {
			labels = map[string]string{}
		}
		key := canonicalKey(labels)
		g := groups[key]
		if g == nil {
			g = &group{labels: labels}
			groups[key] = g
			order = append(order, key)
		}
		g.values = append(g.values, [2]string{strconv.FormatInt(e.TS.UnixNano(), 10), e.Message})
	}
	out := make([]streamResult, 0, len(order))
	for _, k := range order {
		g := groups[k]
		out = append(out, streamResult{Stream: g.labels, Values: g.values})
	}
	return out
}

func canonicalKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(strconv.Itoa(len(k)))
		b.WriteByte(':')
		b.WriteString(k)
		b.WriteString(strconv.Itoa(len(m[k])))
		b.WriteByte(':')
		b.WriteString(m[k])
	}
	return b.String()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"status": "error", "error": msg})
}
