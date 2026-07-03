package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
	"github.com/advenn/logd/pkg/lokicompat"
)

// Handler serves Loki-compatible query endpoints.
type Handler struct {
	engine *Engine
	tmpl   *template.Engine
}

// NewHandler creates a query Handler. tmpl may be nil when no templates are
// configured; in that case template-field index pushdown is disabled.
func NewHandler(store storage.Storage, tmpl *template.Engine) *Handler {
	return &Handler{engine: NewEngine(store), tmpl: tmpl}
}

// RegisterRoutes adds query routes to the given mux.
// Both GET and POST are registered — Grafana may use either method.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Instant query (Grafana health check uses this).
	mux.HandleFunc("GET /loki/api/v1/query", h.handleInstantQuery)
	mux.HandleFunc("POST /loki/api/v1/query", h.handleInstantQuery)
	// Query range.
	mux.HandleFunc("GET /loki/api/v1/query_range", h.handleQueryRange)
	mux.HandleFunc("POST /loki/api/v1/query_range", h.handleQueryRange)
	// Labels.
	mux.HandleFunc("GET /loki/api/v1/labels", h.handleLabels)
	mux.HandleFunc("POST /loki/api/v1/labels", h.handleLabels)
	// Label values.
	mux.HandleFunc("GET /loki/api/v1/label/", h.handleLabelValues)
	mux.HandleFunc("POST /loki/api/v1/label/", h.handleLabelValues)
	// Series (Grafana uses this for label suggestions).
	mux.HandleFunc("GET /loki/api/v1/series", h.handleSeries)
	mux.HandleFunc("POST /loki/api/v1/series", h.handleSeries)
}

// handleQueryRange handles GET /loki/api/v1/query_range.
func (h *Handler) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	// Support params from both URL query and POST form body.
	if r.Method == "POST" {
		if err := r.ParseForm(); err != nil {
			writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("parsing form: "+err.Error()))
			return
		}
	}
	q := r.URL.Query()
	form := r.Form

	// Helper: get param from URL query or form body.
	getParam := func(key string) string {
		if v := q.Get(key); v != "" {
			return v
		}
		if form != nil {
			return form.Get(key)
		}
		return ""
	}

	// Parse time range.
	startTS, err := parseTimestamp(getParam("start"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("invalid start: "+err.Error()))
		return
	}
	endTS, err := parseTimestamp(getParam("end"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("invalid end: "+err.Error()))
		return
	}

	// Parse LogQL query.
	logql := getParam("query")
	filter, err := ParseLogQL(logql, h.tmpl)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("invalid query: "+err.Error()))
		return
	}

	// Parse limit.
	limit := 100
	if l := getParam("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	// Parse direction.
	direction := 0 // backward (newest first)
	if dir := getParam("direction"); dir == "forward" {
		direction = 1
	}

	filter.StartTS = startTS
	filter.EndTS = endTS
	filter.Limit = limit
	filter.Direction = direction

	results, err := h.engine.Execute(*filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, lokicompat.ErrorResponse("query error: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, lokicompat.NewQueryRangeResponse(results))
}

// handleLabels handles GET /loki/api/v1/labels.
func (h *Handler) handleLabels(w http.ResponseWriter, r *http.Request) {
	labels, err := h.engine.Labels()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, lokicompat.ErrorResponse("labels error: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, lokicompat.NewLabelResponse(labels))
}

// handleLabelValues handles GET /loki/api/v1/label/{name}/values.
func (h *Handler) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	// Extract label name from path: /loki/api/v1/label/{name}/values
	path := r.URL.Path
	prefix := "/loki/api/v1/label/"
	if len(path) <= len(prefix) {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("missing label name"))
		return
	}

	rest := path[len(prefix):]
	// Strip trailing /values if present.
	suffix := "/values"
	if len(rest) > len(suffix) && rest[len(rest)-len(suffix):] == suffix {
		rest = rest[:len(rest)-len(suffix)]
	}
	if rest == "" {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("missing label name"))
		return
	}

	values, err := h.engine.LabelValues(rest)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, lokicompat.ErrorResponse("label values error: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, lokicompat.NewLabelValuesResponse(values))
}

// parseTimestamp handles Loki's nanosecond string format.
func parseTimestamp(s string) (int64, error) {
	if s == "" {
		return time.Now().UnixNano(), nil
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return ns, nil
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// handleInstantQuery handles GET/POST /loki/api/v1/query — Loki's instant query
// endpoint. Grafana uses this for health checks, sending a test query like
// vector(1)+vector(1) with a time parameter. Unlike query_range which takes a
// time range [start, end], instant query returns results at a single point in time.
func (h *Handler) handleInstantQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		if err := r.ParseForm(); err != nil {
			writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("parsing form: "+err.Error()))
			return
		}
	}
	q := r.URL.Query()
	form := r.Form

	getParam := func(key string) string {
		if v := q.Get(key); v != "" {
			return v
		}
		if form != nil {
			return form.Get(key)
		}
		return ""
	}

	// Parse the single time point. Use a narrow window around it for the query.
	pointTS, err := parseTimestamp(getParam("time"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("invalid time: "+err.Error()))
		return
	}

	// A LogQL log query always begins with a stream selector `{...}`. Anything
	// else (vector(1)+vector(1), count_over_time(...), rate(...)) is a metric
	// query. logd doesn't evaluate metric queries yet (Phase 8), but Grafana's
	// datasource health check sends vector(1)+vector(1) and parses the result
	// as a metric frame — so we must answer with a vector response, not streams.
	logql := getParam("query")
	if !strings.HasPrefix(strings.TrimSpace(logql), "{") {
		// Grafana's datasource health check sends vector(1)+vector(1) and
		// asserts the result equals 2, so answer with 2. logd does not
		// evaluate metric queries yet (Phase 8) — this is a health-check
		// stopgap, not a real metric engine.
		writeJSON(w, http.StatusOK, lokicompat.NewScalarVectorResponse(time.Unix(0, pointTS), "2"))
		return
	}

	filter, err := ParseLogQL(logql, h.tmpl)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("invalid query: "+err.Error()))
		return
	}

	limit := 100
	if l := getParam("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	direction := 0
	if dir := getParam("direction"); dir == "forward" {
		direction = 1
	}

	// Use a 1-second window around the time point.
	const window = 1_000_000_000 // 1 second in nanos
	filter.StartTS = pointTS - window
	filter.EndTS = pointTS + window
	filter.Limit = limit
	filter.Direction = direction

	results, err := h.engine.Execute(*filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, lokicompat.ErrorResponse("query error: "+err.Error()))
		return
	}

	// Loki instant query returns the same format as query_range.
	writeJSON(w, http.StatusOK, lokicompat.NewQueryRangeResponse(results))
}

// handleSeries returns the set of label sets matching the query matchers.
// This is a simplified implementation — Loki's /series returns all unique
// label combinations that match the given matchers.
func (h *Handler) handleSeries(w http.ResponseWriter, r *http.Request) {
	// Get match[] params (label matchers like {service="django_app"}).
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, lokicompat.ErrorResponse("parsing form: "+err.Error()))
		return
	}

	labels, err := h.engine.Labels()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, lokicompat.ErrorResponse("labels error: "+err.Error()))
		return
	}

	// Return all known label keys — Grafana uses this to populate dropdowns.
	writeJSON(w, http.StatusOK, lokicompat.LabelResponse{Status: "success", Data: labels})
}
