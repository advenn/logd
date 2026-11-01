package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/advenn/logd/testbed/internal/pgdata"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxLimit bounds a single response. The harness asks for 200,000 rows so the broadest
// query returns its whole answer; anything far beyond that is a typo, not a benchmark.
const maxLimit = 5_000_000

type server struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	streams *streamCache
}

func newServer(pool *pgxpool.Pool, log *slog.Logger) *server {
	return &server{pool: pool, log: log, streams: &streamCache{load: loadStreams(pool)}}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /loki/api/v1/query_range", s.queryRange)
	mux.HandleFunc("GET /ready", s.ready)
	return mux
}

// request is a fully validated query. Everything in it came out of a parser or a whitelist.
type request struct {
	dataset  pgdata.Dataset
	variant  pgdata.Variant
	pred     pgdata.Predicate
	start    int64 // inclusive, ns
	end      int64 // exclusive, ns
	limit    int
	backward bool
}

// parseRequest validates the query parameters. Unknown parameters are ignored, as Loki does,
// so a Grafana-style request with extra fields still works; every parameter the shim does
// use is checked strictly, because a value it misread would be timed as if it were right.
func parseRequest(q url.Values, now time.Time) (request, error) {
	var r request
	pred, err := pgdata.ParsePredicate(q.Get("query"))
	if err != nil {
		return r, err
	}
	r.pred = pred
	r.dataset, _ = pgdata.ByField(pred.Field)
	if name := q.Get("dataset"); name != "" {
		d, ok := pgdata.Lookup(name)
		if !ok {
			return r, fmt.Errorf("unknown dataset %q", name)
		}
		if d.Name != r.dataset.Name {
			return r, fmt.Errorf("field %q belongs to dataset %q, not %q", pred.Field, r.dataset.Name, name)
		}
	}
	if r.variant, err = pgdata.ParseVariant(q.Get("variant")); err != nil {
		return r, err
	}

	if r.end, err = parseTime(q.Get("end"), now.UnixNano()); err != nil {
		return r, fmt.Errorf("end: %w", err)
	}
	if r.start, err = parseTime(q.Get("start"), r.end-int64(time.Hour)); err != nil {
		return r, fmt.Errorf("start: %w", err)
	}
	if r.start > r.end {
		return r, fmt.Errorf("start %d is after end %d", r.start, r.end)
	}

	r.limit = 100 // Loki's default
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > maxLimit {
			return r, fmt.Errorf("limit must be an integer in 1..%d, got %q", maxLimit, v)
		}
		r.limit = n
	}

	switch q.Get("direction") {
	case "", "backward":
		r.backward = true
	case "forward":
	default:
		return r, fmt.Errorf("direction must be backward or forward, got %q", q.Get("direction"))
	}
	return r, nil
}

// parseTime accepts Unix nanoseconds (what the harness and Grafana send) or RFC 3339.
func parseTime(v string, def int64) (int64, error) {
	if v == "" {
		return def, nil
	}
	if ns, err := strconv.ParseInt(v, 10, 64); err == nil {
		return ns, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UnixNano(), nil
	}
	return 0, fmt.Errorf("want Unix nanoseconds or RFC 3339, got %q", v)
}

type row struct {
	streamID int32
	tsNano   int64
	line     string
}

func (s *server) queryRange(w http.ResponseWriter, r *http.Request) {
	req, err := parseRequest(r.URL.Query(), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sql, err := req.dataset.QuerySQL(req.variant, req.pred.Op, req.backward)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pgRows, err := s.pool.Query(r.Context(), sql, req.pred.Value, req.start, req.end, req.limit)
	if err != nil {
		s.fail(w, "query", err)
		return
	}
	rows, err := pgx.CollectRows(pgRows, func(cr pgx.CollectableRow) (row, error) {
		var x row
		err := cr.Scan(&x.streamID, &x.tsNano, &x.line)
		return x, err
	})
	if err != nil {
		s.fail(w, "reading rows", err)
		return
	}

	result, err := groupStreams(rows, func(id int32) (map[string]string, error) {
		return s.streams.get(r.Context(), id)
	})
	if err != nil {
		s.fail(w, "resolving streams", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := response{Status: "success"}
	resp.Data.ResultType = "streams"
	resp.Data.Result = result
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Warn("writing response", "err", err)
	}
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		http.Error(w, "postgres not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ready")
}

func (s *server) fail(w http.ResponseWriter, what string, err error) {
	s.log.Error(what, "err", err)
	http.Error(w, what+": "+err.Error(), http.StatusInternalServerError)
}

// The response is Loki's streams shape, which is all the harness's row counter reads:
// sum(len(r["values"]) for r in data.result).
type response struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string         `json:"resultType"`
		Result     []streamResult `json:"result"`
	} `json:"data"`
}

type streamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// groupStreams groups rows into streams in the order each stream first appears, keeping
// row order within a stream. Rows arrive sorted by ts in the query's direction, so every
// stream's values are sorted the same way, as in a Loki response.
func groupStreams(rows []row, labels func(int32) (map[string]string, error)) ([]streamResult, error) {
	out := []streamResult{} // [] rather than null for an empty answer, as Loki returns
	pos := map[int32]int{}
	for _, x := range rows {
		i, ok := pos[x.streamID]
		if !ok {
			l, err := labels(x.streamID)
			if err != nil {
				return nil, err
			}
			i = len(out)
			pos[x.streamID] = i
			out = append(out, streamResult{Stream: l})
		}
		out[i].Values = append(out[i].Values, [2]string{strconv.FormatInt(x.tsNano, 10), x.line})
	}
	return out, nil
}

// streamCache maps stream ids to label sets. The streams table is tiny and only grows when
// the loader meets a new label set, so it is read once and re-read only on an id it has not
// seen -- one extra round trip per load, not per query. A per-query join would be just as
// correct, but would put a join into every measured plan to fetch what the cache already
// holds.
type streamCache struct {
	load func(context.Context) (map[int32]map[string]string, error)

	mu sync.Mutex
	m  map[int32]map[string]string
}

func (c *streamCache) get(ctx context.Context, id int32) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.m[id]; ok {
		return l, nil
	}
	m, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	c.m = m
	if l, ok := c.m[id]; ok {
		return l, nil
	}
	return nil, fmt.Errorf("stream id %d is not in the streams table", id)
}

func loadStreams(pool *pgxpool.Pool) func(context.Context) (map[int32]map[string]string, error) {
	return func(ctx context.Context) (map[int32]map[string]string, error) {
		rows, err := pool.Query(ctx, "SELECT id, labels FROM streams")
		if err != nil {
			return nil, err
		}
		m := map[int32]map[string]string{}
		for rows.Next() {
			var id int32
			var labels map[string]string
			if err := rows.Scan(&id, &labels); err != nil {
				rows.Close()
				return nil, err
			}
			m[id] = labels
		}
		return m, rows.Err()
	}
}
