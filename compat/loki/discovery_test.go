package loki_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

// TestBuildInfoIsWellFormed pins the endpoint Grafana feature-gates the datasource on.
// A 404 or a malformed body here silently downgrades the whole UI, so the shape matters
// more than the values.
func TestBuildInfoIsWellFormed(t *testing.T) {
	srv, _ := setup(t)
	rr := do(t, srv.Handler(), "GET", "/loki/api/v1/status/buildinfo", "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, body %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rr.Body.String())
	}
	// These are exactly the keys Grafana reads; a missing one is a silent downgrade.
	for _, k := range []string{"version", "revision", "branch", "buildUser", "buildDate", "goVersion"} {
		if _, ok := got[k]; !ok {
			t.Errorf("buildinfo missing key %q; got %v", k, got)
		}
	}
	if got["version"] == "" {
		t.Error("version must be non-empty: Grafana compares against it to enable features")
	}
}

// TestIndexStatsLokiShape asserts the estimate endpoint behind Explore's
// "will process approximately N bytes" banner returns Loki's four uint64 fields.
func TestIndexStatsLokiShape(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()
	seedCorpus(t, h)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	v := url.Values{}
	v.Set("query", `{region="eu"} | latency_ms > 200`)
	v.Set("start", ns(0))
	v.Set("end", ns(100))
	rr := do(t, h, "GET", "/loki/api/v1/index/stats?"+v.Encode(), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, body %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Streams uint64 `json:"streams"`
		Chunks  uint64 `json:"chunks"`
		Entries uint64 `json:"entries"`
		Bytes   uint64 `json:"bytes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rr.Body.String())
	}
	if got.Chunks == 0 {
		t.Errorf("expected at least one segment to be counted, got %+v", got)
	}
	if got.Entries > 0 && got.Bytes == 0 {
		t.Errorf("bytes should scale with entries, got %+v", got)
	}
}

// TestIndexStatsLokiToleratesBadInput: this endpoint is called speculatively by the UI as
// the user types, so a half-written query must not produce a 500.
func TestIndexStatsLokiToleratesBadInput(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	for _, q := range []string{"", "{", `{region=`, `count_over_time({region="eu"}[5m])`} {
		rr := do(t, h, "GET", "/loki/api/v1/index/stats?query="+url.QueryEscape(q), "", "")
		if rr.Code != http.StatusOK {
			t.Errorf("query %q: got %d, want 200 (speculative UI call must not error)", q, rr.Code)
		}
	}
}

// TestFormatQueryRoundTrips asserts the query-builder prettify endpoint returns a string
// that parses back to the same canonical form.
func TestFormatQueryRoundTrips(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()

	rr := do(t, h, "GET", "/loki/api/v1/format_query?query="+url.QueryEscape(`{region="eu"} |= "took"`), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, body %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Status string `json:"status"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rr.Body.String())
	}
	if got.Status != "success" || got.Data == "" {
		t.Fatalf("unexpected response %+v", got)
	}
	// Formatting twice must be a fixed point, otherwise the button would keep changing
	// the user's query.
	rr2 := do(t, h, "GET", "/loki/api/v1/format_query?query="+url.QueryEscape(got.Data), "", "")
	var got2 struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &got2); err != nil {
		t.Fatalf("decode 2: %v", err)
	}
	if got2.Data != got.Data {
		t.Errorf("format is not idempotent: %q then %q", got.Data, got2.Data)
	}
}

// TestFormatQueryRejectsGarbage: unlike index/stats, this one is user-triggered, so a
// parse error should be reported rather than swallowed.
func TestFormatQueryRejectsGarbage(t *testing.T) {
	srv, _ := setup(t)
	rr := do(t, srv.Handler(), "GET", "/loki/api/v1/format_query?query="+url.QueryEscape("{"), "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400; body %s", rr.Code, rr.Body.String())
	}
}
