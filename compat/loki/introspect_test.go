package loki_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

// queryRangeParams builds the shared query string for a range query covering the whole
// test corpus, so the index and scan paths are compared over identical inputs.
func queryRangeParams(expr string) string {
	v := url.Values{}
	v.Set("query", expr)
	v.Set("start", ns(0))
	v.Set("end", ns(100))
	v.Set("limit", "1000")
	return v.Encode()
}

// seedCorpus pushes a small mixed corpus. Callers that need a sealed segment (and hence a
// real .tidx on disk) close the writer afterwards, as the existing end-to-end test does.
func seedCorpus(t *testing.T, h http.Handler) {
	t.Helper()
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{
		{map[string]string{"region": "eu"}, [][2]string{
			{ns(1), "req took 250ms"},
			{ns(2), "req took 50ms"},
			{ns(3), "req took 900ms"},
		}},
		{map[string]string{"region": "us"}, [][2]string{
			{ns(4), "req took 10ms"},
			{ns(5), "req took 400ms"},
		}},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: got %d, body %s", rr.Code, rr.Body.String())
	}
}

// TestIndexStatsReportsConfiguredSchema asserts the index-config feedback endpoint
// reports what the daemon is actually configured to extract. Without this, choosing
// templates for the index list is guesswork.
func TestIndexStatsReportsConfiguredSchema(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()

	rr := do(t, h, "GET", "/logd/api/v1/index_stats", "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("index_stats: got %d, body %s", rr.Code, rr.Body.String())
	}

	var got struct {
		Templates []struct {
			Name       string   `json:"name"`
			Pattern    string   `json:"pattern"`
			Fields     []string `json:"fields"`
			Candidates int64    `json:"candidates"`
			Matches    int64    `json:"matches"`
			Failures   int64    `json:"failures"`
			MatchRate  float64  `json:"match_rate"`
		} `json:"templates"`
		Schema []struct {
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"schema"`
		Labels []string `json:"labels"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rr.Body.String())
	}

	if len(got.Templates) != 1 || got.Templates[0].Name != "latency" {
		t.Fatalf("want one template named latency, got %+v", got.Templates)
	}
	if got.Templates[0].Pattern != "took {ms:int}ms" {
		t.Errorf("pattern: got %q", got.Templates[0].Pattern)
	}
	if len(got.Templates[0].Fields) != 1 || got.Templates[0].Fields[0] != "latency_ms" {
		t.Errorf("fields: got %v, want [latency_ms]", got.Templates[0].Fields)
	}
	// The typed kind is DECLARED, not inferred from a sample — that is the property that
	// makes the range index sound, so assert it explicitly.
	var found bool
	for _, f := range got.Schema {
		if f.Name == "latency_ms" {
			found = true
			if f.Kind != "int" {
				t.Errorf("latency_ms kind: got %q, want int", f.Kind)
			}
		}
	}
	if !found {
		t.Errorf("latency_ms missing from schema %+v", got.Schema)
	}
}

// TestIndexStatsCountersMoveWithTraffic is the reason the candidates/matches counters were
// added: a failure count alone has no denominator. After ingesting lines that all match,
// candidates and matches must both advance and the match rate must be 1.
func TestIndexStatsCountersMoveWithTraffic(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	seedCorpus(t, h)

	rr := do(t, h, "GET", "/logd/api/v1/index_stats", "", "")
	var got struct {
		Templates []struct {
			Name       string  `json:"name"`
			Candidates int64   `json:"candidates"`
			Matches    int64   `json:"matches"`
			Failures   int64   `json:"failures"`
			MatchRate  float64 `json:"match_rate"`
		} `json:"templates"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	tp := got.Templates[0]
	if tp.Candidates != 5 {
		t.Errorf("candidates: got %d, want 5 (one per pushed line)", tp.Candidates)
	}
	if tp.Matches != 5 {
		t.Errorf("matches: got %d, want 5 (every line matches the pattern)", tp.Matches)
	}
	if tp.Failures != 0 {
		t.Errorf("failures: got %d, want 0", tp.Failures)
	}
	if tp.MatchRate != 1 {
		t.Errorf("match_rate: got %v, want 1", tp.MatchRate)
	}
}

// TestIndexStatsDetectsMisspecifiedTemplate is the diagnosis this endpoint exists for.
// A pattern whose anchor fires but whose captures don't parse is exactly the failure mode
// that is invisible without counters — the daemon runs fine and silently indexes nothing.
func TestIndexStatsDetectsMisspecifiedTemplate(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()

	// "took " anchors, but "abc" is not an int, so the capture fails to parse.
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{
		{map[string]string{"region": "eu"}, [][2]string{{ns(1), "req took abcms"}}},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: got %d", rr.Code)
	}

	rr := do(t, h, "GET", "/logd/api/v1/index_stats", "", "")
	var got struct {
		Templates []struct {
			Candidates int64   `json:"candidates"`
			Matches    int64   `json:"matches"`
			Failures   int64   `json:"failures"`
			MatchRate  float64 `json:"match_rate"`
		} `json:"templates"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	tp := got.Templates[0]
	if tp.Candidates != 1 {
		t.Errorf("candidates: got %d, want 1", tp.Candidates)
	}
	if tp.Failures != 1 {
		t.Errorf("failures: got %d, want 1 — the anchor hit but 'abc' is not an int", tp.Failures)
	}
	if tp.Matches != 0 || tp.MatchRate != 0 {
		t.Errorf("matches/rate: got %d/%v, want 0/0", tp.Matches, tp.MatchRate)
	}
}

// TestExplainReportsAccessPath asserts the endpoint that makes the differentiator
// observable rather than merely fast.
func TestExplainReportsAccessPath(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()
	seedCorpus(t, h)
	// Close flushes the final page and seals the segment, which is what writes the .tidx.
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rr := do(t, h, "GET", "/logd/api/v1/explain?"+queryRangeParams(`{region="eu"} | latency_ms > 200`), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("explain: got %d, body %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Segments []struct {
			Mode       string `json:"mode"`
			Candidates int    `json:"candidates"`
		} `json:"segments"`
		Summary struct {
			SegmentsTotal   int `json:"segments_total"`
			SegmentsIndexed int `json:"segments_indexed"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rr.Body.String())
	}
	if got.Summary.SegmentsTotal == 0 {
		t.Fatal("explain reported no segments; the corpus should have sealed one")
	}
	if got.Summary.SegmentsIndexed == 0 {
		t.Errorf("a typed range over a sealed segment should use the index, got %+v", got.Segments)
	}
}

// TestExplainRejectsMetricQuery pins the error path: explain describes a log-query access
// path and has nothing to say about a metric expression.
func TestExplainRejectsMetricQuery(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	rr := do(t, h, "GET", "/logd/api/v1/explain?"+queryRangeParams(`count_over_time({region="eu"}[5m])`), "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400; body %s", rr.Code, rr.Body.String())
	}
}

// TestQueryScanEqualsQueryRange is the load-bearing test of this file. The benchmark's
// headline number is logd-index vs logd-forced-scan, and that comparison is only
// meaningful if both paths return the SAME rows. This asserts the engine's core contract
// (Execute == ExecuteScan) end-to-end through HTTP, which is also what makes the two
// endpoints safe to time against each other.
func TestQueryScanEqualsQueryRange(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()
	seedCorpus(t, h)
	// Close flushes the final page and seals the segment, which is what writes the .tidx.
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, expr := range []string{
		`{region="eu"}`,
		`{region="eu"} | latency_ms > 200`,
		`{region="eu"} | latency_ms < 100`,
		`{region="us"} | latency_ms >= 400`,
		`{region="eu"} |= "took"`,
		`{region="eu"} | latency_ms != 50`,
	} {
		params := queryRangeParams(expr)
		idx := do(t, h, "GET", "/loki/api/v1/query_range?"+params, "", "")
		scan := do(t, h, "GET", "/logd/api/v1/query_scan?"+params, "", "")
		if idx.Code != http.StatusOK || scan.Code != http.StatusOK {
			t.Fatalf("%s: index %d / scan %d", expr, idx.Code, scan.Code)
		}
		if got, want := normalizeStreams(t, scan.Body.Bytes()), normalizeStreams(t, idx.Body.Bytes()); got != want {
			t.Errorf("%s: forced scan disagrees with index path\n index: %s\n  scan: %s", expr, want, got)
		}
	}
}

// normalizeStreams flattens a query_range response into a canonical, order-independent
// string so two responses can be compared for set equality.
func normalizeStreams(t *testing.T, body []byte) string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][2]string       `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body %s)", err, body)
	}
	var lines []string
	for _, r := range resp.Data.Result {
		labels, _ := json.Marshal(r.Stream)
		for _, v := range r.Values {
			lines = append(lines, string(labels)+"|"+v[0]+"|"+v[1])
		}
	}
	sortStrings(lines)
	out, _ := json.Marshal(lines)
	return string(out)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// TestQueryRangeRejectsUnsupportedExpr pins the default: branch. Before it existed, an
// expression the switch didn't handle returned 200 with an empty body, which a client
// reads as "no data" rather than "unsupported".
func TestQueryRangeRejectsUnsupportedExpr(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	rr := do(t, h, "GET", "/loki/api/v1/query_range?"+queryRangeParams(`{region="eu"} | logfmt`), "", "")
	if rr.Code == http.StatusOK {
		t.Fatalf("unsupported pipeline stage returned 200: %s", rr.Body.String())
	}
}

// countRows totals the entries across all streams in a query_range response.
func countRows(t *testing.T, body []byte) int {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body %s)", err, body)
	}
	n := 0
	for _, r := range resp.Data.Result {
		n += len(r.Values)
	}
	return n
}

// TestIndexStatsCountersIgnoreQueryTraffic pins that the counters measure INGESTED data,
// not matcher invocations.
//
// The scan/re-verify path re-extracts every candidate record to compare exact values, so
// an obvious implementation counts there too — and then the numbers are dominated by query
// volume. That is not merely noisy, it inverts the metric's meaning: a template matching
// 1% of ingested lines would look busy simply because it was queried often. Found in
// practice, where a 50,000-line push reported 1,687,140 candidates.
func TestIndexStatsCountersIgnoreQueryTraffic(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()
	seedCorpus(t, h)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	before := templateCandidates(t, h)

	// Run enough queries, on both the index and forced-scan paths, that any per-record
	// counting during re-verification would be unmistakable.
	params := queryRangeParams(`{region="eu"} | latency_ms > 100`)
	for i := 0; i < 20; i++ {
		do(t, h, "GET", "/loki/api/v1/query_range?"+params, "", "")
		do(t, h, "GET", "/logd/api/v1/query_scan?"+params, "", "")
	}

	if after := templateCandidates(t, h); after != before {
		t.Errorf("query traffic moved the ingest counters: %d -> %d", before, after)
	}
}

func templateCandidates(t *testing.T, h http.Handler) int64 {
	t.Helper()
	rr := do(t, h, "GET", "/logd/api/v1/index_stats", "", "")
	var got struct {
		Templates []struct {
			Candidates int64 `json:"candidates"`
		} `json:"templates"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var n int64
	for _, tp := range got.Templates {
		n += tp.Candidates
	}
	return n
}
