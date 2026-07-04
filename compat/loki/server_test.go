package loki_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

const base = int64(1700000000)

// setup builds a full stack (writer + ingester + query engine + Loki server) with a
// latency template and a "region" label allowlist.
func setup(t *testing.T) (*loki.Server, *storage.Writer) {
	t.Helper()
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	ig := ingest.NewWithLabels(eng, w, []string{"region"})
	qe := query.NewEngineWithLabels(w, eng, []string{"region"})
	return loki.NewServer(ig, qe, eng), w
}

// pushJSON builds a Loki JSON push body: streams of (labels, [ns,line] values).
func pushJSON(streams []struct {
	labels map[string]string
	values [][2]string
}) string {
	type s struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	var payload struct {
		Streams []s `json:"streams"`
	}
	for _, st := range streams {
		payload.Streams = append(payload.Streams, s{Stream: st.labels, Values: st.values})
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func do(t *testing.T, h http.Handler, method, target, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func ns(secOffset int) string { return strconv.FormatInt((base+int64(secOffset))*1e9, 10) }

func TestLokiEndToEnd(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()

	// Push two streams via JSON.
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{
		{map[string]string{"region": "eu"}, [][2]string{{ns(1), "req took 250ms"}, {ns(2), "req took 50ms"}}},
		{map[string]string{"region": "us"}, [][2]string{{ns(3), "req took 900ms"}}},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: got %d, body %s", rr.Code, rr.Body.String())
	}

	// Flush + seal so the data is queryable, then exercise the read endpoints.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// query_range: {region="eu"} | latency_ms > 100  → only the 250ms eu record.
	target := "/loki/api/v1/query_range?" +
		"query=" + urlEncode(`{region="eu"} | latency_ms > 100`) +
		"&start=" + ns(0) + "&end=" + ns(10)
	rr := do(t, h, "GET", target, "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("query_range: %d %s", rr.Code, rr.Body.String())
	}
	var qr struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][2]string       `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &qr); err != nil {
		t.Fatalf("decode query_range: %v; body=%s", err, rr.Body.String())
	}
	if qr.Status != "success" || qr.Data.ResultType != "streams" {
		t.Fatalf("unexpected response shape: %+v", qr)
	}
	total := 0
	for _, s := range qr.Data.Result {
		if s.Stream["region"] != "eu" {
			t.Fatalf("wrong stream region: %v", s.Stream)
		}
		total += len(s.Values)
	}
	if total != 1 {
		t.Fatalf("expected 1 matching value (250ms in eu), got %d", total)
	}

	// /labels contains region.
	rr = do(t, h, "GET", "/loki/api/v1/labels", "", "")
	if !strings.Contains(rr.Body.String(), `"region"`) {
		t.Fatalf("/labels missing region: %s", rr.Body.String())
	}
	// /label/region/values contains eu and us.
	rr = do(t, h, "GET", "/loki/api/v1/label/region/values", "", "")
	if !strings.Contains(rr.Body.String(), `"eu"`) || !strings.Contains(rr.Body.String(), `"us"`) {
		t.Fatalf("/label/region/values wrong: %s", rr.Body.String())
	}
	// /series returns streams.
	rr = do(t, h, "GET", "/loki/api/v1/series", "", "")
	if !strings.Contains(rr.Body.String(), `"region"`) {
		t.Fatalf("/series missing region: %s", rr.Body.String())
	}
	// /ready.
	rr = do(t, h, "GET", "/ready", "", "")
	if rr.Code != http.StatusOK || rr.Body.String() != "ready" {
		t.Fatalf("/ready: %d %q", rr.Code, rr.Body.String())
	}
	// Grafana health check: instant metric query → vector response.
	rr = do(t, h, "GET", "/loki/api/v1/query?query="+urlEncode("vector(1)+vector(1)"), "", "")
	if !strings.Contains(rr.Body.String(), `"resultType":"vector"`) {
		t.Fatalf("metric stub not a vector: %s", rr.Body.String())
	}
}

// A metric query_range returns a Loki matrix with per-step counts.
func TestLokiMetricQueryRange(t *testing.T) {
	srv, w := setup(t)
	h := srv.Handler()

	// Push 3 eu records at +5,+15,+25s.
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{
		{map[string]string{"region": "eu"}, [][2]string{{ns(5), "took 100ms"}, {ns(15), "took 200ms"}, {ns(25), "took 300ms"}}},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d", rr.Code)
	}
	w.Close()

	// count_over_time({region="eu"}[10s]) step 10s over [+0, +40].
	target := "/loki/api/v1/query_range?" +
		"query=" + urlEncode(`count_over_time({region="eu"}[10s])`) +
		"&start=" + ns(0) + "&end=" + ns(40) + "&step=10"
	rr := do(t, h, "GET", target, "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("metric query_range: %d %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode matrix: %v; body=%s", err, rr.Body.String())
	}
	if resp.Data.ResultType != "matrix" {
		t.Fatalf("resultType = %q, want matrix", resp.Data.ResultType)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("want 1 series, got %d", len(resp.Data.Result))
	}
	// Each of the 3 records lands in its own 10s window → 3 non-empty points of value 1.
	pts := resp.Data.Result[0].Values
	if len(pts) != 3 {
		t.Fatalf("want 3 points, got %d: %v", len(pts), pts)
	}
	for _, p := range pts {
		if p[1] != "1" {
			t.Fatalf("count point value = %v, want \"1\"", p[1])
		}
	}
}

// A real query whose text merely contains "vector(" must be evaluated, not short-circuited
// to the health-check stub.
func TestHealthCheckNotOverbroad(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	rr := do(t, h, "GET", `/loki/api/v1/query?query=`+urlEncode(`{region="vector("}`), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("query: %d %s", rr.Code, rr.Body.String())
	}
	// A log query returns a streams result, NOT the scalar-vector health stub.
	if strings.Contains(rr.Body.String(), `"resultType":"vector"`) {
		t.Fatalf("a real query containing vector( was wrongly stubbed: %s", rr.Body.String())
	}
}

// End-to-end live tail: real WebSocket handshake, push a matching entry, receive it as a
// text frame.
func TestLokiTailWebSocket(t *testing.T) {
	srv, _ := setup(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	host := strings.TrimPrefix(ts.URL, "http://")

	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// WebSocket upgrade for a tail on {region="eu"}.
	req := "GET /loki/api/v1/tail?query=" + urlEncode(`{region="eu"}`) + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("handshake status: %q (%v)", status, err)
	}
	sawAccept := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "Sec-WebSocket-Accept:") && strings.Contains(line, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
			sawAccept = true
		}
		if line == "\r\n" {
			break
		}
	}
	if !sawAccept {
		t.Fatal("missing/incorrect Sec-WebSocket-Accept header")
	}

	// Let the subscription register, then push a matching entry.
	time.Sleep(150 * time.Millisecond)
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{{map[string]string{"region": "eu"}, [][2]string{{ns(1), "hello tail"}}}})
	resp, err := http.Post(ts.URL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Read one text frame (opcode 0x1, unmasked, small length).
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	if hdr[0] != 0x81 {
		t.Fatalf("first frame opcode byte = %#x, want 0x81 (text)", hdr[0])
	}
	n := int(hdr[1] & 0x7F)
	if n >= 126 {
		t.Fatalf("unexpected large frame len byte %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(br, payload); err != nil {
		t.Fatalf("read frame payload: %v", err)
	}
	var msg struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("decode tail message: %v; body=%q", err, payload)
	}
	if len(msg.Streams) != 1 || msg.Streams[0].Stream["region"] != "eu" {
		t.Fatalf("wrong tail stream: %+v", msg.Streams)
	}
	if len(msg.Streams[0].Values) != 1 || msg.Streams[0].Values[0][1] != "hello tail" {
		t.Fatalf("wrong tail value: %+v", msg.Streams[0].Values)
	}
}

// setupMT builds a multitenant stack (tenant label allowlisted so scoping pushes down).
func setupMT(t *testing.T) (*loki.Server, *storage.Writer) {
	t.Helper()
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	labels := []string{"region", model.TenantLabel}
	srv := loki.NewServer(ingest.NewWithLabels(eng, w, labels), query.NewEngineWithLabels(w, eng, labels), eng)
	srv.EnableMultitenancy()
	return srv, w
}

func doAs(t *testing.T, h http.Handler, method, target, body, contentType, tenant string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// Multitenancy: X-Scope-OrgID tags pushes and isolates reads; the tenant label is hidden
// from discovery.
func TestLokiMultitenancy(t *testing.T) {
	srv, w := setupMT(t)
	h := srv.Handler()

	push := func(tenant, region, line string, sec int) {
		body := pushJSON([]struct {
			labels map[string]string
			values [][2]string
		}{{map[string]string{"region": region}, [][2]string{{ns(sec), line}}}})
		if rr := doAs(t, h, "POST", "/loki/api/v1/push", body, "application/json", tenant); rr.Code != http.StatusNoContent {
			t.Fatalf("push as %s: %d %s", tenant, rr.Code, rr.Body.String())
		}
	}
	push("acme", "eu", "acme took 1ms", 1)
	push("globex", "eu", "globex took 2ms", 2)
	push("acme", "us", "acme took 3ms", 3)
	w.Close()

	queryLines := func(tenant string) []string {
		target := "/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&start=" + ns(0) + "&end=" + ns(10)
		rr := doAs(t, h, "GET", target, "", "", tenant)
		if rr.Code != http.StatusOK {
			t.Fatalf("query as %q: %d %s", tenant, rr.Code, rr.Body.String())
		}
		var resp struct {
			Data struct {
				Result []struct {
					Stream map[string]string `json:"stream"`
					Values [][2]string       `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		json.Unmarshal(rr.Body.Bytes(), &resp)
		var lines []string
		for _, s := range resp.Data.Result {
			if _, leaked := s.Stream[model.TenantLabel]; leaked {
				t.Fatalf("query response leaked the internal tenant label: %v", s.Stream)
			}
			for _, v := range s.Values {
				lines = append(lines, v[1])
			}
		}
		return lines
	}

	if got := queryLines("acme"); len(got) != 1 || got[0] != "acme took 1ms" {
		t.Fatalf("acme should see only its own eu record, got %v", got)
	}
	if got := queryLines("globex"); len(got) != 1 || got[0] != "globex took 2ms" {
		t.Fatalf("globex should see only its own eu record, got %v", got)
	}
	if got := queryLines(""); len(got) != 0 { // no header → "default" tenant → no data
		t.Fatalf("the default tenant should see no data, got %v", got)
	}

	// Discovery is scoped and hides the tenant label.
	labelsBody := doAs(t, h, "GET", "/loki/api/v1/labels", "", "", "acme").Body.String()
	if !strings.Contains(labelsBody, `"region"`) || strings.Contains(labelsBody, model.TenantLabel) {
		t.Fatalf("acme /labels should list region and hide %s: %s", model.TenantLabel, labelsBody)
	}
	seriesBody := doAs(t, h, "GET", "/loki/api/v1/series", "", "", "acme").Body.String()
	if strings.Contains(seriesBody, model.TenantLabel) || strings.Contains(seriesBody, "globex") {
		t.Fatalf("acme /series must not leak the tenant label or other tenants: %s", seriesBody)
	}
	if !strings.Contains(seriesBody, `"us"`) { // acme's own us stream is present
		t.Fatalf("acme /series should include its own streams: %s", seriesBody)
	}
}

// urlEncode is a tiny query-escaper (avoids importing net/url in the test body).
func urlEncode(s string) string {
	repl := strings.NewReplacer(" ", "%20", `"`, "%22", "{", "%7B", "}", "%7D", "|", "%7C", ">", "%3E", "=", "%3D", "+", "%2B", "(", "%28", ")", "%29", "[", "%5B", "]", "%5D")
	return repl.Replace(s)
}
