package loki_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// setupTestbed builds a stack with the testbed's extraction config and label allowlist
// (testbed/stack/logd/logd.yaml), so the queries below are the testbed's own.
func setupTestbed(t *testing.T) (*loki.Server, *storage.Writer) {
	t.Helper()
	ex, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took={ms:int}ms"},
		{Name: "status", Pattern: "status={code:int} "},
	}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWriter(t.TempDir(), storage.Options{Schema: ex.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	labels := []string{"app", "env", "region"}
	return loki.NewServer(ingest.NewWithLabels(ex, w, labels), query.NewEngineWithLabels(w, ex, labels), ex), w
}

// seedTestbed pushes lines in the generator's shapes (testbed/internal/loggen/format.go) to
// two apps, plus logfmt and JSON lines, and seals them.
func seedTestbed(t *testing.T, h http.Handler, w *storage.Writer) {
	t.Helper()
	type stream = struct {
		labels map[string]string
		values [][2]string
	}
	var checkout, billing, structured [][2]string
	for i := 0; i < 60; i++ {
		ms := (i * 97) % 1500
		var line string
		switch i % 4 {
		case 0:
			line = fmt.Sprintf("payment authorization failed order=o-%d code=card_declined took=%dms", i, ms)
		case 1:
			line = fmt.Sprintf("upstream payments-api request failed status=%d took=%dms trace_id=%016x", 500+i%4, ms, i)
		case 2:
			line = fmt.Sprintf("upstream pricing-api slow response took=%dms threshold=500ms", ms)
		default:
			line = fmt.Sprintf("inventory lock contention on sku=s-%d retry=2 waited=%dms", i, ms)
		}
		checkout = append(checkout, [2]string{ns(i), line})
		billing = append(billing, [2]string{ns(i), fmt.Sprintf("invoice generated took=%dms", ms)})
	}
	structured = append(structured,
		[2]string{ns(70), `level=error method=GET dur=812 msg="slow request"`},
		[2]string{ns(71), `level=info method=POST dur=12 msg="ok"`},
		[2]string{ns(72), `{"level":"warn","method":"PUT","dur":640,"req":{"path":"/orders"}}`},
		[2]string{ns(73), `not json at all`},
	)
	body := pushJSON([]stream{
		{map[string]string{"app": "checkout", "env": "prod"}, checkout},
		{map[string]string{"app": "billing", "env": "prod"}, billing},
		{map[string]string{"app": "api", "method": "GET"}, structured},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body.String())
	}
	if err := w.Close(); err != nil { // seal, so the typed index exists
		t.Fatal(err)
	}
}

type lokiStreams struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func queryStreams(t *testing.T, h http.Handler, path, expr string) lokiStreams {
	t.Helper()
	rr := do(t, h, "GET", path+"?"+queryRangeParams(expr), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("%s %s: %d %s", path, expr, rr.Code, rr.Body.String())
	}
	var s lokiStreams
	if err := json.Unmarshal(rr.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return s
}

// rows is a response's (timestamp, line) pairs, sorted: the identity of the result set that
// the testbed's benchmark compares across engines (testbed/scripts/resultset.py).
func rows(s lokiStreams) []string {
	var out []string
	for _, r := range s.Data.Result {
		for _, v := range r.Values {
			out = append(out, v[0]+" "+v[1])
		}
	}
	sort.Strings(out)
	return out
}

type explainBody struct {
	Pipeline []struct {
		Stage    string `json:"stage"`
		Kind     string `json:"kind"`
		Template string `json:"template"`
		Reason   string `json:"reason"`
	} `json:"pipeline"`
	Summary struct {
		SegmentsTotal   int `json:"segments_total"`
		SegmentsIndexed int `json:"segments_indexed"`
	} `json:"summary"`
}

func explain(t *testing.T, h http.Handler, expr string) explainBody {
	t.Helper()
	rr := do(t, h, "GET", "/logd/api/v1/explain?"+queryRangeParams(expr), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("explain %s: %d %s", expr, rr.Code, rr.Body.String())
	}
	var e explainBody
	if err := json.Unmarshal(rr.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	return e
}

// The testbed's Loki query returns the rows of logd's native query, over HTTP, and Explain
// shows it was mapped to the template and answered from the index. The regexp form returns
// the same rows by a dynamic scan.
func TestTestbedPatternQueryEqualsNative(t *testing.T) {
	srv, w := setupTestbed(t)
	h := srv.Handler()
	seedTestbed(t, h, w)

	const (
		native  = `{app="checkout"} | latency_ms > 200`
		pattern = `{app="checkout"} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200`
		regex   = `{app="checkout"} |= "took=" | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms > 200`
	)
	want := rows(queryStreams(t, h, "/loki/api/v1/query_range", native))
	if len(want) < 10 {
		t.Fatalf("the native query returned only %d rows", len(want))
	}
	for _, q := range []string{pattern, regex} {
		got := queryStreams(t, h, "/loki/api/v1/query_range", q)
		if r := rows(got); strings.Join(r, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s returned %d rows, the native query %d\n got: %q\nwant: %q", q, len(r), len(want), r, want)
		}
		if r := rows(queryStreams(t, h, "/logd/api/v1/query_scan", q)); strings.Join(r, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s: the forced scan returned other rows", q)
		}
		// Like Loki, each entry carries the extracted label, beside its stream labels.
		for _, s := range got.Data.Result {
			if s.Stream["app"] != "checkout" || s.Stream["env"] != "prod" {
				t.Fatalf("%s: stream labels lost: %v", q, s.Stream)
			}
			for _, v := range s.Values {
				if !strings.Contains(v[1], "took="+s.Stream["latency_ms"]+"ms") {
					t.Fatalf("%s: entry %q carries latency_ms=%q", q, v[1], s.Stream["latency_ms"])
				}
			}
		}
	}

	ep := explain(t, h, pattern)
	if len(ep.Pipeline) != 2 || ep.Pipeline[0].Kind != "template" || ep.Pipeline[0].Template != "latency" || ep.Pipeline[1].Kind != "typed" {
		t.Fatalf("pattern form not mapped to the template: %+v", ep.Pipeline)
	}
	if ep.Summary.SegmentsIndexed == 0 || ep.Summary.SegmentsIndexed != explain(t, h, native).Summary.SegmentsIndexed {
		t.Fatalf("pattern form index use %+v, native %+v", ep.Summary, explain(t, h, native).Summary)
	}
	er := explain(t, h, regex)
	if len(er.Pipeline) != 2 || er.Pipeline[0].Kind != "dynamic" || er.Pipeline[1].Kind != "dynamic" || !strings.Contains(er.Pipeline[1].Reason, "scan") {
		t.Fatalf("regexp form should be a dynamic scan: %+v", er.Pipeline)
	}
	// A near miss says why it scanned.
	em := explain(t, h, `{app="checkout"} | pattern "<_>took= <latency_ms>ms<_>" | latency_ms > 200`)
	if em.Pipeline[0].Kind != "dynamic" || !strings.Contains(em.Pipeline[0].Reason, "no template has exactly") {
		t.Fatalf("near miss: %+v", em.Pipeline)
	}
}

// Results carry the extracted labels as Loki shows them: a name the stream already has
// becomes name_extracted, a JSON error is __error__, and query_scan agrees with query_range.
func TestResultLabelsFromParserStages(t *testing.T) {
	srv, w := setupTestbed(t)
	h := srv.Handler()
	seedTestbed(t, h, w)

	labelsOf := func(expr string) map[string]map[string]string {
		s := queryStreams(t, h, "/loki/api/v1/query_range", expr)
		if a, b := normalizeStreams(t, mustJSON(t, s)), normalizeStreams(t, mustJSON(t, queryStreams(t, h, "/logd/api/v1/query_scan", expr))); a != b {
			t.Fatalf("%s: query_scan disagrees with query_range", expr)
		}
		out := map[string]map[string]string{}
		for _, r := range s.Data.Result {
			for _, v := range r.Values {
				out[v[1]] = r.Stream
			}
		}
		return out
	}

	got := labelsOf(`{app="api"} | logfmt | dur > 500`)
	l := got[`level=error method=GET dur=812 msg="slow request"`]
	if len(got) != 1 || l["level"] != "error" || l["dur"] != "812" || l["msg"] != "slow request" ||
		l["method"] != "GET" || l["method_extracted"] != "GET" || l["app"] != "api" {
		t.Fatalf("logfmt labels: %v", got)
	}

	got = labelsOf(`{app="api"} | json`)
	if l := got[`{"level":"warn","method":"PUT","dur":640,"req":{"path":"/orders"}}`]; l["req_path"] != "/orders" || l["method"] != "GET" || l["method_extracted"] != "PUT" || l["dur"] != "640" {
		t.Fatalf("json labels: %v", l)
	}
	if l := got["not json at all"]; l["__error__"] != "JSONParserErr" {
		t.Fatalf("a line json cannot parse should carry __error__: %v", l)
	}
	if l := labelsOf(`{app="api"} | json | drop __error__`)["not json at all"]; l == nil || l["__error__"] != "" {
		t.Fatalf("drop __error__ left %v", l)
	}
	// The two logfmt lines are not JSON either.
	if got := labelsOf(`{app="api"} | json | __error__ != ""`); len(got) != 3 {
		t.Fatalf("__error__ != \"\" kept %d lines", len(got))
	}

	// Without a parser stage the labels are the stream's own, unchanged.
	for _, l := range labelsOf(`{app="checkout"} | latency_ms > 1000`) {
		if len(l) != 2 {
			t.Fatalf("native query labels: %v", l)
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A metric query over a parser stage groups by an extracted label and unwraps one, through
// /query_range.
func TestMetricQueryOverParserStage(t *testing.T) {
	srv, w := setupTestbed(t)
	h := srv.Handler()
	seedTestbed(t, h, w)

	v := url.Values{}
	// Each line is its own series (its extracted labels differ), so the max across them is
	// the largest dur; a sum by (method) would add the two lines' maxima, as in Loki.
	v.Set("query", `max by (method) (max_over_time({app="api"} | logfmt | unwrap dur [5m]))`)
	v.Set("start", ns(100))
	v.Set("end", ns(100))
	v.Set("step", "1")
	rr := do(t, h, "GET", "/loki/api/v1/query_range?"+v.Encode(), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	var m struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range m.Data.Result {
		got[s.Metric["method"]] = s.Values[0][1].(string)
	}
	// method is the stream label (GET) for every line; logfmt's own method is method_extracted.
	if len(got) != 1 || got["GET"] != "812" {
		t.Fatalf("max by (method)(max_over_time(unwrap dur)): %s", rr.Body.String())
	}
	v.Set("query", `sum by (method_extracted) (count_over_time({app="api"} | logfmt [5m]))`)
	rr = do(t, h, "GET", "/loki/api/v1/query_range?"+v.Encode(), "", "")
	if !strings.Contains(rr.Body.String(), `"method_extracted":"POST"`) || !strings.Contains(rr.Body.String(), `"method_extracted":"GET"`) {
		t.Fatalf("grouping by an extracted label: %s", rr.Body.String())
	}
}

// Tail applies a parser stage and its filters to live entries and labels them like a query.
func TestTailWithParserStage(t *testing.T) {
	srv, _ := setupTestbed(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	host := strings.TrimPrefix(ts.URL, "http://")
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q := `{app="checkout"} | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200`
	req := "GET /loki/api/v1/tail?query=" + urlEncode(q) + " HTTP/1.1\r\nHost: " + host + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	time.Sleep(150 * time.Millisecond)
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{{map[string]string{"app": "checkout"}, [][2]string{{ns(1), "fast took=20ms"}, {ns(2), "slow took=900ms"}}}})
	resp, err := http.Post(ts.URL+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	_, payload := readServerFrame(t, conn, br)
	var msg struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("%v: %q", err, payload)
	}
	if len(msg.Streams) != 1 || len(msg.Streams[0].Values) != 1 || msg.Streams[0].Values[0][1] != "slow took=900ms" ||
		msg.Streams[0].Stream["latency_ms"] != "900" {
		t.Fatalf("tail: %s", payload)
	}
}
