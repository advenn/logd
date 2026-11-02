package loki_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// doWithin is do() under a deadline, for requests whose failure mode is never returning.
func doWithin(t *testing.T, h http.Handler, target string, d time.Duration) *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest("GET", target, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		done <- rr
	}()
	select {
	case rr := <-done:
		return rr
	case <-time.After(d):
		t.Fatalf("GET %s did not return within %v", target, d)
		return nil
	}
}

// A timestamp no real query uses must be answered with 400, not evaluated. time=MaxInt64
// made the metric evaluator's step cursor wrap and one GET grew the heap until the process
// ran out of memory.
func TestTimestampsOutsideSupportedRangeRejected(t *testing.T) {
	srv, _ := setup(t)
	h := srv.Handler()
	metric := urlEncode(`count_over_time({region="eu"}[1m])`)
	for _, target := range []string{
		"/loki/api/v1/query?query=" + metric + "&time=9223372036854775807",
		"/loki/api/v1/query_range?query=" + metric + "&start=-9223372036854775808&end=9223372036854775807",
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&start=0&end=9223372036854775807",
		"/loki/api/v1/query_range?query=" + metric + "&start=0&end=" + urlEncode("2300-01-01T00:00:00Z"),
	} {
		rr := doWithin(t, h, target, 5*time.Second)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("GET %s: got %d, want 400; body %s", target, rr.Code, rr.Body.String())
		}
	}
	// The edges of the supported range are still accepted.
	rr := doWithin(t, h, "/loki/api/v1/query_range?query="+metric+"&start=0&end=3600000000000&step=60", 5*time.Second)
	if rr.Code != http.StatusOK {
		t.Fatalf("a query from the epoch: got %d; body %s", rr.Code, rr.Body.String())
	}
}

// pushAt stores one line per offset (seconds after base) in region eu and seals the data.
func pushAt(t *testing.T, offsets ...int) http.Handler {
	t.Helper()
	srv, w := setup(t)
	h := srv.Handler()
	var values [][2]string
	for _, o := range offsets {
		values = append(values, [2]string{ns(o), fmt.Sprintf("line %d", o)})
	}
	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{{map[string]string{"region": "eu"}, values}})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body.String())
	}
	w.Close()
	return h
}

// lines runs a log query_range with the given extra parameters and returns its lines in
// response order.
func lines(t *testing.T, h http.Handler, params string) []string {
	t.Helper()
	rr := do(t, h, "GET", "/loki/api/v1/query_range?query="+urlEncode(`{region="eu"}`)+params, "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("query_range%s: %d %s", params, rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(rr.Body.Bytes(), &resp)
	var out []string
	for _, s := range resp.Data.Result {
		for _, v := range s.Values {
			out = append(out, v[1])
		}
	}
	return out
}

// Request parameters are read as Loki reads them (pkg/loghttp/params.go): an integer of
// at most 10 digits is seconds, longer is nanoseconds; a decimal is float seconds; RFC3339
// works; `since` sets the start when there is none; direction is case-insensitive.
// "1700000002" used to be read as nanoseconds (1970), a float fell back to the last hour,
// and since and FORWARD were ignored.
func TestQueryParamsParsedAsLoki(t *testing.T) {
	h := pushAt(t, 1, 2, 3, 8)
	sec := func(o int) string { return strconv.FormatInt(base+int64(o), 10) }
	for params, want := range map[string]string{
		"&start=" + sec(2) + "&end=" + sec(3) + "&direction=forward":                                  "[line 2 line 3]",
		"&start=" + ns(2) + "&end=" + ns(3) + "&direction=forward":                                    "[line 2 line 3]",
		"&start=" + sec(1) + ".5&end=" + sec(2) + ".5":                                                "[line 2]",
		"&start=" + urlEncode("2023-11-14T22:13:21Z") + "&end=" + urlEncode("2023-11-14T22:13:22.5Z"): "[line 2 line 1]",
		"&end=" + ns(10) + "&since=5s":                                                                "[line 8]",
		"&end=" + ns(10) + "&since=1m&direction=FORWARD":                                              "[line 1 line 2 line 3 line 8]",
		"&end=" + ns(10) + "&since=1m&direction=Backward&limit=2":                                     "[line 8 line 3]",
	} {
		if got := fmt.Sprint(lines(t, h, params)); got != want {
			t.Fatalf("%s: got %s, want %s", params, got, want)
		}
	}
}

// A parameter that cannot be parsed is a 400, as in Loki, not silently replaced by a
// default (an unparseable start used to mean "the last hour").
func TestBadQueryParamsRejected(t *testing.T) {
	h := pushAt(t, 1)
	logQ := "/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&start=" + ns(0) + "&end=" + ns(10)
	metricQ := "/loki/api/v1/query_range?query=" + urlEncode(`count_over_time({region="eu"}[1m])`) + "&start=" + ns(0) + "&end=" + ns(10)
	for _, target := range []string{
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&start=yesterday",
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&end=NaN",
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"}`) + "&since=forever",
		"/loki/api/v1/query?query=" + urlEncode(`count_over_time({region="eu"}[1m])`) + "&time=noon",
		logQ + "&limit=-1",
		logQ + "&limit=0",
		logQ + "&limit=lots",
		logQ + "&limit=1000001",
		logQ + "&direction=sideways",
		metricQ + "&step=0",
		metricQ + "&step=-15",
		metricQ + "&step=often",
		metricQ + "&step=1e300",
	} {
		if rr := do(t, h, "GET", target, "", ""); rr.Code != http.StatusBadRequest {
			t.Fatalf("GET %s: got %d %s, want 400", target, rr.Code, rr.Body.String())
		}
	}
	if rr := do(t, h, "GET", logQ+"&limit=1000000", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("limit at the maximum: got %d %s", rr.Code, rr.Body.String())
	}
}

// A query the engine refuses because of the query itself is the client's mistake, answered
// 400 as Loki answers a query over its resolution limit: a metric query over the step cap,
// and a comparison with a non-finite bound, which the native API rejects at plan time
// (design §9.3). All of these were 500. A log query's limit over the maximum is refused
// before it reaches the engine, and was already 400 (TestBadQueryParamsRejected).
func TestQueryRefusedForTheQueryIs400(t *testing.T) {
	h := pushAt(t, 1)
	window := "&start=" + ns(0) + "&end=" + ns(10)
	for _, target := range []string{
		// Two days at a 1 s step is 172,801 points, over the 11,000 cap.
		"/loki/api/v1/query_range?query=" + urlEncode(`count_over_time({region="eu"}[1m])`) + "&start=" + ns(0) + "&end=" + ns(2*86400) + "&step=1",
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"} | logfmt | x > "NaN"`) + window,
		"/logd/api/v1/query_scan?query=" + urlEncode(`{region="eu"} | logfmt | x > "Inf"`) + window,
		"/loki/api/v1/query_range?query=" + urlEncode(`rate({region="eu"} | logfmt | x <= "-Inf" [1m])`) + window,
		"/loki/api/v1/query?query=" + urlEncode(`{region="eu"} | logfmt | x >= "NaN"`) + "&time=" + ns(10),
		"/loki/api/v1/query?query=" + urlEncode(`count_over_time({region="eu"} | logfmt | x < "NaN" [1m])`) + "&time=" + ns(10),
	} {
		if rr := do(t, h, "GET", target, "", ""); rr.Code != http.StatusBadRequest {
			t.Errorf("GET %s: got %d %s, want 400", target, rr.Code, rr.Body.String())
		}
	}
	// Under the cap, and with a finite bound, the same queries are answered.
	for _, target := range []string{
		"/loki/api/v1/query_range?query=" + urlEncode(`count_over_time({region="eu"}[1m])`) + "&start=" + ns(0) + "&end=" + ns(10000) + "&step=1",
		"/loki/api/v1/query_range?query=" + urlEncode(`{region="eu"} | logfmt | x > "1.5"`) + window,
	} {
		if rr := do(t, h, "GET", target, "", ""); rr.Code != http.StatusOK {
			t.Errorf("GET %s: got %d %s, want 200", target, rr.Code, rr.Body.String())
		}
	}
}

// Without a step, a metric query takes Loki's default resolution, max(1s, range/250)
// truncated to whole seconds, instead of a fixed 15s.
func TestMetricDefaultStepMatchesLoki(t *testing.T) {
	h := pushAt(t, 1, 2, 3)
	target := "/loki/api/v1/query_range?query=" + urlEncode(`count_over_time({region="eu"}[1h])`) + "&start=" + ns(0) + "&end=" + ns(1000)
	rr := do(t, h, "GET", target, "", "")
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(rr.Body.Bytes(), &resp)
	if rr.Code != http.StatusOK || len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values) < 2 {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	v := resp.Data.Result[0].Values
	if step := v[1][0].(float64) - v[0][0].(float64); step != 4 { // floor(1000s/250)
		t.Fatalf("default step = %vs, want 4s", step)
	}
}
