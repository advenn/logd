package loki_test

import (
	"net/http"
	"net/http/httptest"
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
