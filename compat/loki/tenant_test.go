package loki_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// seedTenants pushes acme's eu and us streams and globex's eu and ap streams.
func seedTenants(t *testing.T, h http.Handler) {
	t.Helper()
	for _, p := range []struct{ tenant, region string }{{"acme", "eu"}, {"acme", "us"}, {"globex", "eu"}, {"globex", "ap"}} {
		body := pushJSON([]struct {
			labels map[string]string
			values [][2]string
		}{{map[string]string{"region": p.region}, [][2]string{{ns(1), p.tenant + " took 5ms"}}}})
		if rr := doAs(t, h, "POST", "/loki/api/v1/push", body, "application/json", p.tenant); rr.Code != http.StatusNoContent {
			t.Fatalf("push: %d %s", rr.Code, rr.Body.String())
		}
	}
}

// Live tail delivered the internal tenant label: it grouped results itself instead of
// through streamsFor, which strips it.
func TestTailHidesTenantLabel(t *testing.T) {
	srv, _ := setupMT(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	conn, br := dialTailAs(t, ts, "acme")
	time.Sleep(150 * time.Millisecond) // let the subscription register
	seedTenants(t, srv.Handler())
	op, payload := readServerFrame(t, conn, br)
	if op != 0x81 {
		t.Fatalf("opcode byte %#x, want a text frame", op)
	}
	if strings.Contains(string(payload), model.TenantLabel) || strings.Contains(string(payload), "globex") {
		t.Fatalf("tail message leaks tenant data: %s", payload)
	}
	if !strings.Contains(string(payload), "acme took 5ms") {
		t.Fatalf("tail message missing acme's line: %s", payload)
	}
}

// The stats endpoints counted and listed every tenant's data: /loki/api/v1/index/stats
// reported all streams, and /logd/api/v1/index_stats listed every label key including
// __tenant__ itself.
func TestStatsScopedToTenant(t *testing.T) {
	srv, w := setupMT(t)
	h := srv.Handler()
	seedTenants(t, h)
	w.Close()

	v := url.Values{"query": {`{region="eu"}`}, "start": {ns(0)}, "end": {ns(10)}}
	var stats struct{ Streams uint64 }
	json.Unmarshal(doAs(t, h, "GET", "/loki/api/v1/index/stats?"+v.Encode(), "", "", "acme").Body.Bytes(), &stats)
	if stats.Streams != 2 {
		t.Fatalf("acme index/stats streams = %d, want its own 2", stats.Streams)
	}
	json.Unmarshal(doAs(t, h, "GET", "/loki/api/v1/index/stats?"+v.Encode(), "", "", "initech").Body.Bytes(), &stats)
	if stats.Streams != 0 {
		t.Fatalf("a tenant with no data sees %d streams", stats.Streams)
	}

	var intro struct{ Labels []string }
	json.Unmarshal(doAs(t, h, "GET", "/logd/api/v1/index_stats", "", "", "acme").Body.Bytes(), &intro)
	if len(intro.Labels) != 1 || intro.Labels[0] != "region" {
		t.Fatalf("acme index_stats labels = %v, want [region]", intro.Labels)
	}
	json.Unmarshal(doAs(t, h, "GET", "/logd/api/v1/index_stats", "", "", "initech").Body.Bytes(), &intro)
	if len(intro.Labels) != 0 {
		t.Fatalf("a tenant with no data sees labels %v", intro.Labels)
	}
}
