package loki_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// index_stats counts the label values the per-segment cap kept out of the index (design
// §6.2), per key. With multitenancy on a tenant sees the drops among its own records only —
// neither another tenant's counts nor its key names — and the internal tenant label, which
// the cap drops once there are more tenants than it allows, never appears.
func TestIndexStatsLabelCapDrops(t *testing.T) {
	eng, _ := extract.Compile(config.IndexConfig{})
	w, err := storage.NewWriter(t.TempDir(), storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, MaxLabelCardinality: 2})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	labels := []string{"region", "pod", model.TenantLabel}
	ig, qe := ingest.NewWithLabels(eng, w, labels), query.NewEngineWithLabels(w, eng, labels)
	mt := loki.NewServer(ig, qe, eng)
	mt.EnableMultitenancy()
	h := mt.Handler()
	for _, p := range []struct {
		tenant string
		labels map[string]string
	}{
		{"acme", map[string]string{"region": "r0"}},
		{"acme", map[string]string{"region": "r1"}},
		{"acme", map[string]string{"region": "r2"}},                // region over the cap
		{"acme", map[string]string{"region": "r3"}},                // region over the cap
		{"globex", map[string]string{"region": "r4", "pod": "p0"}}, // region over the cap
		{"globex", map[string]string{"region": "r0", "pod": "p1"}},
		{"globex", map[string]string{"region": "r0", "pod": "p2"}}, // pod over the cap
		{"initech", map[string]string{"region": "r0"}},             // a third tenant: __tenant__ over the cap
	} {
		body := pushJSON([]struct {
			labels map[string]string
			values [][2]string
		}{{p.labels, [][2]string{{ns(1), "line"}}}})
		if rr := doAs(t, h, "POST", "/loki/api/v1/push", body, "application/json", p.tenant); rr.Code != http.StatusNoContent {
			t.Fatalf("push: %d %s", rr.Code, rr.Body.String())
		}
	}
	w.Close() // the drops are counted as the writer applies the cap; Close drains its queue

	drops := func(h http.Handler, tenant string) string {
		var intro struct {
			LabelCapDrops map[string]int64 `json:"label_cap_drops"`
		}
		rr := doAs(t, h, "GET", "/logd/api/v1/index_stats", "", "", tenant)
		if err := json.Unmarshal(rr.Body.Bytes(), &intro); err != nil || intro.LabelCapDrops == nil {
			t.Fatalf("index_stats as %q: %v, body %s", tenant, err, rr.Body.String())
		}
		return fmt.Sprint(intro.LabelCapDrops)
	}
	for tenant, want := range map[string]string{
		"acme":     "map[region:2]",
		"globex":   "map[pod:1 region:1]",
		"initech":  "map[]",
		"umbrella": "map[]", // no data at all
	} {
		if got := drops(h, tenant); got != want {
			t.Errorf("label_cap_drops as %s = %s, want %s", tenant, got, want)
		}
	}
	// Without multitenancy, every record counts, and the tenant label still does not show.
	if got, want := drops(loki.NewServer(ig, qe, eng).Handler(), ""), "map[pod:1 region:3]"; got != want {
		t.Errorf("label_cap_drops without multitenancy = %s, want %s", got, want)
	}
}
