package loki_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Pushed level and service labels are queried, grouped and discovered by the value the
// response shows. {level="error"} returned nothing (it resolved to "ERROR"), a pushed
// "warning" grouped as "WARN", and {service="api"} matched nothing.
func TestPushedLevelAndServiceLabelsQueryable(t *testing.T) {
	dir := t.TempDir()
	eng, _ := extract.Compile(config.IndexConfig{})
	allow := []string{"region", "level", "service"}
	w, err := storage.NewWriter(dir, storage.Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, LabelKeys: allow})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	h := loki.NewServer(ingest.NewWithLabels(eng, w, allow), query.NewEngineWithLabels(w, eng, allow), eng).Handler()

	body := pushJSON([]struct {
		labels map[string]string
		values [][2]string
	}{
		{map[string]string{"region": "eu", "level": "error"}, [][2]string{{ns(1), "e1"}, {ns(2), "e2"}}},
		{map[string]string{"region": "eu", "level": "warning"}, [][2]string{{ns(3), "w1"}}},
		{map[string]string{"region": "us"}, [][2]string{{ns(4), "plain"}}},
		{map[string]string{"region": "us", "service": "api"}, [][2]string{{ns(5), "svc"}}},
	})
	if rr := do(t, h, "POST", "/loki/api/v1/push", body, "application/json"); rr.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rr.Code, rr.Body.String())
	}
	w.Close()
	window := "&start=" + ns(0) + "&end=" + ns(60)

	logLines := func(q string) []string {
		rr := do(t, h, "GET", "/loki/api/v1/query_range?query="+urlEncode(q)+window, "", "")
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", q, rr.Code, rr.Body.String())
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
		var out []string
		for _, s := range resp.Data.Result {
			for _, v := range s.Values {
				out = append(out, s.Stream["level"]+"/"+s.Stream["service"]+":"+v[1])
			}
		}
		sort.Strings(out)
		return out
	}
	for q, want := range map[string]string{
		`{level="error"}`:               "[error/:e1 error/:e2]",
		`{level="warning"}`:             "[warning/:w1]",
		`{region="eu"} | level="error"`: "[error/:e1 error/:e2]",
		`{service="api"}`:               "[/api:svc]",
		`{region="us", service!="api"}`: "[/:plain]",
		`{level="WARN"}`:                "[]",
	} {
		if got := fmt.Sprint(logLines(q)); got != want {
			t.Fatalf("%s: got %s, want %s", q, got, want)
		}
	}

	rr := do(t, h, "GET", "/loki/api/v1/query_range?query="+urlEncode(`sum by (level) (count_over_time({region=~".+"}[1m]))`)+window+"&step=60", "", "")
	var mresp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(rr.Body.Bytes(), &mresp)
	groups := map[string]any{}
	for _, s := range mresp.Data.Result {
		groups[s.Metric["level"]] = s.Values[len(s.Values)-1][1]
	}
	if got := fmt.Sprint(groups); got != "map[:2 error:2 warning:1]" {
		t.Fatalf("sum by (level): got %s; body %s", got, rr.Body.String())
	}

	for target, want := range map[string]string{
		"/loki/api/v1/labels":               `["level","region","service"]`,
		"/loki/api/v1/label/level/values":   `["error","warning"]`,
		"/loki/api/v1/label/service/values": `["api"]`,
	} {
		var resp struct{ Data json.RawMessage }
		json.Unmarshal(do(t, h, "GET", target, "", "").Body.Bytes(), &resp)
		if string(resp.Data) != want {
			t.Fatalf("%s: got %s, want %s", target, resp.Data, want)
		}
	}
}
