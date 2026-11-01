package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/advenn/logd/testbed/internal/pgdata"
)

func values(kv ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return v
}

func TestParseRequestReadsTheHarnessURL(t *testing.T) {
	// Exactly what bench-vs.sh sends: the variant in the URL, the rest via --data-urlencode.
	u, err := url.Parse("http://localhost:7300/loki/api/v1/query_range?variant=structured&query=latency_ms+%3E+200&start=100&end=200&limit=200000")
	if err != nil {
		t.Fatal(err)
	}
	r, err := parseRequest(u.Query(), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if r.dataset.Name != "app" || r.variant != pgdata.Structured || r.pred.Op != pgdata.OpGT || r.pred.Value != 200 {
		t.Errorf("got %+v", r)
	}
	if r.start != 100 || r.end != 200 || r.limit != 200000 || !r.backward {
		t.Errorf("window/limit/direction: got start=%d end=%d limit=%d backward=%v", r.start, r.end, r.limit, r.backward)
	}
}

func TestParseRequestDefaults(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	r, err := parseRequest(values("query", "latency_ms > 1", "variant", "unstructured"), now)
	if err != nil {
		t.Fatal(err)
	}
	if r.end != now.UnixNano() || r.start != now.Add(-time.Hour).UnixNano() {
		t.Errorf("default window: got [%d, %d)", r.start, r.end)
	}
	if r.limit != 100 || !r.backward {
		t.Errorf("default limit/direction: got %d, backward=%v", r.limit, r.backward)
	}
	r, err = parseRequest(values("query", "latency_ms > 1", "variant", "structured",
		"start", "2026-01-01T00:00:00Z", "end", "2026-01-01T00:00:01.5Z", "direction", "forward"), now)
	if err != nil {
		t.Fatal(err)
	}
	if r.backward || r.end-r.start != 1_500_000_000 {
		t.Errorf("RFC 3339 window / forward: got [%d, %d) backward=%v", r.start, r.end, r.backward)
	}
}

// TestParseRequestRejects: every parameter the shim uses is checked, because a request it
// half-understood would be timed as an answer to a different question.
func TestParseRequestRejects(t *testing.T) {
	ok := []string{"query", "latency_ms > 200", "variant", "structured"}
	with := func(kv ...string) url.Values { return values(append(append([]string{}, ok...), kv...)...) }
	for name, q := range map[string]url.Values{
		"no variant":       values("query", "latency_ms > 200"),
		"bad variant":      with("variant", "raw"),
		"no query":         values("variant", "structured"),
		"LogQL query":      with("query", `{app="checkout"} | latency_ms > 200`),
		"unknown dataset":  with("dataset", "nginx"),
		"limit zero":       with("limit", "0"),
		"limit negative":   with("limit", "-5"),
		"limit huge":       with("limit", "999999999"),
		"limit not number": with("limit", "all"),
		"bad start":        with("start", "yesterday"),
		"start after end":  with("start", "200", "end", "100"),
		"bad direction":    with("direction", "sideways"),
	} {
		if r, err := parseRequest(q, time.Now()); err == nil {
			t.Errorf("%s: accepted as %+v", name, r)
		}
	}
}

func TestGroupStreamsKeepsOrderAndShape(t *testing.T) {
	labels := map[int32]map[string]string{
		1: {"app": "hdfs", "component": "datanode"},
		2: {"app": "hdfs", "component": "fsnamesystem"},
	}
	rows := []row{
		{streamID: 2, tsNano: 30, line: "c"},
		{streamID: 1, tsNano: 20, line: "b"},
		{streamID: 2, tsNano: 10, line: "a"},
	}
	got, err := groupStreams(rows, func(id int32) (map[string]string, error) { return labels[id], nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Stream["component"] != "fsnamesystem" || got[1].Stream["component"] != "datanode" {
		t.Fatalf("streams not in first-seen order: %+v", got)
	}
	if v := got[0].Values; len(v) != 2 || v[0] != [2]string{"30", "c"} || v[1] != [2]string{"10", "a"} {
		t.Errorf("stream 2 values: %v", v)
	}

	// The harness's row counter: sum(len(r["values"]) for r in data.result).
	resp := response{Status: "success"}
	resp.Data.ResultType = "streams"
	resp.Data.Result = got
	raw, _ := json.Marshal(resp)
	var parsed struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range parsed.Data.Result {
		n += len(r.Values)
	}
	if parsed.Status != "success" || parsed.Data.ResultType != "streams" || n != 3 {
		t.Errorf("Loki shape: %s", raw)
	}
}

func TestGroupStreamsEmptyIsAnArray(t *testing.T) {
	got, err := groupStreams(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	if string(raw) != "[]" {
		t.Errorf("empty result serialises as %s, want []", raw)
	}
}

func TestStreamCacheReloadsOnlyOnMiss(t *testing.T) {
	loads := 0
	table := map[int32]map[string]string{1: {"app": "checkout"}}
	c := &streamCache{load: func(context.Context) (map[int32]map[string]string, error) {
		loads++
		out := map[int32]map[string]string{}
		for k, v := range table {
			out[k] = v
		}
		return out, nil
	}}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.get(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 1 {
		t.Errorf("loaded %d times for one id, want 1", loads)
	}
	table[2] = map[string]string{"app": "hdfs"} // the loader added a stream
	if l, err := c.get(ctx, 2); err != nil || l["app"] != "hdfs" {
		t.Errorf("new stream: %v %v", l, err)
	}
	if loads != 2 {
		t.Errorf("a new id should trigger exactly one reload, loads=%d", loads)
	}
	if _, err := c.get(ctx, 99); err == nil {
		t.Error("an id missing from the table resolved")
	}

	failing := &streamCache{load: func(context.Context) (map[int32]map[string]string, error) {
		return nil, errors.New("down")
	}}
	if _, err := failing.get(ctx, 1); err == nil {
		t.Error("a failed load resolved an id")
	}
}
