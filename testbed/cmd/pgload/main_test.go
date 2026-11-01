package main

import (
	"testing"

	"github.com/advenn/logd/testbed/internal/loggen"
)

// TestManifestValueReadsBothSchemas: the loader's cross-check (rows the generated column
// extracted a value from == rows the manifest says carry one) is only as good as its reading
// of the manifest, which has corpus mode's latency_ms and replay mode's value/value_name.
func TestManifestValueReadsBothSchemas(t *testing.T) {
	ms, size := 247, int64(67108864)
	for name, c := range map[string]struct {
		r     loggen.Record
		field string
		want  int64
		ok    bool
	}{
		"corpus latency":           {loggen.Record{LatencyMS: &ms}, "latency_ms", 247, true},
		"corpus without latency":   {loggen.Record{}, "latency_ms", 0, false},
		"replay value":             {loggen.Record{Value: &size, ValueName: "size_bytes"}, "size_bytes", size, true},
		"replay value, other name": {loggen.Record{Value: &size, ValueName: "size_bytes"}, "latency_ms", 0, false},
		"replay without value":     {loggen.Record{}, "size_bytes", 0, false},
	} {
		got, ok := manifestValue(c.r, c.field)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: got %d %v, want %d %v", name, got, ok, c.want, c.ok)
		}
	}
}
