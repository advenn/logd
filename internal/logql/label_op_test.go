package logql

import (
	"testing"

	"github.com/advenn/logd/internal/storage"
)

// Selector and label-filter `!=`/`!~` must map to the label operators
// (OpNotEq/OpNotRe), not the line-filter Pipe variants — otherwise the
// label-filter evaluator falls through to its default "match everything".
func TestSelectorNotEqualOps(t *testing.T) {
	q, err := Parse(`{service!="api", host!~"db.*"}`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Op{"service": OpNotEq, "host": OpNotRe}
	for _, m := range q.Selector.Matchers {
		if want[m.Label] != m.Op {
			t.Errorf("matcher %q: got op %v (%s), want %v", m.Label, m.Op, m.Op, want[m.Label])
		}
	}
}

// Bare numeric values in label filters must lex and parse (e.g. `latency > 100`),
// and compare numerically against the resolved label value.
func TestLabelFilterNumericValue(t *testing.T) {
	q, err := Parse(`{a="b"} | logfmt | latency > 100`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	pipe, err := CompilePipeline(q)
	if err != nil {
		t.Fatal(err)
	}
	// logfmt extracts latency from the message; 150 passes > 100, 20 does not.
	if !pipe.Match(storage.LogEntry{Message: "latency=150"}) {
		t.Error("latency=150 should pass latency > 100")
	}
	if pipe.Match(storage.LogEntry{Message: "latency=20"}) {
		t.Error("latency=20 should NOT pass latency > 100")
	}
}

func TestLabelFilterNotEqualEvaluates(t *testing.T) {
	pipe, err := CompilePipeline(&Query{Pipeline: []PipelineStage{
		&LabelFilter{Label: "service", Op: OpNotEq, Value: "api"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	api := storage.LogEntry{Extra: `{"service":"api"}`}
	web := storage.LogEntry{Extra: `{"service":"web"}`}
	if pipe.Match(api) {
		t.Error(`service!="api" must NOT match service=api`)
	}
	if !pipe.Match(web) {
		t.Error(`service!="api" must match service=web`)
	}
}
