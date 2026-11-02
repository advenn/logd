package logql_test

import (
	"strings"
	"testing"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/query"
)

// testbedEngine is the testbed's extraction config (testbed/stack/logd/logd.yaml).
func testbedEngine(t *testing.T) *extract.Engine {
	t.Helper()
	ex, err := extract.Compile(config.IndexConfig{Templates: []config.Template{
		{Name: "latency", Pattern: "took={ms:int}ms"},
		{Name: "status", Pattern: "status={code:int} "},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return ex
}

func translateLog(t *testing.T, q string, ex *extract.Engine) *logql.Translation {
	t.Helper()
	ast, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	tr, err := logql.TranslateLog(ast, ex)
	if err != nil {
		t.Fatalf("translate %q: %v", q, err)
	}
	return tr
}

// The four stages parse, print back as they were written, and keep a label filter after
// them; a keyword used as a label name is still a label filter.
func TestParseParserStages(t *testing.T) {
	for _, q := range []string{
		`{app="a"} | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200`,
		`{app="a"} | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms >= 200.5`,
		`{app="a"} | logfmt | level = "error"`,
		`{app="a"} | json | status = 200 |= "x"`,
		`{app="a"} | json | drop __error__`,
	} {
		ast, err := logql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := ast.String(); got != q {
			t.Errorf("printed %s\n   as %s", q, got)
		}
		again, err := logql.Parse(ast.String())
		if err != nil || again.String() != ast.String() {
			t.Errorf("%s does not round-trip: %v", q, err)
		}
	}
	ast, err := logql.Parse("{app=\"a\"} | pattern `<_> <x>` | regexp `(?P<y>\\d)`")
	if err != nil {
		t.Fatal(err)
	}
	if ps := ast.Pipeline[0].(*logql.ParserStage); ps.Parser != "pattern" || ps.Expr != "<_> <x>" {
		t.Fatalf("backtick pattern: %+v", ps)
	}
	if ps := ast.Pipeline[1].(*logql.ParserStage); ps.Parser != "regexp" || ps.Expr != `(?P<y>\d)` {
		t.Fatalf("backtick regexp: %+v", ps)
	}
	ast, err = logql.Parse(`{app="a"} | pattern = "x" | json != "y"`)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := ast.Pipeline[0].(*logql.LabelFilter); !ok || f.Label != "pattern" {
		t.Fatalf("`| pattern = \"x\"` is a label filter, got %#v", ast.Pipeline[0])
	}
}

// Loki forms logd does not implement are refused by name, not misread.
func TestUnsupportedParserFormsSayUnsupported(t *testing.T) {
	for _, q := range []string{
		`{a="b"} | json foo="bar"`,
		`{a="b"} | json foo, bar`,
		`{a="b"} | logfmt foo`,
		`{a="b"} | logfmt --strict`,
		`{a="b"} | logfmt --keep-empty`,
		`{a="b"} | line_format "{{.x}}"`,
		`{a="b"} | label_format a=b`,
		`{a="b"} | unpack`,
		`{a="b"} | decolorize`,
	} {
		_, err := translateExpr(q)
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("%s: err = %v, want a not-supported error", q, err)
		}
	}
	for _, q := range []string{
		`{a="b"} | pattern`,
		`{a="b"} | pattern "no capture"`,
		`{a="b"} | pattern "<a><b>"`,
		`{a="b"} | regexp "no group"`,
		`{a="b"} | regexp "(?P<a>["`,
		`{a="b"} | regexp "(unnamed)"`,
		`{a="b"} | json | x > "abc"`,
	} {
		if _, err := translateExpr(q); err == nil {
			t.Errorf("%s: accepted", q)
		}
	}
}

// After a parser stage, a filter on a name the stage may extract is a DynamicCompare whose
// comparison follows Loki's grammar; a filter on any other name translates as without the
// stage (here: a pushable label equality, and the native typed field).
func TestTranslateFiltersAfterParserStage(t *testing.T) {
	ex := testbedEngine(t)
	tr := translateLog(t, `{app="a"} | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms > 200 | region = "eu" | status_code >= 500 | latency_ms = 250 | latency_ms = "250" | latency_ms =~ "2.."`, ex)
	if _, ok := tr.Preds[0].(query.LabelEqual); !ok {
		t.Fatalf("selector: %#v", tr.Preds[0])
	}
	dc := func(i int) query.DynamicCompare {
		t.Helper()
		d, ok := tr.Preds[i].(query.DynamicCompare)
		if !ok {
			t.Fatalf("pred %d is %T, want DynamicCompare", i, tr.Preds[i])
		}
		return d
	}
	if d := dc(1); !d.Numeric || d.Op != query.OpGt || d.Value != "200" || d.Name != "latency_ms" {
		t.Fatalf("latency_ms > 200: %+v", d)
	}
	if p, ok := tr.Preds[2].(query.LabelEqual); !ok || p.Key != "region" {
		t.Fatalf("region is not extracted, so it stays a label equality: %#v", tr.Preds[2])
	}
	if p, ok := tr.Preds[3].(query.TypedCompare); !ok || p.Field != "status_code" {
		t.Fatalf("status_code is not extracted, so it stays the typed field: %#v", tr.Preds[3])
	}
	if d := dc(4); !d.Numeric || d.Op != query.OpEq {
		t.Fatalf("= 250 is numeric: %+v", d)
	}
	if d := dc(5); d.Numeric || d.Op != query.OpEq {
		t.Fatalf(`= "250" is a string comparison: %+v`, d)
	}
	if d := dc(6); d.Re == nil || d.Re.String() != "^(?:2..)$" {
		t.Fatalf("=~ is an anchored regex: %+v", d)
	}
	kinds := []string{}
	for _, m := range tr.Mappings {
		kinds = append(kinds, m.Kind)
	}
	if got := strings.Join(kinds, ","); got != "dynamic,dynamic,label,typed,dynamic,dynamic,dynamic" {
		t.Fatalf("mappings: %s (%+v)", got, tr.Mappings)
	}

	// json and logfmt may extract any name, so every filter after them is dynamic, even on a
	// template field; __error__ is dynamic after json only.
	tr = translateLog(t, `{app="a"} | json | latency_ms > 200 | __error__ = ""`, ex)
	for i := 1; i < 3; i++ {
		if _, ok := tr.Preds[i].(query.DynamicCompare); !ok {
			t.Fatalf("after json, pred %d is %T", i, tr.Preds[i])
		}
	}
	tr = translateLog(t, `{app="a"} | logfmt | __error__ = ""`, ex)
	if _, ok := tr.Preds[1].(query.LabelEqual); !ok {
		t.Fatalf("logfmt never sets __error__, so its filter is the old no-op: %T", tr.Preds[1])
	}
	// Filters before the parser stage are unchanged: the native typed field.
	tr = translateLog(t, `{app="a"} | latency_ms > 200 | json`, ex)
	if _, ok := tr.Preds[1].(query.TypedCompare); !ok {
		t.Fatalf("a filter before the stage: %T", tr.Preds[1])
	}
}

// A query without a parser stage translates exactly as before, and its results carry only
// their own labels.
func TestTranslateWithoutParserStageUnchanged(t *testing.T) {
	ex := testbedEngine(t)
	tr := translateLog(t, `{app="a"} | latency_ms > 200 | region = "eu"`, ex)
	if tr.Parses() || len(tr.Mappings) != 0 {
		t.Fatalf("no parser stage, yet Parses=%v mappings=%+v", tr.Parses(), tr.Mappings)
	}
	if tc, ok := tr.Preds[1].(query.TypedCompare); !ok || tc.Value != (index.Value{Kind: index.KindInt, Int: 200}) {
		t.Fatalf("native typed field: %#v", tr.Preds[1])
	}
}

// TranslateMetric wires the stage into the metric query: series labels include extracted
// labels (but not the unwrapped one), and an extracted unwrap is read dynamically.
func TestTranslateMetricWithParserStage(t *testing.T) {
	ex := testbedEngine(t)
	ast, err := logql.ParseExpr(`sum by (method) (avg_over_time({app="a"} | regexp "(?P<method>[A-Z]+) took=(?P<latency_ms>[0-9]+)ms" | unwrap latency_ms [5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	mq, err := logql.TranslateMetric(ast.(*logql.MetricExpr), ex)
	if err != nil {
		t.Fatal(err)
	}
	if mq.SeriesLabels == nil || mq.UnwrapValue == nil || mq.Unwrap != "latency_ms" {
		t.Fatalf("hooks not set: %+v", mq)
	}
	ast, _ = logql.ParseExpr(`avg_over_time({app="a"} | unwrap latency_ms [5m])`)
	mq, _ = logql.TranslateMetric(ast.(*logql.MetricExpr), ex)
	if mq.UnwrapValue != nil || mq.SeriesLabels != nil {
		t.Fatalf("no parser stage, no hooks: %+v", mq)
	}
}
