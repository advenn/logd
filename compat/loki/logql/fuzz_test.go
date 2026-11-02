package logql_test

import (
	"testing"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
)

// FuzzParseTranslate feeds arbitrary text to the LogQL parser, which every query endpoint
// hands a raw request parameter. Parsing must never panic; a query that parses must
// translate without panicking, with and without declared templates; and the predicates,
// labels and unwrap hooks it translates to must evaluate on a few lines without panicking,
// as they do for every record a query reads. Every typed comparison it translates to must
// also carry the field's declared kind, or the engine refuses the query (design §9.3).
//
// `go test` runs the seeds below (and any crashers kept in testdata/fuzz/). To search:
//
//	go test -run '^$' -fuzz '^FuzzParseTranslate$' -fuzztime 30s ./compat/loki/logql/
func FuzzParseTranslate(f *testing.F) {
	for _, s := range []string{
		``,
		`{`,
		`{}`,
		`{app="api"}`,
		`{app="api", region!="eu", env=~"prod|dev", x!~"y.*"}`,
		`{app='api'}`,
		"{app=`raw\\n`}",
		`{app="café"} |= "é"`,
		`{app="a\x41"} |= "\101"`,
		`{app="api"} |= "panic:" != "debug" |~ "err.*" !~ "(?i)ok"`,
		`{app="api"} | latency_ms > 200 | region = "eu"`,
		`{app="api"} | latency_ms >= 200.5`,
		`{app="api"} | dur_s > 2`,
		`{app="api"} | dur_s <= -1.5e3`,
		`{app="api"} | trip_id = "abc" | trip_id > "a"`,
		`{app="api"} | req_id = "550e8400-e29b-41d4-a716-446655440000"`,
		`{app="api"} | latency_ms = "250" | latency_ms =~ "2.."`,
		`{app="api"} | x > 1 and x < 5, y = "z"`,
		`{app="api"} | x > 1 or x < 5`,
		`{app="api"} | d > 1s | size > 1KB`,
		`{app="api"} | json`,
		`{app="api"} | json | status >= 500 | __error__ = ""`,
		`{app="api"} | json foo="bar", baz`,
		`{app="api"} | logfmt | level = "error"`,
		`{app="api"} | logfmt --strict --keep-empty`,
		`{app="api"} | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200`,
		`{app="api"} | pattern "<_> took <ms>ms <_>" | ms > 10`,
		`{app="api"} | pattern "<_>dur <dur_s>s<_>" | dur_s > 2`,
		`count_over_time({app="api"} | pattern "<_>req=<req_id><_>" | req_id = "550e8400-e29b-41d4-a716-446655440000" [5m])`,
		`{app="api"} | pattern "<a><b>"`,
		`{app="api"} | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms >= 200`,
		`{app="api"} | regexp "(?P<a>["`,
		`{app="api"} | json | drop __error__, __error_details__`,
		`{app="api"} | drop __error__="JSONParserErr" | region="eu"`,
		`{app="api"} | keep app`,
		`{app="api"} | line_format "{{.x}}"`,
		`{app="api"} | x > "NaN"`,
		`{app="api"} | dur_s > 1e400`,
		`rate({app="api"}[5m])`,
		`count_over_time({app="api"} |= "error" [1h])`,
		`sum by (region) (count_over_time({app="api"}[5m]))`,
		`sum without (pod) (rate({app="api"} | json [30s]))`,
		`sum_over_time({app="api"} | unwrap latency_ms [5m])`,
		`max_over_time({app="api"} | json | unwrap dur [1m])`,
		`avg by (level) (avg_over_time({app="api"} | logfmt | unwrap took [10m]))`,
		`rate({app="api"} | pattern "<_>took=<latency_ms>ms<_>" | unwrap latency_ms [1m])`,
		`topk(5, rate({app="api"}[5m]))`,
		`rate({app="api"}[0s])`,
		`rate({app="api"}[99999999999999999h])`,
		`vector(1)+vector(1)`,
		`{app="api"} |= "unterminated`,
		`{a="b"} @`,
	} {
		f.Add(s)
	}
	ex, err := extract.Compile(config.IndexConfig{
		Templates: []config.Template{
			{Name: "latency", Pattern: "took={ms:int}ms"},
			{Name: "dur", Pattern: "dur {s:float}s"},
			{Name: "trip", Pattern: "trip:{id:str}"},
			{Name: "req", Pattern: "req={id:uuid}"},
		},
		Literals: []string{"panic:"},
	})
	if err != nil {
		f.Fatal(err)
	}
	lines := []model.LogEntry{
		{Message: `level=error took=250ms dur 1.5s trip:abc req=550e8400-e29b-41d4-a716-446655440000`, Extra: `{"app":"api","region":"eu"}`},
		{Message: `{"status":500,"latency_ms":"12","nested":{"a":[1,2]},"msg":"took 3ms"}`, Extra: `{"app":"api"}`},
		{Message: `GET /x took 3ms status="a \"q\""`, Extra: ``},
		{Message: "\xff\x00 = \"", Extra: `{"level":"warn"}`},
	}
	f.Fuzz(func(t *testing.T, input string) {
		expr, err := logql.ParseExpr(input)
		if err != nil {
			return
		}
		for _, eng := range []*extract.Engine{nil, ex} {
			r := fuzzResolver{ex: eng}
			switch e := expr.(type) {
			case *logql.Query:
				_ = e.String() // /loki/api/v1/format_query prints it
				tr, err := logql.TranslateLog(e, eng)
				if err != nil {
					continue
				}
				checkKinds(t, input, tr.Preds, eng)
				for _, line := range lines {
					for _, p := range tr.Preds {
						p.Match(line, r)
					}
					tr.Labels(line)
				}
			case *logql.MetricExpr:
				mq, err := logql.TranslateMetric(e, eng)
				if err != nil {
					continue
				}
				checkKinds(t, input, mq.Preds, eng)
				for _, line := range lines {
					for _, p := range mq.Preds {
						p.Match(line, r)
					}
					if mq.SeriesLabels != nil {
						mq.SeriesLabels(line, r)
					}
					if mq.UnwrapValue != nil {
						mq.UnwrapValue(line, r)
					}
				}
			}
		}
	})
}

// checkKinds fails if a typed comparison's value is not of the kind ex declares for its
// field.
func checkKinds(t *testing.T, input string, preds []query.Predicate, ex *extract.Engine) {
	t.Helper()
	for _, p := range preds {
		tc, ok := p.(query.TypedCompare)
		if !ok {
			continue
		}
		if ex == nil {
			t.Fatalf("%q: a TypedCompare on %q without an extraction engine", input, tc.Field)
		}
		if kind, declared := ex.FieldKind(tc.Field); !declared || kind != tc.Value.Kind {
			t.Fatalf("%q: TypedCompare on %q has value kind %d, the field is declared %v (kind %d)", input, tc.Field, tc.Value.Kind, declared, kind)
		}
	}
}

// fuzzResolver resolves labels from a record's Extra and typed fields with an extraction
// engine, as the query engine's resolver does.
type fuzzResolver struct{ ex *extract.Engine }

func (r fuzzResolver) Label(e model.LogEntry, key string) (string, bool) {
	if v, ok := model.ExtraLabel(e.Extra, key); ok {
		return v, true
	}
	if key == "level" {
		return e.Level.String(), true
	}
	return "", false
}

func (r fuzzResolver) FieldValues(e model.LogEntry, field string) []index.Value {
	if r.ex == nil {
		return nil
	}
	var out []index.Value
	for _, fv := range r.ex.ExtractValues(e.Message) {
		if fv.Field == field {
			out = append(out, fv.Value)
		}
	}
	return out
}

func (r fuzzResolver) AllLabels(e model.LogEntry) map[string]string {
	return model.ParseExtraLabels(e.Extra)
}

var _ query.Resolver = fuzzResolver{}
