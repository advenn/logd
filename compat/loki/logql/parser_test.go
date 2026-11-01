package logql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/advenn/logd/compat/loki/logql"
)

// translateExpr parses and translates a log or metric expression, returning the translated
// predicates (as text, for comparison) or the first error.
func translateExpr(s string) (string, error) {
	ast, err := logql.ParseExpr(s)
	if err != nil {
		return "", err
	}
	switch e := ast.(type) {
	case *logql.Query:
		p, err := logql.Translate(e, nil)
		return fmt.Sprintf("%#v", p), err
	case *logql.MetricExpr:
		m, err := logql.TranslateMetric(e, nil)
		return fmt.Sprintf("%#v", m), err
	}
	return "", fmt.Errorf("unexpected %T", ast)
}

// Grafana's log-volume histogram query ends its pipeline in `| drop __error__`. logd never
// sets __error__, so dropping it is a no-op — but the stage was a parse error, so the
// histogram failed on every Explore query.
func TestDropErrorLabelIsNoOp(t *testing.T) {
	for _, c := range []struct{ with, without string }{
		{`sum by (level) (count_over_time({app="api"} | drop __error__[1m]))`, `sum by (level) (count_over_time({app="api"}[1m]))`},
		{`sum by (level, detected_level) (count_over_time({app="api"} |= "x" | drop __error__ [5m]))`, `sum by (level, detected_level) (count_over_time({app="api"} |= "x" [5m]))`},
		{`{app="api"} | drop __error__, __error_details__`, `{app="api"}`},
		{`{app="api"} | drop __error__="JSONParserErr" | region="eu"`, `{app="api"} | region="eu"`},
	} {
		got, err := translateExpr(c.with)
		if err != nil {
			t.Fatalf("%s: %v", c.with, err)
		}
		want, err := translateExpr(c.without)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s translated to\n %s\nwant (as without the drop)\n %s", c.with, got, want)
		}
	}
}

// Any other drop, and keep, would change the result set's labels, which logd does not
// implement: it must say so rather than ignore the stage.
func TestDropKeepOtherLabelsUnsupported(t *testing.T) {
	for _, q := range []string{
		`{app="api"} | drop level`,
		`{app="api"} | drop __error__, region`,
		`{app="api"} | keep app`,
		`{app="api"} | keep __error__`,
	} {
		_, err := translateExpr(q)
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("%s: err = %v, want a not-supported error", q, err)
		}
	}
	if _, err := logql.ParseExpr(`{app="api"} | drop`); err == nil {
		t.Fatal("drop without a label list must not parse")
	}
}

// Valid LogQL that the parser rejected: `and` and `,` joining label filters (both AND),
// `==` for `=`, and compound range durations.
func TestLabelFilterConjunctionsAndCompoundRanges(t *testing.T) {
	for _, c := range []struct{ q, same string }{
		{`{a="b"} | x > 1 and x < 5`, `{a="b"} | x > 1 | x < 5`},
		{`{a="b"} | region="eu", app="api"`, `{a="b"} | region="eu" | app="api"`},
		{`{a="b"} | region="eu", app="api" and x >= 2 |= "y"`, `{a="b"} | region="eu" | app="api" | x >= 2 |= "y"`},
		{`{a="b"} | x == 200`, `{a="b"} | x = 200`},
		{`count_over_time({a="b"} | x > 1 and x < 5 [5m])`, `count_over_time({a="b"} | x > 1 | x < 5 [5m])`},
		{`rate({a="b"}[1d12h])`, `rate({a="b"}[36h])`},
		{`rate({a="b"}[1h30m])`, `rate({a="b"}[90m])`},
		{`rate({a="b"}[1w2d])`, `rate({a="b"}[216h])`},
	} {
		got, err := translateExpr(c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		want, err := translateExpr(c.same)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s translated to\n %s\nwant (as %s)\n %s", c.q, got, c.same, want)
		}
	}
}

// What logd does not implement is refused with a message that says so: `or` between
// label filters (by design, §9.3: pipelines are AND; OR lives inside a regex), and
// duration or byte-size comparison values. `==` stays a label-filter operator only.
func TestUnsupportedLabelFilterFormsSayUnsupported(t *testing.T) {
	for _, q := range []string{
		`{a="b"} | x > 1 or x < 5`,
		`{a="b"} | d > 1s`,
		`{a="b"} | d >= 250ms`,
		`{a="b"} | size > 1KB`,
	} {
		_, err := translateExpr(q)
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("%s: err = %v, want a not-supported error", q, err)
		}
	}
	if _, err := logql.ParseExpr(`{a=="b"}`); err == nil {
		t.Fatal("`==` is not a selector operator")
	}
}

// An unterminated string is an error, not a value that runs to the end of the query — the
// lexer used to accept `|= "abc` as `|= "abc"`.
func TestUnterminatedStringRejected(t *testing.T) {
	for _, q := range []string{
		`{app="api"} |= "unterminated`,
		"{app=\"api\"} |= `unterminated",
		`{app='api}`,
		`{app="api\"}`,
	} {
		_, err := logql.ParseExpr(q)
		if err == nil || !strings.Contains(err.Error(), "unterminated") {
			t.Fatalf("%q: err = %v, want an unterminated-string error", q, err)
		}
	}
}

// Double-quoted strings are decoded with Go's escape rules, as Loki decodes them
// (strconv.Unquote): \x, \u, \U and octal escapes were left undecoded. An invalid escape
// is an error.
func TestStringEscapesDecodedAsGo(t *testing.T) {
	for q, want := range map[string]string{
		`{app="a\x41"}`:      "aA",
		`{app="caf\u00e9"}`:  "café",
		`{app="\U0001F600"}`: "\U0001F600",
		`{app="\101\102"}`:   "AB",
		`{app="q\"\\\t"}`:    "q\"\\\t",
		"{app=`a\\x41`}":     `a\x41`, // raw: no escapes
		`{app='it\'s "x"'}`:  `it's "x"`,
	} {
		ast, err := logql.ParseExpr(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := ast.(*logql.Query).Selector.Matchers[0].Value; got != want {
			t.Fatalf("%s: value %q, want %q", q, got, want)
		}
	}
	if _, err := logql.ParseExpr(`{app="a\q"}`); err == nil {
		t.Fatal(`an invalid escape (\q) must be an error`)
	}
}
