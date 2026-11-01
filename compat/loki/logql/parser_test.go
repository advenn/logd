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
