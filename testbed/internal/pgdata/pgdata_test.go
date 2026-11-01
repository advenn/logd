package pgdata

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestParsePredicateAcceptsTheWhitelistedForms(t *testing.T) {
	cases := []struct {
		in    string
		op    Op
		value int64
	}{
		{"latency_ms > 200", OpGT, 200},
		{"latency_ms>200", OpGT, 200},
		{"  latency_ms\t>=\t1000  ", OpGE, 1000},
		{"latency_ms < 5", OpLT, 5},
		{"latency_ms <= 0", OpLE, 0},
		{"latency_ms = -3", OpEQ, -3},
		{"latency_ms > 007", OpGT, 7},
		{"latency_ms > 9223372036854775807", OpGT, 9223372036854775807},
	}
	for _, c := range cases {
		p, err := ParsePredicate(c.in)
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.in, err)
			continue
		}
		if p.Field != "latency_ms" || p.Op != c.op || p.Value != c.value {
			t.Errorf("%q: got %+v, want latency_ms %s %d", c.in, p, c.op, c.value)
		}
	}
}

// TestParsePredicateRejectsEverythingElse is the shim's safety property. Each of these is
// either an injection attempt, a LogQL-ism the shim does not implement, or a number the
// threshold column cannot hold. None may parse, because a predicate that half-parses and
// silently drops the rest would be timed as if it had answered the question.
func TestParsePredicateRejectsEverythingElse(t *testing.T) {
	for _, in := range []string{
		"",
		"   ",
		"latency_ms",
		"latency_ms >",
		"> 200",
		"200 < latency_ms",
		"latency_ms > 200; DROP TABLE logs",
		"latency_ms > 200 OR 1=1",
		"latency_ms > 200 AND latency_ms < 300",
		"latency_ms > (200)",
		"latency_ms > '200'",
		"latency_ms > 2e3",
		"latency_ms > 2.5",
		"latency_ms > +200",
		"latency_ms > --200",
		"latency_ms > 0x10",
		"latency_ms >> 200",
		"latency_ms == 200",
		"latency_ms != 200",
		"latency_ms <> 200",
		"latency_ms ! 200",
		"latency_ms > 99999999999999999999",
		"latency_ms > ２００", // full-width digits
		"LATENCY_MS > 200",
		"line > 200",
		"ts_ns > 200",
		"latency_ms\n> 200",
		`{app="checkout"} | latency_ms > 200`,
		"latency_ms > 200 -- comment",
		"latency_ms > 200\x00",
	} {
		if p, err := ParsePredicate(in); err == nil {
			t.Errorf("%q parsed as %+v; want an error", in, p)
		}
	}
}

// TestQuerySQLContainsNoRequestData pins that a statement is a function of (dataset, variant,
// operator, direction) only. The threshold reaches PostgreSQL as $1, never as text, so the
// same statement text serves every threshold.
func TestQuerySQLContainsNoRequestData(t *testing.T) {
	allowed := regexp.MustCompile(`^SELECT stream_id, ts_ns, line FROM [a-z_]+ WHERE (line LIKE '%[^%'_]+%' AND )?.+ (>|>=|<|<=|=) \$1::bigint AND ts_ns >= \$2 AND ts_ns < \$3 ORDER BY ts_ns (ASC|DESC) LIMIT \$4$`)
	for _, d := range All() {
		for _, v := range []Variant{Structured, Unstructured} {
			for op := range sqlOps {
				for _, backward := range []bool{true, false} {
					sql, err := d.QuerySQL(v, op, backward)
					if err != nil {
						t.Fatalf("%s/%s/%s: %v", d.Name, v, op, err)
					}
					if !allowed.MatchString(sql) {
						t.Errorf("%s/%s/%s: unexpected statement shape:\n%s", d.Name, v, op, sql)
					}
					if !strings.Contains(sql, " "+d.Table(v)+" ") {
						t.Errorf("%s/%s: statement does not read %s:\n%s", d.Name, v, d.Table(v), sql)
					}
					if v == Unstructured && !strings.Contains(sql, d.Expr) {
						t.Errorf("%s: unstructured statement does not extract with Expr:\n%s", d.Name, sql)
					}
				}
			}
		}
	}
	if _, err := All()[0].QuerySQL(Structured, Op("; DROP"), true); err == nil {
		t.Error("an operator outside the whitelist produced a statement")
	}
	if _, err := All()[0].QuerySQL(Variant("x"), OpGT, true); err == nil {
		t.Error("an unknown variant produced a statement")
	}
}

// TestSchemaMatchesDatasets holds the Go description to the SQL that actually created the
// tables. The unstructured variant's query-time extraction must be the generated column's
// expression verbatim, or the two variants are not answering the same question.
func TestSchemaMatchesDatasets(t *testing.T) {
	raw, err := os.ReadFile("../../stack/postgres/schema.sql")
	if err != nil {
		t.Fatalf("reading schema: %v", err)
	}
	schema := string(raw)
	for _, d := range All() {
		gen := d.Field + " "
		idx := strings.Index(schema, "CREATE TABLE "+d.Structured+" (")
		if idx < 0 {
			t.Errorf("%s: schema has no table %s", d.Name, d.Structured)
			continue
		}
		body := schema[idx:]
		body = body[:strings.Index(body, ");")]
		want := "GENERATED ALWAYS AS (" + d.Expr + ") STORED"
		line := ""
		for _, l := range strings.Split(body, "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), gen) {
				line = l
			}
		}
		if !strings.Contains(line, want) {
			t.Errorf("%s: generated column %s is\n  %s\nwant it to contain\n  %s", d.Name, d.Field, strings.TrimSpace(line), want)
		}
		if !strings.Contains(schema, "CREATE TABLE "+d.Unstructured+" (") {
			t.Errorf("%s: schema has no table %s", d.Name, d.Unstructured)
		}
		if strings.ContainsAny(d.Contains, `%_'\`) {
			t.Errorf("%s: Contains %q has LIKE or quoting metacharacters", d.Name, d.Contains)
		}
	}
}

func TestParseVariant(t *testing.T) {
	for _, ok := range []string{"structured", "unstructured"} {
		if _, err := ParseVariant(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Structured", "raw", "structured;"} {
		if _, err := ParseVariant(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
