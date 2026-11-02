package logql_test

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki/logql"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// The tests in this file run translated queries through a real engine and compare them with
// expectations computed here, from the corpus, by straightforward reference code — Go's
// regexp, strconv and encoding/json — that shares nothing with the code under test.

var semBase = time.Unix(1700000000, 0).UTC()

type corpusLine struct {
	app, msg string
}

// mixedCorpus has unstructured lines in the testbed's shape (including every non-conforming
// variant of took=), logfmt lines and JSON lines (some invalid), in two apps.
func mixedCorpus() []corpusLine {
	var out []corpusLine
	add := func(app, msg string) { out = append(out, corpusLine{app, msg}) }
	for i := 0; i < 240; i++ {
		ms := (i * 47) % 1300
		app := []string{"checkout", "billing"}[(i/12)%2]
		switch i % 12 {
		case 0, 1, 2:
			add(app, fmt.Sprintf("GET /api/orders/%d 200 took=%dms trace_id=%x", i, ms, i*7919))
		case 3:
			add(app, fmt.Sprintf("upstream slow response took=%dms threshold=500ms", ms))
		case 4:
			add(app, fmt.Sprintf("lock contention waited=%dms", ms))
		case 5:
			add(app, fmt.Sprintf("level=info dur=%d path=/x/%d msg=\"handled request %d\"", ms, i%5, i))
		case 6:
			add(app, fmt.Sprintf(`{"level":"warn","dur":%d,"req":{"path":"/y/%d","method":"POST"},"tags":["a"]}`, ms, (i/24)%3))
		case 7:
			add(app, fmt.Sprintf(`{"level":"error","dur":"%d.5"`, ms)) // truncated JSON
		case 8:
			add(app, fmt.Sprintf("took=%dms first, then took=%dms", ms%250, ms)) // two occurrences
		case 9:
			add(app, []string{"took=12.5ms", "took=abcms", "took=7", "took=5 ms", "took=ms"}[(i/12)%5])
		case 10:
			add(app, fmt.Sprintf("level=error dur=%d.25 user=u%d", ms, i))
		default:
			add(app, "no duration on this line")
		}
	}
	add("checkout", "took=9007199254740993ms") // beyond 2^53
	return out
}

// buildEngine ingests a corpus across several sealed segments and an active one and returns
// the engine; the writer stays open, so the active segment is part of every query.
func buildEngine(t *testing.T, ex *extract.Engine, lines []corpusLine) *query.Engine {
	t.Helper()
	dir := t.TempDir()
	w, err := storage.NewWriter(dir, storage.Options{Schema: ex.IndexedFields(), SegmentSizeBytes: 3 * storage.PageSize, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	t.Cleanup(func() { w.Close() })
	ig := ingest.NewWithLabels(ex, w, []string{"app"})
	for i, l := range lines {
		e := model.LogEntry{TS: semBase.Add(time.Duration(i) * time.Millisecond), Level: model.LogLevelInfo,
			Extra: fmt.Sprintf(`{"app":%q}`, l.app), Message: l.msg}
		if err := ig.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	eng := query.NewEngineWithLabels(w, ex, []string{"app"})
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := eng.ExecuteScan(fullRange(nil, len(lines)))
		if err == nil && len(got) == len(lines) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records became visible", len(got), len(lines))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A rotation the last flush made due happens on a later tick; let it, so the segment
	// layout is settled before the tests compare plans.
	time.Sleep(50 * time.Millisecond)
	sealed := 0
	for _, p := range eng.Explain(fullRange(nil, 1)) {
		if p.Reason != query.ReasonNotIndexed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Fatal("the corpus sealed no segment; the index path would go untested")
	}
	return eng
}

func fullRange(preds []query.Predicate, n int) query.Query {
	return query.Query{Start: semBase.UnixNano(), End: semBase.Add(time.Hour).UnixNano(), Preds: preds, Limit: n + 10}
}

// run executes a LogQL log query both ways, insists they agree, and returns the messages.
func run(t *testing.T, eng *query.Engine, ex *extract.Engine, q string, n int) []string {
	t.Helper()
	preds := translate(t, q, ex)
	idx, err := eng.Execute(fullRange(preds, n))
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	scan, err := eng.ExecuteScan(fullRange(preds, n))
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if a, b := keys(idx), keys(scan); a != b {
		t.Fatalf("%s: Execute and ExecuteScan disagree\n index: %s\n  scan: %s", q, a, b)
	}
	return messages(idx)
}

func keys(es []model.LogEntry) string {
	var b strings.Builder
	for _, e := range es {
		fmt.Fprintf(&b, "%d|%s|%s\n", e.TS.UnixNano(), e.Extra, e.Message)
	}
	return b.String()
}

func messages(es []model.LogEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Message)
	}
	sort.Strings(out)
	return out
}

// want filters the corpus with a reference predicate.
func want(lines []corpusLine, app string, keep func(msg string) bool) []string {
	var out []string
	for _, l := range lines {
		if l.app == app && keep(l.msg) {
			out = append(out, l.msg)
		}
	}
	sort.Strings(out)
	return out
}

func sameSet(t *testing.T, q string, got, exp []string) {
	t.Helper()
	if len(exp) == 0 {
		t.Fatalf("%s: the reference expects no rows; the case tests nothing", q)
	}
	if strings.Join(got, "\n") != strings.Join(exp, "\n") {
		t.Fatalf("%s: %d rows, the reference %d\n got: %q\nwant: %q", q, len(got), len(exp), got, exp)
	}
}

// Reference extractors.
var (
	refTemplate = regexp.MustCompile(`took=([+-]?[0-9]+)ms`) // the template, every occurrence
	refPattern  = regexp.MustCompile(`took=(.*?)(?:ms|$)`)   // Loki's <_>took=<x>ms<_>: the first
	refRegexp   = regexp.MustCompile(`took=([0-9]+)ms`)      // the regexp stage itself
	refLogfmt   = regexp.MustCompile(`(?:^|\s)dur=([^\s="]+)(?:\s|$)`)
	refJSONDur  = regexp.MustCompile(`"dur":"?([0-9.]+)`)
)

// templateAny is the template's semantics: some occurrence's int satisfies cmp.
func templateAny(msg string, cmp func(*big.Int) bool) bool {
	for _, m := range refTemplate.FindAllStringSubmatch(msg, -1) {
		if n, err := strconv.ParseInt(m[1], 10, 64); err == nil && cmp(big.NewInt(n)) {
			return true
		}
	}
	return false
}

// numericFirst is Loki's numeric label filter on the first match of re, with logd's rule for
// a value that does not convert: no match.
func numericFirst(re *regexp.Regexp, msg string, cmp func(float64) bool) bool {
	m := re.FindStringSubmatch(msg)
	if m == nil {
		return false
	}
	f, err := strconv.ParseFloat(m[1], 64)
	return err == nil && cmp(f)
}

func jsonObject(msg string) (map[string]any, bool) {
	var m map[string]any
	d := json.NewDecoder(strings.NewReader(msg))
	d.UseNumber()
	if err := d.Decode(&m); err != nil {
		return nil, false
	}
	return m, true
}

// Every stage, against the reference, Execute equal to ExecuteScan throughout.
func TestStagesMatchReference(t *testing.T) {
	ex := testbedEngine(t)
	lines := mixedCorpus()
	eng := buildEngine(t, ex, lines)
	gt := func(b float64) func(float64) bool { return func(f float64) bool { return f > b } }
	ge := func(b float64) func(float64) bool { return func(f float64) bool { return f >= b } }

	for _, c := range []struct {
		q    string
		keep func(string) bool
	}{
		// pattern equal to the template: the template's semantics, i.e. the native query's.
		{`{app="checkout"} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200`,
			func(m string) bool {
				return templateAny(m, func(n *big.Int) bool { return n.Cmp(big.NewInt(200)) > 0 })
			}},
		{`{app="checkout"} | latency_ms > 200`,
			func(m string) bool {
				return templateAny(m, func(n *big.Int) bool { return n.Cmp(big.NewInt(200)) > 0 })
			}},
		// a pattern that is not the template (renamed capture): Loki's first-occurrence capture.
		{`{app="checkout"} | pattern "<_>took=<ms>ms<_>" | ms > 200`,
			func(m string) bool { return numericFirst(refPattern, m, gt(200)) }},
		{`{app="checkout"} | pattern "<_>took=<ms>ms<_>" | ms <= 12.5`,
			func(m string) bool { return numericFirst(refPattern, m, func(f float64) bool { return f <= 12.5 }) }},
		// regexp: the leftmost match's group.
		{`{app="checkout"} |= "took=" | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms > 200`,
			func(m string) bool { return numericFirst(refRegexp, m, gt(200)) }},
		{`{app="checkout"} | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms =~ "1.."`,
			func(m string) bool {
				s := refRegexp.FindStringSubmatch(m)
				return s != nil && regexp.MustCompile(`^1..$`).MatchString(s[1])
			}},
		// logfmt
		{`{app="checkout"} | logfmt | dur >= 300`,
			func(m string) bool { return numericFirst(refLogfmt, m, ge(300)) }},
		{`{app="checkout"} | logfmt | level = "error"`,
			func(m string) bool { return strings.HasPrefix(m, "level=error ") }},
		{`{app="checkout"} | logfmt | level != "info"`, // missing reads as "", which is != "info"
			func(m string) bool { return !strings.HasPrefix(m, "level=info ") }},
		// json: numbers, strings and nested names; errors.
		// A line that is not valid JSON keeps the labels read before the error: the
		// truncated lines still have their dur.
		{`{app="checkout"} | json | dur > 300`,
			func(m string) bool { return strings.HasPrefix(m, "{") && numericFirst(refJSONDur, m, gt(300)) }},
		{`{app="checkout"} | json | req_path = "/y/1"`,
			func(m string) bool {
				o, ok := jsonObject(m)
				req, _ := o["req"].(map[string]any)
				return ok && req["path"] == "/y/1"
			}},
		{`{app="checkout"} | json | __error__ = ""`,
			func(m string) bool { _, ok := jsonObject(m); return ok }},
		{`{app="checkout"} | json | __error__ = "JSONParserErr"`,
			func(m string) bool { _, ok := jsonObject(m); return !ok }},
		{`{app="checkout"} | json | drop __error__ | __error__ = ""`,
			func(string) bool { return true }},
		// the record's own label wins over an extracted one of the same name.
		{`{app="checkout"} | json | app = "checkout"`,
			func(string) bool { return true }},
	} {
		sameSet(t, c.q, run(t, eng, ex, c.q, len(lines)), want(lines, "checkout", c.keep))
	}
}

// The structural rule's two sides, line by line: on every conforming line the template's
// answer equals Loki's for every bound, and they differ exactly on the non-conforming
// lines, which structural.go lists. The query itself answers with the template's semantics.
func TestStructuralMatchDiffersFromLokiOnlyOnNonConformingLines(t *testing.T) {
	ex := testbedEngine(t)
	lines := mixedCorpus()
	eng := buildEngine(t, ex, lines)

	// Loki's real behaviour for `| pattern "<_>took=<latency_ms>ms<_>" | latency_ms op b`:
	// the first capture; a missing label drops the line; a value that does not convert
	// KEEPS it, flagged __error__=LabelFilterErr (label_filter.go, NumericLabelFilter).
	loki := func(msg string, cmp func(float64) bool) bool {
		m := refPattern.FindStringSubmatch(msg)
		if m == nil {
			return false
		}
		f, err := strconv.ParseFloat(m[1], 64)
		return err != nil || cmp(f)
	}
	// Conforming, as structural.go defines it, within the ±2^53 the argument assumes.
	conforming := func(msg string) bool {
		first := strings.Index(msg, "took=")
		if first < 0 {
			return true
		}
		ms := refTemplate.FindAllStringSubmatchIndex(msg, -1)
		if len(ms) != 1 || ms[0][0] != first {
			return false
		}
		n, _ := strconv.ParseInt(msg[ms[0][2]:ms[0][3]], 10, 64)
		return n <= 1<<53 && n >= -(1<<53)
	}
	type bound struct {
		op string
		b  string
		f  func(float64) bool
		i  func(*big.Int) bool
	}
	big2e53 := new(big.Int).Lsh(big.NewInt(1), 53)
	bounds := []bound{
		{">", "200", func(f float64) bool { return f > 200 }, func(n *big.Int) bool { return n.Cmp(big.NewInt(200)) > 0 }},
		{">", "10", func(f float64) bool { return f > 10 }, func(n *big.Int) bool { return n.Cmp(big.NewInt(10)) > 0 }},
		{"<", "10", func(f float64) bool { return f < 10 }, func(n *big.Int) bool { return n.Cmp(big.NewInt(10)) < 0 }},
		{"=", "247", func(f float64) bool { return f == 247 }, func(n *big.Int) bool { return n.Cmp(big.NewInt(247)) == 0 }},
		{">", big2e53.String(), func(f float64) bool { return f > 9007199254740992 }, func(n *big.Int) bool { return n.Cmp(big2e53) > 0 }},
	}

	differs := map[string]bool{}
	for _, b := range bounds {
		q := fmt.Sprintf(`{app="checkout"} | pattern "<_>took=<latency_ms>ms<_>" | latency_ms %s %s`, b.op, b.b)
		got := run(t, eng, ex, q, len(lines))
		exp := want(lines, "checkout", func(m string) bool { return templateAny(m, b.i) })
		if strings.Join(got, "\n") != strings.Join(exp, "\n") {
			t.Fatalf("%s: the structural match must answer with the template's semantics\n got: %q\nwant: %q", q, got, exp)
		}
		for _, l := range lines {
			if l.app != "checkout" {
				continue
			}
			if templateAny(l.msg, b.i) != loki(l.msg, b.f) {
				if conforming(l.msg) {
					t.Errorf("conforming line %q: the template and Loki disagree on latency_ms %s %s", l.msg, b.op, b.b)
				}
				differs[l.msg] = true
			}
		}
	}
	// Every kind of non-conforming line shows up as a difference.
	for _, prefix := range []string{
		"took=12.5ms",             // not an int: Loki compares 12.5
		"took=abcms",              // not a number: Loki keeps it, flagged
		"took=7",                  // no "ms" after it: Loki's capture runs to the end of the line
		"took=5 ms",               // displaced literal: Loki's capture is "5 ", which does not convert
		"took=ms",                 // empty capture: Loki keeps it, flagged
		"took=9007199254740993ms", // beyond 2^53: Loki rounds
	} {
		found := false
		for m := range differs {
			if m == prefix {
				found = true
			}
		}
		if !found {
			t.Errorf("no bound separated the template from Loki on %q", prefix)
		}
	}
	multi := 0
	for m := range differs {
		if strings.Contains(m, "first, then") {
			multi++
		}
	}
	if multi == 0 {
		t.Error("no line with two occurrences separated the template from Loki")
	}
	for m := range differs {
		if conforming(m) {
			t.Errorf("the template and Loki differ on the conforming line %q", m)
		}
	}
}

// The structural match changes the access path, not the answer: the pattern form returns
// exactly what the native form does, uses the index where the native form does, and the
// unmatched forms scan.
func TestStructuralMatchUsesIndex(t *testing.T) {
	ex := testbedEngine(t)
	lines := mixedCorpus()
	eng := buildEngine(t, ex, lines)
	indexed := func(q string) int {
		n := 0
		for _, p := range eng.Explain(fullRange(translate(t, q, ex), 1)) {
			if p.Mode == "index" {
				n++
			}
		}
		return n
	}
	native := `{app="checkout"} | latency_ms > 1000`
	pattern := `{app="checkout"} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 1000`
	if a, b := run(t, eng, ex, native, len(lines)), run(t, eng, ex, pattern, len(lines)); strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatalf("pattern form %q\nnative form %q", b, a)
	}
	if indexed(native) == 0 || indexed(pattern) != indexed(native) {
		t.Fatalf("index use: native %d segments, pattern %d", indexed(native), indexed(pattern))
	}
	for _, q := range []string{
		`{app="checkout"} |= "took=" | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms > 1000`,
		`{app="checkout"} | pattern "<_>took=<ms>ms<_>" | ms > 1000`,
	} {
		// Only the label index can be used (app="checkout"), never the typed one: per segment
		// the plan is exactly that of the bare selector.
		// (The open writer's flush tick may seal the active segment between two plans, so a
		// difference is retried once.)
		for attempt := 0; ; attempt++ {
			a := fmt.Sprint(eng.Explain(fullRange(translate(t, q, ex), 1)))
			b := fmt.Sprint(eng.Explain(fullRange(translate(t, `{app="checkout"}`, ex), 1)))
			if a == b {
				break
			}
			if attempt > 0 {
				t.Fatalf("%s: plan %s\nthe selector alone: %s", q, a, b)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Metric queries over parser stages: series split by extracted labels, an extracted unwrap,
// and grouping by an extracted label — against the reference, and equal to the same metric
// over the template where the forms are equivalent.
func TestMetricOverParserStages(t *testing.T) {
	ex := testbedEngine(t)
	lines := mixedCorpus()
	eng := buildEngine(t, ex, lines)
	metric := func(q string) []query.MatrixSeries {
		t.Helper()
		ast, err := logql.ParseExpr(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		mq, err := logql.TranslateMetric(ast.(*logql.MetricExpr), ex)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		at := semBase.Add(time.Minute).UnixNano()
		mq.Start, mq.End, mq.Step = at, at, int64(time.Second)
		out, err := eng.ExecuteMetric(mq)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return out
	}
	one := func(q string) float64 {
		t.Helper()
		s := metric(q)
		if len(s) != 1 || len(s[0].Points) != 1 {
			t.Fatalf("%s: %+v", q, s)
		}
		return s[0].Points[0].Value
	}

	// count: pattern equal to the template, native, and the reference.
	ref := len(want(lines, "checkout", func(m string) bool {
		return templateAny(m, func(n *big.Int) bool { return n.Cmp(big.NewInt(200)) > 0 })
	}))
	if a, b := one(`sum(count_over_time({app="checkout"} | pattern "<_>took=<latency_ms>ms<_>" | latency_ms > 200 [5m]))`),
		one(`sum(count_over_time({app="checkout"} | latency_ms > 200 [5m]))`); a != b || int(a) != ref {
		t.Fatalf("count: pattern %v, native %v, reference %d", a, b, ref)
	}

	// sum of an unwrapped regexp label, against the reference.
	var sum float64
	for _, m := range want(lines, "checkout", func(string) bool { return true }) {
		if s := refRegexp.FindStringSubmatch(m); s != nil {
			f, _ := strconv.ParseFloat(s[1], 64)
			sum += f
		}
	}
	if got := one(`sum(sum_over_time({app="checkout"} | regexp "took=(?P<latency_ms>[0-9]+)ms" | unwrap latency_ms [5m]))`); math.Abs(got-sum) > 1e-12*sum {
		t.Fatalf("sum_over_time(unwrap regexp label) = %v, the reference %v", got, sum)
	}

	// grouping by an extracted label.
	counts := map[string]float64{}
	for _, m := range want(lines, "checkout", func(string) bool { return true }) {
		if o, ok := jsonObject(m); ok {
			counts[fmt.Sprint(o["level"])]++
		}
	}
	got := map[string]float64{}
	for _, s := range metric(`sum by (level) (count_over_time({app="checkout"} | json | __error__ = "" [5m]))`) {
		got[s.Labels["level"]] = s.Points[0].Value
	}
	if fmt.Sprint(got) != fmt.Sprint(counts) || len(counts) == 0 {
		t.Fatalf("sum by (level) over json: %v, the reference %v", got, counts)
	}

	// Without a vector aggregation, each series carries the extracted labels, never the
	// unwrapped one.
	for _, s := range metric(`max_over_time({app="checkout"} | logfmt | unwrap dur [5m])`) {
		if _, ok := s.Labels["dur"]; ok {
			t.Fatalf("a series kept the unwrapped label: %v", s.Labels)
		}
		if s.Labels["level"] == "" {
			t.Fatalf("a series lacks the extracted labels: %v", s.Labels)
		}
	}
}
