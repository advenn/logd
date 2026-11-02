package logql

import (
	"reflect"
	"testing"

	"github.com/advenn/logd/core/model"
)

// collect runs a stage over a line and returns what it emitted, in order, plus its error.
func collect(t *testing.T, st stageParser, line string) ([][2]string, string) {
	t.Helper()
	var got [][2]string
	err := st.each(line, func(name, value string) bool {
		got = append(got, [2]string{name, value})
		return true
	})
	return got, err
}

func kv(pairs ...string) [][2]string {
	var out [][2]string
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, [2]string{pairs[i], pairs[i+1]})
	}
	return out
}

func TestParsePatternValidation(t *testing.T) {
	for _, bad := range []string{
		"took=<_>ms",         // no named capture
		"<a><b>",             // consecutive captures
		"<_><a> x",           // consecutive, one unnamed
		"<a> <a>",            // duplicate name
		"no captures at all", // no capture
		"<1> <2a>",           // not identifiers, so all literal: no capture
	} {
		if _, err := newPatternStage(bad); err == nil {
			t.Errorf("pattern %q accepted", bad)
		}
	}
	// '<' not starting an identifier-and-'>' is literal, as in Loki's lexer.
	nodes, err := parsePattern("a < b <x> c <y z> d")
	if err != nil {
		t.Fatal(err)
	}
	want := []patternNode{{text: "a < b "}, {capture: true, text: "x"}, {text: " c <y z> d"}}
	if !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes = %+v, want %+v", nodes, want)
	}
}

// Loki's pattern semantics (pkg/logql/log/pattern/pattern.go, Matcher.Matches), case by
// case. Each expectation follows from the source, not from logd's implementation.
func TestPatternStageSemantics(t *testing.T) {
	for _, c := range []struct {
		pattern, line string
		want          [][2]string
	}{
		// A leading literal anchors at the start of the line.
		{"took=<ms>ms", "took=5ms", kv("ms", "5")},
		{"took=<ms>ms", "a took=5ms", nil},
		// A leading capture does not; <_> skips to the FIRST occurrence of the next literal.
		{"<_>took=<ms>ms<_>", "a took=5ms b took=7ms", kv("ms", "5")},
		{"<_>took=<ms>ms", "a took=5ms b", kv("ms", "5")},
		// A capture runs to the next literal, and may be empty.
		{"<_>took=<ms>ms", "took=ms", kv("ms", "")},
		{"<_>took=<ms>ms", "took=12.5ms", kv("ms", "12.5")},
		{"<_>took=<ms>ms", "took=5 ms", kv("ms", "5 ")},
		// A missing literal: the capture takes the rest of the line, and matching stops.
		{"<_>took=<ms>ms", "took=7", kv("ms", "7")},
		{"<a> <b> <c>", "x y", kv("a", "x", "b", "y")},
		// A last capture takes the rest of the line.
		{"<a> <rest>", "x y z", kv("a", "x", "rest", "y z")},
		// The first literal missing: no labels.
		{"<_>took=<ms>ms", "no duration", nil},
		{"<_>took=<ms>ms", "", nil},
		// Literals with spaces, several captures.
		{`<ip> - - [<_>] "<method> <path> <_>" <status> <size>`, `1.2.3.4 - - [01/Jan] "GET /x HTTP/1.1" 200 512`,
			kv("ip", "1.2.3.4", "method", "GET", "path", "/x", "status", "200", "size", "512")},
	} {
		st, err := newPatternStage(c.pattern)
		if err != nil {
			t.Fatalf("%q: %v", c.pattern, err)
		}
		got, errLabel := collect(t, st, c.line)
		if errLabel != "" {
			t.Errorf("%q on %q set __error__=%s; pattern never does", c.pattern, c.line, errLabel)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q on %q: got %v, want %v", c.pattern, c.line, got, c.want)
		}
	}
}

func TestRegexpStageSemantics(t *testing.T) {
	st, err := newRegexpStage(`took=(?P<latency_ms>[0-9]+)ms(?: (?P<unit>\w+))?(x)?`)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		line string
		want [][2]string
	}{
		{"a took=5ms b took=7ms", kv("latency_ms", "5", "unit", "b")}, // leftmost match only
		{"took=5ms", kv("latency_ms", "5", "unit", "")},               // a group that took no part is ""
		{"took=ms", nil}, // no match, no labels
	} {
		if got, _ := collect(t, st, c.line); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: got %v, want %v", c.line, got, c.want)
		}
	}
	// A group name is sanitized like any extracted key: a leading digit gets '_'.
	st, err = newRegexpStage(`(?P<1st>\w+)`)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := collect(t, st, "abc"); !reflect.DeepEqual(got, kv("_1st", "abc")) {
		t.Errorf("sanitized name: %v", got)
	}
	for _, bad := range []string{`took=([0-9]+)ms`, `took=(?P<a>\d)(?P<a>\d)`, `took=(?P<a>[`, `no groups`} {
		if _, err := newRegexpStage(bad); err == nil {
			t.Errorf("regexp %q accepted", bad)
		}
	}
}

// Loki's logfmt in its default (non-strict) mode: bad pairs are skipped and decoding goes on,
// empty values are not set, keys are sanitized, quoted values unescaped.
func TestLogfmtStageSemantics(t *testing.T) {
	for _, c := range []struct {
		line string
		want [][2]string
	}{
		{`level=info msg="took a while" dur=12.5`, kv("level", "info", "msg", "took a while", "dur", "12.5")},
		{`a="x\"y\\z" b=2`, kv("a", `x"y\z`, "b", "2")},
		{`a= b="" c d=4`, kv("d", "4")}, // empty values and bare keys are not set
		{`a=b=c d=1`, kv("d", "1")},     // an unexpected '=' skips the pair
		{`a"b=1 c=2`, kv("c", "2")},     // a quote in a key skips it
		{`=1 e=5`, kv("e", "5")},        // an empty key
		{`http.status=200 1x=y`, kv("http_status", "200", "_1x", "y")},
		{`a="unterminated b=1`, nil},
		{`  `, nil},
		{`took=5ms status=200`, kv("took", "5ms", "status", "200")},
	} {
		got, errLabel := collect(t, logfmtStage{}, c.line)
		if errLabel != "" {
			t.Errorf("%q set __error__=%s; non-strict logfmt never does", c.line, errLabel)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: got %v, want %v", c.line, got, c.want)
		}
	}
}

func TestJSONStageSemantics(t *testing.T) {
	for _, c := range []struct {
		line    string
		want    [][2]string
		wantErr string
	}{
		{`{"level":"info","took":12.50,"ok":true,"n":null,"tags":["a",{"x":1}],"req":{"path":"/x","h":{"code":2}}}`,
			kv("level", "info", "took", "12.50", "ok", "true", "req_path", "/x", "req_h_code", "2"), ""},
		{`{"a.b":1," c ":"d","9z":"e"}`, kv("a_b", "1", "c", "d", "_9z", "e"), ""},
		{`{"s":"café \"q\""}`, kv("s", `café "q"`), ""},
		{`{"a":1,"b":}`, kv("a", "1"), errJSON}, // labels before the error are kept
		{`{"a":1`, kv("a", "1"), errJSON},       // truncated
		{`[1,2]`, nil, errJSON},                 // not an object
		{`took=5ms`, nil, errJSON},              // not JSON
		{`{"a":1} trailing`, kv("a", "1"), ""},  // the object ends the parse
	} {
		got, errLabel := collect(t, jsonStage{}, c.line)
		if !reflect.DeepEqual(got, c.want) || errLabel != c.wantErr {
			t.Errorf("%q: got %v (err %q), want %v (err %q)", c.line, got, errLabel, c.want, c.wantErr)
		}
	}
}

// What the pipeline makes of the stages' output: an extracted name the record already
// carries gets _extracted, the first value set under a name wins (across stages and within
// one), __error__ is reported and dropped by `drop __error__`.
func TestProgramLabels(t *testing.T) {
	pat, _ := newPatternStage("<_>took=<latency_ms>ms<_>")
	prog := &program{steps: []step{{parser: jsonStage{}}, {parser: pat}, {parser: logfmtStage{}}}}
	e := model.LogEntry{Extra: `{"app":"api","level":"warn"}`, Message: `{"app":"x","level":"info","latency_ms":"9","dup":1,"dup":2} took=5ms`}
	got := prog.labels(e)
	want := map[string]string{
		"app": "api", "level": "warn", // the record's own
		"app_extracted": "x", "level_extracted": "info", "latency_ms": "9", "dup": "1",
		"took": "5ms", // logfmt skips the JSON (a quote in a key) and reads the pair after it
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("labels = %v\nwant %v", got, want)
	}
	for name, v := range map[string]string{"app": "api", "app_extracted": "x", "latency_ms": "9", "dup": "1"} {
		if got, ok := prog.value(len(prog.steps), e, nil, name); !ok || got != v {
			t.Errorf("value(%s) = %q, %v; want %q", name, got, ok, v)
		}
	}

	bad := model.LogEntry{Message: `not json took=5ms`}
	if got := prog.labels(bad); got["latency_ms"] != "5" || got[errorLabel] != errJSON {
		t.Fatalf("a json error with a later pattern: %v", got)
	}
	if v, ok := prog.value(len(prog.steps), bad, nil, errorLabel); !ok || v != errJSON {
		t.Fatalf("__error__ = %q, %v", v, ok)
	}
	dropped := &program{steps: append(prog.steps[:1:1], step{dropErr: func(string) bool { return true }}, step{parser: pat})}
	if got := dropped.labels(bad); got[errorLabel] != "" || got["latency_ms"] != "5" {
		t.Fatalf("drop __error__ left %v", got)
	}
	if v, ok := dropped.value(1, bad, nil, errorLabel); !ok || v != errJSON {
		t.Fatalf("__error__ before the drop = %q, %v", v, ok)
	}
	if v, ok := dropped.value(2, bad, nil, errorLabel); ok {
		t.Fatalf("__error__ after the drop = %q", v)
	}
}

// mayProduce decides which filters after a parser are dynamic: pattern and regexp produce
// only their names (and the _extracted forms), json and logfmt anything, only json errors.
func TestProgramMayProduce(t *testing.T) {
	pat, _ := newPatternStage("<_>took=<latency_ms>ms<_>")
	re, _ := newRegexpStage(`status=(?P<status>\d+)`)
	prog := &program{steps: []step{{parser: pat}, {parser: re}}}
	for name, want := range map[string]bool{
		"latency_ms": true, "latency_ms_extracted": true, "status": true, "status_extracted": true,
		"region": false, "_extracted": false, errorLabel: false,
	} {
		if got := prog.mayProduce(2, name); got != want {
			t.Errorf("mayProduce(%q) = %v, want %v", name, got, want)
		}
	}
	if prog.mayProduce(1, "status") {
		t.Error("a name the regexp extracts is not available before it")
	}
	js := &program{steps: []step{{parser: jsonStage{}}}}
	if !js.mayProduce(1, "anything") || !js.mayProduce(1, errorLabel) {
		t.Error("json may produce any name and __error__")
	}
	if (&program{steps: []step{{parser: logfmtStage{}}}}).mayProduce(1, errorLabel) {
		t.Error("logfmt never sets __error__")
	}
}
