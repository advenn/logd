package logql

import (
	"testing"
	"time"

	"github.com/advenn/logd/internal/storage"
)

// ---- Lexer tests ----

func TestLexerTokens(t *testing.T) {
	tests := []struct {
		input  string
		tokens []TokenKind
	}{
		{"{}", []TokenKind{TokLBRACE, TokRBRACE}},
		{`{a="b"}`, []TokenKind{TokLBRACE, TokIDENT, TokEQ, TokSTRING, TokRBRACE}},
		{`{a!="b"}`, []TokenKind{TokLBRACE, TokIDENT, TokNEQ, TokSTRING, TokRBRACE}},
		{`{a=~"b"}`, []TokenKind{TokLBRACE, TokIDENT, TokREQ, TokSTRING, TokRBRACE}},
		{`{a!~"b"}`, []TokenKind{TokLBRACE, TokIDENT, TokNRE, TokSTRING, TokRBRACE}},
		{`{a="b",c="d"}`, []TokenKind{TokLBRACE, TokIDENT, TokEQ, TokSTRING, TokCOMMA, TokIDENT, TokEQ, TokSTRING, TokRBRACE}},
		{`|= "err"`, []TokenKind{TokPIPE_EQ, TokSTRING}},
		{`!= "err"`, []TokenKind{TokNEQ, TokSTRING}},
		{`|~ "re"`, []TokenKind{TokPIPE_RE, TokSTRING}},
		{`!~ "re"`, []TokenKind{TokNRE, TokSTRING}},
		{`> "5"`, []TokenKind{TokGT, TokSTRING}},
		{`>= "5"`, []TokenKind{TokGTE, TokSTRING}},
		{`< "5"`, []TokenKind{TokLT, TokSTRING}},
		{`<= "5"`, []TokenKind{TokLTE, TokSTRING}},
		{`{a="b"} | json`, []TokenKind{TokLBRACE, TokIDENT, TokEQ, TokSTRING, TokRBRACE, TokPIPE, TokIDENT}},
		{`{a="b"} | logfmt`, []TokenKind{TokLBRACE, TokIDENT, TokEQ, TokSTRING, TokRBRACE, TokPIPE, TokIDENT}},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			tokens := Tokens(tc.input)
			if len(tokens) != len(tc.tokens) {
				t.Fatalf("got %d tokens, want %d: %v", len(tokens), len(tc.tokens), tokens)
			}
			for i, tok := range tokens {
				if tok.Kind != tc.tokens[i] {
					t.Errorf("token %d: got %s, want %s", i, tok.Kind, tc.tokens[i])
				}
			}
		})
	}
}

func TestLexerStringValues(t *testing.T) {
	tests := []struct {
		input string
		value string
	}{
		{`"hello"`, "hello"},
		{`'hello'`, "hello"},
		{`"escaped\\n"`, "escaped\\n"},
		{`"\"quoted\""`, `"quoted"`},
	}

	for _, tc := range tests {
		tok := Tokens(tc.input)
		if len(tok) < 1 || tok[0].Value != tc.value {
			t.Errorf("%s: got %q, want %q", tc.input, tok[0].Value, tc.value)
		}
	}
}

// ---- Parser tests ----

func TestParseSelector(t *testing.T) {
	tests := []struct {
		input   string
		wantStr string
	}{
		{"{}", "{}"},
		{`{app="api"}`, `{app="api"}`},
		{`{app="api",level="ERROR"}`, `{app="api",level="ERROR"}`},
		{`{app!="api"}`, `{app!="api"}`},
		{`{app=~"api.*"}`, `{app=~"api.*"}`},
		{`{app!~"test.*"}`, `{app!~"test.*"}`},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			q, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Selector.String() != tc.wantStr {
				t.Errorf("got %q, want %q", q.Selector.String(), tc.wantStr)
			}
		})
	}
}

func TestParseFullQuery(t *testing.T) {
	tests := []string{
		`{app="api"} |= "error"`,
		`{app="api"} != "debug"`,
		`{app="api"} |~ "err.*"`,
		`{app="api"} !~ "debug.*"`,
		`{app="api"} | json`,
		`{app="api"} | logfmt`,
		`{app="api"} | json | level = "ERROR"`,
		`{app="api"} | logfmt | duration > "100"`,
		`{app="api"} | logfmt | duration >= "50"`,
		`{app="api"} | logfmt | duration < "500"`,
		`{app="api"} | logfmt | duration <= "1000"`,
		`{app="api"} |= "err" | json | status >= "400" | level = "ERROR"`,
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.String() != input {
				t.Errorf("roundtrip failed:\n  got:  %q\n  want: %q", q.String(), input)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []string{
		``,                      // empty
		`app="api"`,             // no braces
		`{app}`,                 // no operator
		`{app=}`,                // no value
		`{app="api"`,            // unclosed brace
		`{app="api"} |`,         // dangling pipe
		`{app="api"} | unknown`, // unknown keyword
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			if err == nil {
				t.Errorf("expected error for %q", input)
			}
		})
	}
}

// ---- Pipeline tests ----

func makeEntry(msg string, level storage.LogLevel, extra string) storage.LogEntry {
	return storage.LogEntry{
		TS:      time.Now(),
		Level:   level,
		Message: msg,
		Extra:   extra,
	}
}

func TestPipelineLineFilterContains(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeEq, Value: "error"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("this is an error message", storage.LogLevelInfo, "")) {
		t.Error("expected match for 'error'")
	}
	if p.Match(makeEntry("this is fine", storage.LogLevelInfo, "")) {
		t.Error("expected no match for 'fine'")
	}
}

func TestPipelineLineFilterNotContains(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeNotEq, Value: "debug"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("this is an error", storage.LogLevelInfo, "")) {
		t.Error("expected match (does not contain debug)")
	}
	if p.Match(makeEntry("this is a debug message", storage.LogLevelInfo, "")) {
		t.Error("expected no match (contains debug)")
	}
}

func TestPipelineLineFilterRegex(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeRe, Value: `err(or)?`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("an error occurred", storage.LogLevelInfo, "")) {
		t.Error("expected match for 'error'")
	}
	if !p.Match(makeEntry("an err occurred", storage.LogLevelInfo, "")) {
		t.Error("expected match for 'err'")
	}
	if p.Match(makeEntry("all good", storage.LogLevelInfo, "")) {
		t.Error("expected no match")
	}
}

func TestPipelineLineFilterNotRegex(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeNotRe, Value: `debug|trace`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("an error occurred", storage.LogLevelInfo, "")) {
		t.Error("expected match (not debug/trace)")
	}
	if p.Match(makeEntry("debug info", storage.LogLevelInfo, "")) {
		t.Error("expected no match (is debug)")
	}
}

func TestPipelineParserJSON(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&ParserStage{Parser: "json"},
			&LabelFilter{Label: "status", Op: OpEq, Value: "500"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Valid JSON — | json parses the message into extracted labels.
	if !p.Match(makeEntry(`{"status":"500","path":"/api"}`, storage.LogLevelInfo, "")) {
		t.Error("expected match for status=500")
	}
	if p.Match(makeEntry(`{"status":"200","path":"/api"}`, storage.LogLevelInfo, "")) {
		t.Error("expected no match for status=200")
	}

	// Non-JSON message is a no-op for | json — extracted labels are empty,
	// so the label filter on "status" gets a missing label and rejects.
	if p.Match(makeEntry("not json at all", storage.LogLevelInfo, "")) {
		t.Error("expected no match for non-JSON (missing status label)")
	}
}

func TestPipelineParserLogfmt(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&ParserStage{Parser: "logfmt"},
			&LabelFilter{Label: "duration_ms", Op: OpGT, Value: "100"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("duration_ms=150 method=GET", storage.LogLevelInfo, "")) {
		t.Error("expected match for duration_ms=150 > 100")
	}
	if p.Match(makeEntry("duration_ms=50 method=GET", storage.LogLevelInfo, "")) {
		t.Error("expected no match for duration_ms=50 > 100")
	}
}

func TestPipelineLabelFilterLevel(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LabelFilter{Label: "level", Op: OpEq, Value: "ERROR"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !p.Match(makeEntry("msg", storage.LogLevelError, "")) {
		t.Error("expected match for ERROR")
	}
	if p.Match(makeEntry("msg", storage.LogLevelInfo, "")) {
		t.Error("expected no match for INFO")
	}
}

func TestPipelineLabelFilterExtra(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LabelFilter{Label: "service", Op: OpEq, Value: "django_app"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	e := makeEntry("msg", storage.LogLevelInfo, `{"service":"django_app"}`)
	if !p.Match(e) {
		t.Error("expected match for service=django_app")
	}

	e2 := makeEntry("msg", storage.LogLevelInfo, `{"service":"celery_worker"}`)
	if p.Match(e2) {
		t.Error("expected no match for celery_worker")
	}
}

func TestPipelineMultipleStages(t *testing.T) {
	p, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeEq, Value: "error"},
			&ParserStage{Parser: "json"},
			&LabelFilter{Label: "code", Op: OpGTE, Value: "500"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Valid JSON message that also contains "error".
	e := makeEntry(`{"code":500,"msg":"error internal failure"}`, storage.LogLevelInfo, "")
	if !p.Match(e) {
		t.Error("expected match (contains 'error', json with code>=500)")
	}

	// Valid JSON with code < 500 (even though it contains "error").
	e2 := makeEntry(`{"code":200,"msg":"error not found"}`, storage.LogLevelInfo, "")
	if p.Match(e2) {
		t.Error("expected no match (code < 500)")
	}

	// Valid JSON with code>=500 but no "error".
	e3 := makeEntry(`{"code":500,"msg":"warning high load"}`, storage.LogLevelInfo, "")
	if p.Match(e3) {
		t.Error("expected no match (no 'error')")
	}
}

func TestPipelineCompareOperators(t *testing.T) {
	tests := []struct {
		op      Op
		value   string
		label   string
		matches bool
	}{
		{OpGT, "100", "150", true},
		{OpGT, "100", "50", false},
		{OpGT, "100", "100", false},
		{OpGTE, "100", "100", true},
		{OpGTE, "100", "99", false},
		{OpLT, "100", "50", true},
		{OpLT, "100", "150", false},
		{OpLT, "100", "100", false},
		{OpLTE, "100", "100", true},
		{OpLTE, "100", "101", false},
	}

	for _, tc := range tests {
		p, err := CompilePipeline(&Query{
			Pipeline: []PipelineStage{
				&ParserStage{Parser: "json"},
				&LabelFilter{Label: "val", Op: tc.op, Value: tc.value},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		e := makeEntry(`{"val":`+tc.label+`}`, storage.LogLevelInfo, "")
		if p.Match(e) != tc.matches {
			t.Errorf("val=%s %s %s: got match=%v", tc.label, tc.op, tc.value, !tc.matches)
		}
	}
}

func TestCompilePipelineRegexError(t *testing.T) {
	_, err := CompilePipeline(&Query{
		Pipeline: []PipelineStage{
			&LineFilter{Op: OpPipeRe, Value: `[invalid`},
		},
	})
	if err == nil {
		t.Error("expected error for invalid regex")
	}
}

func TestParseAndCompileIntegration(t *testing.T) {
	q, err := Parse(`{app="api"} |= "timeout" | json | status >= "500"`)
	if err != nil {
		t.Fatal(err)
	}

	p, err := CompilePipeline(&Query{Pipeline: q.Pipeline})
	if err != nil {
		t.Fatal(err)
	}

	// Valid JSON messages (the whole message is JSON).
	e := makeEntry(`{"status":502,"path":"/api/orders","msg":"request timeout after 30s"}`,
		storage.LogLevelWarn, `{"app":"api"}`)
	if !p.Match(e) {
		t.Error("expected match for full pipeline")
	}

	e2 := makeEntry(`{"status":200,"path":"/api/orders","msg":"request timeout after 5s"}`,
		storage.LogLevelInfo, `{"app":"api"}`)
	if p.Match(e2) {
		t.Error("expected no match (status < 500)")
	}
}
