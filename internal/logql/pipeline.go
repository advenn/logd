package logql

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/advenn/logd/internal/storage"
)

// Pipeline is a compiled LogQL pipeline that can be executed against log entries.
// It implements storage.EntryPredicate so it slots directly into the query scan loop.
type Pipeline struct {
	stages []pipelineStage
}

// compiledStage is a pipeline stage ready for execution.
type pipelineStage interface {
	// apply runs the stage against an entry. Returns false if the entry is rejected.
	// extracted is the current set of labels extracted by parser stages (mutable).
	apply(entry storage.LogEntry, extracted map[string]string) bool
}

// CompilePipeline converts an AST query's pipeline stages into an executable Pipeline.
// Regex-based filters are compiled eagerly so errors surface at query time.
func CompilePipeline(query *Query) (*Pipeline, error) {
	var stages []pipelineStage

	for _, s := range query.Pipeline {
		cs, err := compileStage(s)
		if err != nil {
			return nil, err
		}
		stages = append(stages, cs)
	}

	return &Pipeline{stages: stages}, nil
}

// Match runs all pipeline stages against an entry. Returns true if the entry passes
// every stage (including selector matchers, if they were compiled in).
// Implements storage.EntryPredicate.
func (p *Pipeline) Match(entry storage.LogEntry) bool {
	extracted := make(map[string]string)
	for _, s := range p.stages {
		if !s.apply(entry, extracted) {
			return false
		}
	}
	return true
}

func compileStage(s PipelineStage) (pipelineStage, error) {
	switch s := s.(type) {
	case *LineFilter:
		return compileLineFilter(s)
	case *ParserStage:
		return compileParserStage(s)
	case *LabelFilter:
		return compileLabelFilter(s)
	default:
		return nil, &ParseError{Msg: "unknown pipeline stage type"}
	}
}

// ---- line filter ----

type compiledLineFilter struct {
	op      Op
	literal string         // for |= and !=
	regex   *regexp.Regexp // for |~ and !~
}

func compileLineFilter(lf *LineFilter) (*compiledLineFilter, error) {
	c := &compiledLineFilter{op: lf.Op, literal: lf.Value}
	if lf.Op.IsRegex() {
		re, err := regexp.Compile(lf.Value)
		if err != nil {
			return nil, &ParseError{Msg: "invalid regex: " + err.Error()}
		}
		c.regex = re
	}
	return c, nil
}

func (c *compiledLineFilter) apply(entry storage.LogEntry, _ map[string]string) bool {
	msg := entry.Message
	switch c.op {
	case OpPipeEq:
		return strings.Contains(msg, c.literal)
	case OpPipeNotEq:
		return !strings.Contains(msg, c.literal)
	case OpPipeRe:
		return c.regex.MatchString(msg)
	case OpPipeNotRe:
		return !c.regex.MatchString(msg)
	}
	return true
}

// ---- parser stage ----

type compiledParserStage struct {
	parser string
}

func compileParserStage(ps *ParserStage) (*compiledParserStage, error) {
	return &compiledParserStage{parser: ps.Parser}, nil
}

func (c *compiledParserStage) apply(entry storage.LogEntry, extracted map[string]string) bool {
	switch c.parser {
	case "json":
		parseJSON(entry.Message, extracted)
		// Also merge Extra labels (stream labels) so they're available to subsequent filters.
		if entry.Extra != "" {
			parseJSON(entry.Extra, extracted)
		}
	case "logfmt":
		parseLogfmt(entry.Message, extracted)
	}
	return true
}

func parseJSON(raw string, dst map[string]string) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return
	}
	for k, v := range m {
		if s, ok := v.(string); ok {
			dst[k] = s
		} else {
			dst[k] = jsonString(v)
		}
	}
}

func jsonString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func parseLogfmt(raw string, dst map[string]string) {
	for raw != "" {
		raw = skipSpace(raw)
		if raw == "" {
			return
		}
		eq := strings.IndexByte(raw, '=')
		if eq < 0 {
			return
		}
		key := raw[:eq]
		raw = raw[eq+1:]

		var val string
		if raw != "" && raw[0] == '"' {
			// Quoted value.
			end := strings.IndexByte(raw[1:], '"')
			if end < 0 {
				val = raw[1:]
				raw = ""
			} else {
				val = raw[1 : end+1]
				raw = raw[end+2:]
			}
		} else {
			end := strings.IndexAny(raw, " \t\n\r")
			if end < 0 {
				val = raw
				raw = ""
			} else {
				val = raw[:end]
				raw = raw[end:]
			}
		}
		dst[key] = val
	}
}

func skipSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	return s
}

// ---- label filter ----

type compiledLabelFilter struct {
	label string
	op    Op
	str   string
	num   float64
	isNum bool           // true if str parses as a number
	regex *regexp.Regexp // for =~ and !~
}

func compileLabelFilter(lf *LabelFilter) (*compiledLabelFilter, error) {
	c := &compiledLabelFilter{
		label: lf.Label,
		op:    lf.Op,
		str:   lf.Value,
	}

	// Try numeric parsing for comparison operators and equality.
	if f, err := strconv.ParseFloat(lf.Value, 64); err == nil {
		c.num = f
		c.isNum = true
	}

	if lf.Op.IsRegex() {
		re, err := regexp.Compile(lf.Value)
		if err != nil {
			return nil, &ParseError{Msg: "invalid regex: " + err.Error()}
		}
		c.regex = re
	}

	return c, nil
}

func (c *compiledLabelFilter) apply(entry storage.LogEntry, extracted map[string]string) bool {
	// Resolve the label value.
	labelVal, found := resolveLabel(entry, c.label, extracted)
	if !found {
		return c.op == OpNotEq || c.op == OpNotRe // missing label matches != and !~
	}

	switch c.op {
	case OpEq:
		return labelVal == c.str
	case OpNotEq:
		return labelVal != c.str
	case OpRe:
		return c.regex.MatchString(labelVal)
	case OpNotRe:
		return !c.regex.MatchString(labelVal)
	case OpGT, OpGTE, OpLT, OpLTE:
		return compareNumeric(labelVal, c)
	default:
		return true
	}
}

// resolveLabel looks up a label value in the priority order:
// 1. Structured entry fields (level, service from Extra)
// 2. Extracted labels from parser stages
// 3. Stream labels from entry.Extra JSON
func resolveLabel(entry storage.LogEntry, label string, extracted map[string]string) (string, bool) {
	// Structured level field.
	if label == "level" {
		return entry.Level.String(), true
	}

	// Check extracted labels first (they override stream labels).
	if v, ok := extracted[label]; ok {
		return v, true
	}

	// Check stream labels from Extra JSON.
	if entry.Extra != "" {
		var extra map[string]interface{}
		if err := json.Unmarshal([]byte(entry.Extra), &extra); err == nil {
			if v, ok := extra[label]; ok {
				if s, ok := v.(string); ok {
					return s, true
				}
				// Non-string values in Extra: convert.
				return jsonString(v), true
			}
		}
	}

	return "", false
}

// compareNumeric attempts numeric comparison; falls back to string comparison
// if either side is non-numeric (Loki-compatible behavior).
func compareNumeric(labelVal string, c *compiledLabelFilter) bool {
	if !c.isNum {
		// Filter value is non-numeric: fall back to string comparison.
		switch c.op {
		case OpGT:
			return labelVal > c.str
		case OpGTE:
			return labelVal >= c.str
		case OpLT:
			return labelVal < c.str
		case OpLTE:
			return labelVal <= c.str
		}
		return false
	}

	labelNum, err := strconv.ParseFloat(labelVal, 64)
	if err != nil {
		// Label value is non-numeric: string comparison.
		switch c.op {
		case OpGT:
			return labelVal > c.str
		case OpGTE:
			return labelVal >= c.str
		case OpLT:
			return labelVal < c.str
		case OpLTE:
			return labelVal <= c.str
		}
		return false
	}

	switch c.op {
	case OpGT:
		return labelNum > c.num
	case OpGTE:
		return labelNum >= c.num
	case OpLT:
		return labelNum < c.num
	case OpLTE:
		return labelNum <= c.num
	}
	return false
}
