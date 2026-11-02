package logql

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// The four LogQL parser stages, implemented to Loki's semantics. Each is ported from, and
// cites, Loki's own source (github.com/grafana/loki, pkg/logql/log), because the docs leave
// the edge cases open and the edge cases are what a Loki user's query depends on.
//
// A stage reports the labels it extracts from a line under their final, sanitized names, in
// the order Loki sets them. What happens next is common to all four and lives in
// program (pipeline.go): a name that is already one of the record's own labels gets
// Loki's "_extracted" suffix, and the first value set under a name wins.

const (
	// duplicateSuffix is appended to an extracted label whose name the record already
	// carries as a stream label or structured metadata (Loki's DuplicateSuffix, parser.go).
	duplicateSuffix = "_extracted"
	// errorLabel is the label Loki reports pipeline errors under (logqlmodel.ErrorLabel).
	errorLabel = "__error__"
	// errJSON is the __error__ value of a line `| json` cannot parse (Loki's errJSON).
	errJSON = "JSONParserErr"
)

// stageParser is one compiled parser stage.
type stageParser interface {
	// each calls emit with every label the stage extracts from line, in Loki's order, and
	// stops early when emit returns false. It returns the __error__ value the stage sets,
	// or "" — always "" when it stopped early.
	each(line string, emit func(name, value string) bool) string
	// mayFail reports whether the stage can set __error__ at all (only json does).
	mayFail() bool
	// produces reports whether the stage can extract a label of this (pre-suffix) name.
	produces(name string) bool
}

// ---- | pattern ----

// patternNode is one element of a parsed Loki pattern expression: a literal run or a
// capture. An unnamed capture is named "_".
type patternNode struct {
	capture bool
	text    string
}

// parsePattern parses a pattern expression as Loki's pattern lexer does
// (pkg/logql/log/pattern/lexer.rl): '<' identifier '>' is a capture, an identifier being a
// letter or '_' followed by letters, digits or '_'; every other byte is literal, and
// adjacent literal bytes are one literal. It then applies Loki's validation
// (pkg/logql/log/pattern/ast.go, expr.validate): at least one named capture, no two
// captures in a row, and no name twice.
func parsePattern(s string) ([]patternNode, error) {
	var nodes []patternNode
	var lit strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '<' {
			if j := captureEnd(s, i); j > 0 {
				if lit.Len() > 0 {
					nodes = append(nodes, patternNode{text: lit.String()})
					lit.Reset()
				}
				nodes = append(nodes, patternNode{capture: true, text: s[i+1 : j]})
				i = j + 1
				continue
			}
		}
		lit.WriteByte(s[i])
		i++
	}
	if lit.Len() > 0 {
		nodes = append(nodes, patternNode{text: lit.String()})
	}
	named := map[string]bool{}
	for i, n := range nodes {
		if !n.capture {
			continue
		}
		if i > 0 && nodes[i-1].capture {
			return nil, fmt.Errorf("logql: pattern %q: consecutive captures <%s><%s> are not allowed", s, nodes[i-1].text, n.text)
		}
		if n.text == "_" {
			continue
		}
		if named[n.text] {
			return nil, fmt.Errorf("logql: pattern %q: duplicate capture name <%s>", s, n.text)
		}
		named[n.text] = true
	}
	if len(named) == 0 {
		return nil, fmt.Errorf("logql: pattern %q: at least one named capture is required", s)
	}
	return nodes, nil
}

// captureEnd returns the index of the '>' closing a capture that starts at s[i] == '<', or
// -1 when what follows is not an identifier and a '>' (then the '<' is literal).
func captureEnd(s string, i int) int {
	j := i + 1
	if j >= len(s) || !isIdentStart(s[j]) {
		return -1
	}
	for j++; j < len(s) && isIdentCont(s[j]); j++ {
	}
	if j < len(s) && s[j] == '>' {
		return j
	}
	return -1
}

type patternStage struct {
	nodes []patternNode
	names map[string]bool
}

func newPatternStage(expr string) (*patternStage, error) {
	nodes, err := parsePattern(expr)
	if err != nil {
		return nil, err
	}
	st := &patternStage{nodes: nodes, names: map[string]bool{}}
	for _, n := range nodes {
		if n.capture && n.text != "_" {
			st.names[n.text] = true
		}
	}
	return st, nil
}

func (st *patternStage) mayFail() bool             { return false }
func (st *patternStage) produces(name string) bool { return st.names[name] }

// each is Loki's Matcher.Matches (pkg/logql/log/pattern/pattern.go), with the parser stage
// around it (PatternParser.Process, parser.go), ported line for line:
//
//   - A pattern that starts with a literal is anchored: the line must begin with it, or
//     nothing is extracted. A pattern that starts with a capture is not.
//   - Each capture extends to the FIRST occurrence of the literal after it, so a capture
//     may be empty, and only the first occurrence of each literal is ever considered.
//   - A capture with no literal after it takes the rest of the line. So does a capture
//     whose literal does not occur, and matching stops there: the captures after it are
//     not set. A trailing literal is not anchored at the end of the line.
//   - <_> skips what it covers.
//   - A line that does not match yields no labels, never an error: the stage always keeps
//     the line.
func (st *patternStage) each(line string, emit func(name, value string) bool) string {
	if len(line) == 0 {
		return ""
	}
	nodes := st.nodes
	in := line
	if !nodes[0].capture {
		if !strings.HasPrefix(in, nodes[0].text) { // Loki: bytes.Index(in, ls) != 0
			return ""
		}
		in = in[len(nodes[0].text):]
		nodes = nodes[1:]
	}
	for len(nodes) > 0 {
		capt := nodes[0]
		if len(nodes) == 1 { // ending on a capture: it takes the rest
			if capt.text != "_" {
				emit(capt.text, in)
			}
			return ""
		}
		lit := nodes[1].text
		nodes = nodes[2:]
		i := strings.Index(in, lit)
		if i < 0 { // the literal is missing: the capture takes the rest, and matching stops
			if capt.text != "_" {
				emit(capt.text, in)
			}
			return ""
		}
		if capt.text != "_" && !emit(capt.text, in[:i]) {
			return ""
		}
		in = in[i+len(lit):]
	}
	return ""
}

// ---- | regexp ----

type regexpStage struct {
	re    *regexp.Regexp
	names []string // per subexpression index: the sanitized label name, "" when unnamed
}

// newRegexpStage compiles a regexp stage as Loki's NewRegexpParser does (parser.go): Go's
// regexp syntax (Loki uses the same package), at least one named group, no name twice. Each
// group name is sanitized like every extracted label key (sanitizeLabelKey).
func newRegexpStage(expr string) (*regexpStage, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("logql: bad regexp %q: %w", expr, err)
	}
	st := &regexpStage{re: re, names: make([]string, len(re.SubexpNames()))}
	seen := map[string]bool{}
	for i, n := range re.SubexpNames() {
		if n == "" {
			continue
		}
		if seen[n] {
			return nil, fmt.Errorf("logql: regexp %q: duplicate extracted label name %q", expr, n)
		}
		seen[n] = true
		st.names[i] = sanitizeLabelKey(n)
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("logql: regexp %q: at least one named capture group, (?P<name>...), is required", expr)
	}
	return st, nil
}

func (st *regexpStage) mayFail() bool { return false }
func (st *regexpStage) produces(name string) bool {
	for _, n := range st.names {
		if n != "" && n == name {
			return true
		}
	}
	return false
}

// each is RegexpParser.Process (parser.go): the leftmost match only (FindSubmatch); every
// named group is set, a group that took no part in the match to ""; no match, no labels.
func (st *regexpStage) each(line string, emit func(name, value string) bool) string {
	m := st.re.FindStringSubmatchIndex(line)
	if m == nil {
		return ""
	}
	for i, name := range st.names {
		if name == "" {
			continue
		}
		v := ""
		if m[2*i] >= 0 {
			v = line[m[2*i]:m[2*i+1]]
		}
		if !emit(name, v) {
			return ""
		}
	}
	return ""
}

// ---- | logfmt ----

type logfmtStage struct{}

func (logfmtStage) mayFail() bool          { return false }
func (logfmtStage) produces(_ string) bool { return true } // any key may appear

// each is LogfmtParser.Process (parser.go) in its default mode, NewLogfmtParser(strict=false,
// keepEmpty=false): a pair that does not decode is skipped and decoding goes on, a key whose
// value is empty is not set, and nothing ever sets __error__ — only --strict does, which
// logd does not offer.
func (logfmtStage) each(line string, emit func(name, value string) bool) string {
	d := logfmtDecoder{line: line}
	for d.pos < len(d.line) {
		if !d.scanKeyval() {
			continue
		}
		key := sanitizeLabelKey(d.key)
		if key == "" || d.value == "" {
			continue
		}
		v := d.value
		if strings.ContainsRune(v, utf8.RuneError) {
			v = strings.Map(removeInvalidUTF8, v)
		}
		if !emit(key, v) {
			return ""
		}
	}
	return ""
}

// logfmtDecoder is Loki's logfmt decoder (pkg/logql/log/logfmt/decode.go, adapted there from
// github.com/go-logfmt/logfmt), ScanKeyval ported state for state. Where it reports a syntax
// error, this returns false; the non-strict stage ignores the error either way.
type logfmtDecoder struct {
	line       string
	pos        int
	key, value string
}

func (d *logfmtDecoder) scanKeyval() bool {
	d.key, d.value = "", ""
	line := d.line

	// garbage
	start := -1
	for p := d.pos; p < len(line); p++ {
		if line[p] > ' ' {
			start = p
			break
		}
	}
	if start < 0 {
		d.pos = len(line)
		return false
	}
	d.pos = start

	// key
	multibyte := false
	for p := d.pos; p < len(line); p++ {
		c := line[p]
		switch {
		case c == '=':
			d.pos = p
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
					return d.skipValue() // invalid key
				}
			}
			if d.key == "" {
				return d.skipValue() // unexpected '='
			}
			return d.equal()
		case c == '"':
			d.pos = p
			return d.skipValue() // unexpected '"'
		case c <= ' ':
			d.pos = p
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
					d.key = ""
					return false
				}
			}
			return true
		case c >= utf8.RuneSelf:
			multibyte = true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.key = line[start:d.pos]
		if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
			d.key = ""
			return false
		}
	}
	return true
}

func (d *logfmtDecoder) equal() bool {
	line := d.line
	d.pos++
	if d.pos >= len(line) {
		return true
	}
	switch c := line[d.pos]; {
	case c <= ' ':
		return true
	case c == '"':
		return d.qvalue()
	}
	// value
	start := d.pos
	for p := d.pos; p < len(line); p++ {
		switch c := line[p]; {
		case c == '=' || c == '"':
			d.pos = p
			d.key = ""
			return d.skipValue() // unexpected byte
		case c <= ' ':
			d.pos = p
			if d.pos > start {
				d.value = line[start:d.pos]
			}
			return true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.value = line[start:d.pos]
	}
	return true
}

func (d *logfmtDecoder) skipValue() bool {
	d.key, d.value = "", ""
	for p := d.pos; p < len(d.line); p++ {
		if d.line[p] <= ' ' {
			d.pos = p
			return false
		}
	}
	d.pos = len(d.line)
	return false
}

func (d *logfmtDecoder) qvalue() bool {
	line := d.line
	rest := line[d.pos+1:]
	if end := strings.IndexByte(rest, '"'); end >= 0 && strings.IndexByte(rest[:end], '\\') < 0 {
		d.pos += end + 2
		d.value = rest[:end]
		return true
	}
	hasEsc, esc := false, false
	start := d.pos
	for p := d.pos + 1; p < len(line); p++ {
		switch c := line[p]; {
		case esc:
			esc = false
		case c == '\\':
			hasEsc, esc = true, true
		case c == '"':
			d.pos = p + 1
			if !hasEsc {
				d.value = line[start+1 : p]
				return true
			}
			// unquoteBytes there is encoding/json's string unquoting.
			var v string
			if err := json.Unmarshal([]byte(line[start:d.pos]), &v); err != nil {
				d.key = ""
				return false // invalid quoted value
			}
			d.value = v
			return true
		}
	}
	d.pos = len(line)
	d.key = ""
	return false // unterminated quoted value
}

// ---- | json ----

type jsonStage struct{}

func (jsonStage) mayFail() bool          { return true }
func (jsonStage) produces(_ string) bool { return true } // any key may appear

// each is JSONParser.Process (parser.go): the line must be a JSON object; every string,
// number and boolean becomes a label (a number keeps its literal text, a boolean is "true" or
// "false"); a nested object's fields are named by their path joined with '_'; arrays and
// nulls are skipped. A line that is not a valid object sets __error__=JSONParserErr and keeps
// the labels extracted before the error, as jsonparser.ObjectEach's callbacks do.
//
// The line is read with encoding/json's tokenizer where Loki uses github.com/buger/jsonparser.
// Both accept the same valid objects and give the same labels for them; on malformed input
// the two may stop at different points, so the labels set before the error can differ.
func (jsonStage) each(line string, emit func(name, value string) bool) string {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return errJSON
	}
	stopped, err := jsonObject(dec, nil, emit)
	if stopped {
		return ""
	}
	if err != nil {
		return errJSON
	}
	return ""
}

// jsonObject walks the members of an object whose '{' has been read, through its '}'.
func jsonObject(dec *json.Decoder, path []string, emit func(name, value string) bool) (stopped bool, err error) {
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return false, err
		}
		key, ok := kt.(string)
		if !ok {
			return false, fmt.Errorf("object key is not a string")
		}
		vt, err := dec.Token()
		if err != nil {
			return false, err
		}
		var value string
		switch v := vt.(type) {
		case json.Delim:
			switch v {
			case '{':
				if stopped, err := jsonObject(dec, append(path, key), emit); stopped || err != nil {
					return stopped, err
				}
			case '[':
				if err := jsonSkip(dec); err != nil {
					return false, err
				}
			}
			continue
		case nil:
			continue
		case string:
			value = v
			if strings.ContainsRune(value, utf8.RuneError) {
				value = strings.Map(removeInvalidUTF8, value)
			}
		case json.Number:
			value = v.String()
		case bool:
			value = "false"
			if v {
				value = "true"
			}
		}
		name := jsonLabelName(path, key)
		if name == "" {
			continue
		}
		if !emit(name, value) {
			return true, nil
		}
	}
	if _, err := dec.Token(); err != nil { // the closing '}'; EOF here is a truncated object
		return false, err
	}
	return false, nil
}

// jsonSkip consumes the rest of an array whose '[' has been read.
func jsonSkip(dec *json.Decoder) error {
	for depth := 1; depth > 0; {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := t.(json.Delim); ok {
			switch d {
			case '[', '{':
				depth++
			case ']', '}':
				depth--
			}
		}
	}
	return nil
}

// jsonLabelName names a JSON value by its path, as JSONParser.buildSanitizedPrefixFromBuffer
// does: each part sanitized (appendSanitized), parts that are empty after trimming skipped,
// and the rest joined with '_'.
func jsonLabelName(path []string, key string) string {
	var b []byte
	add := func(part string) {
		part = strings.TrimSpace(part)
		if part == "" {
			return
		}
		if len(b) > 0 {
			b = append(b, '_')
		} else if part[0] >= '0' && part[0] <= '9' {
			b = append(b, '_')
		}
		for _, r := range part {
			if isLabelRune(r) {
				b = append(b, byte(r))
			} else {
				b = append(b, '_')
			}
		}
	}
	for _, p := range path {
		add(p)
	}
	add(key)
	return string(b)
}

// ---- shared ----

// sanitizeLabelKey is Loki's (pkg/logql/log/util.go): trim spaces, prefix '_' to a leading
// digit, and replace every rune that is not [a-zA-Z0-9_] with '_'.
func sanitizeLabelKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	if key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return strings.Map(func(r rune) rune {
		if isLabelRune(r) {
			return r
		}
		return '_'
	}, key)
}

func isLabelRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (r >= '0' && r <= '9')
}

// removeInvalidUTF8 drops the replacement rune, as Loki's removeInvalidUtf does for values.
func removeInvalidUTF8(r rune) rune {
	if r == utf8.RuneError {
		return -1
	}
	return r
}
