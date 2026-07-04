package logql

import (
	"fmt"
	"strings"
)

// Parser is a recursive-descent LogQL parser.
type Parser struct {
	lex *Lexer
	tok Token // current lookahead token
}

// Parse parses a LogQL LOG query string into an AST (a query that must start with '{').
func Parse(input string) (*Query, error) {
	p := &Parser{lex: NewLexer(input)}
	p.advance()
	q, err := p.parseLogBody()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokEOF {
		return nil, p.parseError("unexpected token %s", p.tok.Kind)
	}
	return q, nil
}

// ParseExpr parses either a log query ({...} | ...) or a metric query
// (rate({...}[5m]), sum by(l)(count_over_time({...}[5m])), …).
func ParseExpr(input string) (Expr, error) {
	p := &Parser{lex: NewLexer(input)}
	p.advance()
	var (
		expr Expr
		err  error
	)
	if p.tok.Kind == TokLBRACE {
		expr, err = p.parseLogBody()
	} else {
		expr, err = p.parseMetric()
	}
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokEOF {
		return nil, p.parseError("unexpected token %s", p.tok.Kind)
	}
	return expr, nil
}

// ---- log query body (selector + pipeline), no trailing-token check ----

func (p *Parser) parseLogBody() (*Query, error) {
	sel, err := p.parseSelector()
	if err != nil {
		return nil, err
	}
	var pipeline []PipelineStage
	for p.tok.Kind == TokPIPE || IsLineFilterOp(p.tok.Kind) {
		if p.tok.Kind == TokPIPE {
			p.advance() // consume |
		}
		stage, err := p.parsePipelineStage()
		if err != nil {
			return nil, err
		}
		pipeline = append(pipeline, stage)
	}
	return &Query{Selector: sel, Pipeline: pipeline}, nil
}

// ---- metric queries ----

func isRangeOp(s string) bool {
	switch s {
	case "count_over_time", "rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time":
		return true
	}
	return false
}

func isVectorOp(s string) bool {
	switch s {
	case "sum", "avg", "max", "min", "count":
		return true
	}
	return false
}

func (p *Parser) parseMetric() (*MetricExpr, error) {
	if p.tok.Kind != TokIDENT {
		return nil, p.parseError("expected a metric function, got %s", p.tok.Kind)
	}
	switch {
	case isRangeOp(p.tok.Value):
		return p.parseRangeAgg()
	case isVectorOp(p.tok.Value):
		return p.parseVectorAgg()
	default:
		return nil, p.parseError("unknown metric function %q", p.tok.Value)
	}
}

func (p *Parser) parseVectorAgg() (*MetricExpr, error) {
	op := p.tok.Value
	p.advance()
	grouping, without, err := p.maybeGrouping()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokLPAREN {
		return nil, p.parseError("expected '(' after %s, got %s", op, p.tok.Kind)
	}
	p.advance()
	inner, err := p.parseRangeAgg()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokRPAREN {
		return nil, p.parseError("expected ')' to close %s, got %s", op, p.tok.Kind)
	}
	p.advance()
	// Loki also allows the grouping AFTER the parens: sum(...) by(a).
	if grouping == nil {
		if grouping, without, err = p.maybeGrouping(); err != nil {
			return nil, err
		}
	}
	inner.VectorOp, inner.Grouping, inner.Without = op, grouping, without
	return inner, nil
}

// maybeGrouping parses an optional `by(...)`/`without(...)` clause.
func (p *Parser) maybeGrouping() ([]string, bool, error) {
	if p.tok.Kind != TokIDENT || (p.tok.Value != "by" && p.tok.Value != "without") {
		return nil, false, nil
	}
	without := p.tok.Value == "without"
	p.advance()
	labels, err := p.parseLabelList()
	return labels, without, err
}

func (p *Parser) parseRangeAgg() (*MetricExpr, error) {
	if p.tok.Kind != TokIDENT || !isRangeOp(p.tok.Value) {
		return nil, p.parseError("expected a range aggregation, got %s", p.tok.Kind)
	}
	op := p.tok.Value
	p.advance()
	if p.tok.Kind != TokLPAREN {
		return nil, p.parseError("expected '(' after %s, got %s", op, p.tok.Kind)
	}
	p.advance()
	log, err := p.parseLogBody()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokLBRACK {
		return nil, p.parseError("expected a '[range]' selector, got %s", p.tok.Kind)
	}
	dur, err := p.parseRangeSelector()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != TokRPAREN {
		return nil, p.parseError("expected ')' to close %s, got %s", op, p.tok.Kind)
	}
	p.advance()

	unwrap := ""
	for _, st := range log.Pipeline {
		if u, ok := st.(*UnwrapStage); ok {
			unwrap = u.Field
		}
	}
	return &MetricExpr{RangeOp: op, Log: log, Range: dur, Unwrap: unwrap}, nil
}

// parseRangeSelector consumes `[<duration>]`, returning the raw duration text (the tokens
// between the brackets concatenated, e.g. "5m", "1h30m", "500ms").
func (p *Parser) parseRangeSelector() (string, error) {
	p.advance() // consume [
	var b strings.Builder
	for p.tok.Kind != TokRBRACK {
		if p.tok.Kind == TokEOF {
			return "", p.parseError("unterminated range selector")
		}
		b.WriteString(p.tok.Value)
		p.advance()
	}
	p.advance() // consume ]
	if b.Len() == 0 {
		return "", p.parseError("empty range selector")
	}
	return b.String(), nil
}

func (p *Parser) parseLabelList() ([]string, error) {
	if p.tok.Kind != TokLPAREN {
		return nil, p.parseError("expected '(' for label list, got %s", p.tok.Kind)
	}
	p.advance()
	var labels []string
	if p.tok.Kind == TokRPAREN {
		p.advance()
		return labels, nil
	}
	for {
		if p.tok.Kind != TokIDENT {
			return nil, p.parseError("expected label name, got %s", p.tok.Kind)
		}
		labels = append(labels, p.tok.Value)
		p.advance()
		if p.tok.Kind != TokCOMMA {
			break
		}
		p.advance()
	}
	if p.tok.Kind != TokRPAREN {
		return nil, p.parseError("expected ')' after label list, got %s", p.tok.Kind)
	}
	p.advance()
	return labels, nil
}

// ---- selector ----

func (p *Parser) parseSelector() (LabelSelector, error) {
	if p.tok.Kind != TokLBRACE {
		return LabelSelector{}, p.parseError("expected '{', got %s", p.tok.Kind)
	}
	p.advance() // consume {

	// Empty selector: {}
	if p.tok.Kind == TokRBRACE {
		p.advance()
		return LabelSelector{}, nil
	}

	matchers, err := p.parseMatcherList()
	if err != nil {
		return LabelSelector{}, err
	}

	if p.tok.Kind != TokRBRACE {
		return LabelSelector{}, p.parseError("expected '}' or ',', got %s", p.tok.Kind)
	}
	p.advance()

	return LabelSelector{Matchers: matchers}, nil
}

func (p *Parser) parseMatcherList() ([]LabelMatcher, error) {
	first, err := p.parseMatcher()
	if err != nil {
		return nil, err
	}
	matchers := []LabelMatcher{first}

	for p.tok.Kind == TokCOMMA {
		p.advance() // consume ,
		m, err := p.parseMatcher()
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

func (p *Parser) parseMatcher() (LabelMatcher, error) {
	if p.tok.Kind != TokIDENT {
		return LabelMatcher{}, p.parseError("expected label name, got %s", p.tok.Kind)
	}
	label := p.tok.Value
	p.advance()

	op, err := p.parseLabelOp()
	if err != nil {
		return LabelMatcher{}, err
	}

	if p.tok.Kind != TokSTRING {
		return LabelMatcher{}, p.parseError("expected string value, got %s", p.tok.Kind)
	}
	value := p.tok.Value
	p.advance()

	return LabelMatcher{Label: label, Op: op, Value: value}, nil
}

func (p *Parser) parseLabelOp() (Op, error) {
	switch p.tok.Kind {
	case TokEQ, TokNEQ, TokREQ, TokNRE:
		op := TokenToLabelOp(p.tok.Kind)
		p.advance()
		return op, nil
	default:
		return OpNone, p.parseError("expected label operator (=, !=, =~, !~), got %s", p.tok.Kind)
	}
}

// ---- pipeline stages ----

func (p *Parser) parsePipelineStage() (PipelineStage, error) {
	switch {
	case IsLineFilterOp(p.tok.Kind):
		return p.parseLineFilter()
	case p.tok.Kind == TokIDENT && p.tok.Value == "unwrap":
		return p.parseUnwrap()
	case p.tok.Kind == TokIDENT && isParserKeyword(p.tok.Value):
		return p.parseParserStage()
	case p.tok.Kind == TokIDENT:
		return p.parseLabelFilter()
	default:
		return nil, p.parseError("expected line filter, parser stage, or label filter after |, got %s", p.tok.Kind)
	}
}

func (p *Parser) parseUnwrap() (*UnwrapStage, error) {
	p.advance() // consume "unwrap"
	if p.tok.Kind != TokIDENT {
		return nil, p.parseError("expected a field name after unwrap, got %s", p.tok.Kind)
	}
	field := p.tok.Value
	p.advance()
	return &UnwrapStage{Field: field}, nil
}

func isParserKeyword(s string) bool {
	return s == "json" || s == "logfmt"
}

func (p *Parser) parseLineFilter() (*LineFilter, error) {
	op := TokenOpToLogQLOp(p.tok.Kind)
	if op == OpNone {
		return nil, p.parseError("invalid line filter operator %s", p.tok.Kind)
	}
	p.advance()

	if p.tok.Kind != TokSTRING {
		return nil, p.parseError("expected string after line filter operator, got %s", p.tok.Kind)
	}
	value := p.tok.Value
	p.advance()

	return &LineFilter{Op: op, Value: value}, nil
}

func (p *Parser) parseParserStage() (*ParserStage, error) {
	parser := p.tok.Value
	p.advance()
	return &ParserStage{Parser: parser}, nil
}

func (p *Parser) parseLabelFilter() (*LabelFilter, error) {
	label := p.tok.Value
	p.advance()

	if !IsLabelOp(p.tok.Kind) {
		return nil, p.parseError("expected label operator after label name, got %s", p.tok.Kind)
	}
	op := TokenToLabelOp(p.tok.Kind)
	p.advance()

	// Label-filter values may be quoted strings or bare numbers (e.g.
	// `| latency > 100`). Numeric comparison is resolved by the compiler.
	if p.tok.Kind != TokSTRING && p.tok.Kind != TokNUMBER {
		return nil, p.parseError("expected string or number value, got %s", p.tok.Kind)
	}
	value := p.tok.Value
	p.advance()

	return &LabelFilter{Label: label, Op: op, Value: value}, nil
}

// ---- helpers ----

func (p *Parser) advance() {
	p.tok = p.lex.Next()
}

func (p *Parser) parseError(format string, args ...interface{}) *ParseError {
	return &ParseError{
		Msg: fmt.Sprintf(format, args...),
		Pos: p.tok.Pos,
	}
}
