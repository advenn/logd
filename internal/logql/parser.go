package logql

import "fmt"

// Parser is a recursive-descent LogQL parser.
type Parser struct {
	lex *Lexer
	tok Token // current lookahead token
}

// Parse parses a LogQL query string into an AST.
func Parse(input string) (*Query, error) {
	p := &Parser{lex: NewLexer(input)}
	p.advance()
	return p.parseQuery()
}

// ---- top-level ----

func (p *Parser) parseQuery() (*Query, error) {
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

	if p.tok.Kind != TokEOF {
		return nil, p.parseError("unexpected token %s", p.tok.Kind)
	}

	return &Query{Selector: sel, Pipeline: pipeline}, nil
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
	case p.tok.Kind == TokIDENT && isParserKeyword(p.tok.Value):
		return p.parseParserStage()
	case p.tok.Kind == TokIDENT:
		return p.parseLabelFilter()
	default:
		return nil, p.parseError("expected line filter, parser stage, or label filter after |, got %s", p.tok.Kind)
	}
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
