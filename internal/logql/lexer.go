package logql

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// TokenKind identifies the type of a lexer token.
type TokenKind int

const (
	TokEOF TokenKind = iota
	TokLBRACE
	TokRBRACE
	TokCOMMA
	TokPIPE    // |
	TokEQ      // =
	TokNEQ     // !=
	TokREQ     // =~
	TokNRE     // !~
	TokPIPE_EQ // |=
	TokPIPE_RE // |~
	TokGT      // >
	TokGTE     // >=
	TokLT      // <
	TokLTE     // <=
	TokSTRING  // "..." or '...'
	TokIDENT   // [a-zA-Z_][a-zA-Z0-9_]*
	TokNUMBER  // 123, -1.5, 2e3
)

// String returns a human-readable name for the token kind.
func (k TokenKind) String() string {
	switch k {
	case TokEOF:
		return "EOF"
	case TokLBRACE:
		return "{"
	case TokRBRACE:
		return "}"
	case TokCOMMA:
		return ","
	case TokPIPE:
		return "|"
	case TokEQ:
		return "="
	case TokNEQ:
		return "!="
	case TokREQ:
		return "=~"
	case TokNRE:
		return "!~"
	case TokPIPE_EQ:
		return "|="
	case TokPIPE_RE:
		return "|~"
	case TokGT:
		return ">"
	case TokGTE:
		return ">="
	case TokLT:
		return "<"
	case TokLTE:
		return "<="
	case TokSTRING:
		return "STRING"
	case TokIDENT:
		return "IDENT"
	case TokNUMBER:
		return "NUMBER"
	default:
		return "?"
	}
}

// Token is a single LogQL token.
type Token struct {
	Kind  TokenKind
	Value string // raw text (unquoted for strings)
	Pos   int    // byte offset in the input
}

// Lexer tokenizes a LogQL query string.
type Lexer struct {
	input string
	pos   int
	start int
}

// NewLexer creates a lexer for the given LogQL query string.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// Next returns the next token from the input.
func (l *Lexer) Next() Token {
	l.skipWhitespace()
	l.start = l.pos

	if l.pos >= len(l.input) {
		return Token{Kind: TokEOF, Pos: l.pos}
	}

	ch := l.current()

	// Multi-character tokens that start with |, !, =, >, <.
	if ch == '|' && l.peek() == '=' {
		return l.advance2(TokPIPE_EQ)
	}
	if ch == '|' && l.peek() == '~' {
		return l.advance2(TokPIPE_RE)
	}
	if ch == '!' && l.peek() == '=' {
		return l.advance2(TokNEQ)
	}
	if ch == '!' && l.peek() == '~' {
		return l.advance2(TokNRE)
	}
	if ch == '=' && l.peek() == '~' {
		return l.advance2(TokREQ)
	}
	if ch == '>' && l.peek() == '=' {
		return l.advance2(TokGTE)
	}
	if ch == '<' && l.peek() == '=' {
		return l.advance2(TokLTE)
	}

	// Single-character tokens.
	switch ch {
	case '{':
		l.pos++
		return Token{Kind: TokLBRACE, Value: "{", Pos: l.start}
	case '}':
		l.pos++
		return Token{Kind: TokRBRACE, Value: "}", Pos: l.start}
	case ',':
		l.pos++
		return Token{Kind: TokCOMMA, Value: ",", Pos: l.start}
	case '|':
		l.pos++
		return Token{Kind: TokPIPE, Value: "|", Pos: l.start}
	case '=':
		l.pos++
		return Token{Kind: TokEQ, Value: "=", Pos: l.start}
	case '>':
		l.pos++
		return Token{Kind: TokGT, Value: ">", Pos: l.start}
	case '<':
		l.pos++
		return Token{Kind: TokLT, Value: "<", Pos: l.start}
	case '"', '\'':
		return l.scanString(ch)
	case '`':
		return l.scanRawString()
	}

	// Numbers (integer or float, optional leading minus): used as label-filter
	// comparison values, e.g. `| latency > 100`.
	if isDigit(ch) || (ch == '-' && isDigit(l.peek())) {
		return l.scanNumber()
	}

	// Identifiers.
	if isIdentStart(ch) {
		return l.scanIdent()
	}

	l.pos++
	return Token{Kind: TokEOF, Value: string(ch), Pos: l.start}
}

// Peek returns the next token without consuming it.
func (l *Lexer) Peek() Token {
	saved := l.pos
	tok := l.Next()
	l.pos = saved
	return tok
}

// Rest returns the remaining unparsed input.
func (l *Lexer) Rest() string {
	return l.input[l.pos:]
}

func (l *Lexer) current() byte {
	return l.input[l.pos]
}

func (l *Lexer) peek() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func (l *Lexer) advance2(kind TokenKind) Token {
	val := l.input[l.pos : l.pos+2]
	l.pos += 2
	return Token{Kind: kind, Value: val, Pos: l.start}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' {
			return
		}
		l.pos++
	}
}

// scanNumber consumes an integer or floating-point literal with an optional
// leading minus and optional exponent. The raw text is kept as the token value
// (parsed to a float by the label-filter compiler).
func (l *Lexer) scanNumber() Token {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
	}
	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		l.pos++
		if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
			l.pos++
		}
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
	}
	return Token{Kind: TokNUMBER, Value: l.input[start:l.pos], Pos: l.start}
}

func isDigit(ch byte) bool { return ch >= '0' && ch <= '9' }

func (l *Lexer) scanString(quote byte) Token {
	// Advance past the opening quote.
	l.pos++

	var buf strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			next := l.input[l.pos+1]
			switch next {
			case '"', '\'', '\\':
				l.pos += 2
				buf.WriteByte(next)
				continue
			case 'n':
				l.pos += 2
				buf.WriteByte('\n')
				continue
			case 't':
				l.pos += 2
				buf.WriteByte('\t')
				continue
			case 'r':
				l.pos += 2
				buf.WriteByte('\r')
				continue
			}
		}
		if ch == quote {
			l.pos++
			return Token{Kind: TokSTRING, Value: buf.String(), Pos: l.start}
		}
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		buf.WriteRune(r)
		l.pos += size
	}

	// Unterminated string — return what we have.
	return Token{Kind: TokSTRING, Value: buf.String(), Pos: l.start}
}

// scanRawString scans a backtick-quoted raw string (Go/LogQL semantics: no
// escape processing, runs to the next backtick). Grafana sends line-filter and
// matcher values this way, including the empty filter `|= “ `. An empty raw
// string is valid and matches every line.
func (l *Lexer) scanRawString() Token {
	l.pos++ // past opening backtick
	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == '`' {
			val := l.input[start:l.pos]
			l.pos++ // past closing backtick
			return Token{Kind: TokSTRING, Value: val, Pos: l.start}
		}
		l.pos++
	}
	// Unterminated — return what we have.
	return Token{Kind: TokSTRING, Value: l.input[start:l.pos], Pos: l.start}
}

func (l *Lexer) scanIdent() Token {
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if !isIdentCont(ch) {
			break
		}
		l.pos++
	}
	return Token{Kind: TokIDENT, Value: l.input[start:l.pos], Pos: l.start}
}

func isIdentStart(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ch == '_'
}

func isIdentCont(ch byte) bool {
	return isIdentStart(ch) || ('0' <= ch && ch <= '9')
}

// ---- Helpers for the parser ----

// IsLineFilterOp reports whether the token kind starts a line filter stage.
func IsLineFilterOp(k TokenKind) bool {
	return k == TokPIPE_EQ || k == TokPIPE_RE || k == TokNEQ || k == TokNRE
}

// TokenOpToLogQLOp maps a token kind to the corresponding line filter Op.
func TokenOpToLogQLOp(k TokenKind) Op {
	switch k {
	case TokPIPE_EQ:
		return OpPipeEq
	case TokPIPE_RE:
		return OpPipeRe
	case TokNEQ:
		return OpPipeNotEq
	case TokNRE:
		return OpPipeNotRe
	case TokEQ:
		return OpEq
	case TokREQ:
		return OpRe
	case TokGT:
		return OpGT
	case TokGTE:
		return OpGTE
	case TokLT:
		return OpLT
	case TokLTE:
		return OpLTE
	default:
		return OpNone
	}
}

// TokenToLabelOp maps a token to the operator used in a label/selector context
// (stream selectors and `| label op value` filters). Unlike TokenOpToLogQLOp,
// `!=` and `!~` map to the label operators OpNotEq/OpNotRe (not the line-filter
// OpPipeNotEq/OpPipeNotRe), so label-filter evaluation handles them correctly.
func TokenToLabelOp(k TokenKind) Op {
	switch k {
	case TokEQ:
		return OpEq
	case TokNEQ:
		return OpNotEq
	case TokREQ:
		return OpRe
	case TokNRE:
		return OpNotRe
	case TokGT:
		return OpGT
	case TokGTE:
		return OpGTE
	case TokLT:
		return OpLT
	case TokLTE:
		return OpLTE
	default:
		return OpNone
	}
}

// IsLabelOp reports whether the token kind is a label filter operator.
func IsLabelOp(k TokenKind) bool {
	switch k {
	case TokEQ, TokNEQ, TokREQ, TokNRE, TokGT, TokGTE, TokLT, TokLTE:
		return true
	}
	return false
}

// ---- Parse error type ----

// ParseError describes a LogQL parse failure with position information.
type ParseError struct {
	Msg string
	Pos int
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("logql: %s at position %d", e.Msg, e.Pos)
}

// ---- Testing helpers ----

// Tokens extracts all tokens from a LogQL string, excluding EOF. Useful for testing.
func Tokens(input string) []Token {
	var toks []Token
	l := NewLexer(input)
	for {
		tok := l.Next()
		if tok.Kind == TokEOF {
			break
		}
		toks = append(toks, tok)
	}
	return toks
}
