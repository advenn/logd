package pgdata

import (
	"fmt"
	"strconv"
)

// Op is a comparison operator a predicate may use.
type Op string

const (
	OpGT Op = ">"
	OpGE Op = ">="
	OpLT Op = "<"
	OpLE Op = "<="
	OpEQ Op = "="
)

// sqlOps maps each accepted operator to the SQL text QuerySQL emits for it. It is the whole
// whitelist: an operator not in this table cannot reach a statement.
var sqlOps = map[Op]string{OpGT: ">", OpGE: ">=", OpLT: "<", OpLE: "<=", OpEQ: "="}

// Predicate is a parsed `field op integer`.
type Predicate struct {
	Field string
	Op    Op
	Value int64
}

// ParsePredicate parses the shim's entire query language:
//
//	predicate := ws* field ws* op ws* integer ws*
//	field     := [a-z_][a-z0-9_]*        (and must be a known dataset field)
//	op        := ">=" | "<=" | ">" | "<" | "="
//	integer   := "-"? [0-9]+             (must fit in int64)
//	ws        := ' ' | '\t'
//
// It is deliberately tiny and hand-written. The shim exists to time PostgreSQL over HTTP,
// not to be a query language, and anything it does not understand is rejected rather than
// passed through: nothing from the request ever becomes SQL text.
func ParsePredicate(s string) (Predicate, error) {
	p := parser{s: s}
	p.skipSpace()
	field := p.ident()
	if field == "" {
		return Predicate{}, p.errorf("expected a field name")
	}
	if _, ok := ByField(field); !ok {
		return Predicate{}, fmt.Errorf("unknown field %q (known: %s)", field, knownFields())
	}
	p.skipSpace()
	op := p.op()
	if op == "" {
		return Predicate{}, p.errorf("expected one of > >= < <= =")
	}
	p.skipSpace()
	num := p.integer()
	if num == "" {
		return Predicate{}, p.errorf("expected an integer")
	}
	v, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return Predicate{}, fmt.Errorf("integer %s out of range", num)
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return Predicate{}, p.errorf("unexpected trailing input")
	}
	return Predicate{Field: field, Op: op, Value: v}, nil
}

type parser struct {
	s string
	i int
}

func (p *parser) errorf(format string, args ...any) error {
	return fmt.Errorf("query %q at offset %d: %s", p.s, p.i, fmt.Sprintf(format, args...))
}

func (p *parser) skipSpace() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

func (p *parser) ident() string {
	start := p.i
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == '_' || (c >= 'a' && c <= 'z') || (p.i > start && c >= '0' && c <= '9') {
			p.i++
			continue
		}
		break
	}
	return p.s[start:p.i]
}

func (p *parser) op() Op {
	rest := p.s[p.i:]
	for _, op := range []Op{OpGE, OpLE, OpGT, OpLT, OpEQ} { // two-byte operators first
		if len(rest) >= len(op) && rest[:len(op)] == string(op) {
			p.i += len(op)
			return op
		}
	}
	return ""
}

func (p *parser) integer() string {
	start := p.i
	if p.i < len(p.s) && p.s[p.i] == '-' {
		p.i++
	}
	digits := p.i
	for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		p.i++
	}
	if p.i == digits {
		p.i = start
		return ""
	}
	return p.s[start:p.i]
}

func knownFields() string {
	var out string
	for i, d := range All() {
		if i > 0 {
			out += ", "
		}
		out += d.Field
	}
	return out
}
