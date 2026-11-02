// Package logql implements a LogQL query parser and pipeline executor for logd.
// It supports label selectors, line filters, parser stages, and label filters
// as described in the logd design doc §6 (discussion-v2.md).
package logql

import "fmt"

// Op is a LogQL operator used in label matchers, line filters, and label filters.
type Op int

const (
	OpNone Op = iota

	// Label matcher ops (selector).
	OpEq    // =
	OpNotEq // !=
	OpRe    // =~
	OpNotRe // !~

	// Line filter ops.
	OpPipeEq    // |=
	OpPipeNotEq // !=  (line filter variant)
	OpPipeRe    // |~
	OpPipeNotRe // !~  (line filter variant)

	// Label filter comparison ops.
	OpGT  // >
	OpGTE // >=
	OpLT  // <
	OpLTE // <=
)

// String returns the LogQL operator string.
func (o Op) String() string {
	switch o {
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpRe:
		return "=~"
	case OpNotRe:
		return "!~"
	case OpPipeEq:
		return "|="
	case OpPipeNotEq:
		return "!="
	case OpPipeRe:
		return "|~"
	case OpPipeNotRe:
		return "!~"
	case OpGT:
		return ">"
	case OpGTE:
		return ">="
	case OpLT:
		return "<"
	case OpLTE:
		return "<="
	default:
		return "?"
	}
}

// IsComparison returns true for ordered comparison operators (>, >=, <, <=).
func (o Op) IsComparison() bool {
	return o == OpGT || o == OpGTE || o == OpLT || o == OpLTE
}

// IsRegex returns true for regex-match operators (=~, !~, |~, !~).
func (o Op) IsRegex() bool {
	return o == OpRe || o == OpNotRe || o == OpPipeRe || o == OpPipeNotRe
}

// ---- AST nodes ----

// Expr is a top-level LogQL expression: either a log query or a metric query.
type Expr interface{ isExpr() }

// Query is a parsed LogQL LOG query: a label selector followed by optional pipeline stages.
type Query struct {
	Selector LabelSelector
	Pipeline []PipelineStage
}

func (*Query) isExpr() {}

// MetricExpr is a parsed LogQL METRIC query: an optional vector aggregation over a range
// aggregation over a log query, e.g. `sum by(region) (rate({app="api"}[5m]))`.
type MetricExpr struct {
	VectorOp string   // "sum"/"avg"/"max"/"min"/"count"; "" = no vector aggregation
	Grouping []string // labels for by(...)/without(...)
	Without  bool
	RangeOp  string // "count_over_time"/"rate"/"sum_over_time"/"avg_over_time"/"max_over_time"/"min_over_time"
	Log      *Query // the inner log query (selector + pipeline, may include an unwrap stage)
	Range    string // range-selector duration text, e.g. "5m"
	Unwrap   string // unwrap field (from a `| unwrap field` pipeline stage), or ""
}

func (*MetricExpr) isExpr() {}

// UnwrapStage is `| unwrap <field>` — marks a numeric field/label to extract for the
// unwrap range aggregations (sum_over_time, avg_over_time, …).
type UnwrapStage struct {
	Field string
}

func (*UnwrapStage) pipelineStage() {}

// LabelSelector is the {key="val",...} stream selector.
type LabelSelector struct {
	Matchers []LabelMatcher
}

// LabelMatcher is a single key=value or key=~regex pair in a selector.
type LabelMatcher struct {
	Label string
	Op    Op // OpEq, OpNotEq, OpRe, OpNotRe
	Value string
}

// PipelineStage is a stage in the LogQL pipeline (line filter, parser, or label filter).
type PipelineStage interface {
	pipelineStage()
}

// LineFilter matches the log line message against a string or regex.
type LineFilter struct {
	Op    Op     // OpPipeEq, OpPipeNotEq, OpPipeRe, OpPipeNotRe
	Value string // the raw pattern string (compiled to regex at pipeline build time)
}

func (*LineFilter) pipelineStage() {}

// ParserStage parses the log line to extract labels: `| json`, `| logfmt`,
// `| pattern "<expr>"` or `| regexp "<re>"`. Only the plain, parameterless forms of json and
// logfmt exist here; the parser refuses the others.
type ParserStage struct {
	Parser string // "json", "logfmt", "pattern" or "regexp"
	Expr   string // the pattern expression or regex (pattern and regexp only)
}

func (*ParserStage) pipelineStage() {}

// LabelsStage is `| drop <labels>` or `| keep <labels>`: it removes labels from (or
// keeps only the named labels on) each result. Each item is a bare name (Op == OpNone) or
// a matcher such as `level="debug"`, which drops the label only where it matches.
type LabelsStage struct {
	Keep   bool // false: drop
	Labels []LabelMatcher
}

func (*LabelsStage) pipelineStage() {}

// LabelFilter applies a filter to stream labels or extracted labels.
type LabelFilter struct {
	Label string
	Op    Op // OpEq, OpNotEq, OpRe, OpNotRe, OpGT, OpGTE, OpLT, OpLTE
	Value string
	// Numeric is true when Value was written as a number (`| x = 200`) rather than a string
	// (`| x = "200"`). Loki's grammar makes the first a numeric filter and the second a string
	// one; the difference matters for labels extracted at query time.
	Numeric bool
}

func (*LabelFilter) pipelineStage() {}

// ---- Pretty-printing ----

// String reconstructs the LogQL query string from the AST.
func (q *Query) String() string {
	s := q.Selector.String()
	for _, p := range q.Pipeline {
		s += " " + stageString(p)
	}
	return s
}

func (s LabelSelector) String() string {
	if len(s.Matchers) == 0 {
		return "{}"
	}
	str := "{"
	for i, m := range s.Matchers {
		if i > 0 {
			str += ","
		}
		str += m.Label + m.Op.String() + fmt.Sprintf("%q", m.Value)
	}
	str += "}"
	return str
}

func stageString(s PipelineStage) string {
	switch s := s.(type) {
	case *LineFilter:
		return s.Op.String() + " " + fmt.Sprintf("%q", s.Value)
	case *ParserStage:
		if s.Parser == "pattern" || s.Parser == "regexp" {
			return "| " + s.Parser + " " + fmt.Sprintf("%q", s.Expr)
		}
		return "| " + s.Parser
	case *LabelFilter:
		if s.Numeric {
			return "| " + s.Label + " " + s.Op.String() + " " + s.Value
		}
		return "| " + s.Label + " " + s.Op.String() + " " + fmt.Sprintf("%q", s.Value)
	case *UnwrapStage:
		return "| unwrap " + s.Field
	case *LabelsStage:
		str := "| drop "
		if s.Keep {
			str = "| keep "
		}
		for i, m := range s.Labels {
			if i > 0 {
				str += ", "
			}
			str += m.Label
			if m.Op != OpNone {
				str += m.Op.String() + fmt.Sprintf("%q", m.Value)
			}
		}
		return str
	default:
		return "| ?"
	}
}
