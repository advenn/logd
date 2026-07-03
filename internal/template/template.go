// Package template implements template-based dynamic field extraction and indexing
// for logd. Users define templates in config.yaml with patterns like
// "order-{order_id:uint32} created". Templates compile to regexes. On ingestion,
// log messages are matched against templates, fields are extracted, and inverted
// indexes are built for O(log n) field-level lookup instead of full segment scans.
package template

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/advenn/logd/internal/config"
)

// FieldType classifies an extracted field's data type. Determines the index
// structure and the regex pattern used during template compilation.
type FieldType uint8

const (
	FieldTypeUint32 FieldType = iota
	FieldTypeUint64
	FieldTypeString
	FieldTypeEnum
)

// String returns the field type name.
func (ft FieldType) String() string {
	switch ft {
	case FieldTypeUint32:
		return "uint32"
	case FieldTypeUint64:
		return "uint64"
	case FieldTypeString:
		return "string"
	case FieldTypeEnum:
		return "enum"
	default:
		return "unknown"
	}
}

// FieldDescriptor describes one extracted field in a compiled template.
type FieldDescriptor struct {
	Name       string // e.g. "order_id"
	Type       FieldType
	EnumValues []string // only populated for FieldTypeEnum
	Indexed    bool     // true if this field should be indexed
	CaptureIdx int      // index into regexp.SubexpNames() for extraction
}

// CompiledTemplate is a template pattern compiled into an executable regexp.
type CompiledTemplate struct {
	Name    string
	Pattern string            // original pattern string from config
	Regex   *regexp.Regexp    // compiled regex
	Fields  []FieldDescriptor // only indexed fields
}

// FieldMatch is the result of matching a single template against a log message.
type FieldMatch struct {
	TemplateName string
	FieldName    string
	Value        string // always stored as string; indexing layer converts for numeric types
}

// Engine holds all compiled templates and runs matching against log messages.
type Engine struct {
	templates []*CompiledTemplate
}

// NewEngine compiles all configured templates into an executable Engine.
// Returns an error if any template pattern is invalid or contains unsupported
// field types.
func NewEngine(cfgTemplates []config.Template) (*Engine, error) {
	if len(cfgTemplates) == 0 {
		return &Engine{}, nil
	}

	var compiled []*CompiledTemplate
	for _, t := range cfgTemplates {
		ct, err := compileTemplate(t)
		if err != nil {
			return nil, fmt.Errorf("template %q: %w", t.Name, err)
		}
		compiled = append(compiled, ct)
	}

	return &Engine{templates: compiled}, nil
}

// Match runs all compiled templates against a log message and returns the
// indexed field matches. Non-indexed fields are not included in the output.
func (e *Engine) Match(message string) []FieldMatch {
	var matches []FieldMatch
	for _, t := range e.templates {
		m := t.Regex.FindStringSubmatch(message)
		if m == nil {
			continue
		}
		for _, fd := range t.Fields {
			if fd.CaptureIdx < len(m) {
				matches = append(matches, FieldMatch{
					TemplateName: t.Name,
					FieldName:    fd.Name,
					Value:        m[fd.CaptureIdx],
				})
			}
		}
	}
	return matches
}

// IsIndexedField reports whether name is an indexed template field — i.e. a
// field that has a `.tidx` index and can be queried with index pushdown.
func (e *Engine) IsIndexedField(name string) bool {
	for _, t := range e.templates {
		for _, fd := range t.Fields {
			if fd.Indexed && fd.Name == name {
				return true
			}
		}
	}
	return false
}

// IndexedFields returns a flat list of all indexed field descriptors across all
// compiled templates. Used by the IndexBuilder to know which fields to track.
func (e *Engine) IndexedFields() []FieldDescriptor {
	var fields []FieldDescriptor
	for _, t := range e.templates {
		fields = append(fields, t.Fields...)
	}
	return fields
}

// compileTemplate converts a config.Template into a CompiledTemplate.
func compileTemplate(t config.Template) (*CompiledTemplate, error) {
	if t.Pattern == "" {
		return nil, fmt.Errorf("pattern is required")
	}
	if len(t.Fields) == 0 {
		return nil, fmt.Errorf("at least one field is required")
	}

	// Build field descriptors from config and validate.
	fieldDesc, err := buildFieldDescriptors(t.Fields)
	if err != nil {
		return nil, err
	}

	// Compile the pattern to a regex.
	regex, captureIdx, err := compilePattern(t.Pattern, fieldDesc)
	if err != nil {
		return nil, err
	}

	// Set capture indices on descriptors.
	for i := range fieldDesc {
		fieldDesc[i].CaptureIdx = captureIdx[fieldDesc[i].Name]
	}

	// Only track indexed fields.
	var indexed []FieldDescriptor
	for _, fd := range fieldDesc {
		if fd.Indexed {
			indexed = append(indexed, fd)
		}
	}

	return &CompiledTemplate{
		Name:    t.Name,
		Pattern: t.Pattern,
		Regex:   regex,
		Fields:  indexed,
	}, nil
}

// buildFieldDescriptors converts config field configs into internal descriptors.
func buildFieldDescriptors(fields []config.FieldConfig) ([]FieldDescriptor, error) {
	seen := make(map[string]bool)
	var descs []FieldDescriptor
	for _, f := range fields {
		if f.Name == "" {
			return nil, fmt.Errorf("field name is required")
		}
		if seen[f.Name] {
			return nil, fmt.Errorf("duplicate field %q", f.Name)
		}
		seen[f.Name] = true

		ft, err := parseFieldType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}

		if ft == FieldTypeEnum && len(f.Values) == 0 {
			return nil, fmt.Errorf("field %q: enum type requires values", f.Name)
		}

		descs = append(descs, FieldDescriptor{
			Name:       f.Name,
			Type:       ft,
			EnumValues: f.Values,
			Indexed:    f.Index,
		})
	}
	return descs, nil
}

func parseFieldType(s string) (FieldType, error) {
	switch s {
	case "uint32":
		return FieldTypeUint32, nil
	case "uint64":
		return FieldTypeUint64, nil
	case "string":
		return FieldTypeString, nil
	case "enum":
		return FieldTypeEnum, nil
	default:
		return 0, fmt.Errorf("unsupported field type %q (valid: uint32, uint64, string, enum)", s)
	}
}

// compilePattern converts a template pattern like "order-{order_id:uint32} created"
// into a compiled regexp and a map of capture group name → index.
func compilePattern(pattern string, fields []FieldDescriptor) (*regexp.Regexp, map[string]int, error) {
	// Build a lookup from field name → type for the pattern parser.
	fieldType := make(map[string]FieldType, len(fields))
	fieldEnum := make(map[string][]string)
	for _, fd := range fields {
		fieldType[fd.Name] = fd.Type
		if fd.Type == FieldTypeEnum {
			fieldEnum[fd.Name] = fd.EnumValues
		}
	}

	var re strings.Builder
	re.WriteByte('^')

	rest := pattern
	for rest != "" {
		brace := strings.IndexByte(rest, '{')
		if brace < 0 {
			// Remainder is literal.
			re.WriteString(regexp.QuoteMeta(rest))
			break
		}

		// Literal segment before brace.
		if brace > 0 {
			re.WriteString(regexp.QuoteMeta(rest[:brace]))
		}

		rest = rest[brace+1:]
		close := strings.IndexByte(rest, '}')
		if close < 0 {
			return nil, nil, fmt.Errorf("unclosed '{' in pattern")
		}

		spec := rest[:close]
		rest = rest[close+1:]

		// Parse field_name:type.
		colon := strings.IndexByte(spec, ':')
		if colon < 0 {
			return nil, nil, fmt.Errorf("invalid field spec %q: expected name:type", spec)
		}
		name := spec[:colon]
		typeStr := spec[colon+1:]

		ft, ok := fieldType[name]
		if !ok {
			// Field not declared in config — treat as a string capture in regex
			// but with default string type.
			ft = FieldTypeString
		}
		_ = typeStr // type is already validated; use fieldType lookup

		switch ft {
		case FieldTypeUint32:
			fmt.Fprintf(&re, `(?P<%s>\d{1,10})`, name)
		case FieldTypeUint64:
			fmt.Fprintf(&re, `(?P<%s>\d{1,20})`, name)
		case FieldTypeString:
			fmt.Fprintf(&re, `(?P<%s>\S+)`, name)
		case FieldTypeEnum:
			vals := fieldEnum[name]
			// Sort longest-first for correct alternation matching.
			sorted := make([]string, len(vals))
			copy(sorted, vals)
			sort.Slice(sorted, func(i, j int) bool {
				return len(sorted[i]) > len(sorted[j])
			})
			escaped := make([]string, len(sorted))
			for i, v := range sorted {
				escaped[i] = regexp.QuoteMeta(v)
			}
			fmt.Fprintf(&re, `(?P<%s>%s)`, name, strings.Join(escaped, "|"))
		}
	}

	re.WriteByte('$')

	compiled, err := regexp.Compile(re.String())
	if err != nil {
		return nil, nil, fmt.Errorf("compiling regex: %w", err)
	}

	// Build capture index lookup.
	captureIdx := make(map[string]int)
	for i, n := range compiled.SubexpNames() {
		if n != "" {
			captureIdx[n] = i
		}
	}

	return compiled, captureIdx, nil
}
