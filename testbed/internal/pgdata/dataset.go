// Package pgdata describes the PostgreSQL baseline's datasets in one place: which table holds
// which variant, which field a predicate may name, and the SQL expression that extracts that
// field from a raw line.
//
// Both the query shim (cmd/pgshim) and the loader (cmd/pgload) read it, and a test holds it
// to stack/postgres/schema.sql. That matters because the two variants must answer the same
// question: the unstructured variant re-extracts the value at query time with Expr, the
// structured one reads a generated column computed at insert from the same expression. If
// the two expressions drifted apart, a "structured vs unstructured" latency difference would
// partly be a difference in what was asked.
package pgdata

import (
	"fmt"
	"sort"
)

// Variant selects how the value is stored.
type Variant string

const (
	// Structured reads a generated column with a B-tree on it.
	Structured Variant = "structured"
	// Unstructured scans raw lines and extracts the value at query time.
	Unstructured Variant = "unstructured"
)

// ParseVariant accepts exactly the two variant names.
func ParseVariant(s string) (Variant, error) {
	switch Variant(s) {
	case Structured, Unstructured:
		return Variant(s), nil
	case "":
		return "", fmt.Errorf("variant is required: %q or %q", Structured, Unstructured)
	default:
		return "", fmt.Errorf("unknown variant %q: want %q or %q", s, Structured, Unstructured)
	}
}

// Dataset is one corpus as PostgreSQL stores it.
type Dataset struct {
	Name         string // selected by the shim's dataset= parameter
	Field        string // the only field a predicate on this dataset may name
	Structured   string // table with the generated column and its index
	Unstructured string // table with raw lines only
	// Expr extracts Field from the column `line`. It must be byte-identical to the
	// generated column's expression in schema.sql, and must agree with the regular
	// expression the manifest's ground truth was computed with (first occurrence).
	Expr string
	// Contains is a literal every line carrying a value contains. The unstructured
	// variant puts `line LIKE '%<Contains>%'` before the extraction, for the same reason
	// the Loki queries carry `|= "took="`: leaving the cheap prefilter out would sandbag
	// the baseline, and the first reader would say so.
	Contains string
}

var datasets = []Dataset{
	{
		Name:         "app",
		Field:        "latency_ms",
		Structured:   "logs",
		Unstructured: "logs_raw",
		Expr:         `(substring(line FROM 'took=([0-9]+)ms'))::integer`,
		Contains:     "took=",
	},
	{
		// Loghub HDFS_v1 replay. The regular expression is loggen's -replay-value-regex,
		// i.e. the manifest's ground truth; "size " is the anchor all five line forms share.
		Name:         "hdfs",
		Field:        "size_bytes",
		Structured:   "hdfs_logs",
		Unstructured: "hdfs_logs_raw",
		Expr:         `(substring(line FROM 'size ([0-9]+)'))::bigint`,
		Contains:     "size ",
	},
}

// Lookup returns the dataset with this name.
func Lookup(name string) (Dataset, bool) {
	for _, d := range datasets {
		if d.Name == name {
			return d, true
		}
	}
	return Dataset{}, false
}

// ByField returns the dataset whose predicate field is field. Field names are unique across
// datasets, so a query names its dataset implicitly.
func ByField(field string) (Dataset, bool) {
	for _, d := range datasets {
		if d.Field == field {
			return d, true
		}
	}
	return Dataset{}, false
}

// All returns every dataset, sorted by name.
func All() []Dataset {
	out := append([]Dataset(nil), datasets...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Table returns the table that stores variant v.
func (d Dataset) Table(v Variant) string {
	if v == Unstructured {
		return d.Unstructured
	}
	return d.Structured
}

// QuerySQL returns the statement for one predicate shape. Parameters are
//
//	$1 the threshold, $2 start (inclusive, ns), $3 end (exclusive, ns), $4 the limit
//
// Only constants from this package are formatted into the SQL text: the operator comes from
// a fixed table keyed by an Op the parser has already validated, and the threshold is always
// a bind parameter, never interpolated.
//
// The window is half-open, [start, end), which is Loki's query_range convention. $1 is cast
// to bigint so a threshold beyond int4 compares instead of failing to encode; int4-vs-int8
// comparison is in the same B-tree operator family, so the index still applies.
func (d Dataset) QuerySQL(v Variant, op Op, backward bool) (string, error) {
	sqlOp, ok := sqlOps[op]
	if !ok {
		return "", fmt.Errorf("unsupported operator %q", op)
	}
	dir := "ASC"
	if backward {
		dir = "DESC"
	}
	var value, from, prefilter string
	switch v {
	case Structured:
		value, from = d.Field, d.Structured
	case Unstructured:
		value, from = d.Expr, d.Unstructured
		prefilter = fmt.Sprintf("line LIKE '%%%s%%' AND ", d.Contains)
	default:
		return "", fmt.Errorf("unknown variant %q", v)
	}
	return fmt.Sprintf(
		"SELECT stream_id, ts_ns, line FROM %s WHERE %s%s %s $1::bigint AND ts_ns >= $2 AND ts_ns < $3 ORDER BY ts_ns %s LIMIT $4",
		from, prefilter, value, sqlOp, dir), nil
}
