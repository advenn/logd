package model

import (
	"encoding/json"
	"strings"
)

// ParseExtraLabels decodes the Extra JSON blob into a flat map of label key → string
// value. Scalar JSON values are stringified (numbers keep their exact textual form via
// UseNumber, bools become "true"/"false"); non-scalar values (objects, arrays, null) are
// omitted. This is the ONE definition of "the labels a record carries in Extra", shared
// by the query resolver (scan path) and the label-index builder (ingest) so the index and
// the scan can never disagree about a label's value.
func ParseExtraLabels(extra string) map[string]string {
	if extra == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(extra))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := scalarString(v); ok {
			out[k] = s
		}
	}
	return out
}

// TenantLabel is the internal label carrying a record's tenant when multitenancy is on
// (design §12). It is a normal indexed Extra label (so tenant-scoping queries push down),
// but the compat layer hides it from label discovery. The double-underscore marks it
// internal, matching the Prometheus/Loki convention.
const TenantLabel = "__tenant__"

// WithExtraLabel returns the Extra JSON blob with key set to value, preserving all
// existing fields (including non-scalar ones). Used to tag a record with its tenant.
func WithExtraLabel(extra, key, value string) string {
	raw := map[string]json.RawMessage{}
	if extra != "" {
		if err := json.Unmarshal([]byte(extra), &raw); err != nil {
			raw = map[string]json.RawMessage{} // malformed/absent → start fresh
		}
	}
	vb, err := json.Marshal(value)
	if err != nil {
		return extra
	}
	raw[key] = vb
	b, err := json.Marshal(raw)
	if err != nil {
		return extra
	}
	return string(b)
}

// IsReservedLabelKey reports whether a label key is resolved from a first-class record
// FIELD rather than from Extra: "level" comes from LogEntry.Level and "service" from the
// interned ServiceID. Such keys must NOT be indexed into the Extra-derived label index
// nor pushed down — otherwise the index (Extra value) and the scan (field value) could
// disagree and drop records. They are always resolved by scanning the record's fields.
func IsReservedLabelKey(key string) bool {
	return key == "level" || key == "service"
}

func scalarString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}
