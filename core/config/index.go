// Package config holds logd's declarative configuration structs. Phase 3 defines the
// index config (which literals and typed templates to extract). These are plain data
// types with yaml/json tags; the compiler that turns them into an extraction engine —
// and enforces the structural rules (anchors, adjacency, overlap) — lives in
// core/extract, so this package stays a dependency-free leaf. YAML file loading is
// wired in with the daemon; the structs are the config.
package config

// IndexConfig declares what logd extracts and indexes from log lines (design §7.1).
type IndexConfig struct {
	// Literals are existence-indexed exact substrings: "which records contain this".
	Literals []string `yaml:"literals" json:"literals"`
	// Templates are typed patterns whose {name:type} captures are value-indexed,
	// enabling range queries on the extracted values.
	Templates []Template `yaml:"templates" json:"templates"`
}

// Labels is the label-key ALLOWLIST (design §6.2 cardinality guard): only these keys are
// pulled from a record's Extra and indexed into the per-segment label index for fast
// label-equality queries and label discovery. Any other Extra key is scan-only. This is
// the primary cardinality guard; the secondary per-key value cap is a later refinement.
type Labels []string

// Template is one typed extraction pattern, e.g. name "latency", pattern
// "took {ms:int}ms". Min/Max/MaxLen are accepted but not enforced in v1
// (parse-and-ignore, design §7.2) — re-verify covers correctness, so they exist only
// so adding semantics later is not a config-format break.
type Template struct {
	Name    string   `yaml:"name" json:"name"`
	Pattern string   `yaml:"pattern" json:"pattern"`
	Min     *float64 `yaml:"min,omitempty" json:"min,omitempty"`
	Max     *float64 `yaml:"max,omitempty" json:"max,omitempty"`
	MaxLen  *int     `yaml:"max_len,omitempty" json:"max_len,omitempty"`
}
