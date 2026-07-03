package logql

import "testing"

// Grafana sends line-filter/matcher values in backticks (raw strings), including
// the empty filter `|= ``` that Explore generates by default. These must lex as
// strings, not fail with "expected string after line filter operator".
func TestBacktickStrings(t *testing.T) {
	cases := []struct {
		query   string
		wantVal string // expected line-filter value
	}{
		{"{c=\"x\"} |= ``", ""},
		{"{c=\"x\"} |= `boom`", "boom"},
		{"{c=\"x\"} |~ `wor.*`", "wor.*"},
		{"{c=\"x\"} |= `with spaces`", "with spaces"},
	}
	for _, tc := range cases {
		q, err := Parse(tc.query)
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", tc.query, err)
			continue
		}
		if len(q.Pipeline) != 1 {
			t.Errorf("Parse(%q): expected 1 pipeline stage, got %d", tc.query, len(q.Pipeline))
			continue
		}
		lf, ok := q.Pipeline[0].(*LineFilter)
		if !ok {
			t.Errorf("Parse(%q): stage is %T, want *LineFilter", tc.query, q.Pipeline[0])
			continue
		}
		if lf.Value != tc.wantVal {
			t.Errorf("Parse(%q): line-filter value = %q, want %q", tc.query, lf.Value, tc.wantVal)
		}
	}
}
