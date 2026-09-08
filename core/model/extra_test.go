package model_test

import (
	"encoding/json"
	"testing"

	"github.com/advenn/logd/core/model"
)

// extraCorpus is the shared body of blobs for both the behaviour table and the
// differential property test. It deliberately includes the awkward cases: non-scalar
// values, nulls, duplicate keys, escapes, unicode, numeric forms that a naive
// float64 round-trip would corrupt, and malformed input.
var extraCorpus = []string{
	``,
	`{}`,
	`{"app":"api"}`,
	`{"app":"api","region":"eu","env":"prod"}`,
	`{"code":200}`,
	`{"code":-17}`,
	`{"ratio":0.5}`,
	`{"big":12345678901234567890}`, // exceeds float64 integer precision
	`{"exp":1e309}`,                // overflows float64
	`{"precise":0.1000000000000000055511151231257827}`,
	`{"ok":true,"bad":false}`,
	`{"nothing":null}`,
	`{"nested":{"a":"b"}}`,
	`{"list":[1,2,3]}`,
	`{"mixed":"x","nested":{"a":"b"},"list":[1],"n":null,"b":true,"num":7}`,
	`{"dup":"first","dup":"second"}`, // JSON duplicate: last wins in a map
	`{"dup":"scalar","dup":{"o":1}}`, // last is non-scalar -> key absent
	`{"dup":{"o":1},"dup":"scalar"}`, // last is scalar -> key present
	`{"esc":"line\nbreak\ttab\"quote\\slash"}`,
	`{"unicode":"héllo ✓ 日本語"}`,
	`{"emptyval":""}`,
	`{"":"emptykey"}`,
	`{"__tenant__":"acme"}`,
	`   {"padded":"yes"}   `,
	`{"a":"1"}trailing garbage`, // Decoder reads the first value, ignores the rest
	`not json at all`,
	`[1,2,3]`,
	`"a bare string"`,
	`{"truncated":`,
	`{`,
	`null`,
}

// TestParseExtraLabelsBehaviour pins the current contract. ParseExtraLabels is documented
// as "the ONE definition of the labels a record carries in Extra", shared by the query
// resolver and the label-index builder, so any drift here desynchronizes the index from
// the scan. It had no tests at all before this.
func TestParseExtraLabelsBehaviour(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		want  map[string]string
	}{
		{"empty string", ``, nil},
		{"empty object", `{}`, map[string]string{}},
		{"strings", `{"app":"api","region":"eu"}`, map[string]string{"app": "api", "region": "eu"}},
		// Numbers keep their exact textual form via UseNumber. A float64 round-trip would
		// render this as 1.2345678901234567e+19 and the label would stop matching.
		{"big int keeps exact text", `{"big":12345678901234567890}`,
			map[string]string{"big": "12345678901234567890"}},
		{"negative int", `{"code":-17}`, map[string]string{"code": "-17"}},
		{"float keeps text", `{"ratio":0.50}`, map[string]string{"ratio": "0.50"}},
		{"bools", `{"ok":true,"bad":false}`, map[string]string{"ok": "true", "bad": "false"}},
		{"null omitted", `{"nothing":null,"kept":"x"}`, map[string]string{"kept": "x"}},
		{"object omitted", `{"nested":{"a":"b"},"kept":"x"}`, map[string]string{"kept": "x"}},
		{"array omitted", `{"list":[1,2],"kept":"x"}`, map[string]string{"kept": "x"}},
		{"duplicate key last wins", `{"dup":"first","dup":"second"}`,
			map[string]string{"dup": "second"}},
		{"duplicate key last non-scalar drops it", `{"dup":"scalar","dup":{"o":1}}`,
			map[string]string{}},
		{"escapes decoded", `{"esc":"a\nb"}`, map[string]string{"esc": "a\nb"}},
		{"unicode", `{"u":"héllo ✓"}`, map[string]string{"u": "héllo ✓"}},
		{"empty key and value", `{"":""}`, map[string]string{"": ""}},
		{"trailing garbage ignored", `{"a":"1"}xyz`, map[string]string{"a": "1"}},
		{"malformed", `not json`, nil},
		{"array at top level", `[1,2]`, nil},
		{"bare string", `"x"`, nil},
		{"truncated", `{"a":`, nil},
		// Decoding JSON null into a map succeeds and leaves it nil, so ParseExtraLabels
		// falls through to an EMPTY (non-nil) map rather than the nil it returns on a
		// decode error. Pinning the distinction because it is easy to "fix" by accident.
		{"json null yields empty map, not nil", `null`, map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := model.ParseExtraLabels(tc.extra)
			if !sameMap(got, tc.want) {
				t.Errorf("ParseExtraLabels(%q)\n got %#v\nwant %#v", tc.extra, got, tc.want)
			}
		})
	}
}

// TestExtraLabelMatchesParseExtraLabels is the guard that matters. ExtraLabel is a
// single-key fast path that must be observationally identical to looking the key up in the
// full map — if the two ever disagree, the label index (built via ParseExtraLabels at
// ingest) and the query scan path silently return different answers for the same record.
func TestExtraLabelMatchesParseExtraLabels(t *testing.T) {
	// Probe keys present in the corpus, plus keys that are absent, plus odd ones.
	keys := []string{
		"app", "region", "env", "code", "ratio", "big", "exp", "precise", "ok", "bad",
		"nothing", "nested", "list", "mixed", "n", "b", "num", "dup", "esc", "unicode",
		"emptyval", "", "__tenant__", "padded", "a", "truncated", "absent", "APP", "app ",
	}
	for _, extra := range extraCorpus {
		full := model.ParseExtraLabels(extra)
		for _, k := range keys {
			wantV, wantOK := full[k]
			gotV, gotOK := model.ExtraLabel(extra, k)
			if gotOK != wantOK || gotV != wantV {
				t.Errorf("extra=%q key=%q\n ExtraLabel        = (%q, %v)\n ParseExtraLabels[] = (%q, %v)",
					extra, k, gotV, gotOK, wantV, wantOK)
			}
		}
	}
}

// FuzzExtraLabelMatchesParseExtraLabels extends the equivalence to inputs nobody thought
// of. The two implementations parse the same bytes by different routes, so this is exactly
// the shape of bug a hand-written table would miss.
func FuzzExtraLabelMatchesParseExtraLabels(f *testing.F) {
	for _, s := range extraCorpus {
		f.Add(s, "app")
		f.Add(s, "dup")
		f.Add(s, "")
	}
	f.Fuzz(func(t *testing.T, extra, key string) {
		wantV, wantOK := model.ParseExtraLabels(extra)[key]
		gotV, gotOK := model.ExtraLabel(extra, key)
		if gotOK != wantOK || gotV != wantV {
			t.Fatalf("extra=%q key=%q: ExtraLabel=(%q,%v) map=(%q,%v)",
				extra, key, gotV, gotOK, wantV, wantOK)
		}
	})
}

func sameMap(a, b map[string]string) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// Benchmarks quantify the win: the resolver needs ONE key, and building a whole map plus
// boxing every value into an `any` to read it is what makes ParseExtraLabels 28.7% of
// query CPU.
const benchExtra = `{"app":"checkout","env":"prod","region":"eu","pod":"checkout-7d9f8b6c4-x2k9p","code":200,"ok":true}`

func BenchmarkParseExtraLabels(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := model.ParseExtraLabels(benchExtra)["region"]; !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkExtraLabel(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := model.ExtraLabel(benchExtra, "region"); !ok {
			b.Fatal("missing key")
		}
	}
}

// BenchmarkExtraLabelMiss is the other common shape: a predicate on a key this record does
// not carry, which must still scan the whole blob.
func BenchmarkExtraLabelMiss(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := model.ExtraLabel(benchExtra, "absent"); ok {
			b.Fatal("unexpected key")
		}
	}
}

var _ = json.Number("") // keep the json import meaningful if the table shrinks
