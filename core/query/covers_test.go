package query_test

import (
	"testing"

	qr "github.com/advenn/logd/core/query"
)

// TestCoveringSelectorRestrictsNothing: a stream selector every stream of a segment satisfies
// is not looked up (its lookup would return every record); the typed lookup alone decides the
// candidates. The answer must not change, and a selector that does restrict must still push.
func TestCoveringSelectorRestrictsNothing(t *testing.T) {
	e := buildCovering(t)
	full := qr.Query{Start: ts(0), End: ts(1000), Limit: 1000}
	app := qr.LabelEqual{Key: "app", Value: "web"}
	eu := qr.LabelEqual{Key: "region", Value: "eu"}
	slow := qr.TypedCompare{Field: "latency_ms", Op: qr.OpGt, Value: intV(900)}

	for _, c := range []struct {
		name  string
		preds []qr.Predicate
	}{
		{"covering selector AND typed", []qr.Predicate{app, slow}},
		{"covering selector alone", []qr.Predicate{app}},
		{"restricting selector AND typed", []qr.Predicate{eu, slow}},
		{"covering AND restricting AND typed", []qr.Predicate{app, eu, slow}},
		{"covering selector, no such value", []qr.Predicate{qr.LabelEqual{Key: "app", Value: "api"}}},
	} {
		q := withPreds(full, c.preds...)
		idx, err := e.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		scan, err := e.ExecuteScan(q)
		if err != nil {
			t.Fatal(err)
		}
		equalResults(t, c.name, idx, scan)
	}

	// The covering selector does not widen the candidates past what the typed lookup found.
	var typedOnly, withApp int
	for _, p := range e.Explain(withPreds(full, slow)) {
		typedOnly += p.Candidates
	}
	for _, p := range e.Explain(withPreds(full, app, slow)) {
		withApp += p.Candidates
		if p.Mode == "index" && p.Reason == qr.ReasonLabelCovers {
			t.Fatalf("segment %s: a typed predicate was pushed, so the reason must not be %q", p.SegmentID, p.Reason)
		}
	}
	if withApp != typedOnly {
		t.Fatalf("covering selector changed the candidate count: %d with it, %d without", withApp, typedOnly)
	}
	// Alone, it leaves nothing to fetch by offset: the segments scan, and say why.
	for _, p := range e.Explain(withPreds(full, app)) {
		if p.State == "sealed" && p.Reason != qr.ReasonLabelCovers {
			t.Fatalf("segment %s: reason %q, want %q", p.SegmentID, p.Reason, qr.ReasonLabelCovers)
		}
	}
	// A selector that holds for only some streams still restricts.
	var euCands int
	for _, p := range e.Explain(withPreds(full, eu, slow)) {
		euCands += p.Candidates
	}
	if euCands == 0 || euCands >= typedOnly {
		t.Fatalf("region=eu AND latency>900: %d candidates, want fewer than the typed lookup's %d and more than 0", euCands, typedOnly)
	}
}
