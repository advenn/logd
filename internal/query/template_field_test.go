package query

import (
	"testing"

	"github.com/advenn/logd/internal/config"
	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/internal/template"
)

func orderEngine(t *testing.T) *template.Engine {
	t.Helper()
	eng, err := template.NewEngine([]config.Template{{
		Name:    "order_created",
		Pattern: "order-{order_id:uint32} created",
		Fields:  []config.FieldConfig{{Name: "order_id", Type: "uint32", Index: true}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

// A `| field = value` filter on an indexed template field must (a) populate
// FieldFilters for index pushdown and (b) produce a predicate that verifies the
// re-extracted value — accepting the matching line, rejecting others.
func TestTemplateFieldPushdownAndPredicate(t *testing.T) {
	eng := orderEngine(t)
	f, err := ParseLogQL(`{service="api"} | order_id = "7777"`, eng)
	if err != nil {
		t.Fatal(err)
	}

	if f.FieldFilters["order_id"] != "7777" {
		t.Errorf("FieldFilters[order_id] = %q, want 7777", f.FieldFilters["order_id"])
	}
	// order_id must NOT leak into LabelMatch (it's not a stream label).
	if _, ok := f.LabelMatch["order_id"]; ok {
		t.Error("order_id should not be in LabelMatch")
	}
	if f.Predicate == nil {
		t.Fatal("expected a correctness predicate")
	}
	if !f.Predicate.Match(storage.LogEntry{Message: "order-7777 created"}) {
		t.Error("predicate should match order-7777")
	}
	if f.Predicate.Match(storage.LogEntry{Message: "order-8888 created"}) {
		t.Error("predicate should reject order-8888")
	}
	if f.Predicate.Match(storage.LogEntry{Message: "unrelated line"}) {
		t.Error("predicate should reject non-matching line")
	}
}

// Range and != operators on template fields must evaluate via re-extraction
// (they were previously broken: > returned nothing, != matched everything).
func TestTemplateFieldRangeAndNotEqual(t *testing.T) {
	eng := orderEngine(t)
	check := func(query string, wantMatch map[string]bool) {
		f, err := ParseLogQL(query, eng)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		for msg, want := range wantMatch {
			got := f.Predicate.Match(storage.LogEntry{Message: msg})
			if got != want {
				t.Errorf("%s on %q = %v, want %v", query, msg, got, want)
			}
		}
	}
	check(`{s="x"} | order_id > 1000`, map[string]bool{
		"order-100 created": false, "order-5000 created": true, "order-9000 created": true,
	})
	check(`{s="x"} | order_id <= 5000`, map[string]bool{
		"order-100 created": true, "order-5000 created": true, "order-9000 created": false,
	})
	check(`{s="x"} | order_id != "5000"`, map[string]bool{
		"order-5000 created": false, "order-9000 created": true,
	})
}

// Without a template engine, the same filter falls back to a normal label filter
// (no FieldFilters, no panic).
func TestTemplateFieldNilEngine(t *testing.T) {
	f, err := ParseLogQL(`{service="api"} | order_id = "7777"`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.FieldFilters) != 0 {
		t.Errorf("expected no FieldFilters with nil engine, got %v", f.FieldFilters)
	}
}
