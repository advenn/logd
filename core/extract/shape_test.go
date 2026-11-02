package extract

import (
	"reflect"
	"testing"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/index"
)

// Shapes reports the fragments matching walks, captures under their queryable field names,
// in config order.
func TestShapesReportCompiledFragments(t *testing.T) {
	e, err := Compile(config.IndexConfig{
		Templates: []config.Template{
			{Name: "latency", Pattern: "took={ms:int}ms"},
			{Name: "req", Pattern: "GET {path:str} in {s:float}s"},
		},
		Literals: []string{"panic:"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Shape{
		{Name: "latency", Pattern: "took={ms:int}ms", Fragments: []Fragment{
			{Literal: "took="}, {Field: "latency_ms", Kind: index.KindInt}, {Literal: "ms"},
		}},
		{Name: "req", Pattern: "GET {path:str} in {s:float}s", Fragments: []Fragment{
			{Literal: "GET "}, {Field: "req_path", Kind: index.KindStr}, {Literal: " in "}, {Field: "req_s", Kind: index.KindFloat}, {Literal: "s"},
		}},
	}
	if got := e.Shapes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Shapes() = %+v\nwant %+v", got, want)
	}
}
