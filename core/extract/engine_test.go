package extract

import (
	"strings"
	"testing"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/index"
)

func mustCompile(t *testing.T, cfg config.IndexConfig) *Engine {
	t.Helper()
	e, err := Compile(cfg)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return e
}

func findKey(kvs []index.KeyedValue, field string) ([index.KeySize]byte, bool) {
	for _, kv := range kvs {
		if kv.Field == field {
			return kv.Key, true
		}
	}
	return [index.KeySize]byte{}, false
}

func sampleCfg() config.IndexConfig {
	return config.IndexConfig{
		Templates: []config.Template{
			{Name: "latency", Pattern: "took {ms:int}ms"},
			{Name: "trip", Pattern: "trip:{id:str}"},
			{Name: "order", Pattern: "order-{id:uuid} created"},
			{Name: "payment", Pattern: "paid {amt:float} usd"},
		},
		Literals: []string{"panic:"},
	}
}

func TestExtractIntCapture(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	kvs := e.Extract("request took 247ms overall")
	key, ok := findKey(kvs, "latency_ms")
	if !ok {
		t.Fatal("latency_ms not extracted")
	}
	if key != index.EncodeKey(index.Value{Kind: index.KindInt, Int: 247}) {
		t.Fatal("latency_ms key does not match encoded int 247")
	}
}

func TestExtractStrAndFloatAndUUID(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	kvs := e.Extract("trip:XYZ99 paid 12.50 usd order-550e8400-e29b-41d4-a716-446655440000 created")

	if key, ok := findKey(kvs, "trip_id"); !ok || key != index.EncodeKey(index.Value{Kind: index.KindStr, Str: "XYZ99"}) {
		t.Fatalf("trip_id wrong: ok=%v", ok)
	}
	if key, ok := findKey(kvs, "payment_amt"); !ok || key != index.EncodeKey(index.Value{Kind: index.KindFloat, Float: 12.50}) {
		t.Fatalf("payment_amt wrong: ok=%v", ok)
	}
	if _, ok := findKey(kvs, "order_id"); !ok {
		t.Fatal("order_id (uuid) not extracted")
	}
}

func TestExtractMultipleOccurrences(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	kvs := e.Extract("took 100ms ... took 200ms")
	var count int
	for _, kv := range kvs {
		if kv.Field == "latency_ms" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("want 2 latency_ms entries (index every occurrence), got %d", count)
	}
}

func TestExtractLiteralExistence(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	kvs := e.Extract("panic: runtime error")
	if _, ok := findKey(kvs, literalFieldPrefix+"panic:"); !ok {
		t.Fatal("literal 'panic:' existence not indexed")
	}
	if kvs := e.Extract("all good"); len(kvs) != 0 {
		t.Fatalf("no literals should match: %v", kvs)
	}
}

func TestExtractParseFailureCounted(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	// "took " anchor matches but "xx" is not an int → failure, no entry.
	kvs := e.Extract("took xxms")
	if _, ok := findKey(kvs, "latency_ms"); ok {
		t.Fatal("a non-int should not be indexed")
	}
	if n := e.FailureCount("latency"); n != 1 {
		t.Fatalf("failure counter: got %d want 1", n)
	}
}

func TestExtractSignedNumbers(t *testing.T) {
	e := mustCompile(t, config.IndexConfig{Templates: []config.Template{
		{Name: "d", Pattern: "delta {n:int} done"},
		{Name: "r", Pattern: "rate {f:float} hz"},
	}})
	// Leading '+' must parse and index (equal to the unsigned value).
	if key, ok := findKey(e.Extract("delta +45 done"), "d_n"); !ok || key != index.EncodeKey(index.Value{Kind: index.KindInt, Int: 45}) {
		t.Fatalf("+45 not indexed as 45: ok=%v", ok)
	}
	if key, ok := findKey(e.Extract("delta -45 done"), "d_n"); !ok || key != index.EncodeKey(index.Value{Kind: index.KindInt, Int: -45}) {
		t.Fatalf("-45 not indexed: ok=%v", ok)
	}
	if _, ok := findKey(e.Extract("rate +3.14 hz"), "r_f"); !ok {
		t.Fatal("+3.14 not indexed as a float")
	}
}

func TestExtractRejectsNaNInf(t *testing.T) {
	e := mustCompile(t, config.IndexConfig{Templates: []config.Template{{Name: "m", Pattern: "val {v:float} end"}}})
	// "Inf"/"NaN" are letters — the float scanner won't consume them, so no capture and
	// a failure is recorded (anchor matched, value didn't parse as float).
	kvs := e.Extract("val Inf end")
	if _, ok := findKey(kvs, "m_v"); ok {
		t.Fatal("Inf must not be indexed as a float")
	}
	if e.FailureCount("m") != 1 {
		t.Fatalf("expected a parse failure for Inf")
	}
}

func TestCompileRejections(t *testing.T) {
	cases := map[string]config.IndexConfig{
		"leading capture": {Templates: []config.Template{{Name: "a", Pattern: "{x:int} tail"}}},
		"no 3-byte anchor": {Templates: []config.Template{{Name: "a", Pattern: "x{n:int}y"}}},
		"adjacent captures": {Templates: []config.Template{{Name: "a", Pattern: "id {x:str}{y:int}"}}},
		"unknown type":      {Templates: []config.Template{{Name: "a", Pattern: "id {x:blob}"}}},
		"duplicate name":    {Templates: []config.Template{{Name: "a", Pattern: "aaa {x:int}"}, {Name: "a", Pattern: "bbb {y:int}"}}},
		"overlap identical": {Templates: []config.Template{{Name: "a", Pattern: "took {ms:int}ms"}, {Name: "b", Pattern: "took {ms:int}ms"}}},
		"duplicate literal": {Literals: []string{"panic:", "panic:"}},
		// int capture followed by a digit-starting literal would let the scanner eat
		// the anchor → silent drop; must be rejected.
		"int then digit anchor":   {Templates: []config.Template{{Name: "a", Pattern: "user{id:int}000"}}},
		"float then dot anchor":   {Templates: []config.Template{{Name: "a", Pattern: "val {x:float}.done"}}},
		"float then e-digit":      {Templates: []config.Template{{Name: "a", Pattern: "val {x:float}e2end"}}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(cfg); err == nil {
				t.Fatalf("expected Compile to reject %q", name)
			}
		})
	}
}

func TestIndexedFieldsSchema(t *testing.T) {
	e := mustCompile(t, sampleCfg())
	got := map[string]index.ValueKind{}
	for _, f := range e.IndexedFields() {
		got[f.Name] = f.Kind
	}
	for field, kind := range map[string]index.ValueKind{
		"latency_ms":  index.KindInt,
		"trip_id":     index.KindStr,
		"order_id":    index.KindUUID,
		"payment_amt": index.KindFloat,
	} {
		if got[field] != kind {
			t.Fatalf("schema %s: got kind %d want %d", field, got[field], kind)
		}
	}
	if !strings.HasPrefix(e.IndexedFields()[len(e.IndexedFields())-1].Name, literalFieldPrefix) {
		t.Log("note: literal fields are also part of the schema")
	}
}
