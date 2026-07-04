package push

import (
	"encoding/binary"
	"testing"

	"github.com/advenn/logd/core/model"
	"google.golang.org/protobuf/encoding/protowire"
)

// A label value containing an escaped quote must not mis-split the label string (which
// would reject the whole batch).
func TestParseLabelsEscapedQuote(t *testing.T) {
	m, err := parseLabels(`{a="x\"",b="y"}`) // a's value is x"
	if err != nil {
		t.Fatalf("parseLabels: %v", err)
	}
	if m["a"] != `x"` {
		t.Fatalf("a = %q, want %q", m["a"], `x"`)
	}
	if m["b"] != "y" {
		t.Fatalf("b = %q, want y", m["b"])
	}
}

func TestParseLabelsCommaInValue(t *testing.T) {
	m, err := parseLabels(`{msg="a,b,c",k="v"}`)
	if err != nil {
		t.Fatal(err)
	}
	if m["msg"] != "a,b,c" || m["k"] != "v" {
		t.Fatalf("wrong split: %v", m)
	}
}

// A snappy block declaring a huge decompressed length must be rejected up front, not
// force a multi-GB allocation (decompression bomb).
func TestDecodeProtoRejectsBomb(t *testing.T) {
	var body []byte
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, 1<<40) // 1 TB declared length
	body = append(body, buf[:n]...)
	body = append(body, 0x00)
	if _, err := decodeProto(body); err == nil {
		t.Fatal("expected the oversized-decompression push to be rejected")
	}
}

// A protobuf EntryAdapter's structured metadata (field 3) is decoded and merged into the
// entry's labels.
func TestDecodeEntryStructuredMetadata(t *testing.T) {
	// LabelPairAdapter{ name="trace_id", value="abc" }
	var lp []byte
	lp = protowire.AppendTag(lp, 1, protowire.BytesType)
	lp = protowire.AppendBytes(lp, []byte("trace_id"))
	lp = protowire.AppendTag(lp, 2, protowire.BytesType)
	lp = protowire.AppendBytes(lp, []byte("abc"))
	// EntryAdapter{ line="hello" (2), structuredMetadata=lp (3) }
	var entry []byte
	entry = protowire.AppendTag(entry, 2, protowire.BytesType)
	entry = protowire.AppendBytes(entry, []byte("hello"))
	entry = protowire.AppendTag(entry, 3, protowire.BytesType)
	entry = protowire.AppendBytes(entry, lp)

	e, err := decodeEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	if e.line != "hello" {
		t.Fatalf("line = %q", e.line)
	}
	if e.meta["trace_id"] != "abc" {
		t.Fatalf("structured metadata not decoded: %v", e.meta)
	}
}

// JSON push with a 3rd (structured metadata) element merges it into Extra alongside the
// stream labels.
func TestDecodeJSONStructuredMetadata(t *testing.T) {
	body := []byte(`{"streams":[{"stream":{"region":"eu"},"values":[["1700000000000000000","hi",{"trace_id":"xyz"}]]}]}`)
	entries, err := Decode("application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	labels := model.ParseExtraLabels(entries[0].Extra)
	if labels["region"] != "eu" || labels["trace_id"] != "xyz" {
		t.Fatalf("structured metadata not merged into Extra: %v", labels)
	}
}

func TestDecodeJSONBasic(t *testing.T) {
	body := []byte(`{"streams":[{"stream":{"region":"eu","level":"warn"},"values":[["1700000000000000000","hello"]]}]}`)
	entries, err := Decode("application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Message != "hello" || e.TS.UnixNano() != 1700000000000000000 {
		t.Fatalf("bad entry: %+v", e)
	}
	if e.Level.String() != "WARN" {
		t.Fatalf("level from label not applied: %s", e.Level)
	}
}
