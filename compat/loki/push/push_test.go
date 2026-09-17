package push

import (
	"encoding/binary"
	"testing"

	"github.com/advenn/logd/core/model"
	"github.com/golang/snappy"
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

// The stream-level label marshal is hoisted out of the per-entry loop, so this pins the
// invariant that makes that safe: every entry under one stream must carry byte-identical
// Extra and the same stream-derived level, and an entry that DOES carry structured
// metadata must still get its own overlaid Extra rather than the hoisted one.
func TestDecodeStreamLabelsHoistedIdentically(t *testing.T) {
	labels := `{app="checkout",level="warn",region="eu"}`
	var stream []byte
	stream = protowire.AppendTag(stream, 1, protowire.BytesType)
	stream = protowire.AppendString(stream, labels)
	for i := 0; i < 3; i++ {
		stream = appendTestEntry(stream, int64(i), "line", nil)
	}
	// A fourth entry overlays region=us via structured metadata.
	stream = appendTestEntry(stream, 3, "line", map[string]string{"region": "us"})

	entries, err := decodeProtoStream(t, stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("decoded %d entries, want 4", len(entries))
	}

	want := `{"app":"checkout","level":"warn","region":"eu"}`
	for i, e := range entries[:3] {
		if e.Extra != want {
			t.Fatalf("entry %d Extra = %q, want %q", i, e.Extra, want)
		}
		if e.Level != model.LogLevelWarn {
			t.Fatalf("entry %d Level = %v, want warn", i, e.Level)
		}
	}
	if entries[3].Extra == want {
		t.Fatal("the structured-metadata entry reused the hoisted stream Extra; its region=us overlay was lost")
	}
	if entries[3].Extra != `{"app":"checkout","level":"warn","region":"us"}` {
		t.Fatalf("metadata entry Extra = %q", entries[3].Extra)
	}
}

func appendTestEntry(dst []byte, ts int64, line string, meta map[string]string) []byte {
	var tsMsg []byte
	tsMsg = protowire.AppendTag(tsMsg, 1, protowire.VarintType)
	tsMsg = protowire.AppendVarint(tsMsg, uint64(ts))
	var msg []byte
	msg = protowire.AppendTag(msg, 1, protowire.BytesType)
	msg = protowire.AppendBytes(msg, tsMsg)
	msg = protowire.AppendTag(msg, 2, protowire.BytesType)
	msg = protowire.AppendString(msg, line)
	for k, v := range meta {
		var pair []byte
		pair = protowire.AppendTag(pair, 1, protowire.BytesType)
		pair = protowire.AppendString(pair, k)
		pair = protowire.AppendTag(pair, 2, protowire.BytesType)
		pair = protowire.AppendString(pair, v)
		msg = protowire.AppendTag(msg, 3, protowire.BytesType)
		msg = protowire.AppendBytes(msg, pair)
	}
	dst = protowire.AppendTag(dst, 2, protowire.BytesType)
	return protowire.AppendBytes(dst, msg)
}

func decodeProtoStream(t *testing.T, stream []byte) ([]model.LogEntry, error) {
	t.Helper()
	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, stream)
	return decodeProto(snappy.Encode(nil, req))
}
