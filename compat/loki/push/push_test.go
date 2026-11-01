package push

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"strings"
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

// emptyEntriesBody is a snappy+protobuf push of one stream carrying n empty EntryAdapters,
// each two bytes on the wire: the cheapest way to make a small body decode into many
// entries.
func emptyEntriesBody(n int) []byte {
	var st []byte
	st = protowire.AppendTag(st, 1, protowire.BytesType)
	st = protowire.AppendString(st, `{a="b"}`)
	for i := 0; i < n; i++ {
		st = append(st, 0x12, 0x00)
	}
	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, st)
	return snappy.Encode(nil, req)
}

// The decompressed-size cap does not bound memory on its own: an empty entry is two bytes
// on the wire but a full LogEntry in memory, so a 768 KB body allocated 5.7 GB. A push
// carrying more than MaxEntries entries must be refused, and refused before it has all been
// materialised.
func TestDecodeRejectsTooManyEntries(t *testing.T) {
	if _, err := Decode("application/x-protobuf", emptyEntriesBody(MaxEntries)); err != nil {
		t.Fatalf("a push of exactly MaxEntries entries must be accepted: %v", err)
	}
	if _, err := Decode("application/x-protobuf", emptyEntriesBody(MaxEntries+1)); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("protobuf push of MaxEntries+1 entries: err = %v, want ErrTooManyEntries", err)
	}

	var b strings.Builder
	b.WriteString(`{"streams":[{"stream":{"a":"b"},"values":[`)
	for i := 0; i <= MaxEntries; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`["1",""]`)
	}
	b.WriteString(`]}]}`)
	if _, err := Decode("application/json", []byte(b.String())); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("JSON push of MaxEntries+1 entries: err = %v, want ErrTooManyEntries", err)
	}

	// The review's amplification body: 8M empty entries in ~770 KB. Decoding must stop at
	// the cap rather than allocate gigabytes first.
	body := emptyEntriesBody(8 << 20)
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	_, err := Decode("application/x-protobuf", body)
	runtime.ReadMemStats(&m1)
	if !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("8M-entry push: err = %v, want ErrTooManyEntries", err)
	}
	if alloc := m1.TotalAlloc - m0.TotalAlloc; alloc > 256<<20 {
		t.Fatalf("decoding a %d KB body allocated %d MB before refusing it", len(body)>>10, alloc>>20)
	}
}

// gzip and deflate bodies are decoded, as Loki accepts them; snappy and no encoding pass
// through (snappy is the protobuf body's own framing); anything else is refused.
func TestDecompressEncodings(t *testing.T) {
	body := []byte(`{"streams":[]}`)
	var gz, fl bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(body)
	zw.Close()
	fw, _ := flate.NewWriter(&fl, flate.DefaultCompression)
	fw.Write(body)
	fw.Close()
	for enc, in := range map[string][]byte{"": body, "snappy": body, "gzip": gz.Bytes(), "deflate": fl.Bytes()} {
		got, err := Decompress(enc, in)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("Content-Encoding %q: %q, %v", enc, got, err)
		}
	}
	if _, err := Decompress("br", body); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("br: err = %v, want not supported", err)
	}
	if _, err := Decompress("gzip", []byte("not gzip")); err == nil {
		t.Fatal("a corrupt gzip body must be an error")
	}
}

// A gzip body is held to the same decompressed-size cap as snappy.
func TestDecompressGzipBombRejected(t *testing.T) {
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, gzip.BestSpeed)
	if _, err := io.CopyN(zw, zeroReader{}, maxDecompressed+1); err != nil {
		t.Fatal(err)
	}
	zw.Close()
	if _, err := Decompress("gzip", gz.Bytes()); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("a %d KB body inflating past %d MB: err = %v", gz.Len()>>10, maxDecompressed>>20, err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// Without a Content-Type a push is snappy-compressed protobuf, as in Loki (which decodes
// JSON only for application/json); it used to be decoded as JSON and refused.
func TestDecodeDefaultsToProtobuf(t *testing.T) {
	stream := appendTestEntry(protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), `{a="b"}`), 1, "hello", nil)
	req := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), stream)
	for _, ct := range []string{"", "application/x-protobuf", "application/octet-stream"} {
		entries, err := Decode(ct, snappy.Encode(nil, req))
		if err != nil || len(entries) != 1 || entries[0].Message != "hello" {
			t.Fatalf("Content-Type %q: %v, %v", ct, entries, err)
		}
	}
	if entries, err := Decode("application/json; charset=utf-8", []byte(`{"streams":[{"stream":{"a":"b"},"values":[["1","x"]]}]}`)); err != nil || len(entries) != 1 {
		t.Fatalf("JSON with a charset: %v, %v", entries, err)
	}
}
