package push

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// The push decoders read request bodies straight off the network. These fuzz targets check
// that no body makes them panic and that none gets past the caps: a decode never returns
// more than MaxEntries entries, and a snappy body declaring more than maxDecompressed bytes
// is refused.
//
// `go test` runs the seeds below (and any crashers kept in testdata/fuzz/). To search:
//
//	go test -run '^$' -fuzz '^FuzzDecodeProto$' -fuzztime 30s ./compat/loki/push/
//	go test -run '^$' -fuzz '^FuzzDecodeJSON$' -fuzztime 30s ./compat/loki/push/

// FuzzDecodeProto decodes a protobuf push. With compress set, the input is the protobuf
// message and the target snappy-encodes it, so mutations reach the protobuf walker instead
// of mostly breaking the snappy framing; without it, the input is the snappy body as sent.
func FuzzDecodeProto(f *testing.F) {
	labels := func(s string) []byte {
		return protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), s)
	}
	request := func(streams ...[]byte) []byte {
		var req []byte
		for _, st := range streams {
			req = protowire.AppendTag(req, 1, protowire.BytesType)
			req = protowire.AppendBytes(req, st)
		}
		return req
	}
	plain := appendTestEntry(labels(`{app="api",level="warn"}`), 1700000000, "took=12ms", nil)
	meta := appendTestEntry(labels(`{app="api"}`), 1, "x", map[string]string{"trace_id": "abc"})
	quoted := appendTestEntry(labels(`{a="x\"",b="y,z"}`), 2, "", nil)
	empty := append(labels(`{a="b"}`), 0x12, 0x00, 0x12, 0x00)
	bomb := binary.AppendUvarint(nil, 1<<40)
	for _, msg := range [][]byte{
		request(plain),
		request(plain, meta, quoted),
		request(empty),
		request(labels(`not labels`)),
		request(labels(``)),
		{0x0a, 0x05, 0x12, 0x03, 0x0a, 0x01, 0x08}, // a timestamp field of the wrong type
		{0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f},       // a length past the end
		{0x0b},                                     // a group, an unknown wire type here
	} {
		f.Add(msg, true)
		f.Add(snappy.Encode(nil, msg), false)
	}
	f.Add(append(bomb, 0x00), false)
	f.Add([]byte{}, false)
	f.Add(emptyEntriesBody(MaxEntries+1), false)

	f.Fuzz(func(t *testing.T, data []byte, compress bool) {
		body := data
		if compress {
			body = snappy.Encode(nil, data)
		}
		entries, err := Decode("application/x-protobuf", body)
		if len(entries) > MaxEntries {
			t.Fatalf("decoded %d entries, over MaxEntries (%d); err = %v", len(entries), MaxEntries, err)
		}
		if n, lenErr := snappy.DecodedLen(body); lenErr == nil && n > maxDecompressed && err == nil {
			t.Fatalf("a body declaring %d decompressed bytes (cap %d) was accepted", n, maxDecompressed)
		}
	})
}

// FuzzDecodeJSON decodes a JSON push. Besides the caps, a body it accepts must be valid
// JSON: the decoder walks the request token by token, and accepting anything else would mean
// it skipped part of what it was sent.
func FuzzDecodeJSON(f *testing.F) {
	for _, s := range []string{
		`{"streams":[{"stream":{"app":"api","level":"warn"},"values":[["1700000000000000000","hello"]]}]}`,
		`{"streams":[{"stream":{"region":"eu"},"values":[["1","hi",{"trace_id":"xyz"}],["2","x",{}]]}]}`,
		`{"streams":[{"stream":{},"values":[]},{"values":null},{"stream":null}]}`,
		`{"Streams":[{"stream":{"a":"b"},"values":[["1",""]]}],"extra":{"x":[1,2,{"y":null}]}}`,
		`{"streams":null}`,
		`null`,
		`{}`,
		`{"streams":[{"stream":{"a":"b"},"values":[["1"]]}]}`,
		`{"streams":[{"stream":{"a":"b"},"values":[[1,"x"]]}]}`,
		`{"streams":[{"stream":{"a":"b"},"values":[["99999999999999999999","x"]]}]}`,
		`{"streams":[{"stream":{"a":1},"values":[]}]}`,
		`{"streams":{}}`,
		`{"streams":[]} trailing`,
		`{"streams":[{"stream":{"a":"\ud800"},"values":[["1","\u0000"]]}]}`,
		`[`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		entries, err := Decode("application/json", body)
		if len(entries) > MaxEntries {
			t.Fatalf("decoded %d entries, over MaxEntries (%d); err = %v", len(entries), MaxEntries, err)
		}
		if err == nil && !json.Valid(body) {
			t.Fatalf("accepted a body that is not valid JSON: %q", body)
		}
	})
}
