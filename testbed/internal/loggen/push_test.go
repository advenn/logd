package loggen_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	lokipush "github.com/advenn/logd/compat/loki/push"
	"github.com/advenn/logd/testbed/internal/loggen"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestEncodePushRoundTripsThroughRealDecoder is the reason this module has a `replace`
// directive pointing at the daemon. The load driver hand-rolls a Loki protobuf encoder;
// if it drifted from what the daemon actually parses, every benchmark number would be
// measuring a subtly different corpus than the one the manifest describes. Asserting
// against the REAL decoder — not a second copy of the format — makes that impossible.
func TestEncodePushRoundTripsThroughRealDecoder(t *testing.T) {
	labels := map[string]string{"app": "checkout", "region": "eu", "env": "prod"}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := []loggen.Entry{
		{TSNano: base.UnixNano(), Line: "GET /api/orders/1000 200 took=247ms"},
		{TSNano: base.Add(1500 * time.Millisecond).UnixNano(), Line: `panic: nil map write "checkout"`},
		{TSNano: base.Add(2 * time.Second).UnixNano(), Line: "unicode ✓ and a backslash \\ survive"},
	}

	body := loggen.EncodePush(labels, in)
	got, err := lokipush.Decode("application/x-protobuf", body)
	if err != nil {
		t.Fatalf("the daemon's decoder rejected our encoder's output: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d entries, want %d", len(got), len(in))
	}

	for i, want := range in {
		if got[i].Message != want.Line {
			t.Errorf("entry %d line: got %q, want %q", i, got[i].Message, want.Line)
		}
		// Nanosecond fidelity matters: the corpus manifest keys on ts_ns, so a timestamp
		// that survives encoding only to the nearest second would break the parity diff.
		if got[i].TS.UnixNano() != want.TSNano {
			t.Errorf("entry %d ts: got %d, want %d", i, got[i].TS.UnixNano(), want.TSNano)
		}
	}

	// Labels must survive into Extra, or the stream identity differs from what the
	// manifest recorded and every {app="checkout"} query silently returns nothing.
	for k, v := range labels {
		if !containsLabel(got[0].Extra, k, v) {
			t.Errorf("label %s=%s missing from Extra %q", k, v, got[0].Extra)
		}
	}
}

func containsLabel(extra, k, v string) bool {
	return len(extra) > 0 &&
		(indexOf(extra, `"`+k+`":"`+v+`"`) >= 0 || indexOf(extra, k+"="+v) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

// TestEncodePushIsDeterministic pins that the same batch encodes to identical bytes. The
// fan-out design POSTs one encoded body to every backend precisely so they cannot receive
// different data; that guarantee is only real if encoding is stable.
func TestEncodePushIsDeterministic(t *testing.T) {
	labels := map[string]string{"b": "2", "a": "1", "c": "3"}
	entries := []loggen.Entry{{TSNano: 1700000000000000000, Line: "took=5ms"}}
	first := loggen.EncodePush(labels, entries)
	for i := 0; i < 5; i++ {
		if got := loggen.EncodePush(labels, entries); string(got) != string(first) {
			t.Fatalf("encoding is not deterministic on attempt %d", i)
		}
	}
}

// TestEncodePushMultiStreamRoundTripsThroughRealDecoder: a replay batch spans several
// component streams in ONE request. Every entry must come back out of the daemon's decoder
// with its own stream's labels, its line and its exact timestamp.
func TestEncodePushMultiStreamRoundTripsThroughRealDecoder(t *testing.T) {
	sinkLabels := map[string]string{"app": "hdfs", "component": "other"}
	dn := loggen.LabelString(map[string]string{"app": "hdfs", "component": "datanode"})
	fs := loggen.LabelString(map[string]string{"app": "hdfs", "component": "fsnamesystem"})
	in := []loggen.Entry{
		{TSNano: 1, Line: "a", Stream: dn},
		{TSNano: 2, Line: "b", Stream: fs},
		{TSNano: 3, Line: "c", Stream: dn},
		{TSNano: 4, Line: "d"}, // no Stream: the sink's labels
	}
	got, err := lokipush.Decode("application/x-protobuf", loggen.EncodePush(sinkLabels, in))
	if err != nil {
		t.Fatalf("the daemon's decoder rejected a multi-stream request: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d entries, want %d", len(got), len(in))
	}
	wantComp := map[int64]string{1: "datanode", 2: "fsnamesystem", 3: "datanode", 4: "other"}
	wantLine := map[int64]string{1: "a", 2: "b", 3: "c", 4: "d"}
	var order []int64
	for _, e := range got {
		ts := e.TS.UnixNano()
		order = append(order, ts)
		if e.Message != wantLine[ts] || !containsLabel(e.Extra, "component", wantComp[ts]) || !containsLabel(e.Extra, "app", "hdfs") {
			t.Errorf("ts %d: line %q extra %q, want %q in component %s", ts, e.Message, e.Extra, wantLine[ts], wantComp[ts])
		}
	}
	// Streams in first-appearance order, entries in order within each: deterministic bytes.
	if want := []int64{1, 3, 2, 4}; fmt.Sprint(order) != fmt.Sprint(want) {
		t.Errorf("decoded order %v, want %v", order, want)
	}
}

// TestEncodePushSingleStreamLayoutUnchanged pins that a corpus-mode batch (one stream, no
// per-entry Stream) still encodes to exactly the PushRequest it did before multi-stream
// batches existed, so corpus-mode measurements remain comparable across this change.
func TestEncodePushSingleStreamLayoutUnchanged(t *testing.T) {
	labels := map[string]string{"app": "checkout", "env": "prod", "region": "eu"}
	entries := []loggen.Entry{{TSNano: 1700000000123456789, Line: "took=5ms"}, {TSNano: 1700000000123456790, Line: "x"}}

	var stream []byte
	stream = protowire.AppendTag(stream, 1, protowire.BytesType)
	stream = protowire.AppendString(stream, `{app="checkout", env="prod", region="eu"}`)
	for _, e := range entries {
		var ts, msg []byte
		ts = protowire.AppendTag(ts, 1, protowire.VarintType)
		ts = protowire.AppendVarint(ts, uint64(e.TSNano/1e9))
		ts = protowire.AppendTag(ts, 2, protowire.VarintType)
		ts = protowire.AppendVarint(ts, uint64(e.TSNano%1e9))
		msg = protowire.AppendTag(msg, 1, protowire.BytesType)
		msg = protowire.AppendBytes(msg, ts)
		msg = protowire.AppendTag(msg, 2, protowire.BytesType)
		msg = protowire.AppendString(msg, e.Line)
		stream = protowire.AppendTag(stream, 2, protowire.BytesType)
		stream = protowire.AppendBytes(stream, msg)
	}
	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, stream)

	if got := loggen.EncodePush(labels, entries); !bytes.Equal(got, snappy.Encode(nil, req)) {
		t.Error("single-stream encoding changed")
	}
}
