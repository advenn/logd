package ingestion

import (
	"testing"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/advenn/logd/pkg/lokicompat"
)

// Encodes a Loki push exactly as Alloy/Promtail would (protobuf + snappy) and
// runs it through logd's decoder + parser to see whether the `container`
// stream label survives onto the LogEntry.
func TestWirePushContainerLabel(t *testing.T) {
	// --- build EntryAdapter ---
	ts := time.Now()
	var tsMsg []byte
	tsMsg = protowire.AppendTag(tsMsg, 1, protowire.VarintType)
	tsMsg = protowire.AppendVarint(tsMsg, uint64(ts.Unix()))
	tsMsg = protowire.AppendTag(tsMsg, 2, protowire.VarintType)
	tsMsg = protowire.AppendVarint(tsMsg, uint64(ts.Nanosecond()))

	var entry []byte
	entry = protowire.AppendTag(entry, 1, protowire.BytesType)
	entry = protowire.AppendBytes(entry, tsMsg)
	entry = protowire.AppendTag(entry, 2, protowire.BytesType)
	entry = protowire.AppendString(entry, "logd listening on :3100")

	// --- build StreamAdapter ---
	var stream []byte
	stream = protowire.AppendTag(stream, 1, protowire.BytesType)
	stream = protowire.AppendString(stream, `{container="logd", service="api"}`)
	stream = protowire.AppendTag(stream, 2, protowire.BytesType)
	stream = protowire.AppendBytes(stream, entry)

	// --- build PushRequest ---
	var push []byte
	push = protowire.AppendTag(push, 1, protowire.BytesType)
	push = protowire.AppendBytes(push, stream)

	compressed := snappy.Encode(nil, push)

	req, err := lokicompat.DecodePushRequest(compressed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	t.Logf("streams=%d", len(req.Streams))
	for _, s := range req.Streams {
		t.Logf("  stream.Labels=%q entries=%d", s.Labels, len(s.Entries))
	}

	entries, err := NewParser().ParseLokiPush(req)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// The stream-label values must survive onto the entry — a regression here
	// is the "container label has no value" bug.
	if got, want := entries[0].Extra, `{"container":"logd","service":"api"}`; got != want {
		t.Fatalf("label values lost in parse:\n got=%s\nwant=%s", got, want)
	}
}
