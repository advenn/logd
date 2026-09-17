package loki_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/compat/loki"
	"github.com/advenn/logd/compat/loki/push"
	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// End-to-end push benchmarks. The testbed measures ingest at the container boundary
// (500 requests x 1,000 entries: VictoriaLogs 2.37s, Loki 3.48s, logd 6.23s) and the
// write-path work concluded that storage is no longer the bottleneck — "HTTP, snappy/
// protobuf decode and extraction" are. Nothing measured that claim: core/storage covers
// the writer and core/ingest covers extraction, but the decode->ingest handler path
// between them had no benchmark at all. These fill that gap.
//
// The decomposition is deliberate. Each benchmark is a prefix of the next, so subtracting
// adjacent rows attributes cost to a stage:
//
//	Decode        snappy + protobuf -> []model.LogEntry
//	DecodeIngest  ...plus extraction, label derivation and the writer enqueue
//	EndToEnd      ...plus net/http, ReadAll and the response
//
// IMPORTANT — same real-disk requirement as core/storage/write_bench_test.go. b.TempDir()
// honours $TMPDIR and /tmp is tmpfs on most Linux dev boxes, where fsync is free.
//
//	LOGD_BENCH_DIR=$HOME/.cache/logd-bench go test ./compat/loki/ -run '^$' -bench Push

// batchEntries is the testbed's batch size, and it matters: the per-batch costs (snappy
// frame, label-string parse) amortize over it, while the per-entry costs do not.
const batchEntries = 1000

func pushBenchDir(b *testing.B) string {
	b.Helper()
	root := os.Getenv("LOGD_BENCH_DIR")
	if root == "" {
		b.Skip("set LOGD_BENCH_DIR to a path on a real (non-tmpfs) filesystem; /tmp is tmpfs and fsync there is free")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "push")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// benchLabels mirrors stack/logd/logd.yaml: three stream labels, of which region and env
// are allowlisted for the label index and app is not.
var benchLabels = map[string]string{"app": "checkout", "env": "prod", "region": "eu"}

var benchAllowlist = []string{"region", "env"}

// benchBody builds one snappy+protobuf Loki push request of batchEntries lines in the
// corpus's shape: unstructured text carrying a `took=NNNms` duration the latency template
// extracts. Encoded once, outside the timer — the generator's cost is not under test.
func benchBody() []byte {
	var stream []byte
	stream = protowire.AppendTag(stream, 1, protowire.BytesType)
	stream = protowire.AppendString(stream, labelString(benchLabels))

	base := time.Unix(1700000000, 0).UnixNano()
	for i := 0; i < batchEntries; i++ {
		line := fmt.Sprintf("GET /api/v1/orders/%d 200 took=%dms trace_id=%016x", i, 10+i%3000, i)
		var msg []byte
		var ts []byte
		tsNano := base + int64(i)*int64(time.Millisecond)
		ts = protowire.AppendTag(ts, 1, protowire.VarintType)
		ts = protowire.AppendVarint(ts, uint64(tsNano/1e9))
		ts = protowire.AppendTag(ts, 2, protowire.VarintType)
		ts = protowire.AppendVarint(ts, uint64(tsNano%1e9))
		msg = protowire.AppendTag(msg, 1, protowire.BytesType)
		msg = protowire.AppendBytes(msg, ts)
		msg = protowire.AppendTag(msg, 2, protowire.BytesType)
		msg = protowire.AppendString(msg, line)
		stream = protowire.AppendTag(stream, 2, protowire.BytesType)
		stream = protowire.AppendBytes(stream, msg)
	}

	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, stream)
	return snappy.Encode(nil, req)
}

func labelString(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, m[k])
	}
	b.WriteByte('}')
	return b.String()
}

// benchStack wires the shipped daemon configuration: one latency template, a two-key
// allowlist, group commit at 50ms, 64MB segments.
func benchStack(b *testing.B) (*loki.Server, *ingest.Ingester, func()) {
	b.Helper()
	eng, err := extract.Compile(config.IndexConfig{
		Templates: []config.Template{{Name: "latency", Pattern: "took={ms:int}ms"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	w, err := storage.NewWriter(pushBenchDir(b), storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 64 << 20,
		FlushInterval:    500 * time.Millisecond,
		SyncInterval:     50 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	ig := ingest.NewShardedWithLabels(eng, []*storage.Writer{w}, benchAllowlist)
	srv := loki.NewServer(ig, query.NewEngineWithLabels(w, eng, benchAllowlist), eng)
	return srv, ig, func() { w.Close() }
}

// BenchmarkPushEndToEnd is the whole path a shipper actually drives: net/http through
// durable enqueue. This is the number the testbed's 80k lines/s should be compared to.
func BenchmarkPushEndToEnd(b *testing.B) {
	srv, _, done := benchStack(b)
	defer done()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := benchBody()
	client := ts.Client()

	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("POST", ts.URL+"/loki/api/v1/push", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			b.Fatalf("push returned %d", resp.StatusCode)
		}
	}
	b.ReportMetric(float64(b.N*batchEntries)/b.Elapsed().Seconds(), "lines/s")
}

// BenchmarkPushDecodeIngest drops net/http: decode plus extraction, label derivation and
// enqueue. EndToEnd minus this is what the HTTP layer costs.
func BenchmarkPushDecodeIngest(b *testing.B) {
	_, ig, done := benchStack(b)
	defer done()
	body := benchBody()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entries, err := push.Decode("application/x-protobuf", body)
		if err != nil {
			b.Fatal(err)
		}
		for j := range entries {
			if err := ig.IngestCtx(b.Context(), entries[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.N*batchEntries)/b.Elapsed().Seconds(), "lines/s")
}

// BenchmarkPushDecode isolates snappy + protobuf + the []LogEntry construction. Nothing
// is stored, so this is the floor the handler can never beat.
func BenchmarkPushDecode(b *testing.B) {
	body := benchBody()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entries, err := push.Decode("application/x-protobuf", body)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != batchEntries {
			b.Fatalf("decoded %d entries, want %d", len(entries), batchEntries)
		}
	}
	b.ReportMetric(float64(b.N*batchEntries)/b.Elapsed().Seconds(), "lines/s")
}
