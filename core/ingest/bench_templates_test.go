package ingest_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// E-4 (RQ3): what typed extraction and indexing cost at ingest as the template count grows.
//
// Three levels, so the cost can be split rather than only totalled:
//
//	BenchmarkTemplateExtract  Engine.Extract per line, nothing else (the handler goroutine's
//	                          extraction share)
//	BenchmarkTemplateWrite    the writer alone, fed keys extracted before the timer: encode,
//	                          page, fsync, typed-range buffer, label postings, seal
//	BenchmarkTemplateIngest   the native ingest API end to end: extraction + label
//	                          derivation + enqueue, and the writer behind it, until Close has
//	                          sealed the last segment
//
// each at 0, 1, 5 and 20 templates (and 5 and 20 templates whose anchors never occur), and at
// two match rates: about 80% of lines carry the matching templates' fields, about 5% do. A
// line that does not match is the same length as one that does, so the rate changes what is
// extracted, not how many bytes the automaton walks.
//
// Write and Ingest need a REAL DISK (see core/storage/write_bench_test.go): they skip unless
// LOGD_BENCH_DIR names a directory on one. Run them with a fixed line count so every
// configuration rotates through the same number of segments:
//
//	LOGD_BENCH_DIR=$HOME/.cache/logd-bench go test ./core/ingest/ -run xxx \
//	    -bench 'Template(Write|Ingest)' -benchtime 1000000x -count 5
//	go test ./core/ingest/ -run xxx -bench TemplateExtract -benchtime 2s -count 5
//
// testbed/scripts/rq3-templates.sh runs all of it and summarises the output.

// matchingTemplates occur in the matching line shapes below, in the order the template sets
// take them: the first five cover every value kind once.
var matchingTemplates = []config.Template{
	{Name: "latency", Pattern: "took={ms:int}ms"},
	{Name: "status", Pattern: "status={code:int}"},
	{Name: "dur", Pattern: "dur={s:float}s"},
	{Name: "user", Pattern: "user={name:str}"},
	{Name: "trace", Pattern: "trace={id:uuid}"},
	{Name: "items", Pattern: "items={n:int}"},
	{Name: "total", Pattern: "total_cents={c:int}"},
	{Name: "sku", Pattern: "sku={id:str}"},
	{Name: "order", Pattern: "order={id:str}"},
	{Name: "cpu", Pattern: "cpu={pct:float}%"},
}

// neverTemplates have anchors that no benchmark line contains: the automaton carries them,
// and they never extract anything.
var neverTemplates = []config.Template{
	{Name: "gc", Pattern: "gc_pause={ms:int}ms"},
	{Name: "queue", Pattern: "queue_depth={n:int}"},
	{Name: "heap", Pattern: "heap_mb={mb:float}"},
	{Name: "tenant", Pattern: "tenant={id:str}"},
	{Name: "session", Pattern: "session={id:uuid}"},
	{Name: "lag", Pattern: "replica_lag={s:float}s"},
	{Name: "conn", Pattern: "conn_id={id:int}"},
	{Name: "shard", Pattern: "shard={n:int}"},
	{Name: "zone", Pattern: "zone={z:str}"},
	{Name: "req", Pattern: "request_id={id:uuid}"},
	{Name: "pid", Pattern: "pid={n:int}"},
	{Name: "thread", Pattern: "thread={name:str}"},
	{Name: "rows", Pattern: "rows_scanned={n:int}"},
	{Name: "ratio", Pattern: "hit_ratio={r:float}"},
	{Name: "tx", Pattern: "txid={id:uuid}"},
	{Name: "batch", Pattern: "batch_size={n:int}"},
	{Name: "temp", Pattern: "temp_c={t:float}C"},
	{Name: "host", Pattern: "hostname={h:str}"},
	{Name: "gen", Pattern: "generation={g:int}"},
	{Name: "chunk", Pattern: "chunk_id={id:uuid}"},
}

// templateSets are the configurations under test. "20" is all ten matching templates plus
// ten that never match, which is what a real config accumulates: templates for formats a
// given stream does not carry.
var templateSets = []struct {
	name  string
	tmpls []config.Template
}{
	{"0", nil},
	{"1", matchingTemplates[:1]},
	{"5", matchingTemplates[:5]},
	{"20", append(append([]config.Template{}, matchingTemplates...), neverTemplates[:10]...)},
	{"5never", neverTemplates[:5]},
	{"20never", neverTemplates},
}

var matchRates = []int{80, 5}

// benchLabels is the shipped allowlist shape; label derivation costs the same in every
// configuration, so it is held constant rather than varied.
var benchLabels = []string{"app", "env", "region"}

const benchExtra = `{"app":"checkout","env":"prod","region":"eu"}`

// benchPoolSize distinct lines are cycled through. Large enough that the values in the
// index buffer are not a handful of repeats, small enough to stay in cache like a real
// handler's working set does.
const benchPoolSize = 8192

// templateLines returns the benchmark's line pool: pct percent of lines in a matching shape,
// the rest in plain shapes of similar length that contain none of the templates' anchors.
// Seeded, so every configuration and every run sees the same lines.
func templateLines(pct int) []string {
	rng := rand.New(rand.NewSource(int64(1000 + pct)))
	users := []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
	out := make([]string, benchPoolSize)
	for i := range out {
		if rng.Intn(100) < pct {
			if i%2 == 0 {
				out[i] = fmt.Sprintf("GET /api/v1/orders/%d status=%d took=%dms dur=%.3fs user=%s trace=%08x-%04x-4%03x-a%03x-%012x",
					rng.Intn(1e6), []int{200, 200, 200, 201, 404, 500}[rng.Intn(6)], rng.Intn(2000), rng.Float64()*2,
					users[rng.Intn(len(users))], rng.Uint32(), rng.Intn(1<<16), rng.Intn(1<<12), rng.Intn(1<<12), rng.Int63n(1<<48))
			} else {
				out[i] = fmt.Sprintf("order confirmed order=ORD-%06d items=%d total_cents=%d sku=SKU-%05d cpu=%.1f%% took=%dms user=%s",
					rng.Intn(1e6), 1+rng.Intn(9), 199+rng.Intn(100000), rng.Intn(1e5), rng.Float64()*100, rng.Intn(2000), users[rng.Intn(len(users))])
			}
			continue
		}
		switch i % 3 {
		case 0:
			out[i] = fmt.Sprintf("GET /api/v1/catalog/items/%d 304 served from cache node=edge-%02d bytes_sent=%d client=10.%d.%d.%d",
				rng.Intn(1e6), rng.Intn(64), rng.Intn(1e5), rng.Intn(256), rng.Intn(256), rng.Intn(256))
		case 1:
			out[i] = fmt.Sprintf("db connection acquired pool=primary idle_conns=%d wait_queue=%d backend=pg-%02d attempt=%d of 3",
				rng.Intn(50), rng.Intn(20), rng.Intn(16), 1+rng.Intn(3))
		default:
			out[i] = fmt.Sprintf("scheduler tick completed jobs_run=%d jobs_skipped=%d next_run_in=%ds worker=w-%03d queue=default",
				rng.Intn(100), rng.Intn(10), rng.Intn(600), rng.Intn(1000))
		}
	}
	return out
}

func compileSet(b *testing.B, tmpls []config.Template) *extract.Engine {
	b.Helper()
	eng, err := extract.Compile(config.IndexConfig{Templates: tmpls})
	if err != nil {
		b.Fatal(err)
	}
	return eng
}

// poolStats reports how many index keys a line yields on average and what fraction of lines
// yield any, so the output shows the match rate each configuration actually ran at.
func poolStats(eng *extract.Engine, lines []string) (keysPerLine, matchedPct float64) {
	keys, matched := 0, 0
	for _, l := range lines {
		k := eng.ReExtract(l)
		keys += len(k)
		if len(k) > 0 {
			matched++
		}
	}
	return float64(keys) / float64(len(lines)), 100 * float64(matched) / float64(len(lines))
}

// templateBenchDir is core/storage's benchDir: a fresh directory on a real filesystem, or a
// skip, because on tmpfs fsync is free and the write path measures nothing.
func templateBenchDir(b *testing.B) string {
	b.Helper()
	root := os.Getenv("LOGD_BENCH_DIR")
	if root == "" {
		b.Skip("set LOGD_BENCH_DIR to a path on a real (non-tmpfs) filesystem; /tmp is tmpfs and fsync there is free")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "tmpl")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// benchWriter opens a writer configured as the daemon ships, except that sealed segments are
// left uncompressed: compression is the same work whatever the template count, runs in the
// background, and would add its own noise to a comparison it takes no part in.
func benchWriter(b *testing.B, eng *extract.Engine) *storage.Writer {
	b.Helper()
	w, err := storage.NewWriter(templateBenchDir(b), storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: 64 << 20,
		FlushInterval:    500 * time.Millisecond,
		SyncInterval:     50 * time.Millisecond,
		BlockPages:       -1,
		LabelKeys:        benchLabels,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(nil); err != nil {
		b.Fatal(err)
	}
	return w
}

func forEachConfig(b *testing.B, fn func(b *testing.B, eng *extract.Engine, lines []string)) {
	for _, rate := range matchRates {
		lines := templateLines(rate)
		for _, set := range templateSets {
			b.Run(fmt.Sprintf("rate=%d/templates=%s", rate, set.name), func(b *testing.B) {
				eng := compileSet(b, set.tmpls)
				kpl, pct := poolStats(eng, lines)
				fn(b, eng, lines)
				b.ReportMetric(kpl, "keys/line")
				b.ReportMetric(pct, "match%")
			})
		}
	}
}

var extractSink []index.KeyedValue

// BenchmarkTemplateExtract is extraction alone: one Engine.Extract per line, counters
// included, exactly as the ingest path calls it.
func BenchmarkTemplateExtract(b *testing.B) {
	forEachConfig(b, func(b *testing.B, eng *extract.Engine, lines []string) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			extractSink = eng.Extract(lines[i%len(lines)])
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lines/s")
	})
}

// BenchmarkTemplateWrite is the writer on its own. Keys and label sets are derived before the
// timer, so the producer does nothing but enqueue and the writer goroutine is the bottleneck:
// what is left is encode, page assembly, fsync, the typed-range buffer, label postings and the
// seal that writes the .tidx files. Its growth with the template count is the indexing cost.
func BenchmarkTemplateWrite(b *testing.B) {
	forEachConfig(b, func(b *testing.B, eng *extract.Engine, lines []string) {
		keys := make([][]index.KeyedValue, len(lines))
		for i, l := range lines {
			keys[i] = eng.ReExtract(l)
		}
		labels := ingest.DeriveLabels(benchExtra, benchLabels)
		w := benchWriter(b, eng)
		ctx := context.Background()
		base := time.Unix(1700000000, 0).UTC()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := model.LogEntry{TS: base.Add(time.Duration(i) * time.Microsecond), Level: model.LogLevelInfo, Extra: benchExtra, Message: lines[i%len(lines)]}
			if err := w.WriteExtractedCtx(ctx, e, keys[i%len(keys)], labels); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil { // drain, flush, seal: every line durable and indexed
			b.Fatal(err)
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lines/s")
	})
}

// BenchmarkTemplateIngest is the native ingest API end to end, timed until Close has sealed
// the last segment. One producer goroutine, as one push request is handled.
func BenchmarkTemplateIngest(b *testing.B) {
	forEachConfig(b, func(b *testing.B, eng *extract.Engine, lines []string) {
		w := benchWriter(b, eng)
		ig := ingest.NewWithLabels(eng, w, benchLabels)
		ctx := context.Background()
		base := time.Unix(1700000000, 0).UTC()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := model.LogEntry{TS: base.Add(time.Duration(i) * time.Microsecond), Level: model.LogLevelInfo, Extra: benchExtra, Message: lines[i%len(lines)]}
			if err := ig.IngestCtx(ctx, e); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lines/s")
	})
}

// BenchmarkTemplateIndexRAM sizes the active segment's in-RAM index (RQ4): it fills one 64 MB
// segment's worth of records into the same structures the writer keeps — the typed-range
// buffer and the label builder — and measures the live heap they hold.
//
// The writer's memory budget (index_mem_budget_mb) estimates the buffer as entries ×
// index.EntryBytes (indexMemExceeded). The heap figure includes what that leaves out: the
// spare capacity append leaves in each field's slice, and the label builder's one posting per
// record. Records per segment come from packing the pool's encoded sizes into pages the way
// the writer does. One iteration is enough; the figures are deterministic.
//
//	go test ./core/ingest/ -run xxx -bench TemplateIndexRAM -benchtime 1x
func BenchmarkTemplateIndexRAM(b *testing.B) {
	forEachConfig(b, func(b *testing.B, eng *extract.Engine, lines []string) {
		recs := recordsPerSegment(lines, 64<<20)
		keys := make([][]index.KeyedValue, len(lines))
		for i, l := range lines {
			keys[i] = eng.ReExtract(l)
		}
		labels := ingest.DeriveLabels(benchExtra, benchLabels)
		var entries int
		var bufHeap, lblHeap int64
		for n := 0; n < b.N; n++ {
			before := liveHeap()
			buf := index.NewBuffer(eng.IndexedFields())
			for i := 0; i < recs; i++ {
				for _, kv := range keys[i%len(keys)] {
					buf.Add(kv.Field, kv.Key, uint64(i)*64)
				}
			}
			bufHeap = heapSince(before)
			entries = buf.EntryCount()
			runtime.KeepAlive(buf)

			before = liveHeap()
			lb := label.NewBuilder()
			canon := labels.Canonical()
			for i := 0; i < recs; i++ {
				lb.AddPosting(lb.InternCanonical(canon, labels), uint64(i)*64)
			}
			lblHeap = heapSince(before)
			runtime.KeepAlive(lb)
		}
		b.ReportMetric(float64(recs), "records/seg")
		b.ReportMetric(float64(entries), "entries/seg")
		b.ReportMetric(float64(entries*index.EntryBytes)/(1<<20), "estMB/seg")
		b.ReportMetric(float64(bufHeap)/(1<<20), "heapMB/seg")
		b.ReportMetric(float64(lblHeap)/(1<<20), "labelMB/seg")
		if entries > 0 {
			b.ReportMetric(float64(bufHeap)/float64(entries), "B/entry")
		}
	})
}

// recordsPerSegment packs the pool's lines, cycled, into 4 KB pages as the writer does and
// returns how many records a segment of segBytes holds (page 0 is the placeholder).
func recordsPerSegment(lines []string, segBytes int64) int {
	pages := segBytes/storage.PageSize - 1
	free := storage.PageSize - storage.PageHeaderSize
	recs := 0
	for p := int64(0); p < pages; {
		e := model.LogEntry{TS: time.Unix(1700000000, 0), IngestedAt: time.Unix(1700000000, 0), Level: model.LogLevelInfo, Extra: benchExtra, Message: lines[recs%len(lines)]}
		size := storage.EncodedSize(e)
		if size > free {
			p++
			free = storage.PageSize - storage.PageHeaderSize
			continue
		}
		free -= size
		recs++
	}
	return recs
}

func liveHeap() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// heapSince is the live heap grown since before, floored at zero: with nothing retained, GC
// noise can otherwise report a few negative bytes.
func heapSince(before int64) int64 {
	return max(liveHeap()-before, 0)
}
