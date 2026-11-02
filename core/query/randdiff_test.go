package query_test

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/ingest"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// Randomised differential probes: Execute vs ExecuteScan vs a brute-force oracle over the
// in-memory corpus (TestRandDiff), and ExecuteMetric vs a brute-force evaluation of the
// windows (t-r, t] (TestRandDiffMetric). Each seed generates a corpus and a storage layout,
// then random queries against it.
//
// A seed fixes the corpus, the layout options and the queries, and a corpus written only to
// writers that are then closed is laid out identically on every run. A writer left open
// flushes its partial page on a short tick, so that every record the oracle knows is
// readable; where those ticks cut its pages depends on timing, as in a running daemon. The
// answers must not depend on it, and are what the probes check; the Explain tallies printed
// at the end can differ a little between runs.
//
// What is varied, per corpus: the shard count; segment size (so a corpus spans several
// sealed segments); compression and block size (sealed segments uncompressed, or in blocks
// of the default, 1 or 3 pages); the per-key label-value cap; whether the tail of the corpus
// — a quarter (the evaluation's T4 layout), half, or all of it — stays in still-open active
// segments, answered from the writer's in-memory index, and whether those rotate; and, with
// LOGD_RANDDIFF_DRIFT=1, config drift: the corpus is ingested with a different template set
// (other fields, or the same fields in another order) and label allowlist than it is
// queried with, recorded in the manifest or not. The corpus has many timestamp ties (one
// per second over a short span, some a nanosecond or two apart) and arrives out of order.
// Per query: one to three ANDed predicates over typed fields of every kind (with a
// cross-kind value now and then, which both paths must refuse), line filters, label equality, inequality, regex and
// comparison, level; the time window; the limit; and the direction. The metric probe
// varies the shard count, open or closed writers (and active segments that never fill), the
// range aggregation, step, range and window, and sum by app, by level or by nothing; its
// oracle takes a record's labels, level included, only from the labels the record carries.
//
// At the default sizes they take about 20 s together (more under -race), so they run only
// when asked:
//
//	LOGD_RANDDIFF=1 go test ./core/query/ -run 'TestRandDiff' -v -count=1
//
// LOGD_RANDDIFF_SEEDS (default 20) and LOGD_RANDDIFF_QUERIES (default 300) size
// TestRandDiff, LOGD_RANDDIFF_FIRST_SEED (default 1) is the first seed of both, and
// LOGD_RANDDIFF_DRIFT=1 switches TestRandDiff to config drift. LOGD_RANDDIFF_METRIC_SEEDS
// (default 30) and LOGD_RANDDIFF_METRIC_QUERIES (default 100) size TestRandDiffMetric.

func rdEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("LOGD_RANDDIFF") == "" {
		t.Skip("set LOGD_RANDDIFF=1 to run the randomised differential probes")
	}
	// The cap-breach and skipped-segment log lines would bury the report.
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
}

func rdEnvInt(t *testing.T, name string, def int64) int64 {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

var rdT0 = int64(1_700_000_000) * 1e9

var rdCfg = config.IndexConfig{
	Templates: []config.Template{
		{Name: "latency", Pattern: "took {ms:int}ms"},
		{Name: "dur", Pattern: "dur {d:float}s"},
		{Name: "trip", Pattern: "trip:{id:str}"},
		{Name: "req", Pattern: "req={id:uuid}"},
	},
	Literals: []string{"panic:", "error"},
}

var rdLabelKeys = []string{"region", "env", "code"}

var rdCfgB = config.IndexConfig{
	Templates: []config.Template{
		{Name: "code", Pattern: "code {c:int} "},
		{Name: "latency", Pattern: "took={ms:int}ms"},
		{Name: "req", Pattern: "req={id:uuid}"},
		{Name: "trip", Pattern: "trip:{id:str}"},
	},
	Literals: []string{"ERROR", "panic:"},
}

var rdCfgC = config.IndexConfig{ // same definitions as rdCfg, different order
	Templates: []config.Template{rdCfg.Templates[3], rdCfg.Templates[2], rdCfg.Templates[1], rdCfg.Templates[0]},
	Literals:  []string{"error", "panic:"},
}

var rdUUIDs = []string{
	"0b5f3c1e-1111-4a2b-9c3d-000000000001",
	"0B5F3C1E-1111-4A2B-9C3D-000000000002",
	"ffffffff-ffff-ffff-ffff-ffffffffffff",
	"00000000-0000-0000-0000-000000000000",
}

const rdPrefix16 = "PREFIX0123456789"

func rdGenMsg(r *rand.Rand) string {
	var parts []string
	n := 1 + r.Intn(3)
	for i := 0; i < n; i++ {
		switch r.Intn(9) {
		case 0, 1:
			parts = append(parts, fmt.Sprintf("took %dms", r.Intn(60)-10))
		case 2:
			parts = append(parts, fmt.Sprintf("took=%dms", r.Intn(60)))
		case 3:
			fs := []string{"-0", "0", "0.0", "-0.0", "1.5", "-2.5", "1e1", "10", "3", "2.999", "+4"}
			parts = append(parts, "dur "+fs[r.Intn(len(fs))]+"s")
		case 4:
			ids := []string{"a", "b", "ab", rdPrefix16, rdPrefix16 + "x", rdPrefix16 + "y", rdPrefix16 + "é", "zz", "A"}
			parts = append(parts, "trip:"+ids[r.Intn(len(ids))])
		case 5:
			parts = append(parts, "req="+rdUUIDs[r.Intn(len(rdUUIDs))])
		case 6:
			ls := []string{"panic:", "error", "errors", "panic", "xerror", "ERROR"}
			parts = append(parts, ls[r.Intn(len(ls))])
		default:
			ws := []string{"GET", "/api", "ok", "took", "ms", "trip:", "req=", "dur s"}
			parts = append(parts, ws[r.Intn(len(ws))])
		}
	}
	return strings.Join(parts, " ")
}

func rdGenExtra(r *rand.Rand) string {
	var kv []string
	switch r.Intn(6) {
	case 0:
	case 1:
		kv = append(kv, `"region":""`)
	default:
		kv = append(kv, fmt.Sprintf(`"region":"r%d"`, r.Intn(5)))
	}
	if r.Intn(2) == 0 {
		kv = append(kv, fmt.Sprintf(`"env":"%s"`, []string{"prod", "dev"}[r.Intn(2)]))
	}
	if r.Intn(3) != 0 {
		kv = append(kv, fmt.Sprintf(`"code":%d`, []int{200, 300, 500, 404}[r.Intn(4)]))
	}
	if r.Intn(10) == 0 {
		kv = append(kv, `"nested":{"a":1}`)
	}
	r.Shuffle(len(kv), func(i, j int) { kv[i], kv[j] = kv[j], kv[i] })
	if len(kv) == 0 && r.Intn(2) == 0 {
		return ""
	}
	return "{" + strings.Join(kv, ",") + "}"
}

func rdGenCorpus(r *rand.Rand, n int, span int) []model.LogEntry {
	out := make([]model.LogEntry, n)
	for i := range out {
		ts := rdT0 + int64(r.Intn(span))*1e9
		if r.Intn(5) == 0 {
			ts += int64(r.Intn(3))
		}
		out[i] = model.LogEntry{
			TS:      time.Unix(0, ts).UTC(),
			Level:   model.LogLevel(r.Intn(4)),
			Extra:   rdGenExtra(r),
			Message: rdGenMsg(r),
		}
	}
	return out
}

type rdBuild struct {
	eng     *qr.Engine
	writers []*storage.Writer
	open    bool
}

type rdOpts struct {
	exIngest   *extract.Engine
	ingestKeys []string
	recordKeys bool
	queryKeys  []string
	shards     int
	segPages   int
	blockPages int
	maxCard    int
	active     bool // keep a final batch in an active segment
	// activeShare is the fraction of the corpus written by the second, still-open writers;
	// activeSegPages their segment size (0 = never rotate, so all of it stays active).
	activeShare    float64
	activeSegPages int
}

func rdBuildEngine(root string, ex *extract.Engine, recs []model.LogEntry, o rdOpts) (*rdBuild, error) {
	var rk []string
	if o.recordKeys {
		rk = o.ingestKeys
	}
	// flush is the partial-page flush tick. The writers that are closed before querying
	// never need one (Close writes the last page), and without it their layout is the same
	// on every run.
	mk := func(segPages int, flush time.Duration) ([]*storage.Writer, error) {
		var ws []*storage.Writer
		size := int64(segPages) * storage.PageSize
		if segPages == 0 {
			size = 1 << 40
		}
		for i := 0; i < o.shards; i++ {
			w, err := storage.NewWriter(filepath.Join(root, fmt.Sprintf("shard-%d", i)), storage.Options{
				Schema: o.exIngest.IndexedFields(), SegmentSizeBytes: size,
				FlushInterval: flush, BlockPages: o.blockPages, MaxLabelCardinality: o.maxCard,
				LabelKeys: rk,
			})
			if err != nil {
				return nil, err
			}
			if err := w.Start(nil); err != nil {
				return nil, err
			}
			ws = append(ws, w)
		}
		return ws, nil
	}
	ws, err := mk(o.segPages, time.Hour)
	if err != nil {
		return nil, err
	}
	split := len(recs)
	if o.active {
		split = int(float64(len(recs)) * (1 - o.activeShare))
	}
	ig := ingest.NewShardedWithLabels(o.exIngest, ws, o.ingestKeys)
	for _, e := range recs[:split] {
		if err := ig.Ingest(e); err != nil {
			return nil, err
		}
	}
	for _, w := range ws {
		w.Close()
	}
	b := &rdBuild{}
	if o.active {
		ws, err = mk(o.activeSegPages, 5*time.Millisecond)
		if err != nil {
			return nil, err
		}
		ig = ingest.NewShardedWithLabels(o.exIngest, ws, o.ingestKeys)
		for _, e := range recs[split:] {
			if err := ig.Ingest(e); err != nil {
				return nil, err
			}
		}
		b.open = true
	}
	var shards []qr.Shard
	for _, w := range ws {
		shards = append(shards, w)
	}
	b.writers = ws
	b.eng = qr.NewShardedEngine(shards, ex, o.queryKeys)
	if b.open {
		// Wait until the flush ticks have written every page, so the oracle (which knows
		// every record) and both paths see the same data. A fixed sleep was not enough
		// under load.
		if err := rdAwaitReadable(b.eng, len(recs)); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (b *rdBuild) close() {
	if b.open {
		for _, w := range b.writers {
			w.Close()
		}
	}
}

// rdAwaitReadable waits until all n records written to still-open writers are readable.
func rdAwaitReadable(e *qr.Engine, n int) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		all, err := e.ExecuteScan(qr.Query{Limit: n + 1})
		if err != nil {
			return err
		}
		if len(all) == n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d records became readable", len(all), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ---- oracle ----

type rdResolver struct{ ex *extract.Engine }

func (o rdResolver) Label(e model.LogEntry, key string) (string, bool) {
	switch key {
	case "level":
		return e.Level.String(), true
	case "service":
		return "", false
	}
	m := model.ParseExtraLabels(e.Extra)
	v, ok := m[key]
	return v, ok
}
func (o rdResolver) FieldValues(e model.LogEntry, field string) []index.Value {
	var out []index.Value
	for _, fv := range o.ex.ExtractValues(e.Message) {
		if fv.Field == field {
			out = append(out, fv.Value)
		}
	}
	return out
}
func (o rdResolver) AllLabels(e model.LogEntry) map[string]string { return nil }

func rdLess(a, b model.LogEntry, dir qr.Direction) bool {
	ta, tb := a.TS.UnixNano(), b.TS.UnixNano()
	if ta != tb {
		if dir == qr.Forward {
			return ta < tb
		}
		return ta > tb
	}
	if a.Message != b.Message {
		return a.Message < b.Message
	}
	if a.Extra != b.Extra {
		return a.Extra < b.Extra
	}
	if a.Level != b.Level {
		return a.Level < b.Level
	}
	return a.ServiceID < b.ServiceID
}

func rdOracle(recs []model.LogEntry, ex *extract.Engine, q qr.Query) []model.LogEntry {
	end := q.End
	if end <= 0 {
		end = 1<<63 - 1
	}
	res := rdResolver{ex}
	var out []model.LogEntry
	for _, e := range recs {
		ts := e.TS.UnixNano()
		if ts < q.Start || ts > end {
			continue
		}
		ok := true
		for _, p := range q.Preds {
			if !p.Match(e, res) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return rdLess(out[i], out[j], q.Direction) })
	lim := q.Limit
	if lim <= 0 {
		lim = 100
	}
	if len(out) > lim {
		out = out[:lim]
	}
	return out
}

func rdKey(e model.LogEntry) string {
	return fmt.Sprintf("%d|%s|%s|%d|%d", e.TS.UnixNano(), e.Message, e.Extra, e.Level, e.ServiceID)
}

func rdDiff(a, b []model.LogEntry) string {
	if len(a) != len(b) {
		return fmt.Sprintf("len %d vs %d", len(a), len(b))
	}
	for i := range a {
		if rdKey(a[i]) != rdKey(b[i]) {
			return fmt.Sprintf("row %d: %s  vs  %s", i, rdKey(a[i]), rdKey(b[i]))
		}
	}
	return ""
}

// ---- random queries ----

func rdRandInt(r *rand.Rand) index.Value {
	return index.Value{Kind: index.KindInt, Int: int64(r.Intn(70) - 15)}
}
func rdRandFloat(r *rand.Rand) index.Value {
	fs := []float64{0, rdNegZero(), 1.5, -2.5, 10, 3, 2.999, 4, -1, 100}
	return index.Value{Kind: index.KindFloat, Float: fs[r.Intn(len(fs))]}
}
func rdNegZero() float64 { z := 0.0; return -z }

func rdRandStr(r *rand.Rand) index.Value {
	ss := []string{"a", "ab", "b", "", rdPrefix16, rdPrefix16 + "x", rdPrefix16 + "w", rdPrefix16 + "é", "zz", "A", "PREFIX"}
	return index.Value{Kind: index.KindStr, Str: ss[r.Intn(len(ss))]}
}
func rdRandUUID(r *rand.Rand) index.Value {
	u := rdUUIDs[r.Intn(len(rdUUIDs))]
	var out [16]byte
	h := strings.ReplaceAll(u, "-", "")
	for i := 0; i < 16; i++ {
		fmt.Sscanf(h[2*i:2*i+2], "%02x", &out[i])
	}
	if r.Intn(3) == 0 {
		out[15] ^= 1
	}
	return index.Value{Kind: index.KindUUID, UUID: out}
}

func rdRandPred(r *rand.Rand) (qr.Predicate, string) {
	op := qr.Op(r.Intn(6))
	switch r.Intn(14) {
	case 0, 1, 2:
		v := rdRandInt(r)
		if r.Intn(15) == 0 {
			v = rdRandFloat(r) // cross-kind
		}
		return qr.TypedCompare{Field: "latency_ms", Op: op, Value: v}, fmt.Sprintf("latency_ms op%d %v", op, v)
	case 3:
		v := rdRandFloat(r)
		return qr.TypedCompare{Field: "dur_d", Op: op, Value: v}, fmt.Sprintf("dur_d op%d %v", op, v.Float)
	case 4:
		v := rdRandStr(r)
		return qr.TypedCompare{Field: "trip_id", Op: op, Value: v}, fmt.Sprintf("trip_id op%d %q", op, v.Str)
	case 5:
		v := rdRandUUID(r)
		return qr.TypedCompare{Field: "req_id", Op: op, Value: v}, fmt.Sprintf("req_id op%d %x", op, v.UUID)
	case 6:
		s := []string{"panic:", "error", "errors", "took", "ERROR"}[r.Intn(5)]
		return qr.LineContains{Sub: s}, "|= " + s
	case 7:
		s := []string{"panic:", "error", "took"}[r.Intn(3)]
		return qr.LineNotContains{Sub: s}, "!= " + s
	case 8, 9:
		k := rdLabelKeys[r.Intn(3)]
		vs := map[string][]string{"region": {"r0", "r1", "r4", ""}, "env": {"prod", "dev", ""}, "code": {"200", "500", ""}}[k]
		v := vs[r.Intn(len(vs))]
		return qr.LabelEqual{Key: k, Value: v}, fmt.Sprintf("%s=%q", k, v)
	case 10:
		k := rdLabelKeys[r.Intn(3)]
		v := []string{"r1", "prod", "200", ""}[r.Intn(4)]
		return qr.LabelNotEqual{Key: k, Value: v}, fmt.Sprintf("%s!=%q", k, v)
	case 11:
		return qr.LabelRegex{Key: "region", Re: regexp.MustCompile(`^(?:r[0-2])$`)}, "region=~r[0-2]"
	case 12:
		return qr.LabelCompare{Key: "code", Op: qr.Op(2 + r.Intn(4)), Value: "300"}, "code cmp 300"
	default:
		lv := []string{"info", "error", "warn"}[r.Intn(3)]
		return qr.LabelEqual{Key: "level", Value: lv}, "level=" + lv
	}
}

func rdRandQuery(r *rand.Rand, recs []model.LogEntry, span int) (qr.Query, string) {
	var q qr.Query
	var desc []string
	n := 1 + r.Intn(3)
	for i := 0; i < n; i++ {
		p, d := rdRandPred(r)
		q.Preds = append(q.Preds, p)
		desc = append(desc, d)
	}
	switch r.Intn(4) {
	case 0:
		q.Start, q.End = 0, 0
	case 1:
		a := recs[r.Intn(len(recs))].TS.UnixNano()
		b := recs[r.Intn(len(recs))].TS.UnixNano()
		if a > b {
			a, b = b, a
		}
		q.Start, q.End = a, b
	default:
		a := rdT0 + int64(r.Intn(span))*1e9
		q.Start, q.End = a, a+int64(r.Intn(span/2+1))*1e9
	}
	q.Limit = []int{0, 1, 2, 3, 5, 7, 17, 50, 1000, 100000}[r.Intn(10)]
	if r.Intn(2) == 0 {
		q.Direction = qr.Forward
	}
	return q, fmt.Sprintf("[%d,%d] lim=%d dir=%d %s", q.Start-rdT0, q.End-rdT0, q.Limit, q.Direction, strings.Join(desc, " AND "))
}

// rdMaxReported bounds how many divergences a probe prints before only counting them.
const rdMaxReported = 20

func TestRandDiff(t *testing.T) {
	rdEnabled(t)
	seeds := rdEnvInt(t, "LOGD_RANDDIFF_SEEDS", 20)
	queries := int(rdEnvInt(t, "LOGD_RANDDIFF_QUERIES", 300))
	first := rdEnvInt(t, "LOGD_RANDDIFF_FIRST_SEED", 1)
	drift := os.Getenv("LOGD_RANDDIFF_DRIFT") == "1"

	reasons := map[string]int{}
	ex, err := extract.Compile(rdCfg)
	if err != nil {
		t.Fatal(err)
	}
	exB, err := extract.Compile(rdCfgB)
	if err != nil {
		t.Fatal(err)
	}
	exC, err := extract.Compile(rdCfgC)
	if err != nil {
		t.Fatal(err)
	}
	fails, total, rejected := 0, 0, 0
	fail := func(format string, args ...any) {
		fails++
		if fails <= rdMaxReported {
			t.Errorf(format, args...)
		}
	}
	for s := first; s < first+seeds; s++ {
		r := rand.New(rand.NewSource(s))
		n := 200 + r.Intn(1500)
		span := 5 + r.Intn(300)
		recs := rdGenCorpus(r, n, span)
		o := rdOpts{
			shards:     []int{1, 1, 2, 3}[r.Intn(4)],
			segPages:   2 + r.Intn(8),
			blockPages: []int{-1, 0, 1, 3}[r.Intn(4)],
			maxCard:    []int{0, 0, 2, 3}[r.Intn(4)],
			active:     r.Intn(3) != 0,
			// a quarter (the evaluation's T4 layout), half, or all of the corpus unsealed
			activeShare:    []float64{0.25, 0.5, 1}[r.Intn(3)],
			activeSegPages: []int{0, 0, 4, 9}[r.Intn(4)],
			exIngest:       ex,
			ingestKeys:     rdLabelKeys,
			recordKeys:     true,
			queryKeys:      rdLabelKeys,
		}
		if drift {
			o.exIngest = []*extract.Engine{ex, exB, exC}[r.Intn(3)]
			ks := [][]string{{"region", "env"}, {"region", "env", "code"}, {"env"}, nil}
			o.ingestKeys = ks[r.Intn(4)]
			o.queryKeys = ks[r.Intn(4)]
			o.recordKeys = r.Intn(2) == 0
		}
		root, err := os.MkdirTemp(t.TempDir(), "corpus")
		if err != nil {
			t.Fatal(err)
		}
		b, err := rdBuildEngine(root, ex, recs, o)
		if err != nil {
			t.Fatalf("seed=%d build: %v", s, err)
		}
		for i := 0; i < queries; i++ {
			q, d := rdRandQuery(r, recs, span)
			total++
			for _, sp := range b.eng.Explain(q) {
				reasons[sp.Reason]++
			}
			got, err1 := b.eng.Execute(q)
			scan, err2 := b.eng.ExecuteScan(q)
			if rdCrossKind(q, ex) {
				// A value of another kind than the field's has no meaningful answer: the
				// native API refuses it at plan time, on both paths (design §9.3).
				rejected++
				if !errors.Is(err1, qr.ErrInvalidPredicate) || !errors.Is(err2, qr.ErrInvalidPredicate) {
					fail("seed=%d %s: cross-kind predicate: Execute err %v, ExecuteScan err %v; want ErrInvalidPredicate from both", s, d, err1, err2)
				}
				continue
			}
			want := rdOracle(recs, ex, q)
			if err1 != nil || err2 != nil {
				fail("seed=%d %s: Execute err %v, ExecuteScan err %v", s, d, err1, err2)
				continue
			}
			if dd := rdDiff(got, want); dd != "" {
				fail("seed=%d opts=%+v n=%d EXECUTE!=ORACLE %s :: %s", s, o, n, d, dd)
			}
			if dd := rdDiff(scan, want); dd != "" {
				fail("seed=%d opts=%+v n=%d SCAN!=ORACLE %s :: %s", s, o, n, d, dd)
			}
		}
		b.close()
		os.RemoveAll(root)
	}
	t.Logf("drift=%v seeds=%d..%d queries=%d (%d cross-kind, refused): failures: %d explain reasons: %v", drift, first, first+seeds-1, total, rejected, fails, reasons)
}

// rdCrossKind reports whether q compares a field the engine's extractor declares with a
// value of another kind.
func rdCrossKind(q qr.Query, ex *extract.Engine) bool {
	for _, p := range q.Preds {
		if tc, ok := p.(qr.TypedCompare); ok {
			if kind, declared := ex.FieldKind(tc.Field); declared && kind != tc.Value.Kind {
				return true
			}
		}
	}
	return false
}

// ---- metric oracle ----

func rdLabelsOf(e model.LogEntry) map[string]string {
	m := model.ParseExtraLabels(e.Extra)
	if m == nil {
		m = map[string]string{}
	}
	// C-04/Q-01: level comes only from the labels the record carries (none here)
	return m
}

func rdLabelKey(m map[string]string) string {
	var ks []string
	for k, v := range m {
		ks = append(ks, k+"="+v)
	}
	sort.Strings(ks)
	return strings.Join(ks, ",")
}

// rdBrute evaluates a metric query over the corpus directly: each step t aggregates the
// records in (t-Range, t] per label set, then sums the series per group.
func rdBrute(recs []model.LogEntry, ex *extract.Engine, q qr.MetricQuery) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	type sv struct {
		labels map[string]string
		vals   map[int64]float64
	}
	res := rdResolver{ex}
	streams := map[string]*sv{}
	for t := q.Start; t <= q.End; t += q.Step {
		per := map[string][]float64{}
		lbls := map[string]map[string]string{}
		for _, e := range recs {
			ts := e.TS.UnixNano()
			if !(ts > t-q.Range && ts <= t) {
				continue
			}
			ok := true
			for _, p := range q.Preds {
				if !p.Match(e, res) {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			v := 0.0
			if q.Unwrap != "" {
				found := false
				for _, fv := range ex.ExtractValues(e.Message) {
					if fv.Field == q.Unwrap {
						v = float64(fv.Value.Int)
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			l := rdLabelsOf(e)
			k := rdLabelKey(l)
			per[k] = append(per[k], v)
			lbls[k] = l
		}
		for k, vs := range per {
			var val float64
			switch q.RangeOp {
			case qr.RangeCount:
				val = float64(len(vs))
			case qr.RangeRate:
				val = float64(len(vs)) / (float64(q.Range) / 1e9)
			case qr.RangeSum:
				for _, x := range vs {
					val += x
				}
			case qr.RangeMax:
				val = math.Inf(-1)
				for _, x := range vs {
					val = math.Max(val, x)
				}
			}
			s := streams[k]
			if s == nil {
				s = &sv{labels: lbls[k], vals: map[int64]float64{}}
				streams[k] = s
			}
			s.vals[t] = val
		}
	}
	if q.VectorOp == qr.VecNone {
		for k, s := range streams {
			out[k] = s.vals
		}
		return out
	}
	for _, s := range streams {
		g := map[string]string{}
		for _, k := range q.Grouping {
			if v, ok := s.labels[k]; ok {
				g[k] = v
			}
		}
		gk := rdLabelKey(g)
		if out[gk] == nil {
			out[gk] = map[int64]float64{}
		}
		for t, v := range s.vals {
			out[gk][t] += v // VecSum
		}
	}
	return out
}

func TestRandDiffMetric(t *testing.T) {
	rdEnabled(t)
	seeds := rdEnvInt(t, "LOGD_RANDDIFF_METRIC_SEEDS", 30)
	queries := int(rdEnvInt(t, "LOGD_RANDDIFF_METRIC_QUERIES", 100))
	first := rdEnvInt(t, "LOGD_RANDDIFF_FIRST_SEED", 1)

	ex, err := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	if err != nil {
		t.Fatal(err)
	}
	fails, total := 0, 0
	reasons := map[string]int{}
	for seed := first; seed < first+seeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		var recs []model.LogEntry
		for i := 0; i < 300+r.Intn(500); i++ {
			recs = append(recs, model.LogEntry{TS: time.Unix(0, rdT0+int64(r.Intn(120))*1e9).UTC(), Level: model.LogLevel(r.Intn(2)),
				Extra: fmt.Sprintf(`{"app":"a%d"}`, r.Intn(3)), Message: fmt.Sprintf("x took %dms", r.Intn(100))})
		}
		shards := 1 + r.Intn(3)
		// Two corpora in three keep their writers open, so the tail — or, with segments
		// that never fill, all — of the data is in the active segment, answered from the
		// writer's in-memory index.
		open := r.Intn(3) != 0
		segSize := int64(3 * storage.PageSize)
		if open && r.Intn(2) == 0 {
			segSize = 1 << 40
		}
		root, err := os.MkdirTemp(t.TempDir(), "corpus")
		if err != nil {
			t.Fatal(err)
		}
		// FlushInterval short, so a writer left open (below) still writes every page; a
		// writer that is closed needs no tick, and is laid out the same on every run.
		flush := time.Hour
		if open {
			flush = 2 * time.Millisecond
		}
		var ws []*storage.Writer
		var sh []qr.Shard
		for i := 0; i < shards; i++ {
			w, err := storage.NewWriter(filepath.Join(root, fmt.Sprint(i)), storage.Options{Schema: ex.IndexedFields(), SegmentSizeBytes: segSize, FlushInterval: flush, LabelKeys: []string{"app"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(nil); err != nil {
				t.Fatal(err)
			}
			ws = append(ws, w)
			sh = append(sh, w)
		}
		ig := ingest.NewShardedWithLabels(ex, ws, []string{"app"})
		for _, e := range recs {
			if err := ig.Ingest(e); err != nil {
				t.Fatal(err)
			}
		}
		e := qr.NewShardedEngine(sh, ex, []string{"app"})
		typed := qr.TypedCompare{Field: "latency_ms", Op: qr.OpGe, Value: index.Value{Kind: index.KindInt, Int: 0}}
		if open {
			if err := rdAwaitReadable(e, len(recs)); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			for _, sp := range e.Explain(qr.Query{Preds: []qr.Predicate{typed}, Limit: len(recs)}) {
				reasons[sp.State+"/"+sp.Reason]++
			}
		} else {
			for _, w := range ws {
				w.Close()
			}
		}
		for k := 0; k < queries; k++ {
			step := int64(1+r.Intn(20)) * 1e9
			rng := int64(1+r.Intn(30)) * 1e9
			start := rdT0 + int64(r.Intn(100)-10)*1e9
			end := start + int64(r.Intn(60))*1e9
			q := qr.MetricQuery{Range: rng, Start: start, End: end, Step: step,
				RangeOp: []qr.RangeOp{qr.RangeCount, qr.RangeRate, qr.RangeSum, qr.RangeMax}[r.Intn(4)]}
			if q.RangeOp == qr.RangeSum || q.RangeOp == qr.RangeMax {
				q.Unwrap = "latency_ms"
			}
			if r.Intn(2) == 0 {
				q.VectorOp = qr.VecSum
				q.Grouping = [][]string{{"app"}, {"level"}, nil}[r.Intn(3)]
			}
			if r.Intn(3) == 0 {
				q.Preds = []qr.Predicate{typed}
			}
			total++
			got, err := e.ExecuteMetric(q)
			if err != nil {
				fails++
				t.Errorf("seed %d q=%+v: %v", seed, q, err)
				continue
			}
			want := rdBrute(recs, ex, q)
			gm := map[string]map[int64]float64{}
			for _, s := range got {
				m := map[int64]float64{}
				for _, p := range s.Points {
					m[p.TS] = p.Value
				}
				gm[rdLabelKey(s.Labels)] = m
			}
			bad := len(gm) != len(want)
			for k, m := range want {
				g := gm[k]
				if len(g) != len(m) {
					bad = true
				}
				for ts, v := range m {
					if math.Abs(g[ts]-v) > 1e-9 {
						bad = true
					}
				}
			}
			if bad {
				fails++
				if fails <= rdMaxReported {
					t.Errorf("seed %d q=%+v\n got=%v\n want=%v", seed, q, gm, want)
				}
			}
		}
		if open {
			for _, w := range ws {
				w.Close()
			}
		}
		os.RemoveAll(root)
	}
	t.Logf("seeds=%d..%d queries=%d: metric failures: %d explain (open corpora, typed predicate): %v", first, first+seeds-1, total, fails, reasons)
}
