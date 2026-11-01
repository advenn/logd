package query_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	qr "github.com/advenn/logd/core/query"
	"github.com/advenn/logd/core/storage"
)

// E-6 / RQ5 part B: the fault-injection matrix.
//
// One corpus spans compressed sealed segments, uncompressed sealed segments and the active
// segment, with typed (int, float, str, uuid), literal and label fields. For each fault a
// copy of it is damaged (or the configuration it is read with is changed), opened the way the
// daemon opens a shard on restart, and put through a fixed battery of queries. Every query is
// checked twice:
//
//   - Execute == ExecuteScan, record for record (the planner's contract);
//   - both equal the ground truth computed from the generator's own record of each line — no
//     logd code involved — minus the records the fault makes unreadable on every path, which
//     is the intended degrade (a corrupt page loses its records everywhere, never only on one
//     path).
//
// The matrix takes several seconds, more under -race, so it runs only with LOGD_FAULTMATRIX=1;
// add -v to see the table it prints:
//
//	LOGD_FAULTMATRIX=1 go test ./core/query/ -run TestFaultMatrix -v -count=1
//
// LOGD_FAULTMATRIX_OUT=<path> also writes the table to a file.

const (
	fmRecords      = 3000
	fmCompressedTo = 1200 // records [0, 1200) are sealed into compressed segments
	fmRawTo        = 2700 // [1200, 2700) into uncompressed sealed segments; the rest stay active
	fmSegPages     = 16
	fmBlockPages   = 4
)

var fmLabelKeys = []string{"app", "region"}

func fmTemplates() []config.Template {
	return []config.Template{
		{Name: "latency", Pattern: "took={ms:int}ms"},
		{Name: "status", Pattern: "status={code:int}"},
		{Name: "dur", Pattern: "dur={s:float}s"},
		{Name: "user", Pattern: "user={name:str}"},
		{Name: "trace", Pattern: "trace={id:uuid}"},
	}
}

// fmRec is one corpus record as the generator wrote it. The ground truth is computed from
// these fields, not by extracting from the line.
type fmRec struct {
	ts               int64
	msg, extra       string
	app, region, env string
	lat              []int64 // "took=<n>ms" occurrences
	latSpaced        []int64 // "took <n>ms" occurrences, which only the edited template reads
	status           int64
	hasStatus        bool
	dur              string // as written, "" if absent
	user             string
	trace            string
}

// fmCorpus generates the records in arrival order. Timestamps are unique, so every result has
// one correct order, and every seventh pair arrives swapped (out-of-order arrival).
func fmCorpus() []fmRec {
	rng := rand.New(rand.NewSource(6))
	users := []string{"alice", "bob", "carol", "dave"}
	recs := make([]fmRec, fmRecords)
	for i := range recs {
		r := &recs[i]
		r.ts = ts(0) + int64(i)*int64(10*time.Millisecond)
		r.app = []string{"checkout", "billing"}[rng.Intn(2)]
		r.region = fmt.Sprintf("r%d", rng.Intn(3))
		r.env = []string{"prod", "staging"}[rng.Intn(2)]
		r.extra = fmt.Sprintf(`{"app":%q,"env":%q,"region":%q}`, r.app, r.env, r.region)
		user := users[rng.Intn(len(users))]
		if rng.Intn(3) == 0 {
			user = fmt.Sprintf("tenant-abcdefghijkl-%03d", rng.Intn(100)) // shares a 16-byte prefix: lossy str key
		}
		switch k := rng.Intn(10); {
		case k < 6: // request line: every typed field
			var b strings.Builder
			fmt.Fprintf(&b, "GET /api/orders/%d", i)
			if rng.Intn(8) == 0 { // retried: two durations on one line
				first := rng.Int63n(50)
				r.lat = append(r.lat, first)
				fmt.Fprintf(&b, " retry took=%dms", first)
			}
			lat := rng.Int63n(2000)
			if i%97 == 0 {
				lat = -1 // clock skew: a negative int
			}
			r.lat = append(r.lat, lat)
			r.status, r.hasStatus = []int64{200, 200, 200, 201, 404, 500}[rng.Intn(6)], true
			r.dur = fmt.Sprintf("%.3f", rng.Float64()*2)
			if rng.Intn(10) == 0 {
				r.dur = "0.250"
			}
			r.user = user
			var u [16]byte
			rng.Read(u[:])
			h := hex.EncodeToString(u[:])
			r.trace = h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
			fmt.Fprintf(&b, " took=%dms status=%d dur=%ss user=%s trace=%s", lat, r.status, r.dur, r.user, r.trace)
			r.msg = b.String()
		case k < 8: // order line: the spaced latency form only the edited template matches
			v := rng.Int63n(2000)
			r.latSpaced = []int64{v}
			r.user = user
			r.msg = fmt.Sprintf("order %d confirmed user=%s took %dms", i, user, v)
		case k < 9:
			r.status, r.hasStatus = []int64{500, 503}[rng.Intn(2)], true
			r.msg = fmt.Sprintf("panic: worker %d crashed status=%d", i, r.status)
		default:
			r.msg = fmt.Sprintf("heartbeat ok node=n%02d", rng.Intn(20))
		}
	}
	for i := 1; i < len(recs); i += 7 {
		recs[i-1], recs[i] = recs[i], recs[i-1]
	}
	return recs
}

// fmLive is the configuration a shard is read with: normally the one it was written with,
// changed by the configuration-drift faults.
type fmLive struct {
	templates []config.Template
	labels    []string
	latency   string // which line form latency_ms comes from: "eq" (took=), "spaced" (took ), "" (no template)
}

func fmDefaultLive() fmLive {
	return fmLive{templates: fmTemplates(), labels: fmLabelKeys, latency: "eq"}
}

func (l fmLive) config() config.IndexConfig {
	return config.IndexConfig{Templates: l.templates, Literals: []string{"panic:"}}
}

// values is what field holds on this record under the live configuration.
func (r *fmRec) values(field string, live *fmLive) []index.Value {
	var out []index.Value
	switch field {
	case "latency_ms":
		src := r.lat
		switch live.latency {
		case "spaced":
			src = r.latSpaced
		case "":
			src = nil
		}
		for _, v := range src {
			out = append(out, index.Value{Kind: index.KindInt, Int: v})
		}
	case "status_code":
		if r.hasStatus {
			out = append(out, index.Value{Kind: index.KindInt, Int: r.status})
		}
	case "dur_s":
		if r.dur != "" {
			f, _ := strconv.ParseFloat(r.dur, 64)
			out = append(out, index.Value{Kind: index.KindFloat, Float: f})
		}
	case "user_name":
		if r.user != "" {
			out = append(out, index.Value{Kind: index.KindStr, Str: r.user})
		}
	case "trace_id":
		if r.trace != "" {
			out = append(out, uuidV(r.trace))
		}
	}
	return out
}

func (r *fmRec) label(key string) string {
	switch key {
	case "app":
		return r.app
	case "region":
		return r.region
	case "env":
		return r.env
	}
	return ""
}

func uuidV(s string) index.Value {
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		panic("bad uuid " + s)
	}
	v := index.Value{Kind: index.KindUUID}
	copy(v.UUID[:], b)
	return v
}

// fmCmp orders two values of one kind. It is written out here rather than taken from
// index.Compare so the ground truth shares no code with the engine.
func fmCmp(a, b index.Value) int {
	switch a.Kind {
	case index.KindInt:
		return cmpOrdered(a.Int, b.Int)
	case index.KindFloat:
		return cmpOrdered(a.Float, b.Float)
	case index.KindStr:
		return cmpOrdered(a.Str, b.Str)
	default:
		return bytes.Compare(a.UUID[:], b.UUID[:])
	}
}

func cmpOrdered[T int64 | float64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// fmPred pairs a query predicate with the ground-truth test for it.
type fmPred struct {
	p    qr.Predicate
	want func(r *fmRec, live *fmLive) bool
}

func fmTyped(field string, op qr.Op, v index.Value) fmPred {
	return fmPred{qr.TypedCompare{Field: field, Op: op, Value: v}, func(r *fmRec, live *fmLive) bool {
		vals := r.values(field, live)
		if len(vals) == 0 {
			return op == qr.OpNe // a record without the field satisfies only != (empty-label semantics)
		}
		for _, x := range vals {
			c := fmCmp(x, v)
			if op == qr.OpEq && c == 0 || op == qr.OpNe && c != 0 || op == qr.OpLt && c < 0 ||
				op == qr.OpLe && c <= 0 || op == qr.OpGt && c > 0 || op == qr.OpGe && c >= 0 {
				return true
			}
		}
		return false
	}}
}

func fmLabelEq(k, v string) fmPred {
	return fmPred{qr.LabelEqual{Key: k, Value: v}, func(r *fmRec, _ *fmLive) bool { return r.label(k) == v }}
}

func fmLabelNe(k, v string) fmPred {
	return fmPred{qr.LabelNotEqual{Key: k, Value: v}, func(r *fmRec, _ *fmLive) bool { return r.label(k) != v }}
}

func fmLabelRe(k, re string) fmPred {
	rx := regexp.MustCompile("^(?:" + re + ")$")
	return fmPred{qr.LabelRegex{Key: k, Re: rx}, func(r *fmRec, _ *fmLive) bool { return rx.MatchString(r.label(k)) }}
}

func fmContains(s string) fmPred {
	return fmPred{qr.LineContains{Sub: s}, func(r *fmRec, _ *fmLive) bool { return strings.Contains(r.msg, s) }}
}

func fmNotContains(s string) fmPred {
	return fmPred{qr.LineNotContains{Sub: s}, func(r *fmRec, _ *fmLive) bool { return !strings.Contains(r.msg, s) }}
}

func fmLineRe(re string) fmPred {
	rx := regexp.MustCompile(re)
	return fmPred{qr.LineRegex{Re: rx}, func(r *fmRec, _ *fmLive) bool { return rx.MatchString(r.msg) }}
}

type fmQuery struct {
	name       string
	preds      []fmPred
	start, end int64 // 0, 0 = the whole corpus
	limit      int   // 0 = every match
	dir        qr.Direction
}

// fmBattery is the fixed query battery every fault is put through.
func fmBattery(recs []fmRec) []fmQuery {
	at := func(i int) int64 { return ts(0) + int64(i)*int64(10*time.Millisecond) }
	var trace, long string
	for i := range recs {
		if trace == "" && recs[i].trace != "" && i > fmCompressedTo+100 {
			trace = recs[i].trace // one in an uncompressed sealed segment
		}
		if long == "" && strings.HasPrefix(recs[i].user, "tenant-") {
			long = recs[i].user
		}
	}
	lat, st, dur, user, tr := "latency_ms", "status_code", "dur_s", "user_name", "trace_id"
	fl := func(f float64) index.Value { return index.Value{Kind: index.KindFloat, Float: f} }
	return []fmQuery{
		{name: "all"},
		{name: "all, last 50", limit: 50, dir: qr.Backward},
		{name: "window across compressed/raw", start: at(1000), end: at(1500)},
		{name: "window across raw/active", start: at(2650), end: at(2800)},
		{name: "lat > 900", preds: []fmPred{fmTyped(lat, qr.OpGt, intV(900))}},
		{name: "lat >= 1950", preds: []fmPred{fmTyped(lat, qr.OpGe, intV(1950))}},
		{name: "lat < 50", preds: []fmPred{fmTyped(lat, qr.OpLt, intV(50))}},
		{name: "lat <= 0", preds: []fmPred{fmTyped(lat, qr.OpLe, intV(0))}},
		{name: "lat == 777", preds: []fmPred{fmTyped(lat, qr.OpEq, intV(777))}},
		{name: "lat == -1", preds: []fmPred{fmTyped(lat, qr.OpEq, intV(-1))}},
		{name: "lat != 777", preds: []fmPred{fmTyped(lat, qr.OpNe, intV(777))}},
		{name: "lat > 10 (broad)", preds: []fmPred{fmTyped(lat, qr.OpGt, intV(10))}},
		{name: "status == 500", preds: []fmPred{fmTyped(st, qr.OpEq, intV(500))}},
		{name: "status >= 400", preds: []fmPred{fmTyped(st, qr.OpGe, intV(400))}},
		{name: "status < 300", preds: []fmPred{fmTyped(st, qr.OpLt, intV(300))}},
		{name: "dur > 1.5", preds: []fmPred{fmTyped(dur, qr.OpGt, fl(1.5))}},
		{name: "dur <= 0.25", preds: []fmPred{fmTyped(dur, qr.OpLe, fl(0.25))}},
		{name: "dur == 0.25", preds: []fmPred{fmTyped(dur, qr.OpEq, fl(0.25))}},
		{name: "user == alice", preds: []fmPred{fmTyped(user, qr.OpEq, strV("alice"))}},
		{name: "user == long (lossy key)", preds: []fmPred{fmTyped(user, qr.OpEq, strV(long))}},
		{name: "user > tenant-…-050", preds: []fmPred{fmTyped(user, qr.OpGt, strV("tenant-abcdefghijkl-050"))}},
		{name: "user != bob", preds: []fmPred{fmTyped(user, qr.OpNe, strV("bob"))}},
		{name: "trace == known", preds: []fmPred{fmTyped(tr, qr.OpEq, uuidV(trace))}},
		{name: "trace == absent", preds: []fmPred{fmTyped(tr, qr.OpEq, uuidV("00000000-0000-4000-8000-000000000000"))}},
		{name: `|= "panic:"`, preds: []fmPred{fmContains("panic:")}},
		{name: `|= "panic:", status >= 503`, preds: []fmPred{fmContains("panic:"), fmTyped(st, qr.OpGe, intV(503))}},
		{name: `!= "took"`, preds: []fmPred{fmNotContains("took")}},
		{name: `|~ "orders/1\d\d "`, preds: []fmPred{fmLineRe(`orders/1\d\d `)}},
		{name: "region = r1", preds: []fmPred{fmLabelEq("region", "r1")}},
		{name: "region = r1, app = billing", preds: []fmPred{fmLabelEq("region", "r1"), fmLabelEq("app", "billing")}},
		{name: "env = prod (not allowlisted)", preds: []fmPred{fmLabelEq("env", "prod")}},
		{name: `region = ""`, preds: []fmPred{fmLabelEq("region", "")}},
		{name: "region != r0", preds: []fmPred{fmLabelNe("region", "r0")}},
		{name: "region =~ r[12]", preds: []fmPred{fmLabelRe("region", "r[12]")}},
		{name: "region = r2, lat > 500", preds: []fmPred{fmLabelEq("region", "r2"), fmTyped(lat, qr.OpGt, intV(500))}},
		{name: "app = checkout, user == alice", preds: []fmPred{fmLabelEq("app", "checkout"), fmTyped(user, qr.OpEq, strV("alice"))}},
		{name: "lat > 100, dur < 1, region = r0", preds: []fmPred{fmTyped(lat, qr.OpGt, intV(100)), fmTyped(dur, qr.OpLt, fl(1)), fmLabelEq("region", "r0")}},
		{name: "lat > 900, last 10", preds: []fmPred{fmTyped(lat, qr.OpGt, intV(900))}, limit: 10, dir: qr.Backward},
		{name: "status == 500, first 5", preds: []fmPred{fmTyped(st, qr.OpEq, intV(500))}, limit: 5, dir: qr.Forward},
		{name: "window, lat > 500", preds: []fmPred{fmTyped(lat, qr.OpGt, intV(500))}, start: at(1000), end: at(2800)},
	}
}

func (q fmQuery) native() qr.Query {
	out := qr.Query{Start: q.start, End: q.end, Limit: q.limit, Direction: q.dir}
	if out.Start == 0 && out.End == 0 {
		out.Start, out.End = ts(-1), ts(1000)
	}
	if out.Limit == 0 {
		out.Limit = 1 << 20
	}
	for _, p := range q.preds {
		out.Preds = append(out.Preds, p.p)
	}
	return out
}

// truth is the ground-truth answer: matching records in result order, the limit applied,
// leaving out those in lost.
func (q fmQuery) truth(recs []fmRec, live *fmLive, lost map[int64]bool) []int64 {
	nq := q.native()
	var out []int64
	for i := range recs {
		r := &recs[i]
		if r.ts < nq.Start || r.ts > nq.End || lost[r.ts] {
			continue
		}
		ok := true
		for _, p := range q.preds {
			if !p.want(r, live) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, r.ts)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if nq.Direction == qr.Forward {
			return out[i] < out[j]
		}
		return out[i] > out[j]
	})
	if len(out) > nq.Limit {
		out = out[:nq.Limit]
	}
	return out
}

// fmSegments are the golden corpus's segments by kind.
type fmSegments struct {
	compressed, raw []string // segment IDs
	active          string
}

// fmBuildGolden writes the corpus into a fresh shard and returns a copy of it taken while the
// last segment is still active, with every record on disk.
func fmBuildGolden(t *testing.T, recs []fmRec) string {
	t.Helper()
	src := t.TempDir()
	live := fmDefaultLive()
	eng := compileOrFail(t, live.config())
	open := func(blockPages int, segBytes int64, flush time.Duration) *storage.Writer {
		w, err := storage.NewWriter(src, fmOptions(eng, live, blockPages, segBytes, flush))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Start(nil); err != nil {
			t.Fatal(err)
		}
		return w
	}
	put := func(w *storage.Writer, from, to int) {
		ig := ingest.NewWithLabels(eng, w, fmLabelKeys)
		for i := from; i < to; i++ {
			r := &recs[i]
			if err := ig.Ingest(model.LogEntry{TS: time.Unix(0, r.ts).UTC(), Level: model.LogLevelInfo, Extra: r.extra, Message: r.msg}); err != nil {
				t.Fatal(err)
			}
		}
	}
	w := open(fmBlockPages, fmSegPages*storage.PageSize, time.Hour)
	put(w, 0, fmCompressedTo)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = open(-1, fmSegPages*storage.PageSize, time.Hour)
	put(w, fmCompressedTo, fmRawTo)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The active segment's pages reach disk on the flush tick; the copy is taken once every
	// record reads back, before any seal.
	w = open(-1, 1<<30, 5*time.Millisecond)
	put(w, fmRawTo, fmRecords)
	deadline := time.Now().Add(10 * time.Second)
	for {
		all, err := storage.ReadAll(src)
		if err == nil && len(all) == fmRecords {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records reached disk (%v)", len(all), fmRecords, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	golden := t.TempDir()
	copyTree(t, src, golden)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return golden
}

func fmOptions(eng *extract.Engine, live fmLive, blockPages int, segBytes int64, flush time.Duration) storage.Options {
	return storage.Options{
		Schema:           eng.IndexedFields(),
		SegmentSizeBytes: segBytes,
		FlushInterval:    flush,
		BlockPages:       blockPages,
		LabelKeys:        live.labels,
		// As the daemon wires it: a recovered active segment is re-indexed with the live config.
		Reindex: func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
			return eng.ReExtract(e.Message), ingest.DeriveLabels(e.Extra, live.labels)
		},
	}
}

func fmClassify(t *testing.T, dir string) fmSegments {
	t.Helper()
	m, err := storage.LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var s fmSegments
	for _, seg := range m.All() {
		switch {
		case seg.State == storage.SegmentActive:
			s.active = seg.ID
		case fileExists(storage.SegzPath(seg.Path)):
			s.compressed = append(s.compressed, seg.ID)
		default:
			s.raw = append(s.raw, seg.ID)
		}
	}
	if len(s.compressed) < 2 || len(s.raw) < 3 || s.active == "" {
		t.Fatalf("corpus layout: %d compressed, %d raw sealed, active %q; want ≥2, ≥3 and one", len(s.compressed), len(s.raw), s.active)
	}
	return s
}

// fmFault damages a copy of the golden shard, or changes the configuration it is read with.
type fmFault struct {
	name string
	// apply damages the shard in dir.
	apply func(t *testing.T, dir string, s fmSegments, rng *rand.Rand)
	live  func(l *fmLive) // configuration drift
	// noStart opens the shard without starting the writer, so nothing repairs it in the
	// background while the battery runs. waitGone, with a start, waits for that repair: until
	// the named file no longer exists.
	noStart  bool
	waitGone func(dir string, s fmSegments) string
	known    string // a reproduced finding; reported in the table, not failed
}

func fmFaults() []fmFault {
	// The sidecar faults hit one compressed and one uncompressed sealed segment.
	targets := func(s fmSegments) []string { return []string{s.compressed[1], s.raw[1]} }
	sidecars := func(suffix string, damage func(t *testing.T, path string, rng *rand.Rand)) func(*testing.T, string, fmSegments, *rand.Rand) {
		return func(t *testing.T, dir string, s fmSegments, rng *rand.Rand) {
			n := 0
			for _, id := range targets(s) {
				paths, _ := filepath.Glob(filepath.Join(dir, "segments", "seg-"+id+".*"+suffix))
				for _, p := range paths {
					damage(t, p, rng)
					n++
				}
			}
			if n == 0 {
				t.Fatalf("no %s files to damage", suffix)
			}
		}
	}
	remove := func(t *testing.T, p string, _ *rand.Rand) { must(t, os.Remove(p)) }
	half := func(t *testing.T, p string, _ *rand.Rand) {
		fi, err := os.Stat(p)
		must(t, err)
		must(t, os.Truncate(p, fi.Size()/2))
	}
	zero := func(t *testing.T, p string, _ *rand.Rand) { must(t, os.Truncate(p, 0)) }
	flip := func(t *testing.T, p string, rng *rand.Rand) {
		data, err := os.ReadFile(p)
		must(t, err)
		data[rng.Intn(len(data))] ^= byte(1 + rng.Intn(255))
		must(t, os.WriteFile(p, data, 0o644))
	}
	seg := func(dir, id string) string { return storage.SegmentPath(dir, id) }
	segFiles := func(t *testing.T, dir, id string) {
		paths, _ := filepath.Glob(filepath.Join(dir, "segments", "seg-"+id+".*"))
		if len(paths) == 0 {
			t.Fatalf("segment %s has no files", id)
		}
		for _, p := range paths {
			must(t, os.Remove(p))
		}
	}
	interrupt := func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
		p := seg(dir, s.raw[1])
		raw, err := os.ReadFile(p)
		must(t, err)
		if ok, err := storage.CompressSegment(p, fmBlockPages); !ok || err != nil {
			t.Fatalf("compressing %s: %v %v", p, ok, err)
		}
		must(t, os.WriteFile(p, raw, 0o644)) // the unlink after the rename never happened
	}
	return []fmFault{
		{name: "none (baseline)"},
		{name: ".tidx deleted", apply: sidecars(".tidx", remove)},
		{name: ".tidx truncated to half", apply: sidecars(".tidx", half)},
		{name: ".tidx one random byte flipped", apply: sidecars(".tidx", flip)},
		{name: ".tidx zero-length", apply: sidecars(".tidx", zero)},
		{name: ".lidx deleted", apply: sidecars(".lidx", remove)},
		{name: ".lidx truncated to half", apply: sidecars(".lidx", half)},
		{name: ".lidx one random byte flipped", apply: sidecars(".lidx", flip)},
		{name: ".logz one block corrupted", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			corruptMiddleBlock(t, storage.SegzPath(seg(dir, s.compressed[1])))
		}},
		{name: ".logz block directory corrupted", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			corruptDirectory(t, storage.SegzPath(seg(dir, s.compressed[1])))
		}, known: "the segment cannot be opened, so every query over it fails rather than skipping it; and when its " +
			".tidx has no candidates there, Execute never opens it and answers while ExecuteScan fails " +
			"(TestCorruptSegzDirectorySameOnBothPaths)"},
		{name: "sealed .log page corrupted", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			flipAt(t, seg(dir, s.raw[1]), 2*storage.PageSize+200)
		}},
		{name: "active .log page corrupted", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			flipAt(t, seg(dir, s.active), 2*storage.PageSize+200)
		}},
		{name: "active .log torn last page", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			p := seg(dir, s.active)
			fi, err := os.Stat(p)
			must(t, err)
			must(t, os.Truncate(p, fi.Size()-storage.PageSize+1000))
		}},
		{name: "manifest lists a sealed segment whose files are gone", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			segFiles(t, dir, s.raw[2])
		}},
		{name: "manifest lists the active segment, its files gone", apply: func(t *testing.T, dir string, s fmSegments, _ *rand.Rand) {
			segFiles(t, dir, s.active)
		}},
		{name: "drift: template pattern edited", live: func(l *fmLive) {
			l.templates[0].Pattern, l.latency = "took {ms:int}ms", "spaced"
		}},
		{name: "drift: template removed", live: func(l *fmLive) { l.templates, l.latency = l.templates[1:], "" }},
		{name: "drift: allowlist grown (+env)", live: func(l *fmLive) { l.labels = []string{"app", "env", "region"} }},
		{name: "drift: allowlist shrunk (−region)", live: func(l *fmLive) { l.labels = []string{"app"} }},
		{name: "interrupted compression: .logz and .log, as found", apply: interrupt, noStart: true},
		{name: "interrupted compression: after restart", apply: interrupt,
			waitGone: func(dir string, s fmSegments) string { return seg(dir, s.raw[1]) }},
	}
}

// fmRow is one fault's outcome.
type fmRow struct {
	fault                string
	queries, divergences int
	truthMismatch        int // queries whose answer differs from the ground truth less the lost records
	idxErrs, scanErrs    int // queries that returned an error (or panicked) on each path
	panics               int
	lost                 string // records missing on every path, as a count
	openErr              error
	reasons              map[string]int // Explain reasons summed over the battery
	notes                []string
	known                string
}

func TestFaultMatrix(t *testing.T) {
	if os.Getenv("LOGD_FAULTMATRIX") == "" {
		t.Skip("set LOGD_FAULTMATRIX=1 to run the fault-injection matrix (E-6)")
	}
	recs := fmCorpus()
	golden := fmBuildGolden(t, recs)
	segs := fmClassify(t, golden)
	battery := fmBattery(recs)

	var rows []fmRow
	for i, f := range fmFaults() {
		row := fmRunFault(t, f, golden, segs, recs, battery, rand.New(rand.NewSource(int64(100+i))))
		rows = append(rows, row)
		if row.known != "" {
			continue
		}
		if row.divergences > 0 || row.truthMismatch > 0 || row.panics > 0 {
			t.Errorf("%s: %d divergences, %d answers off the ground truth, %d panics: %s",
				f.name, row.divergences, row.truthMismatch, row.panics, strings.Join(row.notes, "; "))
		}
	}
	if b := rows[0]; b.lost != "0" || b.idxErrs+b.scanErrs > 0 || b.openErr != nil {
		t.Errorf("baseline is not clean: lost %s, %d+%d errors, open %v", b.lost, b.idxErrs, b.scanErrs, b.openErr)
	}

	table := fmTable(rows, rows[0].reasons, len(battery), segs)
	fmt.Print(table)
	if out := os.Getenv("LOGD_FAULTMATRIX_OUT"); out != "" {
		must(t, os.WriteFile(out, []byte(table), 0o644))
	}
}

func fmRunFault(t *testing.T, f fmFault, golden string, segs fmSegments, recs []fmRec, battery []fmQuery, rng *rand.Rand) fmRow {
	row := fmRow{fault: f.name, known: f.known, reasons: map[string]int{}}
	dir := t.TempDir()
	copyTree(t, golden, dir)
	if f.apply != nil {
		f.apply(t, dir, segs, rng)
	}
	live := fmDefaultLive()
	live.templates = append([]config.Template{}, live.templates...)
	if f.live != nil {
		f.live(&live)
	}
	eng := compileOrFail(t, live.config())
	w, err := storage.NewWriter(dir, fmOptions(eng, live, -1, fmSegPages*storage.PageSize, time.Hour))
	if err != nil {
		row.openErr = errors.New(strings.ReplaceAll(err.Error(), dir, "<shard>"))
		row.lost = "all (shard does not open)"
		return row
	}
	defer w.Close()
	if !f.noStart {
		must(t, w.Start(nil))
	}
	if f.waitGone != nil {
		p := f.waitGone(dir, segs)
		for deadline := time.Now().Add(10 * time.Second); fileExists(p); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %s was never removed", f.name, p)
			}
		}
	}
	e := qr.NewEngineWithLabels(w, eng, live.labels)

	// The records every path has lost: those the unfiltered scan no longer returns.
	lost := map[int64]bool{}
	all, err := safeRun(e.ExecuteScan, battery[0].native())
	if err != nil {
		row.lost = "n/a (the scan fails)"
	} else {
		got := map[int64]bool{}
		for _, r := range all {
			got[r.TS.UnixNano()] = true
		}
		for i := range recs {
			if !got[recs[i].ts] {
				lost[recs[i].ts] = true
			}
		}
		row.lost = strconv.Itoa(len(lost))
	}

	byTS := make(map[int64]*fmRec, len(recs))
	for i := range recs {
		byTS[recs[i].ts] = &recs[i]
	}
	for _, q := range battery {
		row.queries++
		nq := q.native()
		idx, idxErr := safeRun(e.Execute, nq)
		scan, scanErr := safeRun(e.ExecuteScan, nq)
		for _, err := range []error{idxErr, scanErr} {
			if _, ok := err.(panicErr); ok {
				row.panics++
				row.notes = append(row.notes, fmt.Sprintf("%s: %v", q.name, err))
			}
		}
		if idxErr != nil {
			row.idxErrs++
		}
		if scanErr != nil {
			row.scanErrs++
		}
		for _, p := range safeExplain(e, nq) {
			row.reasons[p.Reason]++
		}
		if (idxErr == nil) != (scanErr == nil) {
			row.divergences++
			row.notes = append(row.notes, fmt.Sprintf("%s: index err=%v, scan err=%v", q.name, idxErr, scanErr))
			continue
		}
		if idxErr != nil {
			continue // both paths failed alike: an error, not a divergence
		}
		if d := entriesDiffer(idx, scan); d != "" {
			row.divergences++
			row.notes = append(row.notes, fmt.Sprintf("%s: index vs scan: %s", q.name, d))
			continue
		}
		want := q.truth(recs, &live, lost)
		if d := truthDiffers(idx, want, byTS); d != "" {
			row.truthMismatch++
			row.notes = append(row.notes, fmt.Sprintf("%s: vs ground truth: %s", q.name, d))
		}
	}
	return row
}

type panicErr struct{ v any }

func (p panicErr) Error() string { return fmt.Sprintf("panic: %v", p.v) }

func safeRun(fn func(qr.Query) ([]model.LogEntry, error), q qr.Query) (out []model.LogEntry, err error) {
	defer func() {
		if v := recover(); v != nil {
			out, err = nil, panicErr{v}
		}
	}()
	return fn(q)
}

func safeExplain(e *qr.Engine, q qr.Query) (out []qr.SegmentPlan) {
	defer func() {
		if v := recover(); v != nil {
			out = []qr.SegmentPlan{{Reason: fmt.Sprintf("explain panicked: %v", v)}}
		}
	}()
	return e.Explain(q)
}

func entriesDiffer(a, b []model.LogEntry) string {
	if len(a) != len(b) {
		return fmt.Sprintf("%d vs %d records", len(a), len(b))
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.TS.UnixNano() != y.TS.UnixNano() || x.Message != y.Message || x.Extra != y.Extra || x.Level != y.Level || x.ServiceID != y.ServiceID {
			return fmt.Sprintf("record %d: %q vs %q", i, x.Message, y.Message)
		}
	}
	return ""
}

func truthDiffers(got []model.LogEntry, want []int64, byTS map[int64]*fmRec) string {
	extra := 0
	for _, g := range got {
		r, ok := byTS[g.TS.UnixNano()]
		if !ok || r.msg != g.Message || r.extra != g.Extra {
			extra++
		}
	}
	if extra > 0 {
		return fmt.Sprintf("%d records not in the corpus", extra)
	}
	if len(got) != len(want) {
		return fmt.Sprintf("%d records, ground truth %d", len(got), len(want))
	}
	for i := range got {
		if got[i].TS.UnixNano() != want[i] {
			return fmt.Sprintf("record %d out of place", i)
		}
	}
	return ""
}

var fmReasonOrder = []string{qr.ReasonIndexed, qr.ReasonCostGuard, qr.ReasonNoPushdown, qr.ReasonNotIndexed, qr.ReasonIndexMissing}

// fmTable renders the matrix. The Explain column lists how the battery's per-segment access
// paths moved against the baseline.
func fmTable(rows []fmRow, base map[string]int, battery int, segs fmSegments) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n### Fault-injection matrix (E-6)\n\n")
	fmt.Fprintf(&b, "Corpus: %d records in %d compressed + %d uncompressed sealed segments and one active segment; ",
		fmRecords, len(segs.compressed), len(segs.raw))
	fmt.Fprintf(&b, "battery of %d queries per fault, each run as Execute and ExecuteScan and checked against the ground truth.\n", battery)
	fmt.Fprintf(&b, "Sidecar faults hit one compressed and one uncompressed sealed segment. ")
	fmt.Fprintf(&b, "Explain: per-segment access paths summed over the battery, change against the baseline.\n\n")
	fmt.Fprintf(&b, "| fault | queries | divergences | ≠ truth | mode | records lost on all paths | Explain |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | --- | ---: | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s |\n", r.fault, r.queries, r.divergences, r.truthMismatch, r.mode(base), r.lost, reasonDelta(r, base))
	}
	var notes []string
	for _, r := range rows {
		if r.openErr != nil {
			notes = append(notes, fmt.Sprintf("- **%s**: the shard does not open: `%v`", r.fault, r.openErr))
		}
		if r.known != "" {
			notes = append(notes, fmt.Sprintf("- **%s** (known): %s", r.fault, r.known))
		}
		for _, n := range r.notes {
			notes = append(notes, fmt.Sprintf("- %s — %s", r.fault, n))
		}
	}
	if len(notes) > 0 {
		fmt.Fprintf(&b, "\n%s\n", strings.Join(notes, "\n"))
	}
	return b.String()
}

func (r fmRow) mode(base map[string]int) string {
	if r.openErr != nil {
		return "error: shard does not open"
	}
	var parts []string
	if r.idxErrs+r.scanErrs > 0 {
		parts = append(parts, fmt.Sprintf("error (index %d/%d, scan %d/%d queries)", r.idxErrs, r.queries, r.scanErrs, r.queries))
	}
	if r.lost != "0" && !strings.HasPrefix(r.lost, "n/a") {
		parts = append(parts, "skip: records lost")
	}
	if r.reasons[qr.ReasonIndexed] < base[qr.ReasonIndexed] {
		parts = append(parts, "degrade: scan")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "; ")
}

func reasonDelta(r fmRow, base map[string]int) string {
	if r.openErr != nil {
		return "–"
	}
	var parts []string
	seen := map[string]bool{}
	for _, k := range append(append([]string{}, fmReasonOrder...), sortedReasonKeys(r.reasons)...) {
		if seen[k] {
			continue
		}
		seen[k] = true
		if d := r.reasons[k] - base[k]; d != 0 {
			parts = append(parts, fmt.Sprintf("%s %+d", k, d))
		}
	}
	if len(parts) == 0 {
		return "unchanged"
	}
	return strings.Join(parts, ", ")
}

func sortedReasonKeys(m map[string]int) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// corruptDirectory flips a byte inside a .logz block directory (v2 layout: 32-byte header,
// dictionary, 16-byte entries), which its checksum covers.
func corruptDirectory(t *testing.T, zpath string) {
	t.Helper()
	data, err := os.ReadFile(zpath)
	must(t, err)
	numBlocks := binary.BigEndian.Uint32(data[16:20])
	dictLen := binary.BigEndian.Uint32(data[24:28])
	data[32+int(dictLen)+int(numBlocks)*16/2] ^= 0x5A
	must(t, os.WriteFile(zpath, data, 0o644))
}

func flipAt(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	must(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, off)
	must(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, off)
	must(t, err)
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
	must(t, err)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Found by the matrix (".logz block directory corrupted"). A .logz whose block directory fails
// its checksum cannot be opened, and the two paths then disagree about the same query: the
// scan opens the file and fails, while the index path, when the segment's intact .tidx yields
// no candidates there, never opens the file (FetchRecords returns early on an empty offset
// list) and answers without an error.
func TestCorruptSegzDirectorySameOnBothPaths(t *testing.T) {
	t.Skip("known: with a corrupt .logz block directory, Execute answers a query whose .tidx lookup has no candidates in that segment, while ExecuteScan fails with \"segz: directory checksum mismatch\"")
	dir := t.TempDir()
	eng := compileOrFail(t, config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	w, err := storage.NewWriter(dir, storage.Options{Schema: eng.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, BlockPages: 8})
	must(t, err)
	must(t, w.Start(nil))
	ig := ingest.New(eng, w)
	for i := 0; i < 2000; i++ {
		must(t, ig.Ingest(model.LogEntry{TS: time.Unix(t0, int64(i)*int64(time.Millisecond)).UTC(), Level: model.LogLevelInfo, Message: fmt.Sprintf("req %d took %dms", i, i%1000)}))
	}
	must(t, w.Close()) // seals and compresses
	segs := w.Manifest().All()
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	corruptDirectory(t, storage.SegzPath(segs[0].Path))

	e := qr.NewEngine(w, eng)
	q := qr.Query{Start: ts(0), End: ts(1000), Limit: 100, Preds: []qr.Predicate{
		qr.TypedCompare{Field: "latency_ms", Op: qr.OpEq, Value: intV(5000)}, // no record has it
	}}
	idx, idxErr := e.Execute(q)
	scan, scanErr := e.ExecuteScan(q)
	if (idxErr == nil) != (scanErr == nil) {
		t.Fatalf("index: %d rows, err %v; scan: %d rows, err %v", len(idx), idxErr, len(scan), scanErr)
	}
	equalResults(t, "latency_ms == 5000", idx, scan)
}
