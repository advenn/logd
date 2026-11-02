package storage

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// E-8, machine-crash approximation.
//
// A kill -9 (testbed/scripts/e8-crashloss.sh) loses only what the writer still holds in RAM:
// the kernel keeps every page it was handed and writes it back later. A power cut also loses
// the pages written since the last fsync. Measuring that needs a VM that can be reset, so this
// approximates it instead. It drives the writer's own steps (processEntry, flushTick) from the
// test goroutine at a fixed rate in real time, under the real group-commit policy, and notes
// how many pages each fsync made durable. At a random moment it cuts the power: a copy of the
// shard has its active segment truncated to the last fsynced page, is recovered as a restart
// would recover it, and the records that come back are counted.
//
// This is the worst case the policy allows: it assumes the kernel wrote back nothing unsynced
// on its own (dirty pages expire after 30 s by default, far longer than these windows) and
// that every page since the last fsync is lost whole. Records go straight to processEntry, so
// there is no queue in front of the writer, and the process-crash figure here is the open
// page alone; the script measures the real one.
//
//	LOGD_MACHINECRASH=1 go test ./core/storage/ -run TestMachineCrashLossApproximation -v -count=1
//
// LOGD_MACHINECRASH_RUNS sets the runs per setting (default 10). LOGD_BENCH_DIR puts the
// shards on a real disk, where an fsync takes as long as it really does.
func TestMachineCrashLossApproximation(t *testing.T) {
	if os.Getenv("LOGD_MACHINECRASH") == "" {
		t.Skip("set LOGD_MACHINECRASH=1 to run the machine-crash loss approximation (E-8)")
	}
	runs := 10
	if v, err := strconv.Atoi(os.Getenv("LOGD_MACHINECRASH_RUNS")); err == nil && v > 0 {
		runs = v
	}
	settings := []struct{ sync, flush time.Duration }{
		{50 * time.Millisecond, 500 * time.Millisecond},
		{10 * time.Millisecond, 50 * time.Millisecond},
	}
	rng := rand.New(rand.NewSource(8))
	var b strings.Builder
	fmt.Fprintf(&b, "\n### E-8 machine-crash approximation (power cut at the last fsync)\n\n")
	fmt.Fprintf(&b, "median (max) over %d runs; the cut lands uniformly in [1 s, 2 s) after the first record\n\n", runs)
	fmt.Fprintf(&b, "| rate (lines/s) | sync_interval_ms | flush_interval_ms | lost: open page only (process crash) | lost: + unsynced pages (machine crash) | machine-crash loss, ms of data | since last fsync, ms |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, rate := range []int{10000, 200} {
		for _, s := range settings {
			var proc, mach, unsynced []float64
			for i := 0; i < runs; i++ {
				cut := time.Second + time.Duration(rng.Int63n(int64(time.Second)))
				r := machineCrashRun(t, s.sync, s.flush, rate, cut)
				proc = append(proc, float64(r.processLoss))
				mach = append(mach, float64(r.machineLoss))
				unsynced = append(unsynced, r.sinceSync.Seconds()*1e3)
			}
			msOf := func(v []float64) []float64 {
				out := make([]float64, len(v))
				for i := range v {
					out[i] = v[i] * 1e3 / float64(rate)
				}
				return out
			}
			fmt.Fprintf(&b, "| %d | %d | %d | %s | %s | %s | %s |\n", rate, s.sync.Milliseconds(), s.flush.Milliseconds(),
				medMax(proc, "%.0f"), medMax(mach, "%.0f"), medMax(msOf(mach), "%.1f"), medMax(unsynced, "%.1f"))
		}
	}
	fmt.Print(b.String())
}

type machineCrash struct {
	acked, processLoss, machineLoss int
	sinceSync                       time.Duration // from the last fsync to the cut
}

func machineCrashRun(t *testing.T, syncEvery, flushEvery time.Duration, rate int, cut time.Duration) machineCrash {
	t.Helper()
	dir := machineCrashDir(t)
	w := newSyncWriter(t, dir, Options{SegmentSizeBytes: 64 << 20, FlushInterval: flushEvery, SyncInterval: syncEvery})
	defer w.Close()

	start := time.Now()
	durable := w.currSeg.PageNum // CreateSegment fsynced page 0
	lastSync, syncedAt := w.lastSync, start
	noteSync := func() {
		if !w.lastSync.Equal(lastSync) { // an fsync ran: every page written so far is durable
			lastSync, syncedAt, durable = w.lastSync, w.lastSync, w.currSeg.PageNum
		}
	}
	nextTick := start.Add(flushEvery)
	step := time.Second / time.Duration(rate)
	n := 0
	var now time.Time
	for {
		now = time.Now()
		if now.Sub(start) >= cut {
			break
		}
		if !now.Before(nextTick) {
			w.flushTick(now)
			noteSync()
			nextTick = nextTick.Add(flushEvery)
		}
		for due := int(now.Sub(start) / step); n < due; n++ {
			w.processEntry(record{entry: model.LogEntry{
				TS:      start.Add(time.Duration(n) * step),
				Level:   model.LogLevelInfo,
				Extra:   `{"job":"crashpush","run":"m"}`,
				Message: fmt.Sprintf("crashpush seq=%d", n),
			}})
			noteSync()
		}
		time.Sleep(200 * time.Microsecond)
	}
	res := machineCrash{acked: n, processLoss: int(w.hdr.EntryCount), sinceSync: now.Sub(syncedAt)}

	// The power cut: the disk holds the manifest and every page up to the last fsync.
	img := machineCrashDir(t)
	copyShard(t, dir, img)
	seg := SegmentPath(img, w.currSeg.ID)
	if err := os.Truncate(seg, int64(durable)*PageSize); err != nil {
		t.Fatal(err)
	}
	w2, err := NewWriter(img, Options{SegmentSizeBytes: 64 << 20})
	if err != nil {
		t.Fatalf("recovering the power-cut image: %v", err)
	}
	got, err := ReadAll(img)
	w2.Close()
	if err != nil {
		t.Fatal(err)
	}
	// What survives must be a prefix of what was written: seq 0..len-1, each once.
	seqs := make([]int, 0, len(got))
	for _, e := range got {
		v, err := strconv.Atoi(strings.TrimPrefix(e.Message, "crashpush seq="))
		if err != nil {
			t.Fatalf("unexpected record %q", e.Message)
		}
		seqs = append(seqs, v)
	}
	sort.Ints(seqs)
	for i, v := range seqs {
		if v != i {
			t.Fatalf("recovered records are not a prefix: position %d holds seq %d", i, v)
		}
	}
	res.machineLoss = n - len(seqs)
	if res.machineLoss < res.processLoss {
		t.Fatalf("a power cut lost %d records, fewer than the %d still in RAM", res.machineLoss, res.processLoss)
	}
	return res
}

func machineCrashDir(t *testing.T) string {
	t.Helper()
	root := os.Getenv("LOGD_BENCH_DIR")
	if root == "" {
		return t.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "mcrash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// copyShard copies a shard directory's regular files (manifest, lookup, segments).
func copyShard(t *testing.T, src, dst string) {
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
	if err != nil {
		t.Fatal(err)
	}
}

func medMax(v []float64, f string) string {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	med := s[len(s)/2]
	if len(s)%2 == 0 {
		med = (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	return fmt.Sprintf(f+" ("+f+")", med, s[len(s)-1])
}
