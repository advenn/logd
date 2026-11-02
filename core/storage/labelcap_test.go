package storage

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
)

// labelCapRecord is record i of the cap tests: five regions in turn, two tenants in turn.
func labelCapRecord(i int) (model.LogEntry, label.Set) {
	region, tenant := fmt.Sprintf("r%d", i%5), fmt.Sprintf("t%d", i%2)
	e := model.LogEntry{TS: time.Unix(int64(1700000000+i), 0).UTC(), Level: model.LogLevelInfo,
		Extra:   fmt.Sprintf(`{"region":%q,%q:%q}`, region, model.TenantLabel, tenant),
		Message: fmt.Sprintf("record %04d with some padding to fill the pages faster", i)}
	return e, label.NewSet(map[string]string{"region": region, model.TenantLabel: tenant})
}

// The label-value cap's drops are counted per tenant and key for the writer's lifetime, not
// only logged per segment at seal (design §6.2: "a counter increments"). The expected counts
// are worked out from what each segment holds, applying the cap afresh per segment.
func TestLabelCapDropsCounted(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, Options{SegmentSizeBytes: 3 * PageSize, FlushInterval: time.Hour, MaxLabelCardinality: 2})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(nil)
	if got := w.LabelCapDrops(); len(got) != 0 {
		t.Fatalf("a new writer reports drops: %v", got)
	}
	for i := 0; i < 600; i++ {
		e, labels := labelCapRecord(i)
		if err := w.WriteExtracted(e, nil, labels); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs := w.Manifest().All()
	if len(segs) < 3 {
		t.Fatalf("test setup: want several segments, got %d", len(segs))
	}

	want := map[string]map[string]int64{}
	for _, seg := range segs {
		seen := map[string]bool{} // the region values this segment indexed
		err := ScanSegmentPages(seg.Path, math.MinInt64, math.MaxInt64, false, nil, func(e model.LogEntry) bool {
			m := model.ParseExtraLabels(e.Extra)
			if region := m["region"]; !seen[region] {
				if len(seen) < 2 {
					seen[region] = true
				} else {
					tenant := m[model.TenantLabel]
					if want[tenant] == nil {
						want[tenant] = map[string]int64{}
					}
					want[tenant]["region"]++
				}
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	got := w.LabelCapDrops()
	if fmt.Sprint(got) != fmt.Sprint(want) || len(want) != 2 {
		t.Fatalf("LabelCapDrops = %v, want %v (two tenants, region only)", got, want)
	}
}

// Rebuilding a crash-recovered segment's index re-applies the cap to records whose drops
// were counted when they were written; it does not count them again. Records written after
// recovery are counted.
func TestLabelCapDropsNotRecountedOnRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, MaxLabelCardinality: 2,
		Reindex: func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
			m := model.ParseExtraLabels(e.Extra)
			return nil, label.NewSet(map[string]string{"region": m["region"], model.TenantLabel: m[model.TenantLabel]})
		}}
	w1, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	w1.Start(nil)
	for i := 0; i < 300; i++ {
		e, labels := labelCapRecord(i)
		if err := w1.WriteExtracted(e, nil, labels); err != nil {
			t.Fatal(err)
		}
	}
	w1.stop() // crash: whole pages survive, the partial one does not

	w2, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	w2.Start(nil)
	if got := w2.LabelCapDrops(); len(got) != 0 {
		t.Fatalf("recovery counted the recovered records' drops again: %v", got)
	}
	for i := 300; i < 310; i++ { // r0..r4 for each tenant: r2, r3, r4 over the cap
		e, labels := labelCapRecord(i)
		if err := w2.WriteExtracted(e, nil, labels); err != nil {
			t.Fatal(err)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(w2.LabelCapDrops()), "map[t0:map[region:3] t1:map[region:3]]"; got != want {
		t.Fatalf("LabelCapDrops after recovery = %s, want %s", got, want)
	}
}
