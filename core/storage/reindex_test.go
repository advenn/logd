package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/config"
	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
)

// With a Reindex hook, a crash-recovered segment rebuilds its typed-range + label index by
// re-extracting its records, so it seals FULLY INDEXED (not scan-only) and the rebuilt
// .tidx resolves values to the correct record offsets (design §8).
func TestReindexRecoveredSegment(t *testing.T) {
	dir := t.TempDir()
	ex, _ := extract.Compile(config.IndexConfig{Templates: []config.Template{{Name: "latency", Pattern: "took {ms:int}ms"}}})
	reindex := func(e model.LogEntry) ([]index.KeyedValue, label.Set) {
		keys := ex.Extract(e.Message)
		m := map[string]string{}
		if v, ok := model.ParseExtraLabels(e.Extra)["region"]; ok {
			m["region"] = v
		}
		return keys, label.NewSet(m)
	}
	opts := Options{Schema: ex.IndexedFields(), SegmentSizeBytes: 1 << 40, FlushInterval: time.Hour, Reindex: reindex}

	// Phase 1: ingest, then CRASH (stop without a seal). Only whole flushed pages survive.
	w1, _ := NewWriter(dir, opts)
	w1.Start(nil)
	for i := 0; i < 300; i++ {
		e := model.LogEntry{TS: time.Unix(int64(1700000000+i), 0).UTC(), Level: model.LogLevelInfo,
			Extra: fmt.Sprintf(`{"region":"r%d"}`, i%3), Message: fmt.Sprintf("took %dms", i)}
		keys, labels := reindex(e)
		if err := w1.WriteExtracted(e, keys, labels); err != nil {
			t.Fatal(err)
		}
	}
	w1.stop() // crash: no final flush, no seal

	// Phase 2: recover (rebuilds the index) + Close (seals it with a real .tidx/.lidx).
	w2, err := NewWriter(dir, opts)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	w2.Start(nil)
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	segs := mustManifest(t, dir).All()
	if len(segs) != 1 {
		t.Fatalf("want 1 sealed segment, got %d", len(segs))
	}
	seg := segs[0]
	if !seg.Indexed {
		t.Fatal("a reindexed recovered segment must seal fully-indexed (not scan-only)")
	}
	hasLatency := false
	for _, f := range seg.Schema {
		if f.Name == "latency_ms" {
			hasLatency = true
		}
	}
	if !hasLatency {
		t.Fatalf("recovered segment schema missing latency_ms: %+v", seg.Schema)
	}

	// The rebuilt .tidx resolves a known value to the correct record (latency=100 → i=100,
	// which is in the flushed, recovered set).
	segBase := strings.TrimSuffix(seg.Path, ".log")
	r, err := index.OpenReader(index.TidxPath(segBase, "latency_ms"))
	if err != nil {
		t.Fatalf("opening rebuilt .tidx: %v", err)
	}
	offs := r.LookupEqual(index.EncodeKey(index.Value{Kind: index.KindInt, Int: 100}))
	if len(offs) == 0 {
		t.Fatal("rebuilt .tidx did not resolve latency=100")
	}
	found := false
	FetchRecords(seg.Path, offs, func(rec model.LogEntry) bool {
		if rec.Message == "took 100ms" {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("rebuilt .tidx offset for latency=100 did not point at the right record")
	}
}
