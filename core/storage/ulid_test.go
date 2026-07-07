package storage

import (
	"strings"
	"testing"
	"time"
)

func TestNewULIDFormatAndUniqueness(t *testing.T) {
	seen := make(map[string]bool, 2000)
	for i := 0; i < 2000; i++ {
		id := newULID()
		if len(id) != 26 {
			t.Fatalf("ULID length %d, want 26: %q", len(id), id)
		}
		for _, c := range id {
			if !strings.ContainsRune(crockford, c) {
				t.Fatalf("ULID %q has non-Crockford char %q", id, c)
			}
		}
		if seen[id] {
			t.Fatalf("duplicate ULID %q", id)
		}
		seen[id] = true
	}
}

// The point of ULIDs: segment IDs never collide across shards (each shard used to mint
// 1,2,3…), so a segment is relocatable with no global ID space (§10).
func TestSegmentIDsUniqueAcrossShards(t *testing.T) {
	ids := map[string]bool{}
	total := 0
	for s := 0; s < 3; s++ {
		dir := t.TempDir() // a separate shard folder
		w, _ := NewWriter(dir, Options{SegmentSizeBytes: 2 * PageSize, FlushInterval: time.Hour})
		w.Start(nil)
		for i := 0; i < 300; i++ { // forces several segments per shard
			w.Write(makeEntry(i, 1))
		}
		w.Close()
		m, _ := LoadManifest(dir)
		for _, seg := range m.All() {
			if ids[seg.ID] {
				t.Fatalf("segment ID %q collides across shards", seg.ID)
			}
			ids[seg.ID] = true
			total++
		}
	}
	if total < 3 {
		t.Fatalf("expected several segments across shards, got %d", total)
	}
}
