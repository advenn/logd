package config

import (
	"testing"
	"time"
)

// SyncInterval is the one config knob that trades data durability for throughput, and its
// encoding is easy to get wrong: YAML cannot distinguish "unset" from "explicitly 0", so
// zero means DEFAULT and a negative value means fsync-every-page. Inverting that silently
// would either make every deployment 50x slower or silently weaken durability.
func TestSyncIntervalEncoding(t *testing.T) {
	tests := []struct {
		name string
		ms   int
		want time.Duration
	}{
		{"unset takes the default", 0, 50 * time.Millisecond},
		{"negative means fsync every page", -1, 0},
		{"explicit value is honoured", 200, 200 * time.Millisecond},
		{"explicit 1ms is not mistaken for unset", 1, time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&Config{SyncIntervalMs: tc.ms}).SyncInterval(); got != tc.want {
				t.Errorf("SyncIntervalMs=%d: got %v, want %v", tc.ms, got, tc.want)
			}
		})
	}
}

// IndexCacheBytes uses the same zero-means-default convention.
func TestIndexCacheEncoding(t *testing.T) {
	if got := (&Config{}).IndexCacheBytes(); got != 256<<20 {
		t.Errorf("unset: got %d, want 256MB", got)
	}
	if got := (&Config{IndexCacheMB: -1}).IndexCacheBytes(); got != 0 {
		t.Errorf("negative should disable the cache, got %d", got)
	}
	if got := (&Config{IndexCacheMB: 8}).IndexCacheBytes(); got != 8<<20 {
		t.Errorf("explicit: got %d, want 8MB", got)
	}
}

// Defaults must be usable without a config file for the knobs the daemon reads at startup.
func TestDefaultsAreSane(t *testing.T) {
	d := Default()
	if d.Port == 0 || d.DataDir == "" || d.SegmentSizeMB == 0 || d.FlushIntervalMs == 0 {
		t.Fatalf("Default() left a required field zero: %+v", d)
	}
	if err := d.validate(); err != nil {
		t.Errorf("Default() does not pass its own validation: %v", err)
	}
	if got := d.ShardCount(); got != 1 {
		t.Errorf("ShardCount default: got %d, want 1", got)
	}
}
