package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the daemon's top-level configuration (YAML).
type Config struct {
	Port                 int         `yaml:"port"`
	DataDir              string      `yaml:"data_dir"`
	SegmentSizeMB        int         `yaml:"segment_size_mb"`
	FlushIntervalMs      int         `yaml:"flush_interval_ms"`
	RetentionDays        int         `yaml:"retention_days"`           // delete sealed segments older than this (0 = keep forever)
	IndexMemBudgetMB     int         `yaml:"index_mem_budget_mb"`      // seal early when the active segment's in-RAM index exceeds this (0 = no cap)
	IndexCacheMB         int         `yaml:"index_cache_mb"`           // query-side cache of opened index sidecars (0 = disabled)
	Shards               int         `yaml:"shards"`                   // number of shard writers (shared-nothing folders); default 1
	MaxLabelValuesPerKey int         `yaml:"max_label_values_per_key"` // §6.2 cardinality cap; default 1000
	Multitenancy         bool        `yaml:"multitenancy"`             // tag+isolate by X-Scope-OrgID (§12); default false
	Index                IndexConfig `yaml:"index"`
	Labels               []string    `yaml:"labels"` // label-key allowlist (§6.2)
}

// Default returns a config with sensible defaults, used when no file is given and to
// fill unset fields.
func Default() Config {
	return Config{
		Port:            7100, // not Loki's 3100, so the two can run side by side
		DataDir:         "./data",
		SegmentSizeMB:   64,
		FlushIntervalMs: 500,
	}
}

// Load reads a YAML config file, applying defaults for any unset numeric field.
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("reading config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	d := Default()
	if c.Port == 0 {
		c.Port = d.Port
	}
	if c.DataDir == "" {
		c.DataDir = d.DataDir
	}
	if c.SegmentSizeMB == 0 {
		c.SegmentSizeMB = d.SegmentSizeMB
	}
	if c.FlushIntervalMs == 0 {
		c.FlushIntervalMs = d.FlushIntervalMs
	}
}

func (c *Config) validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port %d out of range", c.Port)
	}
	if c.SegmentSizeMB < 1 || c.SegmentSizeMB > 1024 {
		return fmt.Errorf("segment_size_mb %d out of range", c.SegmentSizeMB)
	}
	if c.FlushIntervalMs < 10 || c.FlushIntervalMs > 60000 {
		return fmt.Errorf("flush_interval_ms %d out of range", c.FlushIntervalMs)
	}
	return nil
}

// SegmentSizeBytes returns the configured segment size in bytes.
func (c *Config) SegmentSizeBytes() int64 { return int64(c.SegmentSizeMB) << 20 }

// FlushInterval returns the configured flush cadence.
func (c *Config) FlushInterval() time.Duration {
	return time.Duration(c.FlushIntervalMs) * time.Millisecond
}

// Retention returns the configured retention window (0 = keep forever).
func (c *Config) Retention() time.Duration {
	return time.Duration(c.RetentionDays) * 24 * time.Hour
}

// IndexCacheBytes returns the query-side sidecar cache budget in bytes.
//
// This is a READ cache, distinct from IndexMemBudget (which bounds the ACTIVE segment's
// in-RAM index during writes). Without it every query re-reads and re-decodes each .tidx
// and .lidx from disk, per segment per predicate — several megabytes each on a real
// deployment. Negative disables it; 0 takes the default.
func (c *Config) IndexCacheBytes() int64 {
	switch {
	case c.IndexCacheMB < 0:
		return 0
	case c.IndexCacheMB == 0:
		return 256 << 20
	default:
		return int64(c.IndexCacheMB) << 20
	}
}

// IndexMemBudgetBytes returns the active-segment index RAM budget in bytes (0 = no cap).
func (c *Config) IndexMemBudgetBytes() int64 {
	return int64(c.IndexMemBudgetMB) << 20
}

// ShardCount returns the number of shard writers (at least 1).
func (c *Config) ShardCount() int {
	if c.Shards < 1 {
		return 1
	}
	return c.Shards
}

// MaxLabelValues returns the per-key value-cardinality cap, defaulting to 1000 (§6.2). Set
// a negative value to disable the cap.
func (c *Config) MaxLabelValues() int {
	switch {
	case c.MaxLabelValuesPerKey < 0:
		return 0 // disabled
	case c.MaxLabelValuesPerKey == 0:
		return 1000 // default
	default:
		return c.MaxLabelValuesPerKey
	}
}
