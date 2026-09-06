package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the logd daemon.
type Config struct {
	Port                  int        `yaml:"port"`
	DataDir               string     `yaml:"data_dir"`
	RetentionDays         int        `yaml:"retention_days"`
	FlushIntervalMs       int        `yaml:"flush_interval_ms"`
	SegmentSizeMB         int        `yaml:"segment_size_mb"`
	QueryToleranceMinutes int        `yaml:"query_tolerance_minutes"`
	Templates             []Template `yaml:"templates"`
}

// Template config for future template-based indexing.
type Template struct {
	Name    string        `yaml:"name"`
	Pattern string        `yaml:"pattern"`
	Fields  []FieldConfig `yaml:"fields"`
}

// FieldConfig defines a typed field in a template.
type FieldConfig struct {
	Name   string   `yaml:"name"`
	Type   string   `yaml:"type"`
	Index  bool     `yaml:"index"`
	Values []string `yaml:"values,omitempty"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Port:                  7100,
		DataDir:               "./data",
		RetentionDays:         30,
		FlushIntervalMs:       500,
		SegmentSizeMB:         64,
		QueryToleranceMinutes: 5,
	}
}

// Load reads a YAML config file, applies defaults for missing fields, and
// validates the result. Returns an error if the file cannot be read, parsed,
// or contains invalid values.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if err = cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port %d", c.Port)
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.RetentionDays < 1 {
		return fmt.Errorf("retention_days must be >= 1, got %d", c.RetentionDays)
	}
	if c.FlushIntervalMs < 10 || c.FlushIntervalMs > 60000 {
		return fmt.Errorf("flush_interval_ms must be 10-60000, got %d", c.FlushIntervalMs)
	}
	if c.SegmentSizeMB < 1 || c.SegmentSizeMB > 1024 {
		return fmt.Errorf("segment_size_mb must be 1-1024, got %d", c.SegmentSizeMB)
	}
	if c.QueryToleranceMinutes < 0 || c.QueryToleranceMinutes > 60 {
		return fmt.Errorf("query_tolerance_minutes must be 0-60, got %d", c.QueryToleranceMinutes)
	}
	return nil
}

// SegmentsDir returns the path to the segments subdirectory under DataDir.
func (c *Config) SegmentsDir() string {
	return filepath.Join(c.DataDir, "segments")
}

// SegmentSizeBytes returns the segment rotation threshold in bytes.
func (c *Config) SegmentSizeBytes() int64 {
	return int64(c.SegmentSizeMB) * 1024 * 1024
}

// FlushInterval returns the flush interval as a Go duration.
func (c *Config) FlushInterval() int64 {
	return int64(c.FlushIntervalMs) * 1_000_000
}
