package loggen

import (
	"testing"
	"time"
)

func TestParseBurst(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Burst
		wantErr bool
	}{
		{name: "empty means no burst", in: "", want: Burst{}},
		{
			name: "documented form",
			in:   "200x for=5s every=60s",
			want: Burst{Mult: 200, For: 5 * time.Second, Every: 60 * time.Second},
		},
		{
			name: "fields in any order",
			in:   "every=30s 3.5x for=2s",
			want: Burst{Mult: 3.5, For: 2 * time.Second, Every: 30 * time.Second},
		},
		{name: "missing every", in: "10x for=5s", wantErr: true},
		{name: "missing multiplier", in: "for=5s every=60s", wantErr: true},
		{name: "for longer than every", in: "10x for=90s every=60s", wantErr: true},
		{name: "junk field", in: "10x nonsense", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBurst(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseBurst(%q) error = nil, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBurst(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseBurst(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestBurstActive(t *testing.T) {
	b := Burst{Mult: 10, For: 5 * time.Second, Every: 60 * time.Second}
	tests := []struct {
		elapsed time.Duration
		want    bool
	}{
		{0, true},
		{4 * time.Second, true},
		{5 * time.Second, false},
		{59 * time.Second, false},
		{60 * time.Second, true},
		{64 * time.Second, true},
		{65 * time.Second, false},
	}
	for _, tt := range tests {
		if got := b.Active(tt.elapsed); got != tt.want {
			t.Errorf("Active(%s) = %v, want %v", tt.elapsed, got, tt.want)
		}
	}

	var none Burst
	if none.Active(time.Second) {
		t.Error("zero-value Burst reported active")
	}
}

func TestParseRotate(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Rotate
		wantErr bool
	}{
		{name: "empty means no rotation", in: "", want: Rotate{}},
		{name: "size and keep", in: "8MB,keep=3", want: Rotate{MaxBytes: 8 << 20, Keep: 3}},
		{name: "bare byte count", in: "1024", want: Rotate{MaxBytes: 1024}},
		{name: "kilobytes", in: "512KB,keep=1", want: Rotate{MaxBytes: 512 << 10, Keep: 1}},
		{name: "keep without size", in: "keep=3", wantErr: true},
		{name: "unparseable size", in: "banana", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRotate(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseRotate(%q) error = nil, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRotate(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseRotate(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseLevels(t *testing.T) {
	l, err := ParseLevels("debug=60,info=30,warn=8,error=2")
	if err != nil {
		t.Fatalf("ParseLevels() error = %v", err)
	}
	if len(l.Names) != 4 || l.total != 100 {
		t.Fatalf("parsed %v weights %v total %d, want 4 levels totalling 100", l.Names, l.Weights, l.total)
	}

	// Boundaries: the picks must partition [0,1) in the declared order.
	tests := []struct {
		u    float64
		want string
	}{
		{0.0, "debug"},
		{0.59, "debug"},
		{0.60, "info"},
		{0.89, "info"},
		{0.90, "warn"},
		{0.97, "warn"},
		{0.98, "error"},
		{0.999, "error"},
	}
	for _, tt := range tests {
		if got := l.Pick(tt.u); got != tt.want {
			t.Errorf("Pick(%v) = %q, want %q", tt.u, got, tt.want)
		}
	}

	if _, err := ParseLevels("info=0,warn=0"); err == nil {
		t.Error("ParseLevels() with zero total weight returned no error")
	}
	if _, err := ParseLevels("nonsense"); err == nil {
		t.Error("ParseLevels() with a malformed pair returned no error")
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "1024", want: 1024},
		{in: "8MB", want: 8 << 20},
		{in: "8mb", want: 8 << 20},
		{in: "512KB", want: 512 << 10},
		{in: "2GB", want: 2 << 30},
		{in: "", wantErr: true},
		{in: "MB", wantErr: true},
	}
	for _, tt := range tests {
		got, err := ParseSize(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSize(%q) error = nil, want an error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSize(%q) error = %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestLoadDefaultsAndOverrides(t *testing.T) {
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("Load(nil) error = %v", err)
	}
	if cfg.Format != "app" || cfg.Rate != 20 || cfg.Services != 3 {
		t.Errorf("defaults = format %q rate %v services %d, want app/20/3", cfg.Format, cfg.Rate, cfg.Services)
	}

	cfg, err = Load([]string{"-format=json", "-rate=2.5", "-services=7", "-out=stdout,file:/tmp/x.log"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Format != "json" || cfg.Rate != 2.5 || cfg.Services != 7 {
		t.Errorf("overrides = format %q rate %v services %d", cfg.Format, cfg.Rate, cfg.Services)
	}
	if len(cfg.Outs) != 2 || cfg.Outs[1] != "file:/tmp/x.log" {
		t.Errorf("Outs = %v, want stdout and the file", cfg.Outs)
	}

	for _, args := range [][]string{
		{"-format=xml"},
		{"-rate=0"},
		{"-services=0"},
		{"-pods=0"},
	} {
		if _, err := Load(args); err == nil {
			t.Errorf("Load(%v) error = nil, want a validation error", args)
		}
	}
}
