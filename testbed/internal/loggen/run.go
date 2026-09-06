package loggen

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Run generates lines until ctx is cancelled.
//
// Pacing is intentionally simple: sleep for one interval between lines, recomputing the
// interval each time so a burst takes effect immediately. It drifts slightly under load,
// which is fine — a real application does not emit on a perfect metronome either.
func Run(ctx context.Context, cfg *Config, log *slog.Logger) error {
	g := NewGenerator(cfg)
	f, err := NewFormatter(cfg.Format)
	if err != nil {
		return err
	}
	sinks, err := NewSinks(cfg.Outs, cfg.Rotate)
	if err != nil {
		return err
	}
	defer CloseAll(sinks)

	logStartup(cfg, g, log)

	start := time.Now()
	var emitted uint64
	for {
		if err := ctx.Err(); err != nil {
			log.Info("loggen stopping", "emitted", emitted, "elapsed", time.Since(start).Round(time.Second))
			return nil
		}

		now := time.Now()
		rate := cfg.Rate
		if cfg.Burst.Active(now.Sub(start)) {
			rate *= cfg.Burst.Mult
		}

		payload := []byte(f.Format(g.Next(now)))
		for _, s := range sinks {
			if err := s.WriteRecord(payload); err != nil {
				return fmt.Errorf("write record: %w", err)
			}
		}
		emitted++

		interval := time.Duration(float64(time.Second) / rate)
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}

func logStartup(cfg *Config, g *Generator, log *slog.Logger) {
	attrs := []any{
		"format", cfg.Format,
		"rate", cfg.Rate,
		"services", cfg.Services,
		"pods", cfg.Pods,
		"out", cfg.Outs,
	}
	if cfg.Burst.Mult > 1 {
		attrs = append(attrs, "burst", fmt.Sprintf("%gx for=%s every=%s",
			cfg.Burst.Mult, cfg.Burst.For, cfg.Burst.Every))
	}
	if cfg.Rotate.Enabled() {
		attrs = append(attrs, "rotate_at", cfg.Rotate.MaxBytes, "keep", cfg.Rotate.Keep)
	}
	if cfg.ClockSkew != 0 {
		skews := make(map[string]string, len(g.Services()))
		for i, name := range g.Services() {
			skews[name] = g.Skew(i).String()
		}
		attrs = append(attrs, "clock_skew", skews)
	}
	log.Info("loggen starting", attrs...)
}
