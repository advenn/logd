// Package loggen produces realistic, shaped log traffic. It exists so the agents under
// observation have something plausible to ship: long-tailed latencies, a controllable
// number of distinct label values, and timestamps that are not all perfectly ordered.
package loggen

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved generator configuration.
type Config struct {
	// Format is "app" or "json".
	Format string
	// Rate is events per second, fractional allowed.
	Rate float64
	// Burst optionally multiplies Rate on a fixed schedule.
	Burst Burst
	// Services is how many distinct service values are emitted. This is the label
	// cardinality knob that matters most.
	Services int
	// Pods is how many distinct pod values are emitted per service.
	Pods int
	// Levels is the level distribution by weight.
	Levels Levels
	// DurationField emits a plausible "took Nms" drawn from a log-normal distribution,
	// so p99 sits far above the median and range queries have realistic selectivity.
	DurationField bool
	// Seed makes a run reproducible.
	Seed int64
	// Outs is where lines go: "stdout" and/or "file:/path".
	Outs []string
	// Rotate controls file rotation, exercising agent inode-change handling.
	Rotate Rotate
	// ClockSkew, when non-zero, gives each service a fixed offset drawn from
	// [-ClockSkew, +ClockSkew]. Whether agents pass that through or quietly correct it is
	// one of the things worth finding out.
	ClockSkew time.Duration
	// Env labels the environment in emitted lines.
	Env string
	// Service, when set, is the single service name this process emits. One container,
	// one service — which is what a real deployment looks like. Empty falls back to the
	// --services pool.
	Service string
	// HugeBytes is the padding length of the noisy format's oversized line.
	HugeBytes int
}

// Burst describes a periodic spike: Mult times the base rate, for For, every Every.
type Burst struct {
	Mult  float64
	For   time.Duration
	Every time.Duration
}

// Active reports whether a burst is in progress at elapsed time since start.
func (b Burst) Active(elapsed time.Duration) bool {
	if b.Mult <= 1 || b.For <= 0 || b.Every <= 0 {
		return false
	}
	return elapsed%b.Every < b.For
}

// Rotate describes file rotation: roll at MaxBytes, keep Keep old files.
type Rotate struct {
	MaxBytes int64
	Keep     int
}

// Enabled reports whether rotation is configured.
func (r Rotate) Enabled() bool { return r.MaxBytes > 0 }

// Levels is a weighted level distribution.
type Levels struct {
	Names   []string
	Weights []int
	total   int
}

// Pick returns a level for a uniform random value in [0,1).
func (l Levels) Pick(u float64) string {
	if l.total == 0 {
		return "info"
	}
	target := int(u * float64(l.total))
	acc := 0
	for i, w := range l.Weights {
		acc += w
		if target < acc {
			return l.Names[i]
		}
	}
	return l.Names[len(l.Names)-1]
}

// Load resolves configuration from the environment, then applies flag overrides.
func Load(args []string) (*Config, error) {
	c := &Config{
		Format:        envString("LOGGEN_FORMAT", "app"),
		Rate:          envFloat("LOGGEN_RATE", 20),
		Services:      int(envInt("LOGGEN_SERVICES", 3)),
		Pods:          int(envInt("LOGGEN_PODS", 2)),
		DurationField: envBool("LOGGEN_DURATION_FIELD", true),
		Seed:          envInt("LOGGEN_SEED", 0),
		Env:           envString("LOGGEN_ENV", "dev"),
		Service:       envString("LOGGEN_SERVICE", ""),
		HugeBytes:     int(envInt("LOGGEN_HUGE_BYTES", 1<<20)),
	}

	levels := envString("LOGGEN_LEVELS", "debug=60,info=30,warn=8,error=2")
	out := envString("LOGGEN_OUT", "stdout")
	burst := envString("LOGGEN_BURST", "")
	rotate := envString("LOGGEN_ROTATE", "")
	skew := envString("LOGGEN_CLOCK_SKEW", "0s")

	fs := flag.NewFlagSet("loggen", flag.ContinueOnError)
	fs.StringVar(&c.Format, "format", c.Format, "output format: "+strings.Join(Formats, "|"))
	fs.IntVar(&c.HugeBytes, "huge-bytes", c.HugeBytes, "padding length of the noisy format's oversized line")
	fs.Float64Var(&c.Rate, "rate", c.Rate, "events per second, fractional allowed")
	fs.IntVar(&c.Services, "services", c.Services, "number of distinct service values")
	fs.IntVar(&c.Pods, "pods", c.Pods, "number of distinct pod values per service")
	fs.BoolVar(&c.DurationField, "duration-field", c.DurationField, `emit a long-tailed "took Nms"`)
	fs.Int64Var(&c.Seed, "seed", c.Seed, "random seed; 0 picks one from the service names for reproducibility")
	fs.StringVar(&c.Env, "env", c.Env, "environment name emitted in lines")
	fs.StringVar(&c.Service, "service", c.Service, "single service name for this process; overrides the --services pool")
	fs.StringVar(&levels, "levels", levels, "level distribution, e.g. debug=60,info=30,warn=8,error=2")
	fs.StringVar(&out, "out", out, "comma-separated sinks: stdout, file:/var/log/app/x.log")
	fs.StringVar(&burst, "burst", burst, `spike schedule, e.g. "200x for=5s every=60s"`)
	fs.StringVar(&rotate, "rotate", rotate, `file rotation, e.g. "8MB,keep=3"`)
	fs.StringVar(&skew, "clock-skew", skew, "per-service timestamp offset bound, e.g. 5s")

	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("parse flags: %w", err)
	}

	var err error
	if c.Levels, err = ParseLevels(levels); err != nil {
		return nil, err
	}
	if c.Burst, err = ParseBurst(burst); err != nil {
		return nil, err
	}
	if c.Rotate, err = ParseRotate(rotate); err != nil {
		return nil, err
	}
	if c.ClockSkew, err = time.ParseDuration(skew); err != nil {
		return nil, fmt.Errorf("parse clock-skew %q: %w", skew, err)
	}
	c.Outs = splitNonEmpty(out, ",")

	if _, err := NewFormatter(c.Format); err != nil {
		return nil, err
	}
	if c.Rate <= 0 {
		return nil, fmt.Errorf("rate must be positive, got %v", c.Rate)
	}
	if c.Services < 1 {
		return nil, fmt.Errorf("services must be at least 1, got %d", c.Services)
	}
	if c.Pods < 1 {
		return nil, fmt.Errorf("pods must be at least 1, got %d", c.Pods)
	}
	if len(c.Outs) == 0 {
		return nil, fmt.Errorf("out must name at least one sink")
	}
	return c, nil
}

// ParseLevels parses "debug=60,info=30,warn=8,error=2" into a weighted distribution.
func ParseLevels(s string) (Levels, error) {
	var l Levels
	for _, part := range splitNonEmpty(s, ",") {
		name, weight, ok := strings.Cut(part, "=")
		if !ok {
			return l, fmt.Errorf("level %q is not name=weight", part)
		}
		w, err := strconv.Atoi(strings.TrimSpace(weight))
		if err != nil {
			return l, fmt.Errorf("level %q weight: %w", part, err)
		}
		if w < 0 {
			return l, fmt.Errorf("level %q weight is negative", part)
		}
		l.Names = append(l.Names, strings.TrimSpace(name))
		l.Weights = append(l.Weights, w)
		l.total += w
	}
	if l.total == 0 {
		return l, fmt.Errorf("level distribution %q has zero total weight", s)
	}
	return l, nil
}

// ParseBurst parses `200x for=5s every=60s`. An empty string means no bursting.
func ParseBurst(s string) (Burst, error) {
	var b Burst
	if strings.TrimSpace(s) == "" {
		return b, nil
	}
	for _, field := range strings.Fields(s) {
		switch {
		case strings.HasSuffix(field, "x"):
			m, err := strconv.ParseFloat(strings.TrimSuffix(field, "x"), 64)
			if err != nil {
				return b, fmt.Errorf("burst multiplier %q: %w", field, err)
			}
			b.Mult = m
		case strings.HasPrefix(field, "for="):
			d, err := time.ParseDuration(strings.TrimPrefix(field, "for="))
			if err != nil {
				return b, fmt.Errorf("burst for: %w", err)
			}
			b.For = d
		case strings.HasPrefix(field, "every="):
			d, err := time.ParseDuration(strings.TrimPrefix(field, "every="))
			if err != nil {
				return b, fmt.Errorf("burst every: %w", err)
			}
			b.Every = d
		default:
			return b, fmt.Errorf("unrecognised burst field %q, want Nx, for=D or every=D", field)
		}
	}
	if b.Mult <= 0 || b.For <= 0 || b.Every <= 0 {
		return b, fmt.Errorf("burst %q needs a multiplier, for= and every=", s)
	}
	if b.For > b.Every {
		return b, fmt.Errorf("burst for=%s exceeds every=%s, which would burst continuously", b.For, b.Every)
	}
	return b, nil
}

// ParseRotate parses "8MB,keep=3". An empty string means no rotation.
func ParseRotate(s string) (Rotate, error) {
	var r Rotate
	if strings.TrimSpace(s) == "" {
		return r, nil
	}
	for _, field := range splitNonEmpty(s, ",") {
		if strings.HasPrefix(field, "keep=") {
			k, err := strconv.Atoi(strings.TrimPrefix(field, "keep="))
			if err != nil {
				return r, fmt.Errorf("rotate keep: %w", err)
			}
			r.Keep = k
			continue
		}
		n, err := ParseSize(field)
		if err != nil {
			return r, err
		}
		r.MaxBytes = n
	}
	if r.MaxBytes <= 0 {
		return r, fmt.Errorf("rotate %q needs a size, e.g. 8MB", s)
	}
	if r.Keep < 0 {
		return r, fmt.Errorf("rotate keep must not be negative, got %d", r.Keep)
	}
	return r, nil
}

// ParseSize parses a byte size such as 1024, 512KB or 8MB.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	switch {
	case strings.HasSuffix(strings.ToUpper(s), "GB"):
		mult, s = 1<<30, s[:len(s)-2]
	case strings.HasSuffix(strings.ToUpper(s), "MB"):
		mult, s = 1<<20, s[:len(s)-2]
	case strings.HasSuffix(strings.ToUpper(s), "KB"):
		mult, s = 1<<10, s[:len(s)-2]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", s, err)
	}
	return n * mult, nil
}

func splitNonEmpty(s, sep string) []string {
	var out []string
	for _, p := range strings.Split(s, sep) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int64) int64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
