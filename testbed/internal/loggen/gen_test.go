package loggen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

func testConfig(t *testing.T, args ...string) *Config {
	t.Helper()
	cfg, err := Load(args)
	if err != nil {
		t.Fatalf("Load(%v) error = %v", args, err)
	}
	return cfg
}

// TestSeedIsReproducible is the property the --seed flag exists for: the same seed must
// produce byte-identical output, or a run cannot be repeated against a changed decoder.
func TestSeedIsReproducible(t *testing.T) {
	for _, format := range []string{"app", "json"} {
		t.Run(format, func(t *testing.T) {
			at := time.Date(2026, 1, 14, 10, 22, 31, 442000000, time.UTC)
			render := func() []string {
				cfg := testConfig(t, "-format="+format, "-seed=42", "-services=4", "-pods=3")
				g := NewGenerator(cfg)
				f, err := NewFormatter(cfg.Format)
				if err != nil {
					t.Fatalf("NewFormatter() error = %v", err)
				}
				out := make([]string, 200)
				for i := range out {
					out[i] = f.Format(g.Next(at))
				}
				return out
			}

			first, second := render(), render()
			for i := range first {
				if first[i] != second[i] {
					t.Fatalf("line %d differs between runs with the same seed:\n  %s\n  %s",
						i, first[i], second[i])
				}
			}
		})
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	at := time.Now()
	render := func(seed string) string {
		cfg := testConfig(t, "-seed="+seed)
		g := NewGenerator(cfg)
		f, _ := NewFormatter(cfg.Format)
		var b strings.Builder
		for i := 0; i < 50; i++ {
			b.WriteString(f.Format(g.Next(at)))
		}
		return b.String()
	}
	if render("1") == render("2") {
		t.Error("different seeds produced identical output")
	}
}

// appLine matches the shape logd's typed-range extraction targets. If this regex stops
// matching, the generator has stopped producing the thing the whole lab is aimed at.
var appLine = regexp.MustCompile(
	`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z (DEBUG|INFO|WARN|ERROR)\s+[a-z0-9-]+ \S.*\d+ms(| threshold=500ms| trace_id=[0-9a-f]+)$`)

func TestAppFormatShape(t *testing.T) {
	cfg := testConfig(t, "-format=app", "-seed=7")
	g := NewGenerator(cfg)
	f, _ := NewFormatter("app")

	at := time.Date(2026, 1, 14, 10, 22, 31, 442000000, time.UTC)
	for i := 0; i < 500; i++ {
		line := f.Format(g.Next(at))
		if !appLine.MatchString(line) {
			t.Fatalf("line %d does not match the expected app shape:\n  %s", i, line)
		}
		if strings.ContainsAny(line, "\n\r") {
			t.Fatalf("line %d contains an embedded newline: %q", i, line)
		}
	}
}

func TestAppFormatWithoutDurationField(t *testing.T) {
	cfg := testConfig(t, "-format=app", "-duration-field=false", "-seed=7")
	g := NewGenerator(cfg)
	f, _ := NewFormatter("app")

	if got := g.Next(time.Now()).DurationMS; got != 0 {
		t.Errorf("duration-field=false still produced DurationMS = %v, want 0", got)
	}
	if line := f.Format(g.Next(time.Now())); line == "" {
		t.Error("duration-field=false produced an empty line")
	}
}

func TestJSONFormatIsValidAndFlat(t *testing.T) {
	cfg := testConfig(t, "-format=json", "-seed=7")
	g := NewGenerator(cfg)
	f, _ := NewFormatter("json")

	for i := 0; i < 200; i++ {
		line := f.Format(g.Next(time.Now()))
		if strings.ContainsAny(line, "\n\r") {
			t.Fatalf("line %d contains an embedded newline: %q", i, line)
		}

		var parsed map[string]any
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n  %s", i, err, line)
		}
		for _, key := range []string{"ts", "level", "logger", "msg", "duration_ms", "trace_id", "span_id"} {
			if _, ok := parsed[key]; !ok {
				t.Errorf("line %d is missing key %q", i, key)
			}
		}
	}
}

// TestDurationIsLongTailed checks the property that makes range queries interesting:
// p99 must sit well above the median, as it does in real latency data.
func TestDurationIsLongTailed(t *testing.T) {
	cfg := testConfig(t, "-seed=99", "-levels=info=100")
	g := NewGenerator(cfg)

	const n = 20000
	values := make([]float64, n)
	at := time.Now()
	for i := range values {
		values[i] = g.Next(at).DurationMS
	}
	sort.Float64s(values)

	p50, p99 := values[n/2], values[n*99/100]
	if p50 < 15 || p50 > 60 {
		t.Errorf("median latency = %.1fms, want roughly 30ms", p50)
	}
	if ratio := p99 / p50; ratio < 5 {
		t.Errorf("p99/p50 = %.1f (p50 %.1fms, p99 %.1fms), want a long tail of at least 5x",
			ratio, p50, p99)
	}
	if values[0] <= 0 {
		t.Errorf("minimum latency = %.1fms, want a positive duration", values[0])
	}
}

func TestLevelDistributionIsHonoured(t *testing.T) {
	cfg := testConfig(t, "-seed=5", "-levels=debug=60,info=30,warn=8,error=2")
	g := NewGenerator(cfg)

	const n = 40000
	counts := map[string]int{}
	at := time.Now()
	for i := 0; i < n; i++ {
		counts[g.Next(at).Level]++
	}

	want := map[string]float64{"debug": 0.60, "info": 0.30, "warn": 0.08, "error": 0.02}
	for level, wantFrac := range want {
		got := float64(counts[level]) / n
		if diff := got - wantFrac; diff > 0.02 || diff < -0.02 {
			t.Errorf("level %s = %.3f of lines, want %.3f", level, got, wantFrac)
		}
	}
}

func TestCardinalityKnobs(t *testing.T) {
	cfg := testConfig(t, "-seed=11", "-services=4", "-pods=3")
	g := NewGenerator(cfg)

	services, pods := map[string]bool{}, map[string]bool{}
	at := time.Now()
	for i := 0; i < 5000; i++ {
		e := g.Next(at)
		services[e.Service] = true
		pods[e.Pod] = true
	}
	if len(services) != 4 {
		t.Errorf("saw %d distinct services, want 4", len(services))
	}
	// 4 services x 3 pods each.
	if len(pods) != 12 {
		t.Errorf("saw %d distinct pods, want 12", len(pods))
	}
}

// TestServiceOverride is the one-container-one-service property: a container labelled
// app=checkout must not emit lines claiming to be some other service.
func TestServiceOverride(t *testing.T) {
	cfg := testConfig(t, "-service=checkout", "-services=5", "-pods=2", "-seed=4")
	g := NewGenerator(cfg)

	at := time.Now()
	pods := map[string]bool{}
	for i := 0; i < 500; i++ {
		e := g.Next(at)
		if e.Service != "checkout" {
			t.Fatalf("event %d service = %q, want checkout", i, e.Service)
		}
		if !strings.HasPrefix(e.Pod, "checkout-") {
			t.Errorf("event %d pod = %q, want a checkout- prefix", i, e.Pod)
		}
		pods[e.Pod] = true
	}
	if len(pods) != 2 {
		t.Errorf("saw %d distinct pods, want 2", len(pods))
	}
}

// TestTemplatesRepeat is the property logd's template routing depends on: a service emits
// a small number of message shapes, not free text.
func TestTemplatesRepeat(t *testing.T) {
	cfg := testConfig(t, "-format=app", "-service=checkout", "-seed=9")
	g := NewGenerator(cfg)
	f, _ := NewFormatter("app")

	// Strip the timestamp and every number, leaving the template skeleton.
	digits := regexp.MustCompile(`[0-9a-f]{6,}|[0-9.]+`)
	shapes := map[string]int{}
	at := time.Now()
	for i := 0; i < 4000; i++ {
		shapes[digits.ReplaceAllString(f.Format(g.Next(at)), "N")]++
	}
	if len(shapes) < 4 || len(shapes) > 40 {
		t.Errorf("saw %d distinct message shapes, want a small handful (4-40)", len(shapes))
	}
}

func TestClockSkewAppliesPerService(t *testing.T) {
	cfg := testConfig(t, "-seed=3", "-services=5", "-clock-skew=5s")
	g := NewGenerator(cfg)

	if got := g.Skew(0); got != 0 {
		t.Errorf("service 0 skew = %s, want 0 so there is an unskewed baseline", got)
	}
	var anySkewed bool
	for i := 1; i < 5; i++ {
		s := g.Skew(i)
		if s < -5*time.Second || s > 5*time.Second {
			t.Errorf("service %d skew = %s, outside the +/-5s bound", i, s)
		}
		if s != 0 {
			anySkewed = true
		}
	}
	if !anySkewed {
		t.Error("no service was skewed despite -clock-skew=5s")
	}

	// The skew must actually reach the emitted timestamp.
	at := time.Date(2026, 1, 14, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 500; i++ {
		e := g.Next(at)
		if want := at.Add(e.Skew); !e.TS.Equal(want) {
			t.Fatalf("event for %s stamped %s, want %s", e.Service, e.TS, want)
		}
	}
}

func TestNoSkewByDefault(t *testing.T) {
	cfg := testConfig(t, "-seed=3", "-services=5")
	g := NewGenerator(cfg)
	at := time.Now()
	for i := 0; i < 100; i++ {
		if e := g.Next(at); !e.TS.Equal(at) {
			t.Fatalf("event stamped %s without -clock-skew, want %s", e.TS, at)
		}
	}
}

func TestFileSinkRotates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	// 200 bytes per file, keeping 2 old files.
	s, err := newFileSink(path, Rotate{MaxBytes: 200, Keep: 2})
	if err != nil {
		t.Fatalf("newFileSink() error = %v", err)
	}
	line := strings.Repeat("x", 49) // 50 bytes with the newline
	for i := 0; i < 40; i++ {
		if err := s.WriteRecord([]byte(line)); err != nil {
			t.Fatalf("WriteRecord() error = %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Current file plus exactly Keep rotated files, and nothing beyond.
	for _, name := range []string{"app.log", "app.log.1", "app.log.2"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
		if info.Size() > 200 {
			t.Errorf("%s is %d bytes, want no more than the 200 byte rotation size", name, info.Size())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "app.log.3")); !os.IsNotExist(err) {
		t.Errorf("app.log.3 exists, want at most Keep=2 rotated files")
	}
}

func TestFileSinkRotationChangesInode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	s, err := newFileSink(path, Rotate{MaxBytes: 100, Keep: 1})
	if err != nil {
		t.Fatalf("newFileSink() error = %v", err)
	}
	defer s.Close()

	line := strings.Repeat("y", 49)
	if err := s.WriteRecord([]byte(line)); err != nil {
		t.Fatalf("WriteRecord() error = %v", err)
	}
	before := inodeOf(t, path)

	// Enough writes to force a roll.
	for i := 0; i < 4; i++ {
		if err := s.WriteRecord([]byte(line)); err != nil {
			t.Fatalf("WriteRecord() error = %v", err)
		}
	}
	if after := inodeOf(t, path); after == before {
		t.Error("inode unchanged after rotation; a tailing agent would not see a new file")
	}
}

func TestFileSinkWithoutRotationGrows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	s, err := newFileSink(path, Rotate{})
	if err != nil {
		t.Fatalf("newFileSink() error = %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := s.WriteRecord([]byte(strings.Repeat("z", 49))); err != nil {
			t.Fatalf("WriteRecord() error = %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 5000 {
		t.Errorf("file is %d bytes, want 5000", info.Size())
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Error("a rotated file exists despite rotation being disabled")
	}
}
