package loggen

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"time"
)

// Event is one log record before formatting. Every format renders the same event, so a
// run with a fixed seed produces the same facts whichever format is selected — which is
// what makes two formats comparable.
type Event struct {
	TS         time.Time
	Level      string
	Service    string
	Pod        string
	Env        string
	Msg        string
	DurationMS float64
	Status     int
	UserID     int
	Method     string
	Path       string
	TraceID    string
	SpanID     string
	// Skew is the offset applied to TS for this service, kept so it can be reported at
	// startup and correlated with whatever the agents end up shipping.
	Skew time.Duration

	// Fields below are used by a subset of the formats.

	// nginx access log fields.
	RemoteAddr string
	RemoteUser string
	UserAgent  string
	Referer    string
	BodyBytes  int

	// Java stack trace shape. StackFrames is the depth of the top exception;
	// StackCausedBy is the depth of the "Caused by" chain, or 0 for none.
	StackSeed     int
	StackFrames   int
	StackCausedBy int

	// NoisyCase selects which hostile shape the noisy format emits, and HugeBytes is
	// the padding length of its oversized line.
	NoisyCase int
	HugeBytes int

	// Template selects which message template renders this event. Real log volume is a
	// handful of templates repeated with varying parameters, and that distribution is
	// what logd's template routing depends on.
	Template int
	// OrderID and SKU are drawn from bounded pools, so the same identifier turns up on
	// several lines and grouping across them is possible.
	OrderID string
	SKU     string
}

// Generator produces events. It is not safe for concurrent use; one generator per
// process is the intended shape.
type Generator struct {
	cfg      *Config
	rng      *rand.Rand
	services []string
	pods     [][]string
	skews    []time.Duration
	// n counts events produced, so the noisy format can cycle deterministically through
	// its cases rather than reaching some of them only by luck.
	n int
}

// servicePool is drawn from in order, so --services=3 always yields the same three names.
var servicePool = []string{
	"order-svc", "payment-svc", "inventory-svc", "auth-svc", "search-svc",
	"notification-svc", "shipping-svc", "cart-svc", "pricing-svc", "recommendation-svc",
}

// NewGenerator builds a generator from cfg. With a fixed seed the whole stream —
// services, pods, levels, latencies, skews — is reproducible.
func NewGenerator(cfg *Config) *Generator {
	seed := cfg.Seed
	if seed == 0 {
		// Derive a stable seed from the shape of the run rather than the clock, so an
		// unseeded container still restarts into the same stream.
		h := fnv.New64a()
		fmt.Fprintf(h, "%s|%d|%d|%s", cfg.Format, cfg.Services, cfg.Pods, cfg.Env)
		seed = int64(h.Sum64() & math.MaxInt64)
	}
	g := &Generator{cfg: cfg, rng: rand.New(rand.NewSource(seed))}

	// A single named service overrides the pool: in a real deployment one container is
	// one service, and the container's app label and its log lines agree.
	if cfg.Service != "" {
		g.services = []string{cfg.Service}
		g.pods = [][]string{make([]string, cfg.Pods)}
		g.skews = []time.Duration{0}
		for j := range g.pods[0] {
			g.pods[0][j] = fmt.Sprintf("%s-%s-%s", cfg.Service, token(g.rng, 6), token(g.rng, 4))
		}
		return g
	}

	g.services = make([]string, cfg.Services)
	g.pods = make([][]string, cfg.Services)
	g.skews = make([]time.Duration, cfg.Services)
	for i := range g.services {
		if i < len(servicePool) {
			g.services[i] = servicePool[i]
		} else {
			g.services[i] = fmt.Sprintf("svc-%02d", i)
		}
		g.pods[i] = make([]string, cfg.Pods)
		for j := range g.pods[i] {
			g.pods[i][j] = fmt.Sprintf("%s-%s-%s", g.services[i], token(g.rng, 6), token(g.rng, 4))
		}
		// Service 0 is always unskewed, so there is a clean baseline to compare against.
		if i > 0 && cfg.ClockSkew != 0 {
			g.skews[i] = time.Duration(g.rng.Int63n(int64(2*cfg.ClockSkew))) - cfg.ClockSkew
		}
	}
	return g
}

// Services returns the generated service names, for startup logging.
func (g *Generator) Services() []string { return g.services }

// Skew returns the offset applied to a service's timestamps.
func (g *Generator) Skew(i int) time.Duration { return g.skews[i] }

// Next produces one event stamped at now plus that service's clock skew.
func (g *Generator) Next(now time.Time) Event {
	si := g.rng.Intn(len(g.services))
	pi := g.rng.Intn(len(g.pods[si]))
	level := g.cfg.Levels.Pick(g.rng.Float64())

	e := Event{
		TS:      now.Add(g.skews[si]),
		Level:   level,
		Service: g.services[si],
		Pod:     g.pods[si][pi],
		Env:     g.cfg.Env,
		UserID:  1000 + g.rng.Intn(9000),
		TraceID: token(g.rng, 32),
		SpanID:  token(g.rng, 16),
		Skew:    g.skews[si],
	}
	e.Method, e.Path = g.route()
	e.Status = g.status(level)
	e.Msg = g.message(level)
	if g.cfg.DurationField {
		e.DurationMS = g.duration(level)
	}

	// nginx access log fields.
	e.RemoteAddr = fmt.Sprintf("%d.%d.%d.%d",
		10+g.rng.Intn(3), g.rng.Intn(256), g.rng.Intn(256), 1+g.rng.Intn(254))
	e.UserAgent = userAgents[g.rng.Intn(len(userAgents))]
	e.BodyBytes = 120 + g.rng.Intn(24000)
	if g.rng.Intn(4) == 0 {
		e.Referer = "https://app.example.com/orders"
	}
	if g.rng.Intn(8) == 0 {
		e.RemoteUser = fmt.Sprintf("user%d", e.UserID)
	}

	// Java stack trace shape: 20-60 frames, with a Caused by chain most of the time.
	e.StackSeed = g.rng.Intn(1 << 20)
	e.StackFrames = 20 + g.rng.Intn(41)
	if g.rng.Intn(10) < 7 {
		e.StackCausedBy = 3 + g.rng.Intn(8)
	}

	// Bounded ID pools, so identifiers repeat and lines can be grouped by order or SKU.
	e.OrderID = fmt.Sprintf("ORD-%07d", 1000000+g.rng.Intn(400))
	e.SKU = fmt.Sprintf("SKU-%05d", 80000+g.rng.Intn(150))
	// A handful of templates per level, picked uniformly.
	e.Template = g.rng.Intn(64)

	// Noisy cycling: every case is reached in turn, so a short run still exercises all
	// of them rather than sampling at random.
	e.NoisyCase = g.n
	e.HugeBytes = g.cfg.HugeBytes
	g.n++

	return e
}

var userAgents = []string{
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
	"curl/8.14.1",
	"okhttp/4.12.0",
	"Go-http-client/2.0",
	"kube-probe/1.31",
	"Datadog Agent/7.58.0",
}

// duration draws from a log-normal distribution: median around 30ms with a long right
// tail, so p99 lands several times the median. A uniform distribution would make every
// range query equally selective and tell us nothing useful.
func (g *Generator) duration(level string) float64 {
	const (
		medianMS = 30.0
		sigma    = 1.15
	)
	d := math.Exp(math.Log(medianMS) + sigma*g.rng.NormFloat64())
	if level == "error" {
		// Failures skew slow: timeouts and retries dominate the tail.
		d *= 3 + g.rng.Float64()*40
	}
	if d < 0.1 {
		d = 0.1
	}
	return math.Round(d*10) / 10
}

func (g *Generator) status(level string) int {
	switch level {
	case "error":
		return []int{500, 502, 503, 504, 400, 404}[g.rng.Intn(6)]
	case "warn":
		return []int{200, 201, 400, 404, 409, 429}[g.rng.Intn(6)]
	default:
		return []int{200, 200, 200, 200, 201, 204, 304}[g.rng.Intn(7)]
	}
}

var routes = []struct {
	method string
	path   string
}{
	{"GET", "/api/v1/orders"},
	{"GET", "/api/v1/orders/%d"},
	{"POST", "/api/v1/orders"},
	{"GET", "/api/v1/users/%d/cart"},
	{"POST", "/api/v1/payments"},
	{"GET", "/api/v1/inventory/check"},
	{"PUT", "/api/v1/users/%d"},
	{"DELETE", "/api/v1/cart/items/%d"},
	{"GET", "/healthz"},
	{"GET", "/api/v1/search?q=widget&page=%d"},
}

func (g *Generator) route() (string, string) {
	r := routes[g.rng.Intn(len(routes))]
	path := r.path
	if containsVerb(path) {
		path = fmt.Sprintf(path, g.rng.Intn(90000)+1000)
	}
	return r.method, path
}

func containsVerb(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '%' && s[i+1] == 'd' {
			return true
		}
	}
	return false
}

var messagesByLevel = map[string][]string{
	"debug": {
		"cache lookup", "connection acquired from pool", "request routed",
		"serialising response", "config reloaded", "span started",
	},
	"info": {
		"request completed", "order created", "payment authorised",
		"inventory reserved", "user session refreshed", "message published",
	},
	"warn": {
		"cache miss", "retrying upstream call", "slow query detected",
		"rate limit approaching", "connection pool near capacity", "deprecated endpoint used",
	},
	"error": {
		"upstream timeout", "payment declined by processor", "database connection lost",
		"failed to reserve inventory", "unhandled exception in handler", "circuit breaker opened",
	},
}

func (g *Generator) message(level string) string {
	msgs, ok := messagesByLevel[level]
	if !ok {
		msgs = messagesByLevel["info"]
	}
	return msgs[g.rng.Intn(len(msgs))]
}

const hexDigits = "0123456789abcdef"

func token(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = hexDigits[r.Intn(len(hexDigits))]
	}
	return string(b)
}
