package loggen

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Formatter renders an event as one record payload, with no line terminator: framing
// belongs to the sink, because the same record goes to stdout, to a file and to a syslog
// socket with three different terminations.
//
// A payload may contain embedded newlines. The Java formatter does exactly that, and
// whether an agent treats it as one entry or many is one of the questions this lab exists
// to answer.
type Formatter interface {
	Format(e Event) string
}

// Formats holds every supported --format value, in the order they are listed in help.
var Formats = []string{"app", "json", "nginx", "logfmt", "java", "syslog", "noisy"}

// NewFormatter returns the formatter for a format name.
func NewFormatter(format string) (Formatter, error) {
	switch format {
	case "app":
		return appFormatter{}, nil
	case "json":
		return jsonFormatter{}, nil
	case "nginx":
		return nginxFormatter{}, nil
	case "logfmt":
		return logfmtFormatter{}, nil
	case "java":
		return javaFormatter{}, nil
	case "syslog":
		return syslogFormatter{}, nil
	case "noisy":
		return noisyFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q, want one of %s", format, strings.Join(Formats, ", "))
	}
}

// appFormatter renders the plain-text application log shape:
//
//	2026-01-14T10:22:31.442Z INFO  [order-svc] request completed took 247ms status=200 user_id=8811
//
// This is the shape logd's typed-range extraction targets, so the typed fields — took,
// status, user_id — appear in a fixed order at the end of every line, while the message
// text in front of them varies.
type appFormatter struct{}

func (appFormatter) Format(e Event) string {
	var b strings.Builder
	b.Grow(160)
	b.WriteString(e.TS.UTC().Format("2006-01-02T15:04:05.000Z"))
	b.WriteByte(' ')
	// Levels padded to five columns, as most real app loggers do.
	fmt.Fprintf(&b, "%-5s ", strings.ToUpper(e.Level))
	b.WriteString(e.Service)
	b.WriteByte(' ')
	b.WriteString(appMessage(e))
	return b.String()
}

// appMessage renders one of a handful of templates per level. Real services emit a small
// number of message shapes with varying parameters, and that distribution — not free text
// — is what logd's template routing depends on.
func appMessage(e Event) string {
	ms := int(e.DurationMS + 0.5)
	switch e.Level {
	case "error":
		switch e.Template % 3 {
		case 0:
			return fmt.Sprintf("payment authorization failed order=%s code=%s took=%dms",
				e.OrderID, declineCodes[e.Template%len(declineCodes)], ms)
		case 1:
			return fmt.Sprintf("inventory reservation failed sku=%s reason=%s took=%dms",
				e.SKU, failReasons[e.Template%len(failReasons)], ms)
		default:
			return fmt.Sprintf("upstream %s request failed status=%d took=%dms trace_id=%s",
				upstreams[e.Template%len(upstreams)], e.Status, ms, e.TraceID[:16])
		}
	case "warn":
		switch e.Template % 2 {
		case 0:
			return fmt.Sprintf("inventory lock contention on sku=%s retry=%d waited=%dms",
				e.SKU, 1+e.Template%4, ms)
		default:
			return fmt.Sprintf("upstream %s slow response took=%dms threshold=500ms",
				upstreams[e.Template%len(upstreams)], ms)
		}
	case "info":
		switch e.Template % 2 {
		case 0:
			return fmt.Sprintf("%s %s %d %dms trace_id=%s", e.Method, e.Path, e.Status, ms, e.TraceID[:16])
		default:
			return fmt.Sprintf("order %s confirmed items=%d total_cents=%d took=%dms",
				e.OrderID, 1+e.Template%6, 1999+e.UserID*7, ms)
		}
	default:
		switch e.Template % 2 {
		case 0:
			return fmt.Sprintf("cache lookup key=order:%s hit=%t took=%dms",
				e.OrderID, e.Template%3 != 0, ms)
		default:
			return fmt.Sprintf("db connection acquired pool=primary idle=%d took=%dms",
				e.Template%12, ms)
		}
	}
}

var (
	declineCodes = []string{"card_declined", "insufficient_funds", "expired_card", "do_not_honor"}
	failReasons  = []string{"out_of_stock", "warehouse_timeout", "sku_not_found", "lock_timeout"}
	upstreams    = []string{"payments-api", "inventory-api", "pricing-api", "shipping-api"}
	orderStates  = []string{"pending", "confirmed", "packed", "shipped", "delivered", "cancelled"}
	loggers      = []string{"http.server", "db.pool", "cache.redis", "auth.jwt"}
)

// jsonFormatter renders one JSON object per line with nested fields, which is what
// exercises an agent's JSON label extraction and produces the larger lines.
type jsonFormatter struct{}

// jsonLine is flat, as structured loggers actually emit. Optional fields are omitted
// rather than sent empty, so each logger produces its own field set.
type jsonLine struct {
	TS         string  `json:"ts"`
	Level      string  `json:"level"`
	Logger     string  `json:"logger"`
	Msg        string  `json:"msg"`
	Method     string  `json:"method,omitempty"`
	Path       string  `json:"path,omitempty"`
	Status     int     `json:"status,omitempty"`
	DurationMS float64 `json:"duration_ms"`
	Bytes      int     `json:"bytes,omitempty"`
	Query      string  `json:"query,omitempty"`
	Rows       int     `json:"rows,omitempty"`
	Key        string  `json:"key,omitempty"`
	OrderID    string  `json:"order_id,omitempty"`
	CustomerID string  `json:"customer_id,omitempty"`
	TraceID    string  `json:"trace_id"`
	SpanID     string  `json:"span_id"`
}

func (jsonFormatter) Format(e Event) string {
	l := jsonLine{
		// RFC3339 with milliseconds. Nanosecond precision deliberately does not appear
		// in the log text: any nanoseconds the receiver sees came from the agent, not
		// from the line.
		TS:         e.TS.UTC().Format("2006-01-02T15:04:05.000Z"),
		Level:      e.Level,
		DurationMS: e.DurationMS,
		TraceID:    e.TraceID[:16],
		SpanID:     e.SpanID[:8],
	}

	switch loggers[e.Template%len(loggers)] {
	case "db.pool":
		l.Logger, l.Msg = "db.pool", "query executed"
		l.Query = "SELECT * FROM orders WHERE customer_id = $1"
		l.Rows = e.Template % 200
		l.CustomerID = fmt.Sprintf("CUS-%06d", 100000+e.UserID)
	case "cache.redis":
		l.Logger, l.Msg = "cache.redis", "cache miss"
		l.Key = "order:" + e.OrderID
	case "auth.jwt":
		l.Logger, l.Msg = "auth.jwt", "token validated"
		l.CustomerID = fmt.Sprintf("CUS-%06d", 100000+e.UserID)
	default:
		l.Logger, l.Msg = "http.server", "request completed"
		l.Method, l.Path, l.Status = e.Method, e.Path, e.Status
		l.Bytes = e.BodyBytes
		l.OrderID = e.OrderID
	}

	out, err := json.Marshal(l)
	if err != nil {
		// Cannot happen for this struct, but a generator that silently stops is worse
		// than one that emits a visible marker.
		return `{"level":"error","logger":"loggen","msg":"failed to marshal event"}`
	}
	return string(out)
}

// nginxFormatter renders the nginx "combined" access log, with the upstream response time
// appended as most production configs do:
//
//	10.4.2.9 - - [10/Aug/2026:14:30:00 +0000] "GET /api/v1/orders HTTP/1.1" 200 1234 "-" "Mozilla/5.0 ..." 0.247
//
// Classic unstructured text: no level, no service name, and the fields that matter
// (status, bytes, duration) are positional rather than named.
type nginxFormatter struct{}

func (nginxFormatter) Format(e Event) string {
	referer := "-"
	if e.Referer != "" {
		referer = e.Referer
	}
	return fmt.Sprintf(`%s - %s [%s] "%s %s HTTP/1.1" %d %d %q %q %.3f`,
		e.RemoteAddr,
		dashIfEmpty(e.RemoteUser),
		e.TS.UTC().Format("02/Jan/2006:15:04:05 -0700"),
		e.Method,
		e.Path,
		e.Status,
		e.BodyBytes,
		referer,
		e.UserAgent,
		e.DurationMS/1000,
	)
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// logfmtFormatter renders key=value pairs:
//
//	ts=2026-08-10T14:30:00.000Z level=warn msg="cache miss" latency_ms=13.4
//
// Values containing spaces are quoted; everything else is bare. That inconsistency is the
// whole difficulty of the format.
type logfmtFormatter struct{}

func (logfmtFormatter) Format(e Event) string {
	var b strings.Builder
	b.Grow(180)
	b.WriteString("ts=")
	b.WriteString(e.TS.UTC().Format("2006-01-02T15:04:05.000Z"))
	b.WriteString(" level=")
	b.WriteString(e.Level)
	b.WriteByte(' ')
	b.WriteString(logfmtMessage(e))
	b.WriteString(" duration_ms=")
	b.WriteString(strconv.FormatFloat(e.DurationMS, 'f', -1, 64))
	return b.String()
}

// logfmtMessage renders the msg= field and the identifiers that go with it. Note that
// some values are quoted and some are bare — that inconsistency is the whole difficulty
// of parsing real logfmt.
func logfmtMessage(e Event) string {
	switch e.Level {
	case "error":
		return fmt.Sprintf("msg=%s order_id=%s reason=%s",
			logfmtValue("order fulfilment failed"), e.OrderID, failReasons[e.Template%len(failReasons)])
	case "warn":
		return fmt.Sprintf("msg=%s order_id=%s sku=%s attempt=%d",
			logfmtValue("stock reservation retry"), e.OrderID, e.SKU, 1+e.Template%4)
	case "info":
		if e.Template%2 == 0 {
			from := orderStates[e.Template%len(orderStates)]
			to := orderStates[(e.Template+1)%len(orderStates)]
			return fmt.Sprintf("msg=%s order_id=%s from=%s to=%s",
				logfmtValue("order state transition"), e.OrderID, from, to)
		}
		return fmt.Sprintf("msg=%s order_id=%s customer_id=CUS-%06d items=%d total_cents=%d",
			logfmtValue("order created"), e.OrderID, 100000+e.UserID, 1+e.Template%6, 1999+e.UserID*7)
	default:
		return fmt.Sprintf("msg=%s key=order:%s hit=%t",
			logfmtValue("cache lookup"), e.OrderID, e.Template%3 != 0)
	}
}

// logfmtValue quotes only when it has to, which is what makes real logfmt awkward to
// parse: the same key is bare on one line and quoted on the next.
func logfmtValue(s string) string {
	if s == "" {
		return `""`
	}
	if strings.ContainsAny(s, " \"=\\") {
		return strconv.Quote(s)
	}
	return s
}

// javaFormatter renders a multi-line exception: a header line, an exception, 20-60 stack
// frames, and usually a "Caused by" chain.
//
// The payload contains embedded newlines on purpose. Whether an agent ships this as one
// entry with escaped newlines, or as forty separate entries, is exactly what the
// multiline parsers in the agent configs are there to reveal.
type javaFormatter struct{}

var javaExceptions = []struct {
	class   string
	message string
}{
	{"java.lang.NullPointerException", `Cannot invoke "com.example.Order.getId()" because "order" is null`},
	{"java.lang.IllegalStateException", "connection pool exhausted after 30000ms"},
	{"java.util.concurrent.TimeoutException", "future timed out after 5000 milliseconds"},
	{"org.springframework.dao.DataAccessResourceFailureException", "unable to acquire JDBC connection"},
	{"java.io.IOException", "broken pipe writing response body"},
	{"com.example.OrderProcessingException", "order 88213 failed validation in stage RESERVE"},
}

var javaPackages = []string{
	"com.example.order.OrderService",
	"com.example.order.OrderController",
	"com.example.payment.PaymentGateway",
	"com.example.inventory.InventoryClient",
	"org.springframework.web.servlet.DispatcherServlet",
	"org.springframework.transaction.interceptor.TransactionInterceptor",
	"org.apache.catalina.core.ApplicationFilterChain",
	"java.util.concurrent.ThreadPoolExecutor",
	"jdk.internal.reflect.NativeMethodAccessorImpl",
	"io.netty.channel.AbstractChannelHandlerContext",
}

var javaMethods = []string{
	"process", "handle", "invoke", "doFilter", "execute", "call", "run",
	"submit", "reserve", "authorise", "serialize", "await",
}

func (javaFormatter) Format(e Event) string {
	var b strings.Builder
	b.Grow(2048)

	// Header line, in the same shape as the app format, so a receiver sees a familiar
	// first line followed by an unfamiliar continuation.
	b.WriteString(e.TS.UTC().Format("2006-01-02T15:04:05.000Z"))
	b.WriteString(" ERROR ")
	b.WriteString(e.Service)
	b.WriteByte(' ')
	fmt.Fprintf(&b, "invoice generation failed order=%s reason=%s took=%dms",
		e.OrderID, failReasons[e.Template%len(failReasons)], int(e.DurationMS+0.5))
	b.WriteByte('\n')

	exc := javaExceptions[e.StackSeed%len(javaExceptions)]
	fmt.Fprintf(&b, "%s: %s\n", exc.class, exc.message)
	writeFrames(&b, e.StackSeed, e.StackFrames)

	if e.StackCausedBy > 0 {
		cause := javaExceptions[(e.StackSeed+3)%len(javaExceptions)]
		fmt.Fprintf(&b, "Caused by: %s: %s\n", cause.class, cause.message)
		writeFrames(&b, e.StackSeed+7, e.StackCausedBy)
		fmt.Fprintf(&b, "\t... %d more", e.StackFrames-e.StackCausedBy)
		return b.String()
	}

	// Trim the trailing newline: framing belongs to the sink.
	return strings.TrimRight(b.String(), "\n")
}

func writeFrames(b *strings.Builder, seed, n int) {
	for i := 0; i < n; i++ {
		pkg := javaPackages[(seed+i)%len(javaPackages)]
		method := javaMethods[(seed+i*3)%len(javaMethods)]
		file := pkg[strings.LastIndexByte(pkg, '.')+1:]
		line := 40 + (seed*7+i*13)%900
		fmt.Fprintf(b, "\tat %s.%s(%s.java:%d)\n", pkg, method, file, line)
	}
}

// syslogFormatter renders RFC 5424, which is what gen-syslog sends straight to the
// receiver with no agent in between:
//
//	<134>1 2026-08-10T14:30:00.123456Z host order-svc 1234 ID47 [logd@32473 env="dev"] request completed
type syslogFormatter struct{}

// Facility 16 (local0), per RFC 5424 section 6.2.1. PRI = facility*8 + severity.
const syslogFacility = 16

// syslogSeverity maps a level name to an RFC 5424 severity.
func syslogSeverity(level string) int {
	switch level {
	case "error":
		return 3 // Error
	case "warn":
		return 4 // Warning
	case "info":
		return 6 // Informational
	default:
		return 7 // Debug
	}
}

func (syslogFormatter) Format(e Event) string {
	pri := syslogFacility*8 + syslogSeverity(e.Level)

	var b strings.Builder
	b.Grow(256)
	fmt.Fprintf(&b, "<%d>1 ", pri)
	// RFC 5424 wants at least millisecond precision; microseconds are used here so the
	// receiver can show what survives the trip.
	b.WriteString(e.TS.UTC().Format("2006-01-02T15:04:05.000000Z"))
	b.WriteByte(' ')
	b.WriteString(e.Pod) // HOSTNAME
	b.WriteByte(' ')
	b.WriteString(e.Service) // APP-NAME
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(1000 + e.UserID%9000)) // PROCID
	b.WriteString(" ID")
	b.WriteString(strconv.Itoa(e.Status)) // MSGID
	// STRUCTURED-DATA, which is the part most syslog parsers get wrong.
	fmt.Fprintf(&b, ` [logd@32473 env="%s" level="%s" took_ms="%s"] `,
		e.Env, e.Level, strconv.FormatFloat(e.DurationMS, 'f', -1, 64))
	b.WriteString(appMessage(e))
	return b.String()
}

// noisyFormatter emits deliberately hostile records, cycling through a fixed set so every
// case is reached and the cycle is reproducible. The point is to find out what agents do
// with garbage before logd has to.
type noisyFormatter struct{}

// noisyCase names each hostile shape, in cycle order.
const noisyCases = 8

func (noisyFormatter) Format(e Event) string {
	// A plausible batch-job prefix, so the line looks like something a real legacy
	// service emits rather than a lab marker. The hostile part is the payload.
	head := e.TS.UTC().Format("2006-01-02T15:04:05.000Z") + " " +
		strings.ToUpper(e.Level) + " " + e.Service + " "

	switch e.NoisyCase % noisyCases {
	case 0:
		// A very large single line: a batch job dumping a payload inline.
		return head + fmt.Sprintf("export chunk written rows=%d payload=%s",
			1000+e.Template, strings.Repeat("x", e.HugeBytes))
	case 1:
		// Invalid UTF-8: a mis-decoded legacy field.
		return head + "record decoded from cp1252 name=\xff\xfe broken=\xc3 truncated=\xe2\x82"
	case 2:
		// ANSI colour codes, as any TTY-detecting logger emits under docker.
		return head + "\x1b[31mFAILED\x1b[0m batch=nightly-reconcile \x1b[1;33m2 warnings\x1b[0m"
	case 3:
		// Embedded NUL bytes, from a fixed-width binary field.
		return head + "fixed width field name=alice\x00\x00\x00 dept=ops\x00"
	case 4:
		// Bare carriage returns: progress output with no newline.
		return head + "reindex progress: 10%\r reindex progress: 50%\r reindex progress: 100%"
	case 5:
		// An empty record. Some agents drop these, some ship them.
		return ""
	case 6:
		// Whitespace only.
		return "   \t  "
	default:
		// Control characters and a very long single token.
		return head + "malformed row \x01\x02\x07\x08 vertical\x0btab formfeed\x0c checksum=" +
			strings.Repeat("z", 4096)
	}
}

func tier(userID int) string {
	switch userID % 4 {
	case 0:
		return "gold"
	case 1:
		return "silver"
	case 2:
		return "bronze"
	default:
		return "free"
	}
}
