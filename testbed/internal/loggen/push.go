package loggen

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// This file turns the generator into a load driver that can feed several log backends the
// SAME bytes.
//
// The fan-out design matters for the benchmark's credibility. The obvious alternative —
// point one Grafana Alloy at three loki.write endpoints — looks equivalent and is not:
// each endpoint block carries its own queue, backoff and retry state, so the moment one
// backend is slower or returns 429, the three receive different data, and the throughput
// number measures Alloy's queue rather than any store. Encoding once here and POSTing the
// identical body to N URLs, waiting for all of them, removes that entire class of skew.
//
// The wire format is Loki's PushRequest, mirroring the decoder in compat/loki/push so the
// two can be round-trip tested against each other:
//
//	PushRequest    { repeated StreamAdapter streams = 1 }
//	StreamAdapter  { string labels = 1; repeated EntryAdapter entries = 2 }
//	EntryAdapter   { Timestamp timestamp = 1; string line = 2 }
//	Timestamp      { int64 seconds = 1; int32 nanos = 2 }

// Entry is one log line at a point in time.
type Entry struct {
	TSNano int64
	Line   string
}

// labelString renders a label set in Loki's `{k="v", k2="v2"}` form with keys sorted, so
// the same label map always produces the same stream identity.
func labelString(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func appendTimestamp(dst []byte, tsNano int64) []byte {
	secs, nanos := tsNano/1e9, int32(tsNano%1e9)
	var msg []byte
	msg = protowire.AppendTag(msg, 1, protowire.VarintType)
	msg = protowire.AppendVarint(msg, uint64(secs))
	msg = protowire.AppendTag(msg, 2, protowire.VarintType)
	msg = protowire.AppendVarint(msg, uint64(nanos))
	dst = protowire.AppendTag(dst, 1, protowire.BytesType)
	return protowire.AppendBytes(dst, msg)
}

func appendEntry(dst []byte, e Entry) []byte {
	var msg []byte
	msg = appendTimestamp(msg, e.TSNano)
	msg = protowire.AppendTag(msg, 2, protowire.BytesType)
	msg = protowire.AppendString(msg, e.Line)
	dst = protowire.AppendTag(dst, 2, protowire.BytesType)
	return protowire.AppendBytes(dst, msg)
}

// EncodePush builds a snappy-compressed Loki PushRequest for one stream.
func EncodePush(labels map[string]string, entries []Entry) []byte {
	var stream []byte
	stream = protowire.AppendTag(stream, 1, protowire.BytesType)
	stream = protowire.AppendString(stream, labelString(labels))
	for _, e := range entries {
		stream = appendEntry(stream, e)
	}

	var req []byte
	req = protowire.AppendTag(req, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, stream)

	return snappy.Encode(nil, req)
}

// EndpointStats accumulates per-target delivery accounting. Keeping these per URL is the
// point of the direct-push design: a slow or erroring backend is attributable rather than
// hidden inside a shared agent queue.
type EndpointStats struct {
	Requests  int64
	Entries   int64
	Bytes     int64
	Failures  int64
	RateLimit int64 // HTTP 429 — a rate-limited run is not a valid measurement
	Retries   int64 // HTTP 503 — legitimate backpressure, waited out and retried
	Elapsed   time.Duration
}

// maxPushAttempts bounds how long a batch will ride out backpressure before the run is
// declared failed. Ten attempts with the backoff below is ~5s of waiting per batch, which
// is far past "briefly busy" and firmly into "this backend cannot keep up".
const maxPushAttempts = 10

// PushSink POSTs identical encoded batches to every configured URL.
//
// It is closed-loop: a batch is not considered done until every endpoint has answered, so
// the backends cannot drift apart in what they have received. It is also the one place
// buffering is correct — the parent Sink interface deliberately does not buffer (it exists
// to observe how real agents batch), but a benchmark driver that issued one HTTP request
// per line would measure request overhead instead of the store.
type PushSink struct {
	URLs   []string
	Labels map[string]string
	Batch  int
	Client *http.Client

	mu      sync.Mutex
	pending []Entry
	stats   map[string]*EndpointStats
}

// NewPushSink builds a sink fanning out to urls. batch of 0 defaults to 1000.
func NewPushSink(urls []string, labels map[string]string, batch int) *PushSink {
	if batch <= 0 {
		batch = 1000
	}
	st := make(map[string]*EndpointStats, len(urls))
	for _, u := range urls {
		st[u] = &EndpointStats{}
	}
	return &PushSink{
		URLs:   urls,
		Labels: labels,
		Batch:  batch,
		Client: &http.Client{Timeout: 60 * time.Second},
		stats:  st,
	}
}

// Add queues an entry, flushing when the batch is full.
func (p *PushSink) Add(e Entry) error {
	p.mu.Lock()
	p.pending = append(p.pending, e)
	full := len(p.pending) >= p.Batch
	p.mu.Unlock()
	if full {
		return p.Flush()
	}
	return nil
}

// Flush encodes the pending batch once and delivers it to every endpoint concurrently,
// returning only when all have answered.
func (p *PushSink) Flush() error {
	p.mu.Lock()
	batch := p.pending
	p.pending = nil
	p.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}

	body := EncodePush(p.Labels, batch)

	var wg sync.WaitGroup
	errs := make([]error, len(p.URLs))
	for i, u := range p.URLs {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			errs[i] = p.post(u, body, len(batch))
		}(i, u)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// post delivers one batch, riding out backpressure.
//
// The three response classes are treated differently on purpose:
//
//	503 — the receiver's ingest queue is full. This is honest backpressure and exactly what
//	      a real log shipper waits out, so the driver backs off and retries. Retrying is
//	      also what makes the throughput figure meaningful: the loop runs as fast as the
//	      SLOWEST backend can actually absorb, which is the number worth publishing.
//	429 — the receiver is rate-limited by configuration. Waiting would just measure a
//	      config knob, so this is fatal and the operator is told to raise the limit.
//	4xx/5xx otherwise — a real error; fail loudly rather than silently lose lines.
func (p *PushSink) post(url string, body []byte, n int) error {
	st := p.stats[url]

	backoff := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		start := time.Now()
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")

		resp, err := p.Client.Do(req)
		if err != nil {
			st.Failures++
			return fmt.Errorf("push to %s: %w", url, err)
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		st.Requests++
		st.Elapsed += time.Since(start)

		switch {
		case resp.StatusCode/100 == 2:
			st.Entries += int64(n)
			st.Bytes += int64(len(body))
			return nil

		case resp.StatusCode == http.StatusTooManyRequests:
			st.RateLimit++
			return fmt.Errorf("push to %s: HTTP 429 rate limited — raise the receiver's ingestion limits; a throttled run is not a valid measurement (%s)", url, strings.TrimSpace(string(msg)))

		case resp.StatusCode == http.StatusServiceUnavailable:
			st.Retries++
			if attempt >= maxPushAttempts {
				st.Failures++
				return fmt.Errorf("push to %s: still returning 503 after %d attempts — the receiver cannot keep up with this offered load (%s)", url, attempt, strings.TrimSpace(string(msg)))
			}
			time.Sleep(backoff)
			if backoff < time.Second {
				backoff *= 2
			}

		default:
			st.Failures++
			return fmt.Errorf("push to %s: HTTP %d: %s", url, resp.StatusCode, strings.TrimSpace(string(msg)))
		}
	}
}

// Stats returns a snapshot of per-endpoint accounting.
func (p *PushSink) Stats() map[string]EndpointStats {
	out := make(map[string]EndpointStats, len(p.stats))
	for u, s := range p.stats {
		out[u] = *s
	}
	return out
}
