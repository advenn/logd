// Command crashpush measures what a crash loses of acknowledged pushes (E-8). It has two
// halves, run either side of a kill -9 by scripts/e8-crashloss.sh:
//
//	crashpush push   -url http://127.0.0.1:7100 -run r1 -rate 10000 -batch 100 -out push.json
//	crashpush verify -url http://127.0.0.1:7100 -push push.json -kill-ns <ns> -out verify.json
//
// push sends lines carrying a sequence number ("crashpush seq=N"), in batches, at a fixed
// rate, one request at a time, and records every batch's send and acknowledgement time. It
// stops at the first request that fails to connect or is cut off — the daemon is gone — or
// after -duration. Requests are sequential, so the acknowledged lines are exactly seq 0..K.
//
// verify queries every line of the run back after the restart and reports which acknowledged
// sequence numbers are missing, and the span of time they cover.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/advenn/logd/testbed/internal/loggen"
)

// Batch is one push request.
type Batch struct {
	First, Last int64 // sequence numbers it carried
	SentNS      int64
	AckNS       int64 // 0 if never acknowledged
	Status      int   // HTTP status, 0 if the request failed
}

// PushLog is what push writes and verify reads.
type PushLog struct {
	Run        string
	Rate       int
	Batch      int
	StartNS    int64 // event time of seq 0; seq n is stamped StartNS + n*1e9/Rate
	Batches    []Batch
	LastAcked  int64 // highest acknowledged sequence number, -1 if none
	StopReason string
}

// Result is what verify writes.
type Result struct {
	Run             string
	Acked           int64   // acknowledged lines (seq 0..LastAcked)
	Found           int64   // distinct sequence numbers returned after the restart
	LostAcked       int64   // acknowledged and missing
	LostFirst       int64   // lowest missing acknowledged seq, -1 if none
	LostLast        int64   // highest
	LostTail        bool    // the missing lines are exactly the newest acknowledged ones
	LostSpanMS      float64 // event-time span of the missing lines, at the push rate
	LostAckSpanMS   float64 // from the first to the last acknowledgement of a missing line
	OldestLostAgeMS float64 // how long before the kill the oldest missing line was acknowledged
	UnackedFound    int64   // lines whose push was never acknowledged but were stored
	Duplicates      int64
	KillAfterMS     float64 // kill time after the first push
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: crashpush push|verify [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "push":
		err = push(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "crashpush:", err)
		os.Exit(1)
	}
}

func labels(run string) map[string]string {
	return map[string]string{"job": "crashpush", "run": run}
}

func push(args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	base := fs.String("url", "http://127.0.0.1:7100", "logd base URL")
	run := fs.String("run", "r1", "run id (a stream label, so runs never mix)")
	rate := fs.Int("rate", 10000, "lines per second")
	batch := fs.Int("batch", 100, "lines per push request")
	maxDur := fs.Duration("duration", 60*time.Second, "stop after this long even if the daemon is alive")
	out := fs.String("out", "push.json", "where to write the push log")
	fs.Parse(args)
	if *rate <= 0 || *batch <= 0 {
		return errors.New("-rate and -batch must be positive")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	start := time.Now()
	pl := PushLog{Run: *run, Rate: *rate, Batch: *batch, StartNS: start.UnixNano(), LastAcked: -1}
	step := time.Second / time.Duration(*rate)
	lbl := labels(*run)
	for seq := int64(0); ; seq += int64(*batch) {
		// Batch k is due when its first line is: a fixed rate, not as fast as the daemon goes.
		if d := time.Until(start.Add(time.Duration(seq) * step)); d > 0 {
			time.Sleep(d)
		}
		if time.Since(start) > *maxDur {
			pl.StopReason = "duration reached"
			break
		}
		entries := make([]loggen.Entry, *batch)
		for i := range entries {
			n := seq + int64(i)
			entries[i] = loggen.Entry{TSNano: pl.StartNS + n*int64(step), Line: fmt.Sprintf("crashpush seq=%d", n)}
		}
		b := Batch{First: seq, Last: seq + int64(*batch) - 1, SentNS: time.Now().UnixNano()}
		status, err := post(client, *base+"/loki/api/v1/push", loggen.EncodePush(lbl, entries))
		b.Status = status
		if err == nil && status == http.StatusNoContent {
			b.AckNS = time.Now().UnixNano()
			pl.LastAcked = b.Last
		}
		pl.Batches = append(pl.Batches, b)
		if err != nil {
			pl.StopReason = err.Error() // the daemon is gone
			break
		}
		if status != http.StatusNoContent {
			pl.StopReason = fmt.Sprintf("HTTP %d", status)
			break
		}
	}
	fmt.Fprintf(os.Stderr, "crashpush: %d batches, last acknowledged seq %d, stopped: %s\n", len(pl.Batches), pl.LastAcked, pl.StopReason)
	data, err := json.Marshal(pl) // compact: a run at 100 requests/s logs thousands of batches
	if err != nil {
		return err
	}
	return os.WriteFile(*out, append(data, '\n'), 0o644)
}

func post(c *http.Client, u string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

var seqRE = regexp.MustCompile(`seq=(\d+)`)

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	base := fs.String("url", "http://127.0.0.1:7100", "logd base URL")
	in := fs.String("push", "push.json", "the push log")
	killNS := fs.Int64("kill-ns", 0, "when the daemon was killed (unix ns)")
	out := fs.String("out", "verify.json", "where to write the result")
	fs.Parse(args)

	var pl PushLog
	data, err := os.ReadFile(*in)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &pl); err != nil {
		return err
	}
	found, err := queryRun(*base, pl)
	if err != nil {
		return err
	}

	res := Result{Run: pl.Run, Acked: pl.LastAcked + 1, LostFirst: -1, LostLast: -1}
	if *killNS > 0 && len(pl.Batches) > 0 {
		res.KillAfterMS = ms(*killNS - pl.Batches[0].SentNS)
	}
	seen := map[int64]bool{}
	for _, s := range found {
		if seen[s] {
			res.Duplicates++
			continue
		}
		seen[s] = true
		if s > pl.LastAcked {
			res.UnackedFound++
		}
	}
	res.Found = int64(len(seen))
	ackOf := func(seq int64) int64 { return pl.Batches[seq/int64(pl.Batch)].AckNS }
	for s := int64(0); s <= pl.LastAcked; s++ {
		if seen[s] {
			continue
		}
		res.LostAcked++
		if res.LostFirst < 0 {
			res.LostFirst = s
		}
		res.LostLast = s
	}
	if res.LostAcked > 0 {
		step := 1e3 / float64(pl.Rate) // ms per line
		res.LostSpanMS = float64(res.LostLast-res.LostFirst+1) * step
		res.LostAckSpanMS = ms(ackOf(res.LostLast) - ackOf(res.LostFirst))
		if *killNS > 0 {
			res.OldestLostAgeMS = ms(*killNS - ackOf(res.LostFirst))
		}
		// A tail loss: nothing at or after the first missing line survived among the
		// acknowledged ones, so the missing set is exactly seq LostFirst..LastAcked.
		res.LostTail = res.LostLast == pl.LastAcked && res.LostAcked == pl.LastAcked-res.LostFirst+1
	}
	return writeJSON(*out, res)
}

// queryRun returns the sequence numbers of every stored line of the run, in time order.
func queryRun(base string, pl PushLog) ([]int64, error) {
	var lastNS int64 = pl.StartNS
	if n := len(pl.Batches); n > 0 {
		lastNS = pl.StartNS + (pl.Batches[n-1].Last+1)*int64(time.Second)/int64(pl.Rate)
	}
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`{job="crashpush", run=%q}`, pl.Run))
	q.Set("start", strconv.FormatInt(pl.StartNS-int64(time.Second), 10))
	q.Set("end", strconv.FormatInt(lastNS+int64(time.Second), 10))
	q.Set("limit", "1000000")
	q.Set("direction", "forward")
	resp, err := http.Get(base + "/loki/api/v1/query_range?" + q.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query_range: HTTP %d: %s", resp.StatusCode, body)
	}
	var r struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	var out []int64
	for _, s := range r.Data.Result {
		for _, v := range s.Values {
			m := seqRE.FindStringSubmatch(v[1])
			if m == nil {
				return nil, fmt.Errorf("line without a sequence number: %q", v[1])
			}
			n, _ := strconv.ParseInt(m[1], 10, 64)
			out = append(out, n)
		}
	}
	if len(out) >= 1000000 {
		return nil, errors.New("query hit its 1,000,000-line limit; push for less time")
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func ms(ns int64) float64 { return float64(ns) / 1e6 }

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
