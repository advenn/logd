package loggen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// hdfsSample is one real line of each form Loghub HDFS_v1 renders a block size in, plus lines
// without one, verbatim from HDFS.log (CRLF as in the file).
const hdfsSample = "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010\r\n" +
	"081109 203518 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /mnt/hadoop/mapred/system/job_200811092030_0001/job.jar. blk_-1608999687919862906\r\n" +
	"081109 203519 145 INFO dfs.DataNode$PacketResponder: Received block blk_-1608999687919862906 of size 91178 from /10.250.10.6\r\n" +
	"081109 203519 29 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.10.6:50010 is added to blk_-1608999687919862906 size 91178\r\n" +
	"081109 203521 143 INFO dfs.DataNode$DataXceiver: Received block blk_-1608999687919862906 src: /10.251.215.16:52002 dest: /10.251.215.16:50010 of size 91178\r\n" +
	"081109 204917 35 WARN dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: Redundant addStoredBlock request received for blk_3888635850409849568 on 10.251.107.227:50010 size 67108864\r\n" +
	"081109 213823 29 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: addStoredBlock request received for blk_5398314277015661293 on 10.251.199.86:50010 size 67108864 But it does not belong to any file.\r\n" +
	"081110 105519 19 INFO dfs.FSDataset: Deleting block blk_-7066263958233651271 file /mnt/hadoop/dfs/data/current/subdir28/blk_-7066263958233651271\r\n" +
	"081110 101143 29 INFO dfs.DataBlockScanner: Verification succeeded for blk_-4980916519894289629\r\n" +
	"081111 095149 26 INFO dfs.PendingReplicationBlocks$PendingReplicationMonitor: PendingReplicationMonitor timed out block blk_-4378282722431181818\r\n"

var sizeRe = regexp.MustCompile(`size ([0-9]+)`)

func hdfsOpts(n int) ReplayOpts {
	return ReplayOpts{
		Count:      n,
		Base:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Step:       time.Millisecond,
		Labels:     map[string]string{"app": "hdfs"},
		ValueRegex: sizeRe,
		ValueName:  "size_bytes",
	}
}

func replayAll(t *testing.T, src string, o ReplayOpts) ([]Record, ReplayStats) {
	t.Helper()
	var out []Record
	st, err := Replay(strings.NewReader(src), o, func(r Record) error {
		out = append(out, r)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out, st
}

// TestReplayTimestampsStrictlyMonotonic: ts_ns is the parity diff's primary key, and the
// source timestamps cannot serve (one-second resolution, thousands of lines per second).
func TestReplayTimestampsStrictlyMonotonic(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&b, "081109 203518 %d INFO dfs.DataNode$PacketResponder: line %d\r\n", i, i)
	}
	o := hdfsOpts(1 << 30)
	recs, st := replayAll(t, b.String(), o)
	if len(recs) != 3000 || st.Lines != 3000 {
		t.Fatalf("replayed %d (stats %d), want 3000", len(recs), st.Lines)
	}
	for i, r := range recs {
		if r.I != i {
			t.Fatalf("record %d has I=%d", i, r.I)
		}
		if want := o.Base.Add(time.Duration(i) * o.Step).UnixNano(); r.TSNano != want {
			t.Fatalf("record %d ts %d, want base+i*step = %d", i, r.TSNano, want)
		}
		if i > 0 && r.TSNano <= recs[i-1].TSNano {
			t.Fatalf("timestamps not strictly increasing at %d", i)
		}
	}
}

// TestReplayKeepsLineTextIntact: only the terminator goes. A CR inside a line, tabs, double
// spaces and trailing spaces are all part of the text the engines must store and match.
func TestReplayKeepsLineTextIntact(t *testing.T) {
	src := "a  b\tc \r\n" + "mid\rcr\r\n" + "lf only\n" + "\n" + "no terminator at eof\r"
	recs, st := replayAll(t, src, ReplayOpts{Count: 100, Base: time.Unix(0, 0), Step: time.Millisecond})
	want := []string{"a  b\tc ", "mid\rcr", "lf only", "no terminator at eof"}
	if len(recs) != len(want) {
		t.Fatalf("got %d lines, want %d: %+v", len(recs), len(want), recs)
	}
	for i, w := range want {
		if recs[i].Line != w {
			t.Errorf("line %d: got %q, want %q", i, recs[i].Line, w)
		}
	}
	if st.Skipped != 1 {
		t.Errorf("empty lines skipped: %d, want 1", st.Skipped)
	}
}

func TestReplayStopsAtCount(t *testing.T) {
	recs, st := replayAll(t, hdfsSample, hdfsOpts(4))
	if len(recs) != 4 || st.Lines != 4 {
		t.Fatalf("got %d lines, want 4", len(recs))
	}
	n, err := CountReplayLines(strings.NewReader(hdfsSample+"\n\n"), 1<<30)
	if err != nil || n != 10 {
		t.Errorf("CountReplayLines = %d, %v; want 10 (empty lines are not replayed)", n, err)
	}
	if n, _ := CountReplayLines(strings.NewReader(hdfsSample), 4); n != 4 {
		t.Errorf("CountReplayLines with max 4 = %d", n)
	}
}

// TestReplayValueGroundTruth pins the five real line forms that carry a size, and that the
// lines without one carry none -- including the two forms where the number is followed by
// more text and the three where it ends the line, which is what makes a single Loki
// `| pattern` unable to express the extraction.
func TestReplayValueGroundTruth(t *testing.T) {
	recs, st := replayAll(t, hdfsSample, hdfsOpts(100))
	want := []int64{-1, -1, 91178, 91178, 91178, 67108864, 67108864, -1, -1, -1}
	for i, r := range recs {
		switch {
		case want[i] < 0 && r.Value != nil:
			t.Errorf("line %d: got value %d, want none: %q", i, *r.Value, r.Line)
		case want[i] >= 0 && (r.Value == nil || *r.Value != want[i] || r.ValueName != "size_bytes"):
			t.Errorf("line %d: got %v %q, want %d size_bytes: %q", i, r.Value, r.ValueName, want[i], r.Line)
		}
	}
	if st.WithValue != 5 || st.MultiValue != 0 || st.Unparsable != 0 {
		t.Errorf("stats %+v", st)
	}

	// Two occurrences: ground truth takes the first, and the stats say so, because logd's
	// any-occurrence semantics can disagree with it on exactly these lines.
	recs, st = replayAll(t, "x size 5 then size 900\n x size 99999999999999999999\n", hdfsOpts(10))
	if recs[0].Value == nil || *recs[0].Value != 5 || st.MultiValue != 1 {
		t.Errorf("first-occurrence ground truth: %v, multi=%d", recs[0].Value, st.MultiValue)
	}
	if recs[1].Value != nil || st.Unparsable != 1 {
		t.Errorf("an int64 overflow must carry no value and be counted: %v, unparsable=%d", recs[1].Value, st.Unparsable)
	}
}

func TestComponentLabelMapping(t *testing.T) {
	for line, want := range map[string]string{
		"081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving":                      "datanode",
		"081109 203518 145 INFO dfs.DataNode$PacketResponder: Received":                   "datanode",
		"081109 203518 145 INFO dfs.DataNode$DataTransfer: x":                             "datanode",
		"081109 203518 145 INFO dfs.DataNode$BlockReceiver: x":                            "datanode",
		"081109 203518 145 INFO dfs.DataNode: x":                                          "datanode",
		"081109 203518 35 INFO dfs.FSNamesystem: BLOCK*":                                  "fsnamesystem",
		"081110 105519 19 INFO dfs.FSDataset: Deleting":                                   "fsdataset",
		"081110 101143 29 INFO dfs.DataBlockScanner: Verification":                        "datablockscanner",
		"081111 095149 26 INFO dfs.PendingReplicationBlocks$PendingReplicationMonitor: t": "pendingreplicationblocks",
		"Toplevel: no package":                                                            "other", // too few fields
		"081109 203518 143 INFO dfs.DataNode no colon":                                    "other",
		"081109 203518 143 INFO :":                                                        "other",
		"081109 203518 143 INFO dfs.$Inner: x":                                            "other",
		"081109 203518 143 INFO dfs.Daten-ode: x":                                         "other",
		"": "other",
	} {
		if got := componentOf(line); got != want {
			t.Errorf("componentOf(%q) = %q, want %q", line, got, want)
		}
	}
	if lv := levelOf("081109 204917 35 WARN dfs.FSNamesystem: x"); lv != "warn" {
		t.Errorf("level: %q", lv)
	}
}

// TestComponentCardinalityIsCapped: a file with a different layout must collapse into
// "other" rather than mint a stream per distinct token.
func TestComponentCardinalityIsCapped(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&b, "081109 203518 1 INFO dfs.Class%d: x\n", i)
	}
	b.WriteString("garbage\n")
	_, st := replayAll(t, b.String(), ReplayOpts{Count: 1000, Base: time.Unix(0, 0), Step: time.Millisecond, Labels: map[string]string{"app": "x"}})
	if len(st.Components) > maxComponentValues {
		t.Fatalf("%d component values, cap is %d: %v", len(st.Components), maxComponentValues, st.Components)
	}
	if st.Components["other"] != 50-(maxComponentValues-1)+1 {
		t.Errorf("other = %d, want the %d classes past the cap plus the garbage line", st.Components["other"], 50-(maxComponentValues-1))
	}
}

// TestReplayLabelsShareOneMapPerStream: millions of records over a handful of streams must
// not each carry their own map, and every record carries the static labels plus component.
func TestReplayLabelsShareOneMapPerStream(t *testing.T) {
	recs, st := replayAll(t, hdfsSample, hdfsOpts(100))
	if len(st.Components) != 5 {
		t.Errorf("components %v, want 5 (datanode, fsnamesystem, fsdataset, datablockscanner, pendingreplicationblocks)", st.Components)
	}
	byComp := map[string]map[string]string{}
	for _, r := range recs {
		if r.Labels["app"] != "hdfs" || r.Labels[ComponentLabel] == "" || len(r.Labels) != 2 {
			t.Fatalf("labels %v", r.Labels)
		}
		c := r.Labels[ComponentLabel]
		if m, ok := byComp[c]; ok && fmt.Sprintf("%p", m) != fmt.Sprintf("%p", r.Labels) {
			t.Errorf("component %s: records carry different label maps", c)
		}
		byComp[c] = r.Labels
	}
}

func TestReplayRejects(t *testing.T) {
	ok := hdfsOpts(10)
	for name, c := range map[string]struct {
		src string
		o   func(ReplayOpts) ReplayOpts
	}{
		"invalid utf-8":       {"fine\n\xff\xfe broken\n", func(o ReplayOpts) ReplayOpts { return o }},
		"static component":    {hdfsSample, func(o ReplayOpts) ReplayOpts { o.Labels = map[string]string{ComponentLabel: "x"}; return o }},
		"regex without group": {hdfsSample, func(o ReplayOpts) ReplayOpts { o.ValueRegex = regexp.MustCompile(`size [0-9]+`); return o }},
		"regex without name":  {hdfsSample, func(o ReplayOpts) ReplayOpts { o.ValueName = ""; return o }},
		"zero count":          {hdfsSample, func(o ReplayOpts) ReplayOpts { o.Count = 0; return o }},
		"zero step":           {hdfsSample, func(o ReplayOpts) ReplayOpts { o.Step = 0; return o }},
		"line over 1 MiB":     {strings.Repeat("x", 2<<20) + "\n", func(o ReplayOpts) ReplayOpts { return o }},
	} {
		if _, err := Replay(strings.NewReader(c.src), c.o(ok), func(Record) error { return nil }); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestReplayManifestIsCorpusCompatible: window.sh reads ts_ns and labels.app off the first
// and last line, and ReadManifest must give back exactly what was written, including the
// generic value. A corpus-mode record must still serialise without the new keys.
func TestReplayManifestIsCorpusCompatible(t *testing.T) {
	recs, _ := replayAll(t, hdfsSample, hdfsOpts(100))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	var first map[string]any
	if err := json.Unmarshal(bytes.SplitN(buf.Bytes(), []byte("\n"), 2)[0], &first); err != nil {
		t.Fatal(err)
	}
	if _, ok := first["ts_ns"].(float64); !ok || first["labels"].(map[string]any)["app"] != "hdfs" {
		t.Errorf("first manifest line lacks ts_ns or labels.app: %v", first)
	}
	var back []Record
	if err := ReadManifest(&buf, func(r Record) error { back = append(back, r); return nil }); err != nil {
		t.Fatal(err)
	}
	for i := range recs {
		a, b := recs[i], back[i]
		if a.TSNano != b.TSNano || a.Line != b.Line || (a.Value == nil) != (b.Value == nil) ||
			(a.Value != nil && *a.Value != *b.Value) || a.ValueName != b.ValueName {
			t.Errorf("record %d did not round-trip: %+v vs %+v", i, a, b)
		}
	}

	raw, _ := json.Marshal(Record{I: 1, TSNano: 2, Labels: map[string]string{"app": "checkout"}, Line: "x"})
	if strings.Contains(string(raw), "value") {
		t.Errorf("corpus record grew new keys: %s", raw)
	}
}
