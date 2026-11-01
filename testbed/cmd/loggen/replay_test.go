package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/testbed/internal/loggen"
)

// TestReplayAutoBaseEndsJustBeforeNow: "auto" must place the replay so it ENDS a minute
// before now, as corpus mode does -- a replay dated in the past is accepted by Loki and then
// silently invisible. The file here is shorter than -replay-count, which is the case where
// placing the base by the requested count would end the replay early.
func TestReplayAutoBaseEndsJustBeforeNow(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "HDFS.log")
	var b strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&b, "081109 203518 %d INFO dfs.DataNode$PacketResponder: Received block blk_%d of size %d from /10.0.0.1\r\n", i, i, i)
	}
	if err := os.WriteFile(src, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "m.jsonl")

	before := time.Now()
	if err := runReplay(replayArgs{
		path: src, count: 2000000, valueRegex: `size ([0-9]+)`, valueName: "size_bytes",
		baseTS: "auto", step: time.Millisecond, batch: 1000, manifest: manifest,
	}); err != nil {
		t.Fatalf("runReplay: %v", err)
	}
	after := time.Now()

	f, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var recs []loggen.Record
	if err := loggen.ReadManifest(f, func(r loggen.Record) error { recs = append(recs, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(recs) != 500 {
		t.Fatalf("manifest has %d records, want 500", len(recs))
	}
	last := time.Unix(0, recs[len(recs)-1].TSNano)
	if last.After(after.Add(-time.Minute)) || last.Before(before.Add(-time.Minute-time.Second)) {
		t.Errorf("replay ends at %v, want one minute before the run (%v .. %v)", last, before.Add(-time.Minute), after.Add(-time.Minute))
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].TSNano-recs[i-1].TSNano != int64(time.Millisecond) {
			t.Fatalf("records %d and %d are not one step apart", i-1, i)
		}
	}
	if recs[0].Labels["app"] != "hdfs" || recs[0].Labels["component"] != "datanode" {
		t.Errorf("default labels %v, want app=hdfs (from the file name) and component=datanode", recs[0].Labels)
	}
	if recs[7].Value == nil || *recs[7].Value != 7 || recs[7].ValueName != "size_bytes" {
		t.Errorf("record 7 value %v %q", recs[7].Value, recs[7].ValueName)
	}
}

func TestReplayFlagsGoTogether(t *testing.T) {
	if err := runReplay(replayArgs{path: "x", count: 1, valueRegex: `size ([0-9]+)`, step: time.Millisecond}); err == nil {
		t.Error("a value regex without a value name was accepted")
	}
}
