package loggen

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Replay mode pushes a REAL dataset through the same pipeline as the synthetic corpus.
//
// The synthetic corpus was designed alongside logd's template, which is a threat to validity
// on its own: a generator and a template written by the same person agree by construction.
// A public dataset (Loghub HDFS_v1 is the one wired up) answers that, provided it gets the
// same treatment -- the same push driver, the same batching, and a manifest with exact
// ground truth -- so its numbers are comparable with the corpus's.
//
// What replay keeps and what it replaces:
//
//   - The line text is kept byte-for-byte. Only the line terminator is removed, including the
//     carriage return of a CRLF file (HDFS.log is CRLF throughout); a CR inside a line stays.
//     Nothing is prefixed: the original "081109 203518" timestamp stays in the text as text.
//   - The TIMESTAMP is replaced with a synthetic one, base + i*step, for the same two reasons
//     as in corpus mode: ts_ns is the primary key the parity diff uses, so it must be strictly
//     monotonic (HDFS.log has one-second resolution and thousands of lines per second), and
//     the replay must end just before now, or Loki silently hides it (see resolveBase).
//   - Labels are the static ones given plus one derived `component`, kept low-cardinality.
//   - The ground-truth value is the FIRST match of a regular expression, as an int64.

// ComponentLabel is the label replay derives from each line's logger class.
const ComponentLabel = "component"

// maxComponentValues caps the component label's cardinality, "other" included. Loghub HDFS
// has 5 outer classes over the whole file (4 in the first 2M lines); the cap exists so that
// pointing replay at a file with a different layout degrades to "other" instead of minting a
// stream per distinct token, which would change what every engine's ingest figure measures.
const maxComponentValues = 10

// ReplayOpts controls a replay.
type ReplayOpts struct {
	Count  int               // replay at most this many (non-empty) lines from the start
	Base   time.Time         // first synthetic timestamp
	Step   time.Duration     // spacing between consecutive timestamps
	Labels map[string]string // static labels on every record, e.g. app=hdfs
	// ValueRegex's first group is the line's ground-truth integer; nil records no value.
	ValueRegex *regexp.Regexp
	ValueName  string // what the value is called in the manifest and in queries
}

// ReplayStats is what a replay saw. MultiValue is the number that decides whether "first
// occurrence" ground truth is safe: logd's typed predicate matches a line if ANY occurrence
// of its template satisfies it, Loki's regexp, VictoriaLogs' extract_regexp and PostgreSQL's
// substring all take the first. They can only disagree on a line with two occurrences.
type ReplayStats struct {
	Lines      int            `json:"lines"`
	WithValue  int            `json:"with_value"`
	MultiValue int            `json:"multi_value"`   // value regex matched more than once
	Unparsable int            `json:"unparsable"`    // matched, but the group is not an int64
	Skipped    int            `json:"skipped_empty"` // empty lines are not replayed (some stores reject them)
	Components map[string]int `json:"components"`
	ValueName  string         `json:"value_name,omitempty"`
}

// Replay reads lines from r and emits up to o.Count records.
func Replay(r io.Reader, o ReplayOpts, emit func(Record) error) (ReplayStats, error) {
	st := ReplayStats{Components: map[string]int{}, ValueName: o.ValueName}
	if o.Count <= 0 {
		return st, fmt.Errorf("replay count must be positive, got %d", o.Count)
	}
	if o.Step <= 0 {
		return st, fmt.Errorf("replay step must be positive, got %v", o.Step)
	}
	if _, ok := o.Labels[ComponentLabel]; ok {
		return st, fmt.Errorf("label %q is derived from each line in replay mode and cannot also be set statically", ComponentLabel)
	}
	if o.ValueRegex != nil {
		if o.ValueRegex.NumSubexp() < 1 {
			return st, fmt.Errorf("value regex %q has no capture group; its first group is the value", o.ValueRegex)
		}
		if o.ValueName == "" {
			return st, fmt.Errorf("a value regex needs a value name")
		}
	}

	comps := newComponentMapper(o.Labels)
	sc := newLineScanner(r)
	src := 0
	for st.Lines < o.Count && sc.Scan() {
		src++
		line := sc.Text()
		if line == "" {
			st.Skipped++
			continue
		}
		if !utf8.ValidString(line) {
			// Loki, VictoriaLogs, logd and PostgreSQL disagree on invalid UTF-8 (reject, replace,
			// store as-is), so such a line would break parity for a reason unrelated to the
			// query. Fail instead of guessing.
			return st, fmt.Errorf("source line %d is not valid UTF-8", src)
		}

		comp, labels := comps.labelsFor(line)
		st.Components[comp]++
		rec := Record{
			I:      st.Lines,
			TSNano: o.Base.Add(time.Duration(st.Lines) * o.Step).UnixNano(),
			Labels: labels,
			Line:   line,
			Level:  levelOf(line),
		}
		if o.ValueRegex != nil {
			m := o.ValueRegex.FindAllStringSubmatch(line, 2)
			if len(m) > 1 {
				st.MultiValue++
			}
			if len(m) > 0 {
				if v, err := strconv.ParseInt(m[0][1], 10, 64); err == nil {
					rec.Value, rec.ValueName = &v, o.ValueName
					st.WithValue++
				} else {
					st.Unparsable++
				}
			}
		}
		if err := emit(rec); err != nil {
			return st, err
		}
		st.Lines++
	}
	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("reading source line %d: %w", src+1, err)
	}
	return st, nil
}

// CountReplayLines returns how many lines Replay would emit from r with Count = max, without
// decoding them, so "auto" can place the base timestamp by what is actually replayed.
func CountReplayLines(r io.Reader, max int) (int, error) {
	sc := newLineScanner(r)
	n := 0
	for n < max && sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	return n, sc.Err()
}

// newLineScanner splits on newlines. bufio.ScanLines drops the newline and ONE carriage
// return before it -- exactly the CRLF terminator and nothing else. A line longer than the
// buffer fails the replay rather than being split or truncated.
func newLineScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	return sc
}

// componentMapper turns logger classes into a capped set of label values, handing out one
// shared label map per value: a replay emits millions of records over a handful of streams.
type componentMapper struct {
	static map[string]string
	labels map[string]map[string]string
	named  int // distinct values other than "other"
}

func newComponentMapper(static map[string]string) *componentMapper {
	return &componentMapper{static: static, labels: map[string]map[string]string{}}
}

func (c *componentMapper) labelsFor(line string) (string, map[string]string) {
	comp := componentOf(line)
	// One value is reserved for "other", so the label never exceeds maxComponentValues.
	if _, seen := c.labels[comp]; !seen && comp != "other" && c.named >= maxComponentValues-1 {
		comp = "other"
	}
	l, ok := c.labels[comp]
	if !ok {
		l = make(map[string]string, len(c.static)+1)
		for k, v := range c.static {
			l[k] = v
		}
		l[ComponentLabel] = comp
		c.labels[comp] = l
		if comp != "other" {
			c.named++
		}
	}
	return comp, l
}

// componentOf derives the component from a Loghub-layout line,
//
//	<date> <time> <pid> <level> <class>: <message>
//	081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608...
//
// as the OUTER class name, lowercased: dfs.DataNode$DataXceiver and dfs.DataNode$PacketResponder
// are both "datanode". The inner classes are threads of one daemon; keeping them apart would
// nearly double the stream count (9 values over the whole file instead of 5) for a label no
// realistic query selects on. Anything that does not fit the layout is "other".
func componentOf(line string) string {
	class, ok := field(line, 4)
	if !ok || len(class) < 2 || class[len(class)-1] != ':' {
		return "other"
	}
	class = class[:len(class)-1]
	if i := strings.LastIndexByte(class, '.'); i >= 0 {
		class = class[i+1:]
	}
	if i := strings.IndexByte(class, '$'); i >= 0 {
		class = class[:i]
	}
	class = strings.ToLower(class)
	if class == "" {
		return "other"
	}
	for i := 0; i < len(class); i++ {
		if c := class[i]; !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return "other"
		}
	}
	return class
}

// levelOf reads the Loghub level column (the 4th field), lowercased like corpus mode's.
func levelOf(line string) string {
	lv, _ := field(line, 3)
	switch lv {
	case "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL":
		return strings.ToLower(lv)
	}
	return ""
}

// field returns the n-th (0-based) space-separated field without splitting the whole line.
func field(line string, n int) (string, bool) {
	for i := 0; i < n; i++ {
		j := strings.IndexByte(line, ' ')
		if j < 0 {
			return "", false
		}
		line = line[j+1:]
	}
	if j := strings.IndexByte(line, ' '); j >= 0 {
		line = line[:j]
	}
	return line, line != ""
}
