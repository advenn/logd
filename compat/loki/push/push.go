// Package push decodes Loki push requests (snappy+protobuf — what Promtail/Alloy send
// — and JSON) into native model.LogEntry values. All stream labels are placed into the
// entry's Extra blob as JSON, so the ingest layer derives the label index from Extra
// exactly as it does for native ingestion (one label source of truth). Level is taken
// from a "level" label when present, else defaults to INFO.
package push

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// MaxEntries caps the entries one push request may carry. The body and decompressed-size
// caps bound bytes, not memory: an empty protobuf entry is two bytes on the wire but a full
// LogEntry once decoded, so a 768 KB snappy body could decode into 8M entries and allocate
// 5.7 GB. Real shippers batch far below this (Promtail and Alloy send ~1 MB batches; the
// testbed generator sends 1,000 entries per request), so it only refuses abuse.
const MaxEntries = 100_000

// ErrTooManyEntries is returned for a push carrying more than MaxEntries entries. Decoding
// stops at the first entry over the cap, so the refusal itself costs bounded memory.
var ErrTooManyEntries = fmt.Errorf("push carries more than %d entries; send it in smaller batches", MaxEntries)

// Decode decodes a push request body into entries, choosing protobuf+snappy or JSON by
// Content-Type (empty/JSON → JSON; x-protobuf → protobuf).
func Decode(contentType string, body []byte) ([]model.LogEntry, error) {
	if strings.Contains(contentType, "application/x-protobuf") {
		return decodeProto(body)
	}
	return decodeJSON(body)
}

// mergeLabels overlays per-entry structured metadata onto the stream labels (metadata
// wins on a key collision), returning a fresh map so the shared stream-label map isn't
// mutated. When meta is empty it returns base unchanged.
func mergeLabels(base, meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return base
	}
	out := make(map[string]string, len(base)+len(meta))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range meta {
		out[k] = v
	}
	return out
}

// stream is the per-stream constant part of an entry: the level and the marshalled Extra
// blob. The Loki wire format carries labels ONCE per stream and then N entries under it,
// so deriving these per entry re-marshals an identical map for every line in the batch —
// at the 1,000-entry batches real shippers send, that was 25% of the whole push path's
// allocations, and the ingest layer then parsed the JSON straight back out again.
//
// Hoisting it is exactly equivalent: json.Marshal of a map[string]string sorts its keys,
// so the bytes are byte-identical for every entry, and the level is read from the same map.
type stream struct {
	level model.LogLevel
	extra string
}

func newStream(labels map[string]string) stream {
	s := stream{level: model.LogLevelInfo}
	if lv, ok := labels["level"]; ok {
		if parsed, err := model.ParseLogLevel(lv); err == nil {
			s.level = parsed
		}
	}
	if len(labels) > 0 {
		if b, err := json.Marshal(labels); err == nil {
			s.extra = string(b)
		}
	}
	return s
}

func (s stream) entry(ts time.Time, line string) model.LogEntry {
	return model.LogEntry{TS: ts, IngestedAt: time.Now(), Level: s.level, Extra: s.extra, Message: line}
}

// toEntry is the per-entry path, used only when an entry carries structured metadata that
// overlays the stream labels and so genuinely needs its own Extra.
func toEntry(labels map[string]string, ts time.Time, line string) model.LogEntry {
	return newStream(labels).entry(ts, line)
}

// ---- JSON push ----

// jsonStream is one element of a JSON push's "streams" array.
type jsonStream struct {
	Stream map[string]string `json:"stream"`
	// Each value is [ "<unix_nano>", "<line>" ] or [ "<unix_nano>", "<line>",
	// {structured metadata} ] — the optional 3rd element carries per-entry labels. Kept raw
	// so the values are decoded one at a time (see decodeJSONValues).
	Values json.RawMessage `json:"values"`
}

// decodeJSON walks {"streams":[...]} one stream at a time and each stream's values one at
// a time, so what is retained is the entries and nothing else. Unmarshalling the whole
// request first held every value twice — as [][]json.RawMessage, then as a LogEntry — and a
// 31 MB body of empty values allocated 3.8 GB before anything could refuse it.
func decodeJSON(body []byte) ([]model.LogEntry, error) {
	var out []model.LogEntry
	if err := decodeJSONRequest(json.NewDecoder(bytes.NewReader(body)), &out); err != nil {
		if errors.Is(err, ErrTooManyEntries) {
			return nil, err
		}
		return nil, fmt.Errorf("loki json push: %w", err)
	}
	return out, nil
}

func decodeJSONRequest(dec *json.Decoder, out *[]model.LogEntry) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok != nil { // a JSON null request carries nothing, as json.Unmarshal treated it
		if tok != json.Delim('{') {
			return errors.New("request must be a JSON object")
		}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			// encoding/json matches field names case-insensitively; keep doing so.
			if k, _ := key.(string); !strings.EqualFold(k, "streams") {
				var skip json.RawMessage
				if err := dec.Decode(&skip); err != nil {
					return err
				}
				continue
			}
			if err := decodeJSONStreams(dec, out); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil { // the closing }
			return err
		}
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("unexpected data after the request object")
	}
	return nil
}

func decodeJSONStreams(dec *json.Decoder, out *[]model.LogEntry) error {
	tok, err := dec.Token()
	if err != nil || tok == nil { // "streams": null is an empty push
		return err
	}
	if tok != json.Delim('[') {
		return errors.New("streams must be an array")
	}
	for dec.More() {
		var s jsonStream
		if err := dec.Decode(&s); err != nil {
			return err
		}
		if err := decodeJSONValues(s, out); err != nil {
			return err
		}
	}
	_, err = dec.Token() // the closing ]
	return err
}

// decodeJSONValues appends one stream's values to out, refusing the push once it exceeds
// MaxEntries.
func decodeJSONValues(s jsonStream, out *[]model.LogEntry) error {
	if len(s.Values) == 0 || string(s.Values) == "null" {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(s.Values))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return errors.New("values must be an array")
	}
	st := newStream(s.Stream)
	for dec.More() {
		if len(*out) >= MaxEntries {
			return ErrTooManyEntries
		}
		var v []json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return err
		}
		if len(v) < 2 {
			return fmt.Errorf("value needs [ts, line], got %d elements", len(v))
		}
		var tsStr, line string
		if err := json.Unmarshal(v[0], &tsStr); err != nil {
			return fmt.Errorf("bad timestamp: %w", err)
		}
		if err := json.Unmarshal(v[1], &line); err != nil {
			return fmt.Errorf("bad line: %w", err)
		}
		nano, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			return fmt.Errorf("bad timestamp %q: %w", tsStr, err)
		}
		if len(v) >= 3 {
			var meta map[string]string
			if err := json.Unmarshal(v[2], &meta); err != nil {
				return fmt.Errorf("bad structured metadata: %w", err)
			}
			if len(meta) > 0 {
				*out = append(*out, toEntry(mergeLabels(s.Stream, meta), time.Unix(0, nano), line))
				continue
			}
		}
		*out = append(*out, st.entry(time.Unix(0, nano), line))
	}
	return nil
}

// ---- protobuf+snappy push ----

// maxDecompressed caps the snappy-decompressed push size. snappy.Decode allocates the
// declared decompressed length BEFORE validating the payload, so an adversarial length
// header in a tiny body could otherwise force a multi-GB allocation (decompression
// bomb). Reject oversized declarations up front.
const maxDecompressed = 128 << 20 // 128 MB

func decodeProto(body []byte) ([]model.LogEntry, error) {
	dlen, err := snappy.DecodedLen(body)
	if err != nil {
		return nil, fmt.Errorf("snappy: %w", err)
	}
	if dlen > maxDecompressed {
		return nil, fmt.Errorf("push too large: decompresses to %d bytes (max %d)", dlen, maxDecompressed)
	}
	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	var out []model.LogEntry
	err = walk(decompressed, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if num != 1 || typ != protowire.BytesType {
			return nil
		}
		return decodeStream(buf, &out)
	})
	return out, err
}

type protoEntry struct {
	ts   time.Time
	line string
	meta map[string]string // per-entry structured metadata (EntryAdapter field 3)
}

// decodeStream appends one StreamAdapter's entries to out, converting each straight into a
// LogEntry rather than collecting the stream's entries into a slice of their own first, and
// refuses the push once it exceeds MaxEntries. Converting needs the stream labels, and
// protobuf fields may arrive in any order, so a first pass over the (already decompressed)
// bytes finds them; skipping fields allocates nothing.
func decodeStream(data []byte, out *[]model.LogEntry) error {
	var labels string
	if err := walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if num == 1 && typ == protowire.BytesType {
			labels = string(buf)
		}
		return nil
	}); err != nil {
		return err
	}
	m, err := parseLabels(labels)
	if err != nil {
		return err
	}
	st := newStream(m)
	return walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if num != 2 || typ != protowire.BytesType {
			return nil
		}
		if len(*out) >= MaxEntries {
			return ErrTooManyEntries
		}
		e, err := decodeEntry(buf)
		if err != nil {
			return err
		}
		if len(e.meta) == 0 {
			*out = append(*out, st.entry(e.ts, e.line))
		} else {
			*out = append(*out, toEntry(mergeLabels(m, e.meta), e.ts, e.line))
		}
		return nil
	})
}

func decodeEntry(data []byte) (protoEntry, error) {
	var e protoEntry
	err := walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			ts, err := decodeTimestamp(buf)
			if err != nil {
				return err
			}
			e.ts = ts
		case num == 2 && typ == protowire.BytesType:
			e.line = string(buf)
		case num == 3 && typ == protowire.BytesType:
			// structuredMetadata: repeated LabelPairAdapter{ name=1, value=2 }.
			name, value, err := decodeLabelPair(buf)
			if err != nil {
				return err
			}
			if e.meta == nil {
				e.meta = map[string]string{}
			}
			e.meta[name] = value
		}
		return nil
	})
	return e, err
}

func decodeLabelPair(data []byte) (name, value string, err error) {
	err = walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if typ != protowire.BytesType {
			return nil
		}
		switch num {
		case 1:
			name = string(buf)
		case 2:
			value = string(buf)
		}
		return nil
	})
	return name, value, err
}

func decodeTimestamp(data []byte) (time.Time, error) {
	var secs int64
	var nanos int32
	err := walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if typ != protowire.VarintType {
			return nil
		}
		v, n := protowire.ConsumeVarint(buf)
		if n < 0 {
			return fmt.Errorf("bad timestamp varint")
		}
		switch num {
		case 1:
			secs = int64(v)
		case 2:
			nanos = int32(v)
		}
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, int64(nanos)), nil
}

// walk iterates protobuf wire-format fields, passing each field's raw value bytes to fn.
func walk(data []byte, fn func(protowire.Number, []byte, protowire.Type) error) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fmt.Errorf("bad protobuf tag")
		}
		data = data[n:]
		var fieldBuf []byte
		switch typ {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("bad varint for field %d", num)
			}
			fieldBuf, data = data[:n], data[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("bad length for field %d", num)
			}
			data = data[n:]
			if uint64(len(data)) < v {
				return fmt.Errorf("truncated field %d", num)
			}
			fieldBuf, data = data[:v], data[v:]
		case protowire.Fixed32Type:
			if len(data) < 4 {
				return fmt.Errorf("truncated fixed32 field %d", num)
			}
			fieldBuf, data = data[:4], data[4:]
		case protowire.Fixed64Type:
			if len(data) < 8 {
				return fmt.Errorf("truncated fixed64 field %d", num)
			}
			fieldBuf, data = data[:8], data[8:]
		default:
			return fmt.Errorf("unknown wire type %d for field %d", typ, num)
		}
		if err := fn(num, fieldBuf, typ); err != nil {
			return err
		}
	}
	return nil
}

// ---- Loki label string parsing ----

var (
	labelBraces = regexp.MustCompile(`^\{(.*)\}$`)
	labelPair   = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*("(?:[^"\\]|\\.)*")\s*$`)
)

// parseLabels parses Loki's `{k="v",...}` label string into a map.
func parseLabels(s string) (map[string]string, error) {
	if s == "" {
		return map[string]string{}, nil
	}
	s = labelBraces.ReplaceAllString(s, "${1}")
	if strings.TrimSpace(s) == "" {
		return map[string]string{}, nil
	}
	labels := map[string]string{}
	for _, part := range splitLabelPairs(s) {
		m := labelPair.FindStringSubmatch(part)
		if m == nil {
			return nil, fmt.Errorf("invalid label pair %q", part)
		}
		val, err := strconv.Unquote(m[2])
		if err != nil {
			return nil, fmt.Errorf("invalid label value in %q: %w", part, err)
		}
		labels[m[1]] = val
	}
	return labels, nil
}

// splitLabelPairs splits a label string on commas that are outside quoted values. It is
// escape-aware: a backslash-escaped quote (\") inside a value must NOT toggle the quoted
// state, or a legitimate value containing an escaped quote would mis-split and the whole
// batch would be rejected.
func splitLabelPairs(s string) []string {
	var parts []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++ // skip the escaped byte (e.g. \" or \\)
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}
