// Package push decodes Loki push requests (snappy+protobuf — what Promtail/Alloy send
// — and JSON) into native model.LogEntry values. All stream labels are placed into the
// entry's Extra blob as JSON, so the ingest layer derives the label index from Extra
// exactly as it does for native ingestion (one label source of truth). Level is taken
// from a "level" label when present, else defaults to INFO.
package push

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/advenn/logd/core/model"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

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

func toEntry(labels map[string]string, ts time.Time, line string) model.LogEntry {
	level := model.LogLevelInfo
	if lv, ok := labels["level"]; ok {
		if parsed, err := model.ParseLogLevel(lv); err == nil {
			level = parsed
		}
	}
	extra := ""
	if len(labels) > 0 {
		if b, err := json.Marshal(labels); err == nil {
			extra = string(b)
		}
	}
	return model.LogEntry{TS: ts, IngestedAt: time.Now(), Level: level, Extra: extra, Message: line}
}

// ---- JSON push ----

type jsonPush struct {
	Streams []struct {
		Stream map[string]string   `json:"stream"`
		// Each value is [ "<unix_nano>", "<line>" ] or [ "<unix_nano>", "<line>",
		// {structured metadata} ] — the optional 3rd element carries per-entry labels.
		Values [][]json.RawMessage `json:"values"`
	} `json:"streams"`
}

func decodeJSON(body []byte) ([]model.LogEntry, error) {
	var p jsonPush
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("loki json push: %w", err)
	}
	var out []model.LogEntry
	for _, s := range p.Streams {
		for _, v := range s.Values {
			if len(v) < 2 {
				return nil, fmt.Errorf("loki json push: value needs [ts, line], got %d elements", len(v))
			}
			var tsStr, line string
			if err := json.Unmarshal(v[0], &tsStr); err != nil {
				return nil, fmt.Errorf("loki json push: bad timestamp: %w", err)
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				return nil, fmt.Errorf("loki json push: bad line: %w", err)
			}
			nano, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("loki json push: bad timestamp %q: %w", tsStr, err)
			}
			labels := s.Stream
			if len(v) >= 3 {
				var meta map[string]string
				if err := json.Unmarshal(v[2], &meta); err != nil {
					return nil, fmt.Errorf("loki json push: bad structured metadata: %w", err)
				}
				labels = mergeLabels(s.Stream, meta)
			}
			out = append(out, toEntry(labels, time.Unix(0, nano), line))
		}
	}
	return out, nil
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
		labels, entries, err := decodeStream(buf)
		if err != nil {
			return err
		}
		m, err := parseLabels(labels)
		if err != nil {
			return err
		}
		for _, e := range entries {
			out = append(out, toEntry(mergeLabels(m, e.meta), e.ts, e.line))
		}
		return nil
	})
	return out, err
}

type protoEntry struct {
	ts   time.Time
	line string
	meta map[string]string // per-entry structured metadata (EntryAdapter field 3)
}

func decodeStream(data []byte) (labels string, entries []protoEntry, err error) {
	err = walk(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			labels = string(buf)
		case num == 2 && typ == protowire.BytesType:
			e, err := decodeEntry(buf)
			if err != nil {
				return err
			}
			entries = append(entries, e)
		}
		return nil
	})
	return labels, entries, err
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
