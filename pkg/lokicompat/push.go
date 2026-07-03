// Package lokicompat provides Loki-compatible HTTP API types and wire format
// handling. It supports decoding snappy-compressed protobuf push requests (what
// Promtail sends) and encoding query responses in Loki's JSON format (what
// Grafana expects).
package lokicompat

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// ---- Protobuf message types ----

// PushRequest is the top-level Loki push message containing log streams.
type PushRequest struct {
	Streams []StreamAdapter
}

// StreamAdapter groups log entries sharing the same label set.
type StreamAdapter struct {
	Labels  string         // serialized label string "{key=\"val\",...}"
	Entries []EntryAdapter // log entries in this stream
}

// EntryAdapter is a single log line with timestamp.
type EntryAdapter struct {
	Timestamp time.Time
	Line      string
}

// ---- Public API ----

// DecodePushRequest decodes a snappy-compressed protobuf push request body.
// This handles the wire format Promtail sends to POST /loki/api/v1/push.
func DecodePushRequest(data []byte) (*PushRequest, error) {
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	return decodePushRequest(decompressed)
}

// ParseLabels parses a Loki label string like `{key="val",key2="val2"}` into
// a map. Returns an error if the string is malformed.
func ParseLabels(s string) (map[string]string, error) {
	if s == "" {
		return nil, fmt.Errorf("empty label string")
	}

	// Strip outer braces.
	s = labelBraces.ReplaceAllString(s, "${1}")
	if s == "" {
		return make(map[string]string), nil
	}

	labels := make(map[string]string)

	// Split on comma not inside quotes.
	parts := splitLabelPairs(s)
	for _, part := range parts {
		m := labelPair.FindStringSubmatch(part)
		if m == nil {
			return nil, fmt.Errorf("invalid label pair: %q", part)
		}
		// m[2] is the full double-quoted token, e.g. `"logd"`; Unquote strips
		// the quotes and resolves any escapes.
		val, err := strconv.Unquote(m[2])
		if err != nil {
			return nil, fmt.Errorf("invalid label value in %q: %w", part, err)
		}
		labels[m[1]] = val
	}
	return labels, nil
}

var (
	labelBraces = regexp.MustCompile(`^\{(.*)\}$`)
	// Matches key="value"; the value is a Go double-quoted string (the form
	// Loki/Prometheus and Alloy emit), capturing the quotes so strconv.Unquote
	// can resolve escapes.
	labelPair = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*("(?:[^"\\]|\\.)*")\s*$`)
)

func splitLabelPairs(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range s {
		switch c {
		case '"':
			depth ^= 1
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// ---- Protobuf wire format decoder ----
//
// Wire format for Loki push:
//
//	PushRequest:  field 1 (repeated StreamAdapter, wire type 2/LEN)
//	  StreamAdapter: field 1 (string labels, wire 2), field 2 (repeated EntryAdapter, wire 2)
//	    EntryAdapter: field 1 (Timestamp, wire 2), field 2 (string line, wire 2)
//	      Timestamp: field 1 (int64 seconds, wire 0/VARINT), field 2 (int32 nanos, wire 0)

func decodePushRequest(data []byte) (*PushRequest, error) {
	req := &PushRequest{}
	err := unmarshalMessage(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		if num == 1 && typ == protowire.BytesType {
			stream, err := decodeStreamAdapter(buf)
			if err != nil {
				return err
			}
			req.Streams = append(req.Streams, *stream)
		}
		return nil
	})
	return req, err
}

func decodeStreamAdapter(data []byte) (*StreamAdapter, error) {
	s := &StreamAdapter{}
	err := unmarshalMessage(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			s.Labels = string(buf)
		case num == 2 && typ == protowire.BytesType:
			entry, err := decodeEntryAdapter(buf)
			if err != nil {
				return err
			}
			s.Entries = append(s.Entries, *entry)
		}
		return nil
	})
	return s, err
}

func decodeEntryAdapter(data []byte) (*EntryAdapter, error) {
	e := &EntryAdapter{}
	err := unmarshalMessage(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			ts, err := decodeTimestamp(buf)
			if err != nil {
				return err
			}
			e.Timestamp = ts
		case num == 2 && typ == protowire.BytesType:
			e.Line = string(buf)
		}
		return nil
	})
	return e, err
}

func decodeTimestamp(data []byte) (time.Time, error) {
	var secs int64
	var nanos int32
	err := unmarshalMessage(data, func(num protowire.Number, buf []byte, typ protowire.Type) error {
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return fmt.Errorf("invalid varint for timestamp seconds")
			}
			secs = int64(v)
		case num == 2 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return fmt.Errorf("invalid varint for timestamp nanos")
			}
			nanos = int32(v)
		}
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, int64(nanos)), nil
}

// unmarshalMessage walks protobuf wire-format fields, calling fn for each.
func unmarshalMessage(data []byte, fn func(protowire.Number, []byte, protowire.Type) error) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return fmt.Errorf("invalid protobuf tag at offset %d", len(data))
		}
		data = data[n:]

		var fieldBuf []byte
		switch typ {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("invalid varint for field %d", num)
			}
			// Pass the raw bytes (including the varint) for ConsumeVarint in callers.
			// Actually, callers need the raw field value. For varint, pass data[:n].
			fieldBuf = data[:n]
			data = data[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("invalid length for field %d", num)
			}
			data = data[n:]
			if uint64(len(data)) < v {
				return fmt.Errorf("truncated field %d: need %d bytes, have %d", num, v, len(data))
			}
			fieldBuf = data[:v]
			data = data[v:]
		case protowire.Fixed32Type:
			if len(data) < 4 {
				return fmt.Errorf("truncated fixed32 field %d", num)
			}
			fieldBuf = data[:4]
			data = data[4:]
		case protowire.Fixed64Type:
			if len(data) < 8 {
				return fmt.Errorf("truncated fixed64 field %d", num)
			}
			fieldBuf = data[:8]
			data = data[8:]
		default:
			return fmt.Errorf("unknown wire type %d for field %d", typ, num)
		}

		if err := fn(num, fieldBuf, typ); err != nil {
			return err
		}
	}
	return nil
}

// ---- Loki JSON response helpers ----

// LokiResponse wraps all Loki API responses.
type LokiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// OKResponse returns a LokiResponse with status "success".
func OKResponse(data json.RawMessage) LokiResponse {
	return LokiResponse{Status: "success", Data: data}
}

// ErrorResponse returns a LokiResponse with status "error".
func ErrorResponse(msg string) LokiResponse {
	return LokiResponse{Status: "error", Error: msg}
}
