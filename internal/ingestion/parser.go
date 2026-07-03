package ingestion

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/pkg/lokicompat"
)

// levelInLineRe finds a logfmt-style level token in a log line, e.g.
// `level=info`, `lvl="error"`, `severity: WARN`. This is how raw container
// logs (which carry no level label) get a meaningful level.
var levelInLineRe = regexp.MustCompile(`(?i)\b(?:level|lvl|severity)\s*[=:]\s*"?([a-zA-Z]+)`)

// detectLevel attempts to infer a log level from the message content. Returns
// false when no recognizable level token is present.
func detectLevel(msg string) (storage.LogLevel, bool) {
	if m := levelInLineRe.FindStringSubmatch(msg); m != nil {
		if lvl, err := storage.ParseLogLevel(m[1]); err == nil {
			return lvl, true
		}
	}
	return 0, false
}

// Parser converts between wire formats and internal LogEntry types.
type Parser struct{}

// NewParser creates a Parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseLokiPush converts a Loki PushRequest into LogEntry records.
// Each stream's labels are parsed into the Extra JSON field, and the log line
// becomes the Message.
func (p *Parser) ParseLokiPush(req *lokicompat.PushRequest) ([]storage.LogEntry, error) {
	var entries []storage.LogEntry

	for _, stream := range req.Streams {
		labels, err := lokicompat.ParseLabels(stream.Labels)
		if err != nil {
			return nil, fmt.Errorf("parsing labels: %w", err)
		}

		for _, entry := range stream.Entries {
			le := storage.LogEntry{
				TS:         entry.Timestamp,
				IngestedAt: time.Now(),
				Message:    entry.Line,
			}

			// Set Level from the stream label if present, otherwise try to
			// detect it from the line content (raw container logs carry no
			// level label but often log it logfmt-style inside the message).
			if lvl, ok := labels["level"]; ok {
				if parsed, err := storage.ParseLogLevel(lvl); err == nil {
					le.Level = parsed
				}
			} else if lvl, ok := detectLevel(entry.Line); ok {
				le.Level = lvl
			}

			// Store all labels in Extra JSON so they're queryable.
			extra := make(map[string]string)
			for k, v := range labels {
				extra[k] = v
			}
			extraBytes, _ := json.Marshal(extra)
			le.Extra = string(extraBytes)

			entries = append(entries, le)
		}
	}

	return entries, nil
}

// nativeEntry is the JSON format for POST /v1/ingest.
type nativeEntry struct {
	TS      string `json:"ts"`
	Level   string `json:"level"`
	Service string `json:"service"`
	Msg     string `json:"msg"`
	// All other fields go into Extra.
}

// ParseNativeJSON converts a single native JSON log entry to a LogEntry.
// Known fields (ts, level, service, msg) are mapped directly; all other fields
// are preserved in the Extra JSON blob.
func (p *Parser) ParseNativeJSON(body []byte) (storage.LogEntry, error) {
	// Decode known fields.
	var native nativeEntry
	if err := json.Unmarshal(body, &native); err != nil {
		return storage.LogEntry{}, fmt.Errorf("parsing native JSON: %w", err)
	}

	le := storage.LogEntry{
		IngestedAt: time.Now(),
		Message:    native.Msg,
	}

	// Parse timestamp.
	if native.TS != "" {
		ts, err := time.Parse(time.RFC3339, native.TS)
		if err != nil {
			return storage.LogEntry{}, fmt.Errorf("parsing ts %q: %w", native.TS, err)
		}
		le.TS = ts
	} else {
		le.TS = le.IngestedAt
	}

	// Parse level; fall back to detecting it from the message when absent.
	if native.Level != "" {
		lvl, err := storage.ParseLogLevel(native.Level)
		if err != nil {
			lvl = storage.LogLevelInfo // default
		}
		le.Level = lvl
	} else if lvl, ok := detectLevel(native.Msg); ok {
		le.Level = lvl
	}

	// Decode all fields into a map, then remove known fields to build Extra.
	var allFields map[string]interface{}
	if err := json.Unmarshal(body, &allFields); err != nil {
		return storage.LogEntry{}, fmt.Errorf("parsing native JSON fields: %w", err)
	}

	known := map[string]bool{"ts": true, "level": true, "service": true, "msg": true}
	for k := range known {
		delete(allFields, k)
	}

	// Ensure service is in the extra map.
	if native.Service != "" {
		allFields["service"] = native.Service
	}

	if len(allFields) > 0 {
		extraBytes, err := json.Marshal(allFields)
		if err != nil {
			return storage.LogEntry{}, fmt.Errorf("serializing extra fields: %w", err)
		}
		le.Extra = string(extraBytes)
	}

	return le, nil
}
