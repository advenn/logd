// Package model holds logd's core, protocol-agnostic data types. It has no
// dependencies on any other logd package (and must never import storage, query,
// or compat) so that the on-the-wire and on-disk layers can both depend on it
// without creating an import cycle. See design §3 (package layout) and §4 (data
// model).
package model

import (
	"fmt"
	"strings"
	"time"
)

// LogLevel is the severity of a log entry, encoded on disk as a single byte.
// The numeric order is meaningful (DEBUG < INFO < WARN < ERROR), which lets the
// future typed-range index compare levels directly.
type LogLevel uint8

const (
	LogLevelDebug LogLevel = 0x00
	LogLevelInfo  LogLevel = 0x01
	LogLevelWarn  LogLevel = 0x02
	LogLevelError LogLevel = 0x03
)

// String returns the canonical upper-case name of the level. Unknown values
// (which decoding should already have rejected) render as UNKNOWN(n) rather than
// panicking, so a corrupt byte can never crash a query.
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(l))
	}
}

// ParseLogLevel maps a textual level to a LogLevel. Matching is case-insensitive
// and folds in the many aliases that apps, Grafana Alloy, and Loki emit, so that
// "warning", "err", "fatal", "trace", etc. all land on the right bucket. An
// unrecognized name is an error (the caller decides whether to default it).
func ParseLogLevel(s string) (LogLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "trace":
		return LogLevelDebug, nil
	case "info", "information", "notice":
		return LogLevelInfo, nil
	case "warn", "warning":
		return LogLevelWarn, nil
	case "error", "err", "fatal", "critical", "crit", "panic":
		return LogLevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level: %s", s)
	}
}

// LogEntry is a single log record in memory. Its on-disk encoding lives in
// core/storage/record.go (a storage concern that depends on this type, never the
// reverse).
//
// ServiceID and StreamID are interning references, not values: ServiceID is a
// denormalized fast-path filter for the originating service, and StreamID is the
// canonical reference into the label index. StreamID stays 0 until the label
// phase (design §4); it occupies its 4 bytes on disk now so adding label indexing
// later is not an on-disk format break.
type LogEntry struct {
	TS         time.Time // event time (when the log line happened)
	IngestedAt time.Time // arrival time (stamped by logd on ingestion)
	Level      LogLevel  // DEBUG=0 INFO=1 WARN=2 ERROR=3
	ServiceID  uint16    // interned service name (hot-path filter)
	StreamID   uint32    // interned label set (label-index ref; 0 until label phase)
	Message    string    // the log line body
	Extra      string    // raw JSON: high-cardinality / structured fields
}
