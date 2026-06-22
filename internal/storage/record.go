// Package storage implements the on-disk binary storage format for logd.
//
// All binary encoding uses big-endian byte order for portability and because
// network byte order is big-endian — logd entries may eventually be shipped
// across the network, and keeping one byte order avoids conversions.
package storage

import (
	"encoding/binary"
	"fmt"
	"time"
)

// LogLevel represents the severity of a log entry.
// Encoded as a single byte: 0x00=DEBUG, 0x01=INFO, 0x02=WARN, 0x03=ERROR.
type LogLevel uint8

const (
	LogLevelDebug LogLevel = 0x00
	LogLevelInfo  LogLevel = 0x01
	LogLevelWarn  LogLevel = 0x02
	LogLevelError LogLevel = 0x03
)

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
		return fmt.Sprintf("UNKNOWN(%d)", l)
	}
}

func ParseLogLevel(s string) (LogLevel, error) {
	switch s {
	case "DEBUG":
		return LogLevelDebug, nil
	case "INFO":
		return LogLevelInfo, nil
	case "WARN":
		return LogLevelWarn, nil
	case "ERROR":
		return LogLevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level: %s", s)
	}
}

// LogEntry represents a single log record in memory.
// on disk it is serialized to a compact binary format using big-endian encoding
type LogEntry struct {
	TS         time.Time
	IngestedAt time.Time
	Level      LogLevel
	ServiceID  uint16
	Message    string
	Extra      string
}

const (
	// maxMessageSize is the maximum allowed length of a log message in bytes.
	// Limited to 65535 (max uint16) to fit in the 2-byte length field.
	maxMessageSize = 65535

	// maxExtraSize is the maximum allowed length of the extra JSON field in bytes.
	maxExtraSize = 65535
)

// EncodedSize returns the exact number of bytes the serialized entry will occupy.
// This avoids allocating and encoding just to check whether an entry fits in a page.
func (e *LogEntry) EncodedSize() int {
	// 8(ts) + 8(ingested_at) + 1(level) + 2(service_id) + 2(msg_len) + N(msg) + 2(extra_len) + M(extra)
	return 8 + 8 + 1 + 2 + 2 + len(e.Message) + 2 + len(e.Extra)
}

// Encode serializes the LogEntry to its on-disk binary representation.
//
// Binary format (big-endian):
//
//	[8 bytes:  ts unix nano]
//	[8 bytes:  ingested_at unix nano]
//	[1 byte:   level]
//	[2 bytes:  service_id]
//	[2 bytes:  msg_len]
//	[N bytes:  msg]          ← UTF-8, not null-terminated
//	[2 bytes:  extra_len]
//	[M bytes:  extra]        ← UTF-8 JSON
//
// Returns ErrEntryTooLarge if Message or Extra exceed the maximum size.
func (e *LogEntry) Encode() ([]byte, error) {
	if len(e.Message) > maxMessageSize {
		return nil, fmt.Errorf("%w: message size %d exceeds maximum %d", ErrEntryTooLarge, len(e.Message), maxMessageSize)
	}
	if len(e.Extra) > maxExtraSize {
		return nil, fmt.Errorf("%w: extra size %d exceeds maximum %d", ErrEntryTooLarge, len(e.Extra), maxExtraSize)
	}

	size := e.EncodedSize()
	buf := make([]byte, size)

	binary.BigEndian.PutUint64(buf[0:8], uint64(e.TS.Unix()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.IngestedAt.Unix()))
	buf[16] = byte(e.Level)
	binary.BigEndian.PutUint16(buf[17:19], e.ServiceID)
	binary.BigEndian.PutUint16(buf[19:21], uint16(len(e.Message)))

	offset := 21
	copy(buf[offset:], e.Message)
	offset += len(e.Message)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(e.Extra)))
	offset += 2
	copy(buf[offset:], e.Extra)

	return buf, nil
}

// DecodeLogEntry deserializes a LogEntry from its binary representation.
// Returns ErrMalformedRecord if the data is too short, has an invalid level,
// or has length fields that extend past the end of the slice.
func DecodeLogEntry(data []byte) (LogEntry, error) {
	// Minimum valid entry: 8+8+1+2+2+0+2+0 = 23 bytes (empty message and extra).
	const minEntrySize = 23
	if len(data) < minEntrySize {
		return LogEntry{}, fmt.Errorf("%w: data too short (%d bytes, minimum %d)", ErrMalformedRecord, len(data), minEntrySize)
	}

	tsNano := int64(binary.BigEndian.Uint64(data[0:8]))
	ingestedNano := int64(binary.BigEndian.Uint64(data[8:16]))
	level := LogLevel(data[16])
	if level > LogLevelError {
		return LogEntry{}, fmt.Errorf("%w: invalid log level %d", ErrMalformedRecord, level)
	}

	serviceId := binary.BigEndian.Uint16(data[17:19])

	msgLen := binary.BigEndian.Uint16(data[19:21])

	offset := 21
	if len(data) < offset+int(msgLen) {
		return LogEntry{}, fmt.Errorf("%w: data too short for message (declared %d bytes, have %d)", ErrMalformedRecord, msgLen, len(data)-offset)
	}
	msg := string(data[offset : offset+int(msgLen)])
	offset += int(msgLen)

	if len(data) < offset+2 {
		return LogEntry{}, fmt.Errorf("%w: data too short for extra_len field", ErrMalformedRecord)
	}

	extraLen := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2
	if len(data) < offset+int(extraLen) {
		return LogEntry{}, fmt.Errorf("%w: data too short for extra (declared %d bytes, have %d)", ErrMalformedRecord, extraLen, len(data)-offset)
	}
	extra := string(data[offset : offset+int(extraLen)])

	return LogEntry{
		TS:         time.Unix(0, tsNano),
		IngestedAt: time.Unix(0, ingestedNano),
		Level:      level,
		ServiceID:  serviceId,
		Message:    msg,
		Extra:      extra,
	}, nil
}
