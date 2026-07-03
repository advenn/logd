package storage

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseLogLevel_CaseInsensitiveAndAliases(t *testing.T) {
	cases := map[string]LogLevel{
		"debug": LogLevelDebug, "DEBUG": LogLevelDebug, "trace": LogLevelDebug,
		"info": LogLevelInfo, "Info": LogLevelInfo, "notice": LogLevelInfo,
		"warn": LogLevelWarn, "WARNING": LogLevelWarn, "warning": LogLevelWarn,
		"error": LogLevelError, "ERROR": LogLevelError, "err": LogLevelError,
		"fatal": LogLevelError, "critical": LogLevelError, "panic": LogLevelError,
		" error ": LogLevelError,
	}
	for in, want := range cases {
		got, err := ParseLogLevel(in)
		if err != nil {
			t.Errorf("ParseLogLevel(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseLogLevel("bogus"); err == nil {
		t.Error("ParseLogLevel(\"bogus\") expected error, got nil")
	}
}

func TestEncodeDecodeRoundtrip_FullEntry(t *testing.T) {
	original := LogEntry{
		TS:         time.Date(2026, 5, 4, 10, 30, 0, 123456789, time.UTC),
		IngestedAt: time.Date(2026, 5, 4, 10, 30, 1, 987654321, time.UTC),
		Level:      LogLevelError,
		ServiceID:  42,
		Message:    "task failed: timeout",
		Extra:      `{"trace_id":"4bf92f35","span_id":"00f067aa","order_id":"998"}`,
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	decoded, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry() returned error: %v", err)
	}

	if !decoded.TS.Equal(original.TS) {
		t.Errorf("TS = %v, want %v", decoded.TS, original.TS)
	}
	if !decoded.IngestedAt.Equal(original.IngestedAt) {
		t.Errorf("IngestedAt = %v, want %v", decoded.IngestedAt, original.IngestedAt)
	}
	if decoded.Level != original.Level {
		t.Errorf("Level = %d, want %d", decoded.Level, original.Level)
	}
	if decoded.ServiceID != original.ServiceID {
		t.Errorf("ServiceID = %d, want %d", decoded.ServiceID, original.ServiceID)
	}
	if decoded.Message != original.Message {
		t.Errorf("Message = %q, want %q", decoded.Message, original.Message)
	}
	if decoded.Extra != original.Extra {
		t.Errorf("Extra = %q, want %q", decoded.Extra, original.Extra)
	}
}

func TestEncodeDecodeRoundtrip_EmptyMessage(t *testing.T) {
	original := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelInfo,
		ServiceID:  1,
		Message:    "",
		Extra:      `{"key":"value"}`,
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	decoded, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry() returned error: %v", err)
	}

	if decoded.Message != "" {
		t.Errorf("Message = %q, want empty", decoded.Message)
	}
	if decoded.Extra != original.Extra {
		t.Errorf("Extra = %q, want %q", decoded.Extra, original.Extra)
	}
}

func TestEncodeDecodeRoundtrip_EmptyExtra(t *testing.T) {
	original := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelDebug,
		ServiceID:  3,
		Message:    "some message",
		Extra:      "",
	}

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	decoded, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry() returned error: %v", err)
	}

	if decoded.Message != original.Message {
		t.Errorf("Message = %q, want %q", decoded.Message, original.Message)
	}
	if decoded.Extra != "" {
		t.Errorf("Extra = %q, want empty", decoded.Extra)
	}
}

func TestDecode_TruncatedData(t *testing.T) {
	// A valid full entry
	entry := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelWarn,
		ServiceID:  7,
		Message:    "hello",
		Extra:      `{}`,
	}
	full, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"empty slice", []byte{}},
		{"one byte", full[:1]},
		{"header only (no lengths)", full[:21]},
		{"missing extra_len", full[:21+len(entry.Message)]},
		{"truncated in extra", full[:len(full)-1]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeLogEntry(tt.data)
			if err == nil {
				t.Error("expected error for truncated data, got nil")
			}
			if !errors.Is(err, ErrMalformedRecord) {
				t.Errorf("error should wrap ErrMalformedRecord, got: %v", err)
			}
		})
	}
}

func TestEncodedSize_MatchesActual(t *testing.T) {
	tests := []struct {
		name  string
		entry LogEntry
	}{
		{
			name: "full entry",
			entry: LogEntry{
				TS:         time.Now(),
				IngestedAt: time.Now(),
				Level:      LogLevelError,
				ServiceID:  99,
				Message:    "some log message here",
				Extra:      `{"trace_id":"abc"}`,
			},
		},
		{
			name: "empty message and extra",
			entry: LogEntry{
				TS:         time.Now(),
				IngestedAt: time.Now(),
				Level:      LogLevelDebug,
				ServiceID:  1,
				Message:    "",
				Extra:      "",
			},
		},
		{
			name: "unicode message",
			entry: LogEntry{
				TS:         time.Now(),
				IngestedAt: time.Now(),
				Level:      LogLevelInfo,
				ServiceID:  2,
				Message:    "こんにちは世界",
				Extra:      `{"key":"値"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := tt.entry.Encode()
			if err != nil {
				t.Fatalf("Encode() returned error: %v", err)
			}
			expected := tt.entry.EncodedSize()
			if len(encoded) != expected {
				t.Errorf("EncodedSize() = %d, actual encoded length = %d", expected, len(encoded))
			}
		})
	}
}

func TestEncode_MessageAtMaxSize(t *testing.T) {
	// maxMessageSize is 65535. Must encode successfully.
	msg := strings.Repeat("x", maxMessageSize)
	entry := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelInfo,
		ServiceID:  1,
		Message:    msg,
		Extra:      "",
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode() at max message size returned error: %v", err)
	}

	decoded, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry() returned error: %v", err)
	}
	if len(decoded.Message) != maxMessageSize {
		t.Errorf("message length = %d, want %d", len(decoded.Message), maxMessageSize)
	}
}

func TestEncode_MessageExceedsMaxSize(t *testing.T) {
	msg := strings.Repeat("x", maxMessageSize+1)
	entry := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelInfo,
		ServiceID:  1,
		Message:    msg,
	}

	_, err := entry.Encode()
	if err == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("error should wrap ErrEntryTooLarge, got: %v", err)
	}
}

func TestEncode_ExtraExceedsMaxSize(t *testing.T) {
	extra := strings.Repeat("x", maxExtraSize+1)
	entry := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelInfo,
		ServiceID:  1,
		Message:    "ok",
		Extra:      extra,
	}

	_, err := entry.Encode()
	if err == nil {
		t.Fatal("expected error for oversized extra, got nil")
	}
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("error should wrap ErrEntryTooLarge, got: %v", err)
	}
}

func TestLogLevel_EncodeDecode(t *testing.T) {
	levels := []LogLevel{LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError}
	now := time.Now()

	for _, level := range levels {
		entry := LogEntry{
			TS:         now,
			IngestedAt: now,
			Level:      level,
			ServiceID:  1,
			Message:    "test",
		}

		encoded, err := entry.Encode()
		if err != nil {
			t.Fatalf("Encode() for level %s returned error: %v", level, err)
		}

		decoded, err := DecodeLogEntry(encoded)
		if err != nil {
			t.Fatalf("DecodeLogEntry() for level %s returned error: %v", level, err)
		}

		if decoded.Level != level {
			t.Errorf("level = %d (%s), want %d (%s)", decoded.Level, decoded.Level, level, level)
		}
	}
}

func TestLogLevel_String(t *testing.T) {
	tests := []struct {
		level LogLevel
		want  string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
		{LogLevel(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.want {
			t.Errorf("LogLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input string
		want  LogLevel
		err   bool
	}{
		{"DEBUG", LogLevelDebug, false},
		{"INFO", LogLevelInfo, false},
		{"WARN", LogLevelWarn, false},
		{"ERROR", LogLevelError, false},
		{"debug", LogLevelDebug, false}, // case-insensitive
		{"TRACE", LogLevelDebug, false}, // alias for debug
		{"", 0, true},                   // empty is still invalid
	}

	for _, tt := range tests {
		got, err := ParseLogLevel(tt.input)
		if tt.err && err == nil {
			t.Errorf("ParseLogLevel(%q) expected error, got nil", tt.input)
		}
		if !tt.err && err != nil {
			t.Errorf("ParseLogLevel(%q) returned error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseLogLevel(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestDecode_InvalidLogLevel(t *testing.T) {
	entry := LogEntry{
		TS:         time.Now(),
		IngestedAt: time.Now(),
		Level:      LogLevelError,
		ServiceID:  1,
		Message:    "test",
	}
	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode() returned error: %v", err)
	}

	// Corrupt the level byte to an invalid value.
	encoded[16] = 0xFF

	_, err = DecodeLogEntry(encoded)
	if err == nil {
		t.Fatal("expected error for invalid log level, got nil")
	}
	if !errors.Is(err, ErrMalformedRecord) {
		t.Errorf("error should wrap ErrMalformedRecord, got: %v", err)
	}
}
