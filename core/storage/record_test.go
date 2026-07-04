package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/advenn/logd/core/model"
)

// sampleEntry returns a fully-populated entry (every field non-zero) for
// round-trip tests. Timestamps are fixed instants so tests are deterministic.
func sampleEntry() model.LogEntry {
	return model.LogEntry{
		TS:         time.Unix(1700000000, 123456789).UTC(),
		IngestedAt: time.Unix(1700000001, 987654321).UTC(),
		Level:      model.LogLevelWarn,
		ServiceID:  0xABCD,
		StreamID:   0xDEADBEEF,
		Message:    "took 247ms to serve order-42",
		Extra:      `{"trace_id":"abc","span_id":"def"}`,
	}
}

// assertRoundTrip encodes then decodes e and checks every field survives. It
// compares instants via UnixNano (not ==) because time.Time equality is sensitive
// to Location and the monotonic-clock reading, neither of which the wire preserves.
func assertRoundTrip(t *testing.T, e model.LogEntry) {
	t.Helper()
	buf, err := EncodeEntry(e)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	if got := EncodedSize(e); got != len(buf) {
		t.Fatalf("EncodedSize=%d but EncodeEntry produced %d bytes", got, len(buf))
	}
	got, err := DecodeEntry(buf)
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if got.TS.UnixNano() != e.TS.UnixNano() {
		t.Errorf("TS: got %d, want %d", got.TS.UnixNano(), e.TS.UnixNano())
	}
	if got.IngestedAt.UnixNano() != e.IngestedAt.UnixNano() {
		t.Errorf("IngestedAt: got %d, want %d", got.IngestedAt.UnixNano(), e.IngestedAt.UnixNano())
	}
	if got.Level != e.Level {
		t.Errorf("Level: got %v, want %v", got.Level, e.Level)
	}
	if got.ServiceID != e.ServiceID {
		t.Errorf("ServiceID: got %d, want %d", got.ServiceID, e.ServiceID)
	}
	if got.StreamID != e.StreamID {
		t.Errorf("StreamID: got %d, want %d", got.StreamID, e.StreamID)
	}
	if got.Message != e.Message {
		t.Errorf("Message: got %q, want %q", got.Message, e.Message)
	}
	if got.Extra != e.Extra {
		t.Errorf("Extra: got %q, want %q", got.Extra, e.Extra)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	assertRoundTrip(t, sampleEntry())
}

func TestEncodeDecodeEmptyMessage(t *testing.T) {
	e := sampleEntry()
	e.Message = ""
	assertRoundTrip(t, e)
}

func TestEncodeDecodeEmptyExtra(t *testing.T) {
	e := sampleEntry()
	e.Extra = ""
	assertRoundTrip(t, e)
}

func TestEncodeDecodeEmptyMessageAndExtra(t *testing.T) {
	e := sampleEntry()
	e.Message = ""
	e.Extra = ""
	assertRoundTrip(t, e)
	// The smallest possible record is exactly the 27-byte fixed prefix.
	buf, _ := EncodeEntry(e)
	if len(buf) != recordPrefixSize {
		t.Fatalf("empty record is %d bytes, want %d (the fixed prefix)", len(buf), recordPrefixSize)
	}
	if recordPrefixSize != 27 {
		t.Fatalf("record prefix must be 27 bytes per design §4, got %d", recordPrefixSize)
	}
}

func TestEncodedSizeMatchesEncode(t *testing.T) {
	e := sampleEntry()
	buf, _ := EncodeEntry(e)
	if EncodedSize(e) != len(buf) {
		t.Fatalf("EncodedSize=%d, len(Encode)=%d", EncodedSize(e), len(buf))
	}
	if EncodedSize(e) != 27+len(e.Message)+len(e.Extra) {
		t.Fatalf("EncodedSize formula wrong: %d", EncodedSize(e))
	}
}

func TestAllLogLevelsRoundTrip(t *testing.T) {
	for _, lvl := range []model.LogLevel{model.LogLevelDebug, model.LogLevelInfo, model.LogLevelWarn, model.LogLevelError} {
		e := sampleEntry()
		e.Level = lvl
		assertRoundTrip(t, e)
	}
}

func TestDecodeTruncated(t *testing.T) {
	buf, _ := EncodeEntry(sampleEntry())
	// Every strict prefix of a valid record must be rejected, never panic.
	for n := 0; n < len(buf); n++ {
		if _, err := DecodeEntry(buf[:n]); err == nil {
			t.Fatalf("DecodeEntry accepted truncated %d-byte slice", n)
		} else if !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("truncated at %d: got %v, want ErrMalformedRecord", n, err)
		}
	}
}

func TestDecodeInvalidLevel(t *testing.T) {
	buf, _ := EncodeEntry(sampleEntry())
	buf[16] = 0x04 // one past ERROR
	if _, err := DecodeEntry(buf); !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("got %v, want ErrMalformedRecord for level 4", err)
	}
}

func TestEncodeMessageAtMax(t *testing.T) {
	e := sampleEntry()
	e.Message = strings.Repeat("x", maxFieldSize)
	if _, err := EncodeEntry(e); err != nil {
		t.Fatalf("message of exactly %d bytes should encode, got %v", maxFieldSize, err)
	}
}

func TestEncodeMessageTooLarge(t *testing.T) {
	e := sampleEntry()
	e.Message = strings.Repeat("x", maxFieldSize+1)
	if _, err := EncodeEntry(e); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("got %v, want ErrEntryTooLarge", err)
	}
}

func TestEncodeExtraTooLarge(t *testing.T) {
	e := sampleEntry()
	e.Extra = strings.Repeat("x", maxFieldSize+1)
	if _, err := EncodeEntry(e); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("got %v, want ErrEntryTooLarge", err)
	}
}

// Unrepresentable timestamps must be rejected, not silently wrapped (which would
// read back as a different instant and poison the page/segment time bounds).
func TestEncodeRejectsUnrepresentableTimestamp(t *testing.T) {
	cases := map[string]time.Time{
		"zero time.Time":  {},
		"after year 2262": time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC),
		"before 1678":     time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for name, ts := range cases {
		t.Run(name, func(t *testing.T) {
			e := sampleEntry()
			e.TS = ts
			if _, err := EncodeEntry(e); !errors.Is(err, ErrTimestampRange) {
				t.Fatalf("TS %v: got %v, want ErrTimestampRange", ts, err)
			}
		})
	}
	// An unrepresentable IngestedAt is rejected too.
	e := sampleEntry()
	e.IngestedAt = time.Time{}
	if _, err := EncodeEntry(e); !errors.Is(err, ErrTimestampRange) {
		t.Fatalf("zero IngestedAt: got %v, want ErrTimestampRange", err)
	}
}
