package storage

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/advenn/logd/core/model"
)

// This file implements the on-disk binary encoding of a single log record.
//
// All integers are BigEndian. We pick one byte order and use it everywhere:
// BigEndian is network byte order, so records that may one day be shipped across
// a wire need no conversion, and hex dumps of the file read left-to-right like
// the struct.
//
// Record layout (design §4). Fixed-size prefix is 27 bytes; the two variable
// fields are each length-prefixed with a uint16 (so each is capped at 65535):
//
//	[8]  TS         int64 unixnano
//	[8]  IngestedAt int64 unixnano
//	[1]  Level      uint8
//	[2]  ServiceID  uint16
//	[4]  StreamID   uint32     <- new vs the original 23-byte format; sits after ServiceID
//	[2]  MsgLen     uint16   [N] Message
//	[2]  ExtraLen   uint16   [M] Extra
//
//	EncodedSize = 27 + N + M
//
// Byte offsets of the fixed prefix (handy when reading a hex dump):
//
//	TS[0:8] IngestedAt[8:16] Level[16] ServiceID[17:19] StreamID[19:23]
//	MsgLen[23:25] Message[25:25+N] ExtraLen[25+N:27+N] Extra[27+N:27+N+M]
const (
	// recordPrefixSize is the fixed portion preceding the variable fields:
	// 8+8+1+2+4 (=23, up to and including StreamID) + 2 (MsgLen) + 2 (ExtraLen).
	recordPrefixSize = 27

	// maxFieldSize bounds Message and Extra. It is dictated by the 2-byte length
	// prefixes: a uint16 cannot describe more than 65535 bytes.
	maxFieldSize = 65535
)

// EncodedSize returns the exact number of bytes EncodeEntry will produce, without
// allocating or encoding. The writer uses this to decide whether an entry fits in
// the current page before committing to encode it.
func EncodedSize(e model.LogEntry) int {
	return recordPrefixSize + len(e.Message) + len(e.Extra)
}

// EncodeEntry serializes a LogEntry to its on-disk binary form.
//
// Returns ErrEntryTooLarge if Message or Extra exceed maxFieldSize; oversized
// fields cannot be length-prefixed and must be rejected at the edge, never
// silently truncated.
func EncodeEntry(e model.LogEntry) ([]byte, error) {
	if len(e.Message) > maxFieldSize {
		return nil, fmt.Errorf("%w: message size %d exceeds maximum %d", ErrEntryTooLarge, len(e.Message), maxFieldSize)
	}
	if len(e.Extra) > maxFieldSize {
		return nil, fmt.Errorf("%w: extra size %d exceeds maximum %d", ErrEntryTooLarge, len(e.Extra), maxFieldSize)
	}
	// A timestamp outside the int64-nanosecond range (before ~1678 / after ~2262),
	// or a zero time.Time, would wrap under UnixNano and read back as a different
	// instant — and, worse, corrupt the page/segment MinTS bounds and the sparse
	// time index. Reject it here so a bad value can never reach disk. (The writer
	// defaults a zero TS to IngestedAt before calling this; a caller encoding a
	// zero TS directly is misuse and is rejected.)
	if !representableNano(e.TS) {
		return nil, fmt.Errorf("%w: event timestamp %v", ErrTimestampRange, e.TS)
	}
	if !representableNano(e.IngestedAt) {
		return nil, fmt.Errorf("%w: ingested-at timestamp %v", ErrTimestampRange, e.IngestedAt)
	}

	buf := make([]byte, EncodedSize(e))

	binary.BigEndian.PutUint64(buf[0:8], uint64(e.TS.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.IngestedAt.UnixNano()))
	buf[16] = byte(e.Level)
	binary.BigEndian.PutUint16(buf[17:19], e.ServiceID)
	binary.BigEndian.PutUint32(buf[19:23], e.StreamID)

	binary.BigEndian.PutUint16(buf[23:25], uint16(len(e.Message)))
	offset := 25
	offset += copy(buf[offset:], e.Message)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(e.Extra)))
	offset += 2
	copy(buf[offset:], e.Extra)

	return buf, nil
}

// DecodeEntry deserializes a LogEntry from data. It bounds-checks before every
// read so a truncated or malformed slice yields ErrMalformedRecord rather than an
// out-of-range panic — the decoder is fed bytes off disk that a crash may have cut
// short, so it can never trust the length fields.
func DecodeEntry(data []byte) (model.LogEntry, error) {
	if len(data) < recordPrefixSize {
		return model.LogEntry{}, fmt.Errorf("%w: data too short (%d bytes, minimum %d)", ErrMalformedRecord, len(data), recordPrefixSize)
	}

	tsNano := int64(binary.BigEndian.Uint64(data[0:8]))
	ingestedNano := int64(binary.BigEndian.Uint64(data[8:16]))
	level := model.LogLevel(data[16])
	if level > model.LogLevelError {
		return model.LogEntry{}, fmt.Errorf("%w: invalid log level %d", ErrMalformedRecord, level)
	}
	serviceID := binary.BigEndian.Uint16(data[17:19])
	streamID := binary.BigEndian.Uint32(data[19:23])
	msgLen := int(binary.BigEndian.Uint16(data[23:25]))

	offset := 25
	if len(data) < offset+msgLen {
		return model.LogEntry{}, fmt.Errorf("%w: data too short for message (declared %d bytes, have %d)", ErrMalformedRecord, msgLen, len(data)-offset)
	}
	msg := string(data[offset : offset+msgLen])
	offset += msgLen

	if len(data) < offset+2 {
		return model.LogEntry{}, fmt.Errorf("%w: data too short for extra_len field", ErrMalformedRecord)
	}
	extraLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if len(data) < offset+extraLen {
		return model.LogEntry{}, fmt.Errorf("%w: data too short for extra (declared %d bytes, have %d)", ErrMalformedRecord, extraLen, len(data)-offset)
	}
	extra := string(data[offset : offset+extraLen])

	return model.LogEntry{
		TS:         timeFromNano(tsNano),
		IngestedAt: timeFromNano(ingestedNano),
		Level:      level,
		ServiceID:  serviceID,
		StreamID:   streamID,
		Message:    msg,
		Extra:      extra,
	}, nil
}

// timeFromNano rebuilds a time.Time from a stored unix-nano value. We normalize to
// UTC so a decoded entry is independent of the reader's local timezone; only the
// instant matters, and callers compare instants (UnixNano/Equal), never the wall
// clock or Location. EncodeEntry guarantees only representable instants are ever
// stored, so this is always exact.
func timeFromNano(nano int64) time.Time {
	return time.Unix(0, nano).UTC()
}

// representableNano reports whether t survives an exact UnixNano round-trip — i.e.
// its instant fits in an int64 of unix-nanoseconds (roughly the years 1678–2262).
// The zero time.Time (year 1) is not representable and returns false. time.Time.Equal
// compares instants, ignoring Location and the monotonic-clock reading, so a value
// that is merely in a different zone still round-trips true.
func representableNano(t time.Time) bool {
	return time.Unix(0, t.UnixNano()).Equal(t)
}
