package storage

import "errors"

// Sentinel errors returned by storage operations. Callers match these with
// errors.Is rather than string comparison.
var (
	// ErrPageCorrupted is returned when a page has an invalid magic number or a
	// checksum mismatch, indicating the page's bytes have been corrupted (or the
	// page was only partially written before a crash — a "torn page").
	ErrPageCorrupted = errors.New("page corruption detected: invalid magic or checksum")

	// ErrEntryTooLarge is returned when a log entry's Message or Extra field
	// exceeds the maximum allowed size (65535 bytes, bounded by the 2-byte length
	// prefixes in the record format).
	ErrEntryTooLarge = errors.New("log entry exceeds maximum size")

	// ErrMalformedRecord is returned when binary record data cannot be decoded
	// because it is truncated or contains an invalid field value (e.g. a log level
	// byte greater than ERROR).
	ErrMalformedRecord = errors.New("malformed binary record")

	// ErrTimestampRange is returned by EncodeEntry when a timestamp cannot be
	// represented as an int64 of unix-nanoseconds — i.e. before ~1678 or after
	// ~2262. Storing such a value would silently wrap and read back as a different
	// instant (and poison the page/segment time bounds), so it is rejected at the
	// edge rather than corrupted.
	ErrTimestampRange = errors.New("timestamp out of representable range")
)
