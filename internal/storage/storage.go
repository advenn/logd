package storage

import "errors"

var (
	ErrPageCorrupted = errors.New("page corruption detected: invalid magic or checksum")
	ErrEntryTooLarge = errors.New("log entry exceeds maximum size")

	ErrMalformedRecord = errors.New("malformed binary record")
)
