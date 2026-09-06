package storage

import "errors"

// Sentinel errors returned by storage operations.
var (
	// ErrPageCorrupted is returned when a page has an invalid magic number or
	// a checksum mismatch, indicating the page data has been corrupted.
	ErrPageCorrupted = errors.New("page corruption detected: invalid magic or checksum")

	// ErrEntryTooLarge is returned when a log entry's message or extra field
	// exceeds the maximum allowed size (65535 bytes).
	ErrEntryTooLarge = errors.New("log entry exceeds maximum size")

	// ErrMalformedRecord is returned when binary record data cannot be decoded
	// because it is truncated or contains invalid field values.
	ErrMalformedRecord = errors.New("malformed binary record")
)

// EntryPredicate is an optional per-entry filter applied during the scan loop.
// Implementations include compiled LogQL pipeline stages (line filters, parsers,
// label filters). Returning true means the entry passes this filter.
type EntryPredicate interface {
	Match(entry LogEntry) bool
}

// QueryFilter describes a log query. Zero values mean "no filter on that field".
// LabelMatch supports exact key=value pairs parsed from LogQL like {key="val"}.
type QueryFilter struct {
	StartTS      int64             // earliest event timestamp (unix nano), inclusive
	EndTS        int64             // latest event timestamp (unix nano), inclusive
	LabelMatch   map[string]string // exact label matchers from LogQL query
	FieldFilters map[string]string // template field filters, e.g. {"order_id": "1234"}
	Limit        int               // max results to return (0 = default 100)
	Direction    int               // 0 = backward (newest first), 1 = forward
	Predicate    EntryPredicate    // optional per-entry filter (line filters, parsers, label filters)
}

// Storage is the interface all storage backends implement.
// The default implementation is FileStorage which writes to the local filesystem
// using append-only 4KB pages and time-segmented files.
type Storage interface {
	// Write enqueues a log entry for persistence. Returns immediately;
	// the entry is actually flushed by the background writer goroutine.
	Write(entry LogEntry) error

	// Query returns log entries matching the filter. Results are sorted
	// by timestamp according to Direction.
	Query(filter QueryFilter) ([]LogEntry, error)

	// Labels returns all known label keys (e.g. "service", "level").
	Labels() ([]string, error)

	// LabelValues returns all known values for a given label key.
	LabelValues(label string) ([]string, error)

	// Close shuts down the storage backend, flushing any buffered data.
	Close() error
}
