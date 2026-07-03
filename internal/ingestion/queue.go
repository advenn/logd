package ingestion

import (
	"fmt"

	"github.com/advenn/logd/internal/storage"
)

// WriteQueue wraps a buffered channel to feed the storage writer goroutine.
// It provides backpressure: when full, Enqueue returns an error rather than
// blocking the HTTP handler.
type WriteQueue struct {
	store storage.Storage
}

// NewWriteQueue creates a WriteQueue backed by the given Storage.
func NewWriteQueue(store storage.Storage) *WriteQueue {
	return &WriteQueue{store: store}
}

// Enqueue pushes a log entry to the writer. Returns an error if the write
// channel is full.
func (q *WriteQueue) Enqueue(entry storage.LogEntry) error {
	return q.store.Write(entry)
}

// EnqueueBatch pushes multiple entries, stopping on first error.
func (q *WriteQueue) EnqueueBatch(entries []storage.LogEntry) error {
	for i, e := range entries {
		if err := q.store.Write(e); err != nil {
			return fmt.Errorf("enqueue entry %d: %w", i, err)
		}
	}
	return nil
}
