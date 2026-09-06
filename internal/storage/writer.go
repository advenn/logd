package storage

import (
	"sync"

	"github.com/advenn/logd/internal/config"
)

// FileStorage implements the Storage interface using the local filesystem.
// It uses a single background writer goroutine to serialize all I/O, eliminating
// file locking. HTTP handlers push entries to a channel; the writer goroutine
// is the sole reader of that channel and the sole writer to disk.
type FileStorage struct {
	cfg     *config.Config
	dataDir string

	// Write channel — HTTP handlers push here, writer goroutine consumes.
	writeCh chan LogEntry
	closeCh chan struct{}
	doneCh  chan struct{}
	writeWG sync.WaitGroup

	// Current write state (only touched by writer goroutine).
	mu       sync.Mutex
	manifest *Manifest
}
