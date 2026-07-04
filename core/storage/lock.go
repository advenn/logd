package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// acquireShardLock takes an exclusive, non-blocking advisory (flock) lock on <dir>/LOCK so
// only one process writes a shard at a time (design §10). The returned file must stay open
// for the lock's lifetime and be closed to release it.
func acquireShardLock(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening shard lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("shard %q is already locked by another process: %w", dir, err)
	}
	return f, nil
}

// releaseShardLock unlocks and closes the shard lock file.
func releaseShardLock(f *os.File) {
	if f == nil {
		return
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}
