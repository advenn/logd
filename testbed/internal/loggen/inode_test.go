package loggen

import (
	"os"
	"syscall"
	"testing"
)

// inodeOf returns the inode of a path, used to prove that rotation replaces the file
// rather than truncating it in place.
func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("inode not available on this platform")
	}
	return st.Ino
}
