package label

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeFileAtomic writes data durably: tmp → fsync → rename → parent-dir fsync, so a
// reader or a crash sees either the old file or the complete new one, never a torn
// write. (Re-implemented here to keep the label package a stdlib-only leaf.)
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer d.Close()
	return d.Sync()
}
