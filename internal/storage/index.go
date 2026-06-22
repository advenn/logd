package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// indexRecordSize is 16 bytes: [8 bytes ts nano][8 bytes page_number].
const indexRecordSize = 16

// IndexWriter appends timestamp→page entries to a sparse index file.
// The file format is a flat array of fixed 16-byte records sorted by ts,
// enabling binary search with direct offset calculation.
type IndexWriter struct {
	file *os.File
}

// OpenIndexWriter creates an index file for writing (append-only).
func OpenIndexWriter(path string) (*IndexWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening index writer %s: %w", path, err)
	}
	return &IndexWriter{file: f}, nil
}

// WriteEntry appends a timestamp→page_entry to the index.
func (w *IndexWriter) WriteEntry(ts int64, pageNumber uint64) error {
	buf := make([]byte, indexRecordSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(ts))
	binary.BigEndian.PutUint64(buf[8:16], pageNumber)
	_, err := w.file.Write(buf)
	return err
}

// Sync fsyncs the index file.
func (w *IndexWriter) Sync() error {
	return w.file.Sync()
}

// Close closes the index file.
func (w *IndexWriter) Close() error {
	return w.file.Close()
}

// IndexReader reads and searches a sparse index file.
type IndexReader struct {
	file    *os.File
	numRecs int64
}

// OpenIndexReader opens an existing index file for reading.
func OpenIndexReader(path string) (*IndexReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening index file %s: %w", path, err)
	}
	info, err := f.Stat()

	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stating index file %s: %w", path, err)
	}
	size := info.Size()
	if size%indexRecordSize != 0 {
		f.Close()
		return nil, fmt.Errorf("index file %s size %d is not a multiple of %d", path, size, indexRecordSize)
	}
	return &IndexReader{
		file:    f,
		numRecs: size / indexRecordSize,
	}, nil
}

// Search finds the page number containing entries with timestamp >= ts.
// Returns the page number and the timestamp of the index entry.
// Uses binary search over the fixed-size records — O(log n) with direct seeks.
func (r *IndexReader) Search(ts int64) (uint64, int64, error) {
	if r.numRecs == 0 {
		return 0, 0, fmt.Errorf("empty index")
	}

	// Binary search for the rightmost entry with ts <= target.
	idx := sort.Search(int(r.numRecs), func(i int) bool {
		recTS := r.readTimestamp(int64(i))
		return recTS > ts
	})

	// The entry before idx has ts <= target (or idx == 0 means target < first entry).
	if idx == 0 {
		// Target is before first entry; return first entry.
		ts, pn := r.readEntry(0)
		return pn, ts, nil
	}
	ts, pn := r.readEntry(int64(idx - 1))
	return pn, ts, nil

}

// NumRecords returns the number of index entries.
func (r *IndexReader) NumRecords() int64 {
	return r.numRecs
}

func (r *IndexReader) readTimestamp(rec int64) int64 {
	buf := make([]byte, 8)
	offset := rec * indexRecordSize
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(buf))
}

func (r *IndexReader) readEntry(rec int64) (int64, uint64) {
	buf := make([]byte, indexRecordSize)
	offset := rec * indexRecordSize
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return 0, 0
	}
	ts := int64(binary.BigEndian.Uint64(buf[0:8]))
	pn := binary.BigEndian.Uint64(buf[8:16])
	return ts, pn
}

// Close closes the index file.
func (r *IndexReader) Close() error {
	return r.file.Close()
}
