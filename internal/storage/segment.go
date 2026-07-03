package storage

import (
	"fmt"
	"os"
)

// Segment wraps an os.File representing a single log segment.
// Each segment is an append-only file of 4KB pages. Only one segment is
// "active" (accepting writes) at a time; sealed segments are read-only.
type Segment struct {
	ID      uint64
	Path    string
	File    *os.File
	Size    int64  // current file size in bytes
	MinTS   int64  // earliest entry timestamp in this segment
	MaxTS   int64  // latest entry timestamp in this segment
	PageNum uint64 // next page number to write
}

// CreateSegment creates a new empty segment file and writes an empty first page.
func CreateSegment(path string) (*Segment, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating segment %s: %w", path, err)
	}

	// Initialize the first page with a header.
	h := NewPageHeader()
	h.Checksum = h.CalculateChecksum()

	buf := make([]byte, PageSize)
	encodePageHeader(buf, h)
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return nil, fmt.Errorf("writing initial page: %w", err)
	}

	return &Segment{
		Path:    path,
		File:    f,
		Size:    PageSize,
		PageNum: 1, // next page to write
		MinTS:   1<<63 - 1,
		MaxTS:   -1 << 63,
	}, nil
}

// OpenSegment opens an existing segment file for reading and appending.
func OpenSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening segment %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}

	size := info.Size()
	if size < PageSize {
		f.Close()
		return nil, fmt.Errorf("segment %s too small: %d bytes", path, size)
	}

	// Scan pages to determine MinTS, MaxTS, and next page number.
	numPages := uint64(size / PageSize)
	minTS := int64(1<<63 - 1)
	maxTS := int64(-1 << 63)

	for i := uint64(1); i < numPages; i++ {
		offset := PageOffset(i)
		h, err := ReadPageHeaderAt(f, offset)
		if err != nil {
			continue
		}
		if err := h.Validate(); err != nil {
			continue
		}
		if h.EntryCount > 0 {
			if h.MinTS < minTS {
				minTS = h.MinTS
			}
			if h.MaxTS > maxTS {
				maxTS = h.MaxTS
			}
		}
	}

	return &Segment{
		Path:    path,
		File:    f,
		Size:    size,
		PageNum: numPages,
		MinTS:   minTS,
		MaxTS:   maxTS,
	}, nil
}

// WritePage writes a full 4KB page at the given page number.
func (seg *Segment) WritePage(pageNum uint64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(data))
	}
	offset := PageOffset(pageNum)
	n, err := seg.File.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("writing page %d at offset %d: %w", pageNum, offset, err)
	}
	if n != PageSize {
		return fmt.Errorf("short write: %d of %d bytes", n, PageSize)
	}

	newSize := offset + int64(PageSize)
	if newSize > seg.Size {
		seg.Size = newSize
	}
	return nil
}

// ReadPage reads a full 4KB page at the given page number.
func (seg *Segment) ReadPage(pageNum uint64) ([]byte, error) {
	buf := make([]byte, PageSize)
	offset := PageOffset(pageNum)
	n, err := seg.File.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("reading page %d: %w", pageNum, err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read at page %d: %d of %d bytes", pageNum, n, PageSize)
	}
	return buf, nil
}

// ReadPageHeader reads and validates the header of a specific page.
func (seg *Segment) ReadPageHeader(pageNum uint64) (PageHeader, error) {
	return ReadPageHeaderAt(seg.File, PageOffset(pageNum))
}

// Sync fsyncs the segment file to disk.
func (seg *Segment) Sync() error {
	return seg.File.Sync()
}

// Close closes the segment file.
func (seg *Segment) Close() error {
	return seg.File.Close()
}
