package storage

import (
	"fmt"
	"os"
)

// Segment is a single append-only log file: an array of 4KB pages wrapping an
// os.File. Page 0 is a header-only placeholder; data pages start at page 1. Only
// one segment is "active" (accepting appends) at a time; sealed segments are
// immutable and read-only (design §2 invariants 3, 4).
type Segment struct {
	ID      uint64
	Path    string
	File    *os.File
	Size    int64  // current file size in bytes
	MinTS   int64  // earliest event timestamp across valid pages (sentinel maxint64 if empty)
	MaxTS   int64  // latest event timestamp across valid pages (sentinel minint64 if empty)
	PageNum uint64 // next page number to write (== count of pages currently in the file)
}

const (
	emptyMinTS = int64(1<<63 - 1) // max int64: an empty segment's MinTS sentinel
	emptyMaxTS = int64(-1 << 63)  // min int64: an empty segment's MaxTS sentinel
)

// CreateSegment creates a fresh segment file and writes the page-0 placeholder
// header. It fsyncs so the file exists durably even if a crash follows immediately.
func CreateSegment(path string) (*Segment, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating segment %s: %w", path, err)
	}

	// Page 0: a finalized (full-page-checksummed) placeholder. It holds no records
	// — data begins at page 1 — but writing a valid page keeps the file uniform.
	page := make([]byte, PageSize)
	FinalizePage(page, NewPageHeader())
	if _, err := f.Write(page); err != nil {
		f.Close()
		return nil, fmt.Errorf("writing initial page: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("syncing new segment: %w", err)
	}

	return &Segment{
		Path:    path,
		File:    f,
		Size:    PageSize,
		PageNum: 1, // page 0 written; next data page is 1
		MinTS:   emptyMinTS,
		MaxTS:   emptyMaxTS,
	}, nil
}

// OpenSegment opens an existing segment read-write and performs crash recovery on
// it. It reads every data page in full and validates it (magic + full-page
// checksum). The FIRST page that fails validation is treated as a torn trailing
// page from a crash mid-write: the file is truncated to the last known-good page
// boundary and scanning stops. MinTS/MaxTS and the next page number are derived
// from the surviving valid pages (design §8).
//
// Because torn-page truncation mutates the file, OpenSegment is the writer's
// recovery primitive — it opens for append. (The read-only query path, added
// later, validates per page and degrades to skipping corrupt pages, never
// truncates.)
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

	// A trailing partial page (size not a page multiple) is itself a torn write:
	// clamp the scan to whole pages; the tail gets truncated below.
	numWholePages := uint64(size / PageSize)

	minTS := emptyMinTS
	maxTS := emptyMaxTS
	lastGoodPage := uint64(0) // page 0 is the placeholder; data starts at 1
	page := make([]byte, PageSize)

	for i := uint64(1); i < numWholePages; i++ {
		if _, err := f.ReadAt(page, PageOffset(i)); err != nil {
			break // short read: treat the rest as torn
		}
		if err := ValidatePage(page); err != nil {
			break // first corrupt/torn page ends the valid prefix
		}
		h := decodePageHeader(page[:PageHeaderSize])
		if h.EntryCount > 0 {
			if h.MinTS < minTS {
				minTS = h.MinTS
			}
			if h.MaxTS > maxTS {
				maxTS = h.MaxTS
			}
		}
		lastGoodPage = i
	}

	// Truncate away any torn trailing page(s) / partial tail so the file contains
	// only whole, valid pages. nextPage is where the writer will append.
	nextPage := lastGoodPage + 1
	goodSize := PageOffset(nextPage)
	if goodSize != size {
		if err := f.Truncate(goodSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncating torn tail of %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("syncing after truncate %s: %w", path, err)
		}
	}

	return &Segment{
		Path:    path,
		File:    f,
		Size:    goodSize,
		PageNum: nextPage,
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
		return fmt.Errorf("short write at page %d: %d of %d bytes", pageNum, n, PageSize)
	}
	if newSize := offset + PageSize; newSize > seg.Size {
		seg.Size = newSize
	}
	return nil
}

// ReadPage reads a full 4KB page at the given page number.
func (seg *Segment) ReadPage(pageNum uint64) ([]byte, error) {
	buf := make([]byte, PageSize)
	n, err := seg.File.ReadAt(buf, PageOffset(pageNum))
	if err != nil {
		return nil, fmt.Errorf("reading page %d: %w", pageNum, err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read at page %d: %d of %d bytes", pageNum, n, PageSize)
	}
	return buf, nil
}

// ReadPageHeader reads (header-only) the header of a specific page.
func (seg *Segment) ReadPageHeader(pageNum uint64) (PageHeader, error) {
	return ReadPageHeaderAt(seg.File, PageOffset(pageNum))
}

// Sync fsyncs the segment file to disk. This is the durability primitive the
// writer's group commit relies on.
func (seg *Segment) Sync() error {
	return seg.File.Sync()
}

// Close closes the segment file.
func (seg *Segment) Close() error {
	return seg.File.Close()
}
