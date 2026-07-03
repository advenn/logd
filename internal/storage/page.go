package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Page-oriented I/O constants. Every read and write operates on full 4KB pages.
// This matches the Postgres-inspired fixed-page design: the unit of I/O is always
// one page, never individual records.
const (
	// PageSize is the fixed size of every data page in bytes.
	PageSize = 4096

	// PageHeaderSize is the size of the page header in bytes.
	// Must fit exactly in a 32-byte struct for efficient binary encoding.
	PageHeaderSize = 32

	// PageMagic is the magic number identifying a valid logd data page.
	// The bytes "L0GD" encoded as 0x4C30474E in big-endian.
	PageMagic uint32 = 0x4C30474E
)

// ieeeCRC is the CRC32 table using the IEEE polynomial, used for page checksums.
var ieeeCRC = crc32.MakeTable(crc32.IEEE)

// PageHeader is the 32-byte header at the beginning of every 4KB data page.
// It stores metadata that enables fast page-level decisions: the magic number
// detects corruption, min_ts/max_ts allow skipping entire pages during time-range
// scans without reading individual entries.
type PageHeader struct {
	Magic           uint32 // 4 bytes — always PageMagic
	MinTS           int64  // 8 bytes — earliest event timestamp in this page (unix nano)
	MaxTS           int64  // 8 bytes — latest event timestamp in this page (unix nano)
	EntryCount      uint16 // 2 bytes — number of log entries stored in this page
	FreeSpaceOffset uint16 // 2 bytes — byte offset from page start where the next entry would be written
	Checksum        uint32 // 4 bytes — CRC32 of the header with Checksum field zeroed during calculation
	Reserved        uint32 // 4 bytes — reserved for future use, always 0
}

// NewPageHeader returns a PageHeader with Magic pre-set, timestamps initialized
// to their sentinel values (max int64 for MinTS, min int64 for MaxTS) so the
// first entry's timestamp replaces both, and FreeSpaceOffset pointing right after
// the header.
func NewPageHeader() PageHeader {
	return PageHeader{
		Magic:           PageMagic,
		MinTS:           1<<63 - 1, // max int64 — first entry will lower this
		MaxTS:           -1 << 63,  // min int64 — first entry will raise this
		EntryCount:      0,
		FreeSpaceOffset: PageHeaderSize,
		Checksum:        0,
		Reserved:        0,
	}
}

// CalculateChecksum computes the CRC32 checksum of the header struct.
// The Checksum field is treated as zero during calculation so the checksum
// can be verified independently of the stored value.
func (h *PageHeader) CalculateChecksum() uint32 {
	buf := make([]byte, PageHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint64(buf[4:12], uint64(h.MinTS))
	binary.BigEndian.PutUint64(buf[12:20], uint64(h.MaxTS))
	binary.BigEndian.PutUint16(buf[20:22], h.EntryCount)
	binary.BigEndian.PutUint16(buf[22:24], h.FreeSpaceOffset)
	// bytes 24..28: Checksum field, left as zero
	binary.BigEndian.PutUint32(buf[28:32], h.Reserved)
	return crc32.Checksum(buf, ieeeCRC)
}

// Validate checks the magic number and verifies the checksum.
// Returns ErrPageCorrupted if either check fails.
func (h *PageHeader) Validate() error {
	if h.Magic != PageMagic {
		return fmt.Errorf("%w: invalid magic 0x%08X, expected 0x%08X", ErrPageCorrupted, h.Magic, PageMagic)
	}
	expected := h.CalculateChecksum()
	if h.Checksum != expected {
		return fmt.Errorf("%w: checksum mismatch (stored 0x%08X, computed 0x%08X)", ErrPageCorrupted, h.Checksum, expected)
	}
	return nil
}

// WritePageHeader serializes the PageHeader to w in big-endian binary format.
func WritePageHeader(w io.Writer, h PageHeader) error {
	buf := make([]byte, PageHeaderSize)
	encodePageHeader(buf, h)
	_, err := w.Write(buf)
	return err
}

// ReadPageHeader deserializes a PageHeader from r.
func ReadPageHeader(r io.Reader) (PageHeader, error) {
	buf := make([]byte, PageHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return PageHeader{}, fmt.Errorf("reading page header: %w", err)
	}
	return decodePageHeader(buf), nil
}

// ReadPageHeaderAt reads a PageHeader from a specific byte offset using r.
// This is the primary read path during queries — it lets us seek directly to
// a page number and read its header without sequential scanning.
func ReadPageHeaderAt(r io.ReaderAt, offset int64) (PageHeader, error) {
	buf := make([]byte, PageHeaderSize)
	if _, err := r.ReadAt(buf, offset); err != nil {
		return PageHeader{}, fmt.Errorf("reading page header at offset %d: %w", offset, err)
	}
	return decodePageHeader(buf), nil
}

// encodePageHeader writes the header fields into a pre-allocated 32-byte buffer.
func encodePageHeader(buf []byte, h PageHeader) {
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint64(buf[4:12], uint64(h.MinTS))
	binary.BigEndian.PutUint64(buf[12:20], uint64(h.MaxTS))
	binary.BigEndian.PutUint16(buf[20:22], h.EntryCount)
	binary.BigEndian.PutUint16(buf[22:24], h.FreeSpaceOffset)
	binary.BigEndian.PutUint32(buf[24:28], h.Checksum)
	binary.BigEndian.PutUint32(buf[28:32], h.Reserved)
}

// decodePageHeader reads header fields from a 32-byte buffer into a PageHeader.
func decodePageHeader(buf []byte) PageHeader {
	return PageHeader{
		Magic:           binary.BigEndian.Uint32(buf[0:4]),
		MinTS:           int64(binary.BigEndian.Uint64(buf[4:12])),
		MaxTS:           int64(binary.BigEndian.Uint64(buf[12:20])),
		EntryCount:      binary.BigEndian.Uint16(buf[20:22]),
		FreeSpaceOffset: binary.BigEndian.Uint16(buf[22:24]),
		Checksum:        binary.BigEndian.Uint32(buf[24:28]),
		Reserved:        binary.BigEndian.Uint32(buf[28:32]),
	}
}

// PageOffset returns the byte offset in a file for a given page number.
// Page 0 starts at byte 0, page 1 at byte 4096, etc.
func PageOffset(pageNumber uint64) int64 {
	return int64(pageNumber * PageSize)
}

// CanFitEntry reports whether an entry of entrySize bytes fits in the remaining
// free space of a page. pageFreeSpace should be (PageSize - header.FreeSpaceOffset).
func CanFitEntry(pageFreeSpace uint16, entrySize int) bool {
	return int(pageFreeSpace) >= entrySize
}
