package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Page-oriented I/O. Every read and write operates on a full 4KB page — never on
// individual records. This is the Postgres-inspired fixed-page design: records are
// assembled into a page in RAM and only whole pages ever touch the disk, so a page
// is the atom of both I/O and corruption detection.
const (
	// PageSize is the fixed size of every data page in bytes.
	PageSize = 4096

	// PageHeaderSize is the size of the page header in bytes (a fixed 32-byte struct).
	PageHeaderSize = 32

	// PageMagic identifies a valid logd data page. Note: the bytes 0x4C 0x30 0x47
	// 0x4E are ASCII "L0GN" (0x4E = 'N'). (An earlier comment claimed "L0GD"; the
	// constant value — not the mnemonic — is authoritative for on-disk validation.)
	PageMagic uint32 = 0x4C30474E
)

// ieeeCRC is the CRC32 table using the IEEE polynomial, used for page checksums.
var ieeeCRC = crc32.MakeTable(crc32.IEEE)

// PageHeader is the 32-byte header at the start of every 4KB page. MinTS/MaxTS let
// a query skip an entire page during a time-range scan without decoding any record;
// the checksum detects corruption or a torn (partially written) page after a crash.
type PageHeader struct {
	Magic           uint32 // 4 bytes — always PageMagic
	MinTS           int64  // 8 bytes — earliest event timestamp in this page (unix nano)
	MaxTS           int64  // 8 bytes — latest event timestamp in this page (unix nano)
	EntryCount      uint16 // 2 bytes — number of records stored in this page
	FreeSpaceOffset uint16 // 2 bytes — byte offset from page start where the next record goes
	Checksum        uint32 // 4 bytes — CRC32-IEEE over the FULL page with this field zeroed
	Reserved        uint32 // 4 bytes — reserved, always 0
}

// Header field offsets within the 32-byte header (and within a page, since the
// header is at the page's front). checksumFieldOffset is the range zeroed before
// computing the full-page checksum.
const (
	checksumFieldOffset = 24 // Checksum occupies bytes [24:28]
	checksumFieldEnd    = 28
)

// NewPageHeader returns a header with Magic set, timestamp bounds at their
// sentinels (max int64 for MinTS, min int64 for MaxTS) so the first record's
// timestamp replaces both, and FreeSpaceOffset pointing just past the header.
func NewPageHeader() PageHeader {
	return PageHeader{
		Magic:           PageMagic,
		MinTS:           1<<63 - 1, // max int64 — first record lowers this
		MaxTS:           -1 << 63,  // min int64 — first record raises this
		FreeSpaceOffset: PageHeaderSize,
	}
}

// ChecksumPage computes CRC32-IEEE over the ENTIRE 4096-byte page with the 4-byte
// checksum field (bytes [24:28]) treated as zero.
//
// This is the load-bearing difference from a header-only checksum: covering the
// whole page means a bit-flip or a torn write anywhere in the RECORD region — not
// just the header — is detected. Torn-page recovery on startup depends on this.
//
// It streams the CRC over the two regions surrounding the checksum field with four
// zero bytes in between, so it reads the page without mutating it — safe to call on
// a shared or read-only buffer (no data race, no in-place write).
func ChecksumPage(page []byte) uint32 {
	h := crc32.New(ieeeCRC)
	h.Write(page[:checksumFieldOffset])       // bytes before the checksum field
	h.Write(zeroChecksum[:])                   // the checksum field, as zero
	h.Write(page[checksumFieldEnd:])           // bytes after the checksum field
	return h.Sum32()
}

// zeroChecksum is the 4-byte zero stand-in for the checksum field during hashing.
var zeroChecksum [4]byte

// ValidatePage checks a full 4096-byte page: correct length, valid magic, and a
// matching full-page checksum. Returns ErrPageCorrupted on any failure. This is the
// authoritative page integrity check used by readers and by crash recovery.
func ValidatePage(page []byte) error {
	if len(page) != PageSize {
		return fmt.Errorf("%w: page length %d, expected %d", ErrPageCorrupted, len(page), PageSize)
	}
	magic := binary.BigEndian.Uint32(page[0:4])
	if magic != PageMagic {
		return fmt.Errorf("%w: invalid magic 0x%08X, expected 0x%08X", ErrPageCorrupted, magic, PageMagic)
	}
	stored := binary.BigEndian.Uint32(page[checksumFieldOffset:checksumFieldEnd])
	computed := ChecksumPage(page)
	if stored != computed {
		return fmt.Errorf("%w: checksum mismatch (stored 0x%08X, computed 0x%08X)", ErrPageCorrupted, stored, computed)
	}
	return nil
}

// FinalizePage stamps the header into the front of a full 4KB page buffer and sets
// the header's Checksum to the full-page checksum, leaving the page ready to write.
// The writer calls this once a page is full (or being flushed): it must be the last
// mutation before the page hits disk, since the checksum covers every byte.
func FinalizePage(page []byte, h PageHeader) {
	h.Checksum = 0
	encodePageHeader(page[:PageHeaderSize], h)
	sum := ChecksumPage(page)
	binary.BigEndian.PutUint32(page[checksumFieldOffset:checksumFieldEnd], sum)
}

// WritePageHeader serializes a PageHeader to w in big-endian binary form.
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

// ReadPageHeaderAt reads a PageHeader from a specific byte offset. This is the
// query fast path: seek to a page number and read just its 32-byte header (for
// MinTS/MaxTS pruning) without pulling the whole 4KB page.
func ReadPageHeaderAt(r io.ReaderAt, offset int64) (PageHeader, error) {
	buf := make([]byte, PageHeaderSize)
	if _, err := r.ReadAt(buf, offset); err != nil {
		return PageHeader{}, fmt.Errorf("reading page header at offset %d: %w", offset, err)
	}
	return decodePageHeader(buf), nil
}

func encodePageHeader(buf []byte, h PageHeader) {
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint64(buf[4:12], uint64(h.MinTS))
	binary.BigEndian.PutUint64(buf[12:20], uint64(h.MaxTS))
	binary.BigEndian.PutUint16(buf[20:22], h.EntryCount)
	binary.BigEndian.PutUint16(buf[22:24], h.FreeSpaceOffset)
	binary.BigEndian.PutUint32(buf[24:28], h.Checksum)
	binary.BigEndian.PutUint32(buf[28:32], h.Reserved)
}

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

// PageOffset returns the byte offset of a page within a segment file. Page 0 starts
// at byte 0, page 1 at 4096, and so on.
func PageOffset(pageNumber uint64) int64 {
	return int64(pageNumber * PageSize)
}

// CanFitEntry reports whether an entry of entrySize bytes fits in a page's
// remaining free space. pageFreeSpace is (PageSize - header.FreeSpaceOffset).
func CanFitEntry(pageFreeSpace uint16, entrySize int) bool {
	return int(pageFreeSpace) >= entrySize
}
