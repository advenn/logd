package storage

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Compressed sealed segments.
//
// A segment's records are stored uncompressed in fixed 4 KB pages, which is what makes a
// record addressable by a segment-relative byte offset — the .tidx stores exactly those
// offsets, and the query path turns one into (page, in-page offset) by division. That
// addressing is the reason the file cannot simply be gzipped: a variable-length stream has
// no O(1) mapping from logical offset to file position.
//
// So compression happens at SEAL, into a sidecar with a block directory, and the logical
// offset contract is preserved exactly. Blocks group several pages because 4 KB alone
// compresses poorly — measured on a real 64 MB segment of application logs:
//
//	per 4 KB page     4.1x
//	per 32 KB block   6.2x   <- default
//	per 64 KB block   6.5x
//	whole file        6.9x
//
// Bigger blocks compress better but decompress more per random lookup. 8 pages is the knee:
// most of the available ratio, and both readers touch blocks in ascending order
// (FetchRecords sorts its offsets; scans walk sequentially), so a one-block cache makes the
// amortized cost of a larger block close to nothing.
//
// The active segment is never compressed — writes stay raw and crash recovery keeps
// operating on whole pages with their full-page CRCs.

const (
	segzMagic     = 0x4C534547 // "LSEG"
	segzVersion   = 1
	segzHeaderLen = 24
	segzDirEntry  = 16 // fileOffset u64 + compLen u32 + rawLen u32

	// DefaultBlockPages is the number of 4 KB pages per compressed block.
	DefaultBlockPages = 8
)

// SegzPath returns the compressed-segment path for a segment's .log path.
func SegzPath(logPath string) string {
	return strings.TrimSuffix(logPath, ".log") + ".logz"
}

// CompressSegment rewrites a sealed segment's pages into a compressed sidecar and removes
// the original .log. It is atomic: the sidecar is fully written, fsynced and renamed before
// the raw file is unlinked, so a crash at any point leaves one readable copy — never zero.
//
// blockPages of 0 uses DefaultBlockPages. Returns false (with no error) when compression is
// disabled or the segment is empty, so the caller leaves it raw.
func CompressSegment(logPath string, blockPages int) (bool, error) {
	if blockPages < 0 {
		return false, nil // disabled
	}
	if blockPages == 0 {
		blockPages = DefaultBlockPages
	}

	src, err := os.Open(logPath)
	if err != nil {
		return false, err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return false, err
	}
	numPages := uint64(info.Size() / PageSize)
	if numPages <= 1 {
		return false, nil // page 0 placeholder only: nothing to gain
	}

	tmp := SegzPath(logPath) + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return false, err
	}
	defer func() {
		if out != nil {
			out.Close()
			os.Remove(tmp)
		}
	}()

	numBlocks := (numPages + uint64(blockPages) - 1) / uint64(blockPages)
	dirLen := int(numBlocks) * segzDirEntry
	body := bytes.NewBuffer(nil)
	dir := make([]byte, dirLen)

	raw := make([]byte, blockPages*PageSize)
	fileOff := uint64(segzHeaderLen + dirLen)
	var comp bytes.Buffer
	for b := uint64(0); b < numBlocks; b++ {
		startPage := b * uint64(blockPages)
		n := blockPages
		if remaining := numPages - startPage; remaining < uint64(blockPages) {
			n = int(remaining)
		}
		buf := raw[:n*PageSize]
		if _, err := src.ReadAt(buf, PageOffset(startPage)); err != nil && err != io.EOF {
			return false, fmt.Errorf("reading pages for block %d: %w", b, err)
		}
		comp.Reset()
		zw, err := flate.NewWriter(&comp, flate.DefaultCompression)
		if err != nil {
			return false, err
		}
		if _, err := zw.Write(buf); err != nil {
			return false, err
		}
		if err := zw.Close(); err != nil {
			return false, err
		}
		e := dir[int(b)*segzDirEntry:]
		binary.BigEndian.PutUint64(e[0:8], fileOff)
		binary.BigEndian.PutUint32(e[8:12], uint32(comp.Len()))
		binary.BigEndian.PutUint32(e[12:16], uint32(len(buf)))
		body.Write(comp.Bytes())
		fileOff += uint64(comp.Len())
	}

	hdr := make([]byte, segzHeaderLen)
	binary.BigEndian.PutUint32(hdr[0:4], segzMagic)
	binary.BigEndian.PutUint16(hdr[4:6], segzVersion)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(blockPages))
	binary.BigEndian.PutUint64(hdr[8:16], numPages)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(numBlocks))
	// CRC over the directory, so a corrupt directory is detected before it is used to
	// compute file offsets. Block payloads carry the pages' own full-page CRCs.
	binary.BigEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(dir))

	for _, chunk := range [][]byte{hdr, dir, body.Bytes()} {
		if _, err := out.Write(chunk); err != nil {
			return false, err
		}
	}
	if err := out.Sync(); err != nil {
		return false, err
	}
	if err := out.Close(); err != nil {
		out = nil
		return false, err
	}
	out = nil
	if err := os.Rename(tmp, SegzPath(logPath)); err != nil {
		os.Remove(tmp)
		return false, err
	}
	if err := syncDir(filepath.Dir(logPath)); err != nil {
		return false, err
	}
	// Only now is it safe to drop the raw file.
	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	return true, syncDir(filepath.Dir(logPath))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// pageSource hides whether a segment is stored raw or compressed. Both read paths reduce to
// "give me logical page N", so this is the only place that has to know.
type pageSource struct {
	f          *os.File
	pages      uint64
	blockPages int
	dir        []byte // nil for a raw segment

	cachedBlock uint64
	cached      []byte
	haveCache   bool
}

// openPageSource opens a segment for reading, preferring the compressed sidecar.
//
// The compressed file is checked FIRST. Checking .log first and falling back on ENOENT
// would confuse "this segment is compressed" with "retention deleted this segment", and the
// latter is deliberately treated as an empty result rather than an error.
func openPageSource(logPath string) (*pageSource, error) {
	if f, err := os.Open(SegzPath(logPath)); err == nil {
		s, err := newCompressedSource(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		return s, nil
	}
	f, err := os.Open(logPath)
	if err != nil {
		return nil, err // includes the retention race; callers map ENOENT to empty
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &pageSource{f: f, pages: uint64(info.Size() / PageSize)}, nil
}

func newCompressedSource(f *os.File) (*pageSource, error) {
	hdr := make([]byte, segzHeaderLen)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("segz: reading header: %w", err)
	}
	if m := binary.BigEndian.Uint32(hdr[0:4]); m != segzMagic {
		return nil, fmt.Errorf("segz: bad magic 0x%08X", m)
	}
	if v := binary.BigEndian.Uint16(hdr[4:6]); v != segzVersion {
		return nil, fmt.Errorf("segz: unsupported version %d", v)
	}
	blockPages := int(binary.BigEndian.Uint16(hdr[6:8]))
	pages := binary.BigEndian.Uint64(hdr[8:16])
	numBlocks := binary.BigEndian.Uint32(hdr[16:20])
	wantCRC := binary.BigEndian.Uint32(hdr[20:24])
	if blockPages <= 0 {
		return nil, fmt.Errorf("segz: invalid block size %d", blockPages)
	}
	dir := make([]byte, int(numBlocks)*segzDirEntry)
	if _, err := f.ReadAt(dir, segzHeaderLen); err != nil {
		return nil, fmt.Errorf("segz: reading directory: %w", err)
	}
	if got := crc32.ChecksumIEEE(dir); got != wantCRC {
		return nil, fmt.Errorf("segz: directory checksum mismatch")
	}
	return &pageSource{f: f, pages: pages, blockPages: blockPages, dir: dir}, nil
}

func (s *pageSource) Close() error { return s.f.Close() }

func (s *pageSource) numPages() uint64 { return s.pages }

// readPage copies logical page n into buf, which must be PageSize bytes. A block that fails
// to decompress yields an error and the caller skips the page, matching how a failed
// ValidatePage already degrades — never a wrong answer.
func (s *pageSource) readPage(n uint64, buf []byte) error {
	if n >= s.pages {
		return io.EOF
	}
	if s.dir == nil {
		_, err := s.f.ReadAt(buf, PageOffset(n))
		return err
	}
	block := n / uint64(s.blockPages)
	if !s.haveCache || s.cachedBlock != block {
		if err := s.loadBlock(block); err != nil {
			return err
		}
	}
	off := int(n%uint64(s.blockPages)) * PageSize
	if off+PageSize > len(s.cached) {
		return io.EOF
	}
	copy(buf, s.cached[off:off+PageSize])
	return nil
}

// loadBlock decompresses one block into the single-entry cache. One entry is the right size
// because every caller walks blocks in ascending order.
func (s *pageSource) loadBlock(block uint64) error {
	e := s.dir[int(block)*segzDirEntry:]
	fileOff := binary.BigEndian.Uint64(e[0:8])
	compLen := binary.BigEndian.Uint32(e[8:12])
	rawLen := binary.BigEndian.Uint32(e[12:16])

	compressed := make([]byte, compLen)
	if _, err := s.f.ReadAt(compressed, int64(fileOff)); err != nil {
		return err
	}
	if cap(s.cached) < int(rawLen) {
		s.cached = make([]byte, rawLen)
	}
	s.cached = s.cached[:rawLen]
	zr := flate.NewReader(bytes.NewReader(compressed))
	defer zr.Close()
	if _, err := io.ReadFull(zr, s.cached); err != nil {
		s.haveCache = false
		return fmt.Errorf("segz: decompressing block %d: %w", block, err)
	}
	s.cachedBlock, s.haveCache = block, true
	return nil
}
