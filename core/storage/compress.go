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
	segzMagic = 0x4C534547 // "LSEG"

	// Version 1: [header 24][directory][blocks], each block deflated independently.
	// Version 2 adds a preset DICTIONARY, stored in the file and used for every block.
	//
	// The dictionary is what a 32 KB block cannot build for itself: deflate starts each
	// block with an empty window, so the repeated shape of log lines — timestamps, level
	// names, the label JSON, path prefixes — has to be re-learned 2,048 times per segment.
	// Priming the window with a sample of the segment's own content measured better than
	// any amount of extra compression effort:
	//
	//	flate -6                6.15x   (v1)
	//	flate -9                6.32x
	//	flate -6 + dictionary   6.72x
	//	flate -7 + dictionary   6.91x   <- v2
	//	flate -9 + dictionary   7.04x   (but 3.3x the seal cost)
	//
	// For scale, zstd -19 measured 7.03x — the stdlib option with a dictionary matches a
	// heavyweight codec, which is why no dependency was added.
	segzVersion1 = 1
	segzVersion2 = 2

	segzHeaderLenV1 = 24
	segzHeaderLenV2 = 32
	segzDirEntry    = 16 // fileOffset u64 + compLen u32 + rawLen u32

	// segzDictBytes is the preset-dictionary size. deflate's window is 32 KB and only the
	// dictionary's LAST 32 KB primes it, so a larger one would be stored and ignored.
	segzDictBytes = 32 << 10

	// segzLevel trades seal CPU for ratio. -7 is the knee: +10.9% over -6 for 2.4s per
	// 64 MB segment, against -9's +12.6% for 4.7s. A segment fills in roughly 4s at
	// measured ingest rates, so -9 would risk compression falling behind segment
	// production and accumulating background work.
	segzLevel = 7

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

	// Sample the segment's own content for the dictionary. A middle block is used rather
	// than the first: page 1 is the oldest data and often unrepresentative of a segment
	// that ran for a while. The dictionary is stored in the file, so it is self-describing
	// and the reader can never disagree with the writer about it.
	dict := sampleDict(src, numPages, blockPages)

	body := bytes.NewBuffer(nil)
	dir := make([]byte, dirLen)

	raw := make([]byte, blockPages*PageSize)
	fileOff := uint64(segzHeaderLenV2 + len(dict) + dirLen)
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
		zw, err := flate.NewWriterDict(&comp, segzLevel, dict)
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

	hdr := make([]byte, segzHeaderLenV2)
	binary.BigEndian.PutUint32(hdr[0:4], segzMagic)
	binary.BigEndian.PutUint16(hdr[4:6], segzVersion2)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(blockPages))
	binary.BigEndian.PutUint64(hdr[8:16], numPages)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(numBlocks))
	binary.BigEndian.PutUint32(hdr[24:28], uint32(len(dict)))
	// CRC over the dictionary AND the directory. The directory must be intact before it is
	// used to compute file offsets; the dictionary must be intact or every block inflates
	// to different bytes than were compressed. Block payloads carry the pages' own
	// full-page CRCs on top of this.
	crc := crc32.NewIEEE()
	crc.Write(dict)
	crc.Write(dir)
	binary.BigEndian.PutUint32(hdr[20:24], crc.Sum32())

	for _, chunk := range [][]byte{hdr, dict, dir, body.Bytes()} {
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

// sampleDict builds the preset dictionary from the segment's own content.
//
// A middle block is sampled rather than the first: page 1 holds the oldest records and a
// segment that ran for a while often looks different by the end. Only deflate's last 32 KB
// of window matters, so the sample is capped there. A short read just yields a shorter
// dictionary — it is stored in the file, so writer and reader cannot disagree about it, and
// a degenerate one costs ratio, never correctness.
func sampleDict(src *os.File, numPages uint64, blockPages int) []byte {
	if numPages <= 1 {
		return nil
	}
	mid := numPages / 2
	if mid == 0 {
		mid = 1
	}
	want := segzDictBytes
	if avail := int(numPages-mid) * PageSize; avail < want {
		want = avail
	}
	if want <= 0 {
		return nil
	}
	buf := make([]byte, want)
	n, err := src.ReadAt(buf, PageOffset(mid))
	if err != nil && err != io.EOF {
		return nil // no dictionary rather than a failed seal; costs ratio only
	}
	return buf[:n]
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
	dict       []byte // preset dictionary (v2); nil for v1 and raw

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
	// Read the larger header unconditionally, then interpret by version. Both layouts share
	// their first 24 bytes, so a v1 file simply leaves the extra 8 unused.
	hdr := make([]byte, segzHeaderLenV2)
	if _, err := f.ReadAt(hdr, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("segz: reading header: %w", err)
	}
	if m := binary.BigEndian.Uint32(hdr[0:4]); m != segzMagic {
		return nil, fmt.Errorf("segz: bad magic 0x%08X", m)
	}
	ver := binary.BigEndian.Uint16(hdr[4:6])
	// v1 files predate the preset dictionary and are still read: rejecting them would make
	// every segment sealed before this change unreadable, and the reader has to handle both
	// anyway while old segments age out.
	if ver != segzVersion1 && ver != segzVersion2 {
		return nil, fmt.Errorf("segz: unsupported version %d", ver)
	}
	blockPages := int(binary.BigEndian.Uint16(hdr[6:8]))
	pages := binary.BigEndian.Uint64(hdr[8:16])
	numBlocks := binary.BigEndian.Uint32(hdr[16:20])
	wantCRC := binary.BigEndian.Uint32(hdr[20:24])
	if blockPages <= 0 {
		return nil, fmt.Errorf("segz: invalid block size %d", blockPages)
	}

	headerLen := segzHeaderLenV1
	var dict []byte
	if ver == segzVersion2 {
		headerLen = segzHeaderLenV2
		dictLen := int(binary.BigEndian.Uint32(hdr[24:28]))
		if dictLen < 0 || dictLen > segzDictBytes {
			return nil, fmt.Errorf("segz: implausible dictionary length %d", dictLen)
		}
		if dictLen > 0 {
			dict = make([]byte, dictLen)
			if _, err := f.ReadAt(dict, int64(headerLen)); err != nil {
				return nil, fmt.Errorf("segz: reading dictionary: %w", err)
			}
		}
	}

	dir := make([]byte, int(numBlocks)*segzDirEntry)
	if _, err := f.ReadAt(dir, int64(headerLen+len(dict))); err != nil {
		return nil, fmt.Errorf("segz: reading directory: %w", err)
	}
	// The CRC covers the dictionary as well as the directory in v2: a corrupt dictionary
	// would inflate every block to different bytes than were compressed, which the pages'
	// own CRCs would catch as wholesale corruption rather than as the one bad field it is.
	crc := crc32.NewIEEE()
	crc.Write(dict)
	crc.Write(dir)
	if crc.Sum32() != wantCRC {
		return nil, fmt.Errorf("segz: directory checksum mismatch")
	}
	return &pageSource{f: f, pages: pages, blockPages: blockPages, dir: dir, dict: dict}, nil
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
	// NewReaderDict with a nil dictionary is equivalent to NewReader, so v1 and v2 share
	// this path.
	zr := flate.NewReaderDict(bytes.NewReader(compressed), s.dict)
	defer zr.Close()
	if _, err := io.ReadFull(zr, s.cached); err != nil {
		s.haveCache = false
		return fmt.Errorf("segz: decompressing block %d: %w", block, err)
	}
	s.cachedBlock, s.haveCache = block, true
	return nil
}
