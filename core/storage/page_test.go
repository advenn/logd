package storage

import (
	"bytes"
	"errors"
	"testing"
)

// buildPage assembles a valid, finalized 4KB page with a couple of dummy record
// bytes in the data region, so tests can then corrupt specific offsets.
func buildPage() []byte {
	page := make([]byte, PageSize)
	h := NewPageHeader()
	h.MinTS = 100
	h.MaxTS = 200
	h.EntryCount = 2
	h.FreeSpaceOffset = PageHeaderSize + 8
	// Put some non-zero "record" bytes just after the header.
	copy(page[PageHeaderSize:], []byte{1, 2, 3, 4, 5, 6, 7, 8})
	FinalizePage(page, h)
	return page
}

func TestPageHeaderRoundTrip(t *testing.T) {
	h := NewPageHeader()
	h.MinTS, h.MaxTS, h.EntryCount, h.FreeSpaceOffset = 10, 99, 3, 64
	var buf bytes.Buffer
	if err := WritePageHeader(&buf, h); err != nil {
		t.Fatalf("WritePageHeader: %v", err)
	}
	got, err := ReadPageHeader(&buf)
	if err != nil {
		t.Fatalf("ReadPageHeader: %v", err)
	}
	if got != h {
		t.Fatalf("header round-trip mismatch:\n got %+v\nwant %+v", got, h)
	}
}

func TestValidatePageAcceptsGoodPage(t *testing.T) {
	if err := ValidatePage(buildPage()); err != nil {
		t.Fatalf("valid page rejected: %v", err)
	}
}

func TestValidatePageDetectsBadMagic(t *testing.T) {
	page := buildPage()
	page[0] ^= 0xFF // corrupt the magic
	if err := ValidatePage(page); !errors.Is(err, ErrPageCorrupted) {
		t.Fatalf("got %v, want ErrPageCorrupted for bad magic", err)
	}
}

// TestValidatePageDetectsCorruptDataRegion is the point of the full-page checksum:
// a bit-flip in the RECORD area (offset past the 32-byte header) must be detected.
// A header-only checksum would miss this and hand back silently-corrupt records.
func TestValidatePageDetectsCorruptDataRegion(t *testing.T) {
	page := buildPage()
	page[PageHeaderSize+3] ^= 0x01 // flip a bit deep in the data region
	if err := ValidatePage(page); !errors.Is(err, ErrPageCorrupted) {
		t.Fatalf("got %v, want ErrPageCorrupted for corrupt data region", err)
	}
}

func TestChecksumPageLeavesBufferUnchanged(t *testing.T) {
	page := buildPage()
	before := bytes.Clone(page)
	_ = ChecksumPage(page)
	if !bytes.Equal(page, before) {
		t.Fatal("ChecksumPage mutated the page buffer")
	}
}

func TestCanFitEntry(t *testing.T) {
	if !CanFitEntry(100, 100) {
		t.Error("entry exactly filling free space should fit")
	}
	if CanFitEntry(100, 101) {
		t.Error("entry larger than free space should not fit")
	}
}

func TestPageOffset(t *testing.T) {
	if PageOffset(0) != 0 || PageOffset(1) != PageSize || PageOffset(3) != 3*PageSize {
		t.Fatal("PageOffset math wrong")
	}
}
