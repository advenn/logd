package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestWriteAndReadPageHeader(t *testing.T) {
	original := PageHeader{
		Magic:           PageMagic,
		MinTS:           1000000000,
		MaxTS:           2000000000,
		EntryCount:      15,
		FreeSpaceOffset: 512,
		Reserved:        0,
	}
	original.Checksum = original.CalculateChecksum()

	var buf bytes.Buffer
	if err := WritePageHeader(&buf, original); err != nil {
		t.Fatalf("WritePageHeader() returned error: %v", err)
	}

	if buf.Len() != PageHeaderSize {
		t.Errorf("WritePageHeader wrote %d bytes, want %d", buf.Len(), PageHeaderSize)
	}

	read, err := ReadPageHeader(&buf)
	if err != nil {
		t.Fatalf("ReadPageHeader() returned error: %v", err)
	}

	if read.Magic != original.Magic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", read.Magic, original.Magic)
	}
	if read.MinTS != original.MinTS {
		t.Errorf("MinTS = %d, want %d", read.MinTS, original.MinTS)
	}
	if read.MaxTS != original.MaxTS {
		t.Errorf("MaxTS = %d, want %d", read.MaxTS, original.MaxTS)
	}
	if read.EntryCount != original.EntryCount {
		t.Errorf("EntryCount = %d, want %d", read.EntryCount, original.EntryCount)
	}
	if read.FreeSpaceOffset != original.FreeSpaceOffset {
		t.Errorf("FreeSpaceOffset = %d, want %d", read.FreeSpaceOffset, original.FreeSpaceOffset)
	}
	if read.Checksum != original.Checksum {
		t.Errorf("Checksum = 0x%08X, want 0x%08X", read.Checksum, original.Checksum)
	}
	if read.Reserved != original.Reserved {
		t.Errorf("Reserved = %d, want %d", read.Reserved, original.Reserved)
	}
}

func TestReadPageHeaderAt(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "page_test")
	if err != nil {
		t.Fatalf("CreateTemp() returned error: %v", err)
	}
	defer f.Close()

	// Write two headers at different offsets to verify ReadPageHeaderAt reads
	// from the correct position.

	// Page 0 header
	h0 := PageHeader{
		Magic:           PageMagic,
		MinTS:           100,
		MaxTS:           200,
		EntryCount:      1,
		FreeSpaceOffset: PageHeaderSize + 25,
	}
	h0.Checksum = h0.CalculateChecksum()

	// Page 1 header
	h1 := PageHeader{
		Magic:           PageMagic,
		MinTS:           300,
		MaxTS:           400,
		EntryCount:      3,
		FreeSpaceOffset: PageHeaderSize + 80,
	}
	h1.Checksum = h1.CalculateChecksum()

	// Write page 0 header + padding to fill the page
	buf := make([]byte, PageSize)
	encodePageHeader(buf[0:PageHeaderSize], h0)
	if _, err := f.Write(buf); err != nil {
		t.Fatalf("Write page 0: %v", err)
	}

	// Write page 1 header
	buf2 := make([]byte, PageSize)
	encodePageHeader(buf2[0:PageHeaderSize], h1)
	if _, err := f.Write(buf2); err != nil {
		t.Fatalf("Write page 1: %v", err)
	}

	// Read page 0 header
	got0, err := ReadPageHeaderAt(f, PageOffset(0))
	if err != nil {
		t.Fatalf("ReadPageHeaderAt(0) returned error: %v", err)
	}
	if got0.MinTS != h0.MinTS {
		t.Errorf("page 0 MinTS = %d, want %d", got0.MinTS, h0.MinTS)
	}

	// Read page 1 header
	got1, err := ReadPageHeaderAt(f, PageOffset(1))
	if err != nil {
		t.Fatalf("ReadPageHeaderAt(1) returned error: %v", err)
	}
	if got1.MinTS != h1.MinTS {
		t.Errorf("page 1 MinTS = %d, want %d", got1.MinTS, h1.MinTS)
	}
}

func TestValidate_ValidHeader(t *testing.T) {
	h := NewPageHeader()
	h.MinTS = 1000
	h.MaxTS = 2000
	h.EntryCount = 5
	h.FreeSpaceOffset = 256
	h.Checksum = h.CalculateChecksum()

	if err := h.Validate(); err != nil {
		t.Errorf("Validate() on valid header returned error: %v", err)
	}
}

func TestValidate_CorruptedMagic(t *testing.T) {
	h := NewPageHeader()
	h.Magic = 0xDEADBEEF
	h.Checksum = h.CalculateChecksum()

	err := h.Validate()
	if err == nil {
		t.Fatal("expected error for corrupted magic, got nil")
	}
	if !errors.Is(err, ErrPageCorrupted) {
		t.Errorf("error should wrap ErrPageCorrupted, got: %v", err)
	}
}

func TestValidate_CorruptedChecksum(t *testing.T) {
	h := NewPageHeader()
	h.Checksum = h.CalculateChecksum()
	h.Checksum ^= 0xFFFFFFFF // flip all bits

	err := h.Validate()
	if err == nil {
		t.Fatal("expected error for corrupted checksum, got nil")
	}
	if !errors.Is(err, ErrPageCorrupted) {
		t.Errorf("error should wrap ErrPageCorrupted, got: %v", err)
	}
}

func TestValidate_TamperedData(t *testing.T) {
	// Valid header, then tamper with a field after checksum calculation.
	h := NewPageHeader()
	h.MinTS = 5000
	h.MaxTS = 6000
	h.Checksum = h.CalculateChecksum()

	// Tamper with MinTS after checksum was stored.
	h.MinTS = 9999

	err := h.Validate()
	if err == nil {
		t.Fatal("expected checksum error after tampering, got nil")
	}
}

func TestCanFitEntry(t *testing.T) {
	tests := []struct {
		freeSpace uint16
		entrySize int
		want      bool
	}{
		{100, 50, true},
		{100, 100, true},
		{100, 101, false},
		{0, 0, true},
		{0, 1, false},
		{PageSize - PageHeaderSize, 25, true},
	}

	for _, tt := range tests {
		got := CanFitEntry(tt.freeSpace, tt.entrySize)
		if got != tt.want {
			t.Errorf("CanFitEntry(%d, %d) = %v, want %v", tt.freeSpace, tt.entrySize, got, tt.want)
		}
	}
}

func TestPageOffset(t *testing.T) {
	tests := []struct {
		pageNumber uint64
		want       int64
	}{
		{0, 0},
		{1, 4096},
		{2, 8192},
		{100, 409600},
	}

	for _, tt := range tests {
		got := PageOffset(tt.pageNumber)
		if got != tt.want {
			t.Errorf("PageOffset(%d) = %d, want %d", tt.pageNumber, got, tt.want)
		}
	}
}

func TestNewPageHeader(t *testing.T) {
	h := NewPageHeader()

	if h.Magic != PageMagic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", h.Magic, PageMagic)
	}
	if h.MinTS != 1<<63-1 {
		t.Errorf("MinTS = %d, want max int64", h.MinTS)
	}
	if h.MaxTS != -1<<63 {
		t.Errorf("MaxTS = %d, want min int64", h.MaxTS)
	}
	if h.EntryCount != 0 {
		t.Errorf("EntryCount = %d, want 0", h.EntryCount)
	}
	if h.FreeSpaceOffset != PageHeaderSize {
		t.Errorf("FreeSpaceOffset = %d, want %d", h.FreeSpaceOffset, PageHeaderSize)
	}
	if h.Checksum != 0 {
		t.Errorf("Checksum = %d, want 0", h.Checksum)
	}
	if h.Reserved != 0 {
		t.Errorf("Reserved = %d, want 0", h.Reserved)
	}
}
