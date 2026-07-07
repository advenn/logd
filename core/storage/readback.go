package storage

import (
	"os"
	"sort"

	"github.com/advenn/logd/core/model"
)

// FetchRecords decodes the records at the given segment-relative byte offsets (the
// offsets a .tidx stores) and calls visit for each. Offsets are sorted so each page is
// read at most once; a corrupt/torn page causes its offsets to be skipped (degrade,
// never a wrong answer). This is the materialization step of an index-pushdown query:
// the planner produces candidate offsets, this reads exactly those records, and the
// caller re-verifies them. visit returns false to stop early.
func FetchRecords(path string, offsets []uint64, visit func(model.LogEntry) bool) error {
	if len(offsets) == 0 {
		return nil
	}
	sorted := make([]uint64, len(offsets))
	copy(sorted, offsets)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	page := make([]byte, PageSize)
	curPage := ^uint64(0) // sentinel: no page loaded
	pageOK := false
	for _, off := range sorted {
		pageNum := off / PageSize
		if pageNum != curPage {
			curPage = pageNum
			pageOK = false
			if _, err := f.ReadAt(page, PageOffset(pageNum)); err == nil {
				pageOK = ValidatePage(page) == nil
			}
		}
		if !pageOK {
			continue
		}
		inPage := off % PageSize
		if int(inPage) >= PageSize {
			continue
		}
		e, err := DecodeEntry(page[inPage:])
		if err != nil {
			continue
		}
		if !visit(e) {
			return nil
		}
	}
	return nil
}

// ReadAll reads every persisted entry from a data directory, in segment (MinTS)
// order. It is a Phase-2 verification aid, NOT the query engine (which arrives in
// Phase 4 with time pruning, label/typed-range indexes, and predicates). It opens
// each segment read-only, validates whole pages, and decodes records; a corrupt or
// torn page is skipped rather than failing the read (design invariant 2: a missing
// or corrupt structure degrades, never a wrong answer).
func ReadAll(dir string) ([]model.LogEntry, error) {
	m, err := LoadManifest(dir)
	if err != nil {
		return nil, err
	}
	var out []model.LogEntry
	for _, meta := range m.All() {
		entries, err := readSegmentEntries(meta.Path)
		if err != nil {
			return out, err
		}
		out = append(out, entries...)
	}
	return out, nil
}

// readSegmentEntries reads all decodable entries from one segment file, read-only.
func readSegmentEntries(path string) ([]model.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numPages := uint64(info.Size() / PageSize)

	var out []model.LogEntry
	page := make([]byte, PageSize)
	for p := uint64(1); p < numPages; p++ { // page 0 is the placeholder
		if _, err := f.ReadAt(page, PageOffset(p)); err != nil {
			break
		}
		if err := ValidatePage(page); err != nil {
			continue // skip a corrupt/torn page
		}
		entries, err := decodePageEntries(page)
		if err != nil {
			continue
		}
		out = append(out, entries...)
	}
	return out, nil
}

// Record pairs a decoded entry with its segment-relative byte offset — the same offset
// the typed-range index stores, so this is what rebuild-and-compare, index-pushdown
// re-verify, and crash-recovery reindexing read records back through.
type Record struct {
	Offset uint64
	Entry  model.LogEntry
}

// ReadAllRecords is ReadAll but also reporting each record's segment-relative byte
// offset (page offset + position within the page) and segment ID.
func ReadAllRecords(dir string) ([]Record, error) {
	m, err := LoadManifest(dir)
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, meta := range m.All() {
		recs, err := readSegmentRecords(meta.Path)
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// ScanSegmentTimeRange opens a segment read-only and calls visit for every record
// whose event time falls in [start, end], skipping pages whose header time bounds
// don't overlap the window. visit returns false to stop the scan early. Corrupt/torn
// pages are skipped (degrade, never a wrong answer). Safe to call while the writer is
// appending: sealed segments are immutable, and the active segment's already-flushed
// pages are immutable too (the writer writes whole pages through a separate fd).
func ScanSegmentTimeRange(path string, start, end int64, visit func(model.LogEntry) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	numPages := uint64(info.Size() / PageSize)

	page := make([]byte, PageSize)
	for p := uint64(1); p < numPages; p++ {
		if _, err := f.ReadAt(page, PageOffset(p)); err != nil {
			break
		}
		if err := ValidatePage(page); err != nil {
			continue
		}
		h := decodePageHeader(page[:PageHeaderSize])
		if h.EntryCount == 0 || h.MaxTS < start || h.MinTS > end {
			continue // page can't hold an in-window record
		}
		endOff := int(h.FreeSpaceOffset)
		off := PageHeaderSize
		for i := 0; i < int(h.EntryCount) && off < endOff; i++ {
			e, err := DecodeEntry(page[off:endOff])
			if err != nil {
				break
			}
			off += EncodedSize(e)
			if ts := e.TS.UnixNano(); ts < start || ts > end {
				continue
			}
			if !visit(e) {
				return nil
			}
		}
	}
	return nil
}

func readSegmentRecords(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numPages := uint64(info.Size() / PageSize)

	var out []Record
	page := make([]byte, PageSize)
	for p := uint64(1); p < numPages; p++ {
		if _, err := f.ReadAt(page, PageOffset(p)); err != nil {
			break
		}
		if err := ValidatePage(page); err != nil {
			continue
		}
		h := decodePageHeader(page[:PageHeaderSize])
		end := int(h.FreeSpaceOffset)
		off := PageHeaderSize
		for i := 0; i < int(h.EntryCount) && off < end; i++ {
			e, err := DecodeEntry(page[off:end])
			if err != nil {
				break
			}
			out = append(out, Record{
				Offset: uint64(PageOffset(p)) + uint64(off),
				Entry:  e,
			})
			off += EncodedSize(e)
		}
	}
	return out, nil
}

// decodePageEntries decodes the records stored in a validated page. Records live in
// [PageHeaderSize : FreeSpaceOffset] with no per-record length prefix, so we walk
// sequentially, advancing by each decoded record's EncodedSize (design §5).
func decodePageEntries(page []byte) ([]model.LogEntry, error) {
	h := decodePageHeader(page[:PageHeaderSize])
	end := int(h.FreeSpaceOffset)
	off := PageHeaderSize

	out := make([]model.LogEntry, 0, h.EntryCount)
	for i := 0; i < int(h.EntryCount); i++ {
		if off >= end {
			break
		}
		e, err := DecodeEntry(page[off:end])
		if err != nil {
			return out, err
		}
		out = append(out, e)
		off += EncodedSize(e)
	}
	return out, nil
}
