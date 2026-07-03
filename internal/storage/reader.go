package storage

import (
	"encoding/json"
	"os"
	"sort"
	"strings"

	"github.com/advenn/logd/internal/template"
)

// querySegments executes a query against all segments in the manifest that
// overlap the time range, then filters and sorts results.
func querySegments(m *Manifest, filter QueryFilter, lt *LookupTable, indexDir string) ([]LogEntry, error) {
	segments := m.Filter(filter.StartTS, filter.EndTS)
	if len(segments) == 0 {
		return nil, nil
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	// Resolve index hints from FieldFilters.
	indexHints := resolveIndexHints(filter.FieldFilters, indexDir)

	var results []LogEntry

	// Resolve service filter from label match.
	var serviceIDFilter uint16
	var hasServiceFilter bool
	if filter.LabelMatch != nil {
		if svcName, ok := filter.LabelMatch["service"]; ok {
			// Look up service ID. Iterate to find it.
			for id := uint16(1); id < 65535; id++ {
				name, found := lt.GetName(id)
				if !found {
					break
				}
				if name == svcName {
					serviceIDFilter = id
					hasServiceFilter = true
					break
				}
			}
		}
	}

	// Scan segments — newest segments first for backward direction.
	if filter.Direction == 0 {
		// Backward: reverse segment order.
		for i := len(segments) - 1; i >= 0; i-- {
			entries, err := querySegment(segments[i], filter, hasServiceFilter, serviceIDFilter, indexHints)
			if err != nil {
				continue
			}
			results = append(results, entries...)
			if len(results) >= limit {
				results = results[:limit]
				break
			}
		}
	} else {
		for _, seg := range segments {
			entries, err := querySegment(seg, filter, hasServiceFilter, serviceIDFilter, indexHints)
			if err != nil {
				continue
			}
			results = append(results, entries...)
			if len(results) >= limit {
				results = results[:limit]
				break
			}
		}
	}

	// Sort results.
	if filter.Direction == 0 {
		sort.Slice(results, func(i, j int) bool {
			return results[i].TS.UnixNano() > results[j].TS.UnixNano()
		})
	} else {
		sort.Slice(results, func(i, j int) bool {
			return results[i].TS.UnixNano() < results[j].TS.UnixNano()
		})
	}

	return results, nil
}

// resolveIndexHints opens index files for each FieldFilter, looks up values,
// and returns a map of segment_id → set of page numbers. If no indexes exist
// or a field cannot be resolved, returns nil (graceful degradation to full scan).
func resolveIndexHints(fieldFilters map[string]string, indexDir string) map[uint64]map[uint64]bool {
	if len(fieldFilters) == 0 || indexDir == "" {
		return nil
	}

	var hints map[uint64]map[uint64]bool // segmentID → set of pageNums

	for field, value := range fieldFilters {
		ir, err := template.OpenIndexReader(indexDir, field)
		if err != nil {
			// Index doesn't exist for this field — fall back to full scan.
			return nil
		}

		refs, err := ir.Lookup(value)
		if err != nil || len(refs) == 0 {
			return nil // no matches at all
		}

		fieldPages := make(map[uint64]map[uint64]bool)
		for _, ref := range refs {
			if fieldPages[ref.SegmentID] == nil {
				fieldPages[ref.SegmentID] = make(map[uint64]bool)
			}
			fieldPages[ref.SegmentID][ref.PageNum] = true
		}

		if hints == nil {
			hints = fieldPages
		} else {
			// Intersect with existing hints.
			for segID, pages := range hints {
				if fieldPages[segID] == nil {
					delete(hints, segID)
				} else {
					for pn := range pages {
						if !fieldPages[segID][pn] {
							delete(pages, pn)
						}
					}
					if len(pages) == 0 {
						delete(hints, segID)
					}
				}
			}
		}

		if len(hints) == 0 {
			return nil
		}
	}

	return hints
}

// querySegment reads entries from a single segment that match the filter.
func querySegment(meta *SegmentMeta, filter QueryFilter, hasSvcFilter bool, svcID uint16, indexHints map[uint64]map[uint64]bool) ([]LogEntry, error) {
	// Open segment for reading.
	seg, err := OpenSegment(meta.Path)
	if err != nil {
		return nil, err
	}
	defer seg.Close()

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	var results []LogEntry

	// If index hints exist for this segment, read only the hinted pages.
	if hintedPages, hasHints := indexHints[meta.ID]; hasHints && len(hintedPages) > 0 {
		// Collect and sort hinted page numbers.
		var pageNums []uint64
		for pn := range hintedPages {
			pageNums = append(pageNums, pn)
		}
		sort.Slice(pageNums, func(i, j int) bool { return pageNums[i] < pageNums[j] })

		for _, pn := range pageNums {
			if pn >= uint64(seg.Size/PageSize) {
				continue
			}
			entries, err := readPageEntries(seg, pn)
			if err != nil {
				continue
			}
			for _, e := range entries {
				ts := e.TS.UnixNano()
				if ts < filter.StartTS || ts > filter.EndTS {
					continue
				}
				if hasSvcFilter && e.ServiceID != svcID {
					continue
				}
				if !matchLabels(e, filter.LabelMatch) {
					continue
				}
				if filter.Predicate != nil && !filter.Predicate.Match(e) {
					continue
				}
				results = append(results, e)
				if len(results) >= limit {
					return results, nil
				}
			}
		}
		return results, nil
	}

	// No index hints — use sparse time index for scan.
	// Open index for binary search.
	idxPath := indexPath(meta.Path)
	idxReader, err := OpenIndexReader(idxPath)
	if err != nil {
		// No index — scan all pages.
		return scanSegment(seg, filter, hasSvcFilter, svcID, indexHints)
	}
	defer idxReader.Close()

	// Binary search index for start page.
	startPage, _, err := idxReader.Search(filter.StartTS)
	if err != nil {
		// Can't search — start from page 1 (page 0 is empty header).
		startPage = 1
	}

	// Scan pages starting from startPage.
	numPages := uint64(seg.Size / PageSize)
	for pn := startPage; pn < numPages; pn++ {
		pageData, err := seg.ReadPage(pn)
		if err != nil {
			continue
		}

		hdr := decodePageHeader(pageData[:PageHeaderSize])
		if err := hdr.Validate(); err != nil {
			continue
		}

		// Skip page if its time range doesn't overlap query.
		if hdr.MaxTS < filter.StartTS || hdr.MinTS > filter.EndTS {
			continue
		}

		// Decode entries from this page.
		entries := decodePageEntries(pageData[PageHeaderSize:hdr.FreeSpaceOffset], int(hdr.EntryCount))

		for _, e := range entries {
			ts := e.TS.UnixNano()
			if ts < filter.StartTS || ts > filter.EndTS {
				continue
			}
			if hasSvcFilter && e.ServiceID != svcID {
				continue
			}
			if !matchLabels(e, filter.LabelMatch) {
				continue
			}
			if filter.Predicate != nil && !filter.Predicate.Match(e) {
				continue
			}
			results = append(results, e)
			if len(results) >= limit {
				return results, nil
			}
		}

		// If page MaxTS > query EndTS and entries are roughly ordered, we can stop.
		if hdr.MinTS > filter.EndTS {
			break
		}
	}

	return results, nil
}

// readPageEntries reads a single page and decodes its entries.
func readPageEntries(seg *Segment, pageNum uint64) ([]LogEntry, error) {
	pageData, err := seg.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	hdr := decodePageHeader(pageData[:PageHeaderSize])
	if err := hdr.Validate(); err != nil {
		return nil, err
	}
	if hdr.EntryCount == 0 {
		return nil, nil
	}
	return decodePageEntries(pageData[PageHeaderSize:hdr.FreeSpaceOffset], int(hdr.EntryCount)), nil
}

// scanSegment reads all pages in a segment without index assistance.
func scanSegment(seg *Segment, filter QueryFilter, hasSvcFilter bool, svcID uint16, indexHints map[uint64]map[uint64]bool) ([]LogEntry, error) {
	var results []LogEntry
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	numPages := uint64(seg.Size / PageSize)
	for pn := uint64(1); pn < numPages; pn++ {
		pageData, err := seg.ReadPage(pn)
		if err != nil {
			continue
		}

		hdr := decodePageHeader(pageData[:PageHeaderSize])
		if err := hdr.Validate(); err != nil {
			continue
		}

		if hdr.EntryCount == 0 {
			continue
		}

		if hdr.MaxTS < filter.StartTS || hdr.MinTS > filter.EndTS {
			continue
		}

		entries := decodePageEntries(pageData[PageHeaderSize:hdr.FreeSpaceOffset], int(hdr.EntryCount))
		for _, e := range entries {
			ts := e.TS.UnixNano()
			if ts < filter.StartTS || ts > filter.EndTS {
				continue
			}
			if hasSvcFilter && e.ServiceID != svcID {
				continue
			}
			if !matchLabels(e, filter.LabelMatch) {
				continue
			}
			if filter.Predicate != nil && !filter.Predicate.Match(e) {
				continue
			}
			results = append(results, e)
			if len(results) >= limit {
				return results, nil
			}
		}
	}

	return results, nil
}

// decodePageEntries extracts LogEntry records from raw page data.
// Each entry is encoded with the Encode() format and must be decoded sequentially
// since there's no entry-level length prefix — we rely on the entry count.
func decodePageEntries(data []byte, expectedCount int) []LogEntry {
	var entries []LogEntry
	offset := 0

	for offset < len(data) && len(entries) < expectedCount {
		entry, err := DecodeLogEntry(data[offset:])
		if err != nil {
			// Corrupt entry; skip remaining.
			break
		}
		entries = append(entries, entry)
		offset += entry.EncodedSize()
	}

	return entries
}

// matchLabels checks if an entry's Extra JSON contains all the label matchers.
// Label "level" is matched against the entry's Level field.
// Label "service" is matched against the lookup table.
func matchLabels(entry LogEntry, matchers map[string]string) bool {
	if matchers == nil {
		return true
	}

	for key, want := range matchers {
		switch key {
		case "level":
			if !strings.EqualFold(entry.Level.String(), want) {
				return false
			}
		case "service":
			// Service comparison is already done via serviceIDFilter in the query path.
			continue
		default:
			var extra map[string]interface{}
			if err := json.Unmarshal([]byte(entry.Extra), &extra); err != nil {
				return false
			}
			v, ok := extra[key]
			if !ok {
				return false
			}
			s, ok := v.(string)
			if !ok || s != want {
				return false
			}
		}
	}
	return true
}

// indexPath returns the .idx path for a .log segment path.
func indexPath(segPath string) string {
	if strings.HasSuffix(segPath, ".log") {
		return segPath[:len(segPath)-4] + ".idx"
	}
	return segPath + ".idx"
}

// ReadSegmentLog is a debug helper that reads and returns all entries from a
// segment file. Useful for inspection.
func ReadSegmentLog(path string) ([]LogEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var results []LogEntry
	for offset := int64(0); offset+PageSize <= int64(len(data)); offset += PageSize {
		pageData := data[offset : offset+PageSize]
		hdr := decodePageHeader(pageData[:PageHeaderSize])
		if err := hdr.Validate(); err != nil {
			continue
		}
		if hdr.EntryCount == 0 {
			continue
		}
		entries := decodePageEntries(pageData[PageHeaderSize:hdr.FreeSpaceOffset], int(hdr.EntryCount))
		results = append(results, entries...)
	}
	return results, nil
}
