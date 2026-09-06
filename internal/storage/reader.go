package storage

import "github.com/advenn/logd/internal/template"

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
}

// resolveIndexHints opens index files for each FieldFilter, looks up values,
// and returns a map of segment_id → set of page numbers. If no indexes exist
// or a field cannot be resolved, returns nil (graceful degradation to full scan).
func resolveIndexHints(fieldFilters map[string]string, indexDir string) map[uint64]map[uint64]bool {
	if len(fieldFilters) == 0 || indexDir == "" {
		return nil
	}

	var hints map[uint64]map[uint64]bool

	for field, value := range fieldFilters {
		ir, err := template.OpenIndexReader(indexDir, field)
	}
}
