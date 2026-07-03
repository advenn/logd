package query

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/advenn/logd/internal/storage"
	"github.com/advenn/logd/pkg/lokicompat"
)

// Engine executes log queries against the Storage backend and returns
// Loki-compatible results.
type Engine struct {
	store storage.Storage
}

// NewEngine creates a QueryEngine.
func NewEngine(store storage.Storage) *Engine {
	return &Engine{store: store}
}

// Execute runs a query and returns Loki-compatible stream results grouped by
// label set, matching Loki's query_range response format.
func (e *Engine) Execute(filter storage.QueryFilter) ([]lokicompat.StreamResult, error) {
	entries, err := e.store.Query(filter)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	// Group entries by their label set so entries with identical labels
	// end up in the same stream (matching Loki/Grafana expectations).
	streams := make(map[string]*streamAccumulator)
	var orderedKeys []string

	for _, entry := range entries {
		labels := labelsFromEntry(entry)
		key := labelsToKey(labels)

		acc, ok := streams[key]
		if !ok {
			acc = &streamAccumulator{labels: labels}
			streams[key] = acc
			orderedKeys = append(orderedKeys, key)
		}

		tsNano := fmt.Sprintf("%d", entry.TS.UnixNano())
		acc.values = append(acc.values, [2]string{tsNano, entry.Message})
	}

	var results []lokicompat.StreamResult
	for _, key := range orderedKeys {
		acc := streams[key]
		results = append(results, lokicompat.StreamResult{
			Stream: acc.labels,
			Values: acc.values,
		})
	}

	return results, nil
}

// Labels returns all known label keys.
func (e *Engine) Labels() ([]string, error) {
	return e.store.Labels()
}

// LabelValues returns all known values for a label key.
func (e *Engine) LabelValues(label string) ([]string, error) {
	return e.store.LabelValues(label)
}

type streamAccumulator struct {
	labels map[string]string
	values [][2]string
}

// labelsFromEntry extracts label key-value pairs from a LogEntry.
// Level comes from the entry's Level field; all other labels come from
// the Extra JSON blob stored on disk.
func labelsFromEntry(entry storage.LogEntry) map[string]string {
	labels := make(map[string]string)

	// Level from the structured field.
	labels["level"] = entry.Level.String()

	// Parse Extra JSON for remaining labels (service, trace_id, etc.).
	if entry.Extra != "" {
		var extra map[string]interface{}
		if err := json.Unmarshal([]byte(entry.Extra), &extra); err == nil {
			for k, v := range extra {
				if s, ok := v.(string); ok && s != "" {
					labels[k] = s
				}
			}
		}
	}

	return labels
}

// labelsToKey creates a deterministic string key from a label map for grouping
// entries into streams.
func labelsToKey(labels map[string]string) string {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var result string
	for _, k := range keys {
		result += k + "=" + labels[k] + ","
	}
	return result
}
