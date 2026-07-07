// Package ingest is logd's native ingestion API. It sits between extraction and
// storage: it runs the extraction engine on each entry's message and derives the
// allowlisted label set from its Extra blob (both the "handler goroutine" work of
// design §7.4), then hands the entry plus its typed keys and label set to the writer,
// which interns the labels and pairs everything with the record's on-disk offset.
package ingest

import (
	"hash/fnv"
	"sync/atomic"

	"github.com/advenn/logd/core/extract"
	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
	"github.com/advenn/logd/core/storage"
)

// Ingester couples an extraction engine and a label allowlist to one or more shard writers,
// routing each record to a shard (design §10). Each writer owns its own folder
// (shared-nothing); the query engine fans in across the same shards.
type Ingester struct {
	engine    *extract.Engine
	writers   []*storage.Writer
	labelKeys []string      // allowlist of Extra keys to index into the label index
	rr        atomic.Uint64 // round-robin cursor for label-less records
}

// New builds a single-shard Ingester with no label indexing (labels remain in Extra).
func New(engine *extract.Engine, w *storage.Writer) *Ingester {
	return NewWithLabels(engine, w, nil)
}

// NewWithLabels builds a single-shard Ingester that also indexes the allowlisted label
// keys. labelKeys must match the allowlist the query engine is given, so index and scan
// agree.
func NewWithLabels(engine *extract.Engine, w *storage.Writer, labelKeys []string) *Ingester {
	return NewShardedWithLabels(engine, []*storage.Writer{w}, labelKeys)
}

// NewShardedWithLabels builds an Ingester that routes records across shard writers.
func NewShardedWithLabels(engine *extract.Engine, writers []*storage.Writer, labelKeys []string) *Ingester {
	return &Ingester{engine: engine, writers: writers, labelKeys: labelKeys}
}

// Ingest extracts typed index keys and the allowlisted label set from the entry, then
// enqueues everything for durable, indexed storage on the routed shard.
func (ig *Ingester) Ingest(e model.LogEntry) error {
	var keys []index.KeyedValue
	if ig.engine != nil {
		keys = ig.engine.Extract(e.Message)
	}
	labels := ig.deriveLabels(e.Extra)
	return ig.writers[ig.route(labels)].WriteExtracted(e, keys, labels)
}

// route picks the shard for a record. A labelled stream is hashed to a stable shard, so a
// stream's data and its per-segment label index stay together; label-less records
// round-robin for write balance. Query-time fan-in merges across shards regardless, so
// routing affects only locality and balance, never correctness.
func (ig *Ingester) route(labels label.Set) int {
	n := len(ig.writers)
	if n <= 1 {
		return 0 // single shard (or, defensively, avoid a divide-by-zero on an empty set)
	}
	if len(labels) == 0 {
		return int(ig.rr.Add(1) % uint64(n))
	}
	h := fnv.New64a()
	h.Write([]byte(labels.Canonical()))
	return int(h.Sum64() % uint64(n))
}

func (ig *Ingester) deriveLabels(extra string) label.Set {
	return DeriveLabels(extra, ig.labelKeys)
}

// DeriveLabels pulls the allowlisted keys out of a record's Extra (using the same parser
// the query resolver uses, so index and scan see identical values) into a canonical label
// set. Exported so crash recovery can rebuild a segment's label index identically to how
// it was first ingested.
func DeriveLabels(extra string, labelKeys []string) label.Set {
	if len(labelKeys) == 0 {
		return nil
	}
	all := model.ParseExtraLabels(extra)
	if len(all) == 0 {
		return nil
	}
	m := make(map[string]string, len(labelKeys))
	for _, k := range labelKeys {
		if model.IsReservedLabelKey(k) {
			continue // level/service resolve from entry fields, never from Extra (see model)
		}
		if v, ok := all[k]; ok {
			m[k] = v
		}
	}
	return label.NewSet(m)
}
