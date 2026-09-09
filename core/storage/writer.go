package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/advenn/logd/core/index"
	"github.com/advenn/logd/core/label"
	"github.com/advenn/logd/core/model"
)

// Options configures a Writer. It takes explicit values rather than importing a
// config package: the elaborate extraction/label config (design §7.1) is a later
// phase, and the storage engine should not depend on it.
type Options struct {
	SegmentSizeBytes int64             // seal + rotate once the active segment reaches this size
	FlushInterval    time.Duration     // group-commit cadence for partial pages
	Schema           []index.FieldType // indexed fields; each segment builds a per-field .tidx for these at seal
	Retention        time.Duration     // delete sealed segments older than this (0 = keep forever)
	IndexMemBudget   int64             // seal early when the active segment's in-RAM index exceeds this (0 = no cap)
	// SyncInterval bounds how long a written page may sit unsynced. 0 fsyncs every page
	// (the strongest durability, and by far the slowest: fsync dominates the write path).
	// A positive value group-commits, trading a bounded loss window for throughput — the
	// "per page or per N ms" choice design §8 leaves open.
	SyncInterval        time.Duration
	MaxLabelCardinality int // max distinct values indexed per label key per segment (§6.2; 0 = unlimited)
	// BlockPages compresses a SEALED segment into blocks of this many 4KB pages. The active
	// segment is never compressed, so writes and crash recovery keep operating on raw pages.
	// 0 = DefaultBlockPages, negative = leave sealed segments uncompressed.
	BlockPages int
	// Reindex re-derives a record's typed-range keys and label set (the same work the
	// ingest layer does). If set, crash recovery re-runs it over the recovered segment's
	// records to rebuild its in-RAM index, so a recovered segment seals fully-indexed
	// rather than scan-only (design §8). Nil → recovered segment is scan-only.
	Reindex func(model.LogEntry) ([]index.KeyedValue, label.Set)
}

const (
	defaultSegmentSizeBytes = 64 << 20 // 64 MB (design's bounded segment size)
	defaultFlushInterval    = 200 * time.Millisecond
)

// record is the unit pushed through the write channel. It is a struct (not a bare
// LogEntry) purely to reserve the extraction seam: Phase 3 will bundle the
// pre-extracted typed values here — handler goroutines do extraction, the writer
// only pairs values with the record's final offset (design §7.4). Adding that
// field is then additive, not a rewrite of the hot loop.
type record struct {
	entry  model.LogEntry
	keys   []index.KeyedValue // pre-extracted, pre-encoded typed index keys (§7.4)
	labels label.Set          // allowlisted label set for the per-segment label index (§6.2)
}

// Writer is the storage engine: a single background goroutine that is the sole
// writer to disk (design §2 invariant 4). Handlers enqueue records on a buffered
// channel; the goroutine assembles them into 4KB pages, flushes whole pages, and
// owns segment rotation, sealing, durability, and crash recovery.
type Writer struct {
	dir    string // data root
	segDir string // dir/segments
	opts   Options

	writeCh   chan record
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	flushTicker     *time.Ticker
	retentionTicker *time.Ticker
	retentionC      <-chan time.Time // retentionTicker.C, or nil when retention is disabled
	lockFile        *os.File         // held flock on <dir>/LOCK for this shard's lifetime

	// Owned exclusively by the writer goroutine after Start (no locking needed):
	currSeg    *Segment
	idxWriter  *IndexWriter       // sparse time index for the active segment
	page       []byte             // the 4KB page currently being assembled in RAM
	hdr        PageHeader         // header for the page being assembled
	schema     []index.FieldType  // indexed fields (from Options), for the buffer + manifest schema
	buffer     *index.Buffer      // per-active-segment typed-range index buffer, flushed at seal
	labelBuf   *label.Builder     // per-active-segment label index (stream dict + postings), flushed at seal
	card       *label.Cardinality // per-active-segment value-cardinality cap (§6.2)
	segRecords uint64             // record count of the active segment (for the cost guard), reset on rotate
	compressed int                // sealed segments successfully compressed (diagnostics)
	lastSync   time.Time          // when the active segment was last fsynced (group commit)
	needsSync  bool               // a page has been written but not yet fsynced

	// liveStreams holds the ACTIVE segment's label streams for immediate discovery
	// (sealed segments are served from their .lidx). Concurrency-safe: written by the
	// writer goroutine, read by query goroutines. Reset on rotate.
	liveMu      sync.Mutex
	liveStreams map[string]label.Set
	// indexIncomplete marks the recovered active segment: its pre-crash records were
	// never buffered, so writing a .tidx would drop them (false negatives). Such a
	// segment is instead sealed with an empty schema → scanned. Cleared on rotate.
	indexIncomplete bool

	// Internally synchronized, safe to touch from other goroutines:
	manifest  *Manifest
	serviceLT *LookupTable

	// servicesPersisted is the LookupTable size at the last durable lookup.bin
	// write; when the table grows past it, flushPage re-persists (writer-goroutine
	// owned after Start).
	servicesPersisted int

	subs *tailRegistry // live-tail subscribers (§11)
}

// NewWriter opens (or creates) the data directory and performs crash recovery. If
// the manifest names an active segment, it is reopened — OpenSegment truncates any
// torn trailing page and re-derives the segment's time bounds — and those recovered
// bounds are republished to the manifest so recent data is visible to time-range
// queries even though the crash never ran a clean seal (fixes the SIGKILL
// data-loss bug, design §8).
func NewWriter(dir string, opts Options) (*Writer, error) {
	if opts.SegmentSizeBytes <= 0 {
		opts.SegmentSizeBytes = defaultSegmentSizeBytes
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}

	segDir := dir + "/segments"
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	// One writer per shard folder (invariant 4/§10): take the flock before touching any
	// shard state, and release it if construction fails after this point.
	lockFile, err := acquireShardLock(dir)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			releaseShardLock(lockFile)
		}
	}()

	manifest, err := LoadManifest(dir)
	if err != nil {
		return nil, fmt.Errorf("loading manifest: %w", err)
	}

	// Restore the service lookup table if a previous run persisted it.
	serviceLT := NewLookupTable()
	if data, err := os.ReadFile(dir + "/lookup.bin"); err == nil {
		if restored, err := DeserializeLookupTable(data); err == nil {
			serviceLT = restored
		}
	}

	w := &Writer{
		dir:         dir,
		segDir:      segDir,
		opts:        opts,
		writeCh:     make(chan record, 4096),
		closeCh:     make(chan struct{}),
		manifest:    manifest,
		serviceLT:   serviceLT,
		schema:      opts.Schema,
		subs:        newTailRegistry(),
		liveStreams: make(map[string]label.Set),
		card:        label.NewCardinality(opts.MaxLabelCardinality),
	}

	w.lockFile = lockFile

	if active := manifest.Active(); active != nil {
		if err := w.recoverActiveSegment(active); err != nil {
			return nil, fmt.Errorf("recovering active segment: %w", err)
		}
		if opts.Reindex != nil {
			// Re-extract the recovered records to rebuild the typed-range + label index in
			// RAM, so the segment seals fully-indexed (design §8).
			if err := w.reindexRecoveredSegment(active); err != nil {
				return nil, fmt.Errorf("rebuilding recovered segment index: %w", err)
			}
		} else {
			// No reindex hook: the segment's index can't be rebuilt, so it must seal as
			// scan-only (empty schema) rather than get a partial .tidx.
			w.indexIncomplete = true
		}
	}
	ok = true
	return w, nil
}

// recoverActiveSegment reopens the active segment (repairing a torn tail), publishes
// its recovered bounds to the manifest, and rebuilds the segment's sparse time index
// from its surviving pages. The time index is derived data, so rebuilding it from the
// pages is always correct and keeps it consistent with a truncated segment.
func (w *Writer) recoverActiveSegment(meta *SegmentMeta) error {
	seg, err := OpenSegment(meta.Path)
	if err != nil {
		return err
	}
	seg.ID = meta.ID

	// Republish recovered bounds (a crash before seal left these stale/zero) and
	// persist the manifest so a second crash keeps them. On any error below, close
	// the freshly-opened segment fd — the Writer is discarded by NewWriter's caller.
	w.manifest.SetBounds(meta.ID, seg.MinTS, seg.MaxTS)
	if err := w.manifest.Save(w.dir); err != nil {
		seg.Close()
		return fmt.Errorf("persisting recovered bounds: %w", err)
	}

	// Rebuild the sparse .idx from the recovered pages.
	idxPath := indexPathFor(meta.Path)
	_ = os.Remove(idxPath)
	iw, err := OpenIndexWriter(idxPath)
	if err != nil {
		seg.Close()
		return fmt.Errorf("reopening index writer: %w", err)
	}
	for p := uint64(1); p < seg.PageNum; p++ {
		h, err := seg.ReadPageHeader(p)
		if err != nil {
			continue
		}
		if h.EntryCount > 0 {
			if err := iw.WriteEntry(h.MinTS, p); err != nil {
				iw.Close()
				seg.Close()
				return err
			}
		}
	}
	if err := iw.Sync(); err != nil {
		iw.Close()
		seg.Close()
		return err
	}
	w.currSeg = seg
	w.idxWriter = iw
	return nil
}

// reindexRecoveredSegment rebuilds the recovered active segment's in-RAM typed-range and
// label indexes by re-extracting each surviving record (design §8). The rebuilt index maps
// re-derived keys/labels to each record's on-disk offset, so it is self-consistent and,
// once the segment seals, its .tidx/.lidx point at the right records. New records appended
// after recovery add to these same buffers, so the sealed segment is fully indexed.
func (w *Writer) reindexRecoveredSegment(meta *SegmentMeta) error {
	recs, err := readSegmentRecords(meta.Path)
	if err != nil {
		return err
	}
	w.buffer = index.NewBuffer(w.schema)
	w.labelBuf = label.NewBuilder()
	w.card = label.NewCardinality(w.opts.MaxLabelCardinality)
	for _, rec := range recs {
		keys, labels := w.opts.Reindex(rec.Entry)
		labels = w.card.Apply(labels)
		// Mirror processEntry EXACTLY: always intern + post (even an empty label set, which
		// interns to the {} stream) so the rebuilt index is identical to the original;
		// only a non-empty set updates the live-discovery snapshot.
		sid := w.labelBuf.Intern(labels)
		w.labelBuf.AddPosting(sid, rec.Offset)
		if len(labels) > 0 {
			w.liveMu.Lock()
			w.liveStreams[labels.Canonical()] = labels
			w.liveMu.Unlock()
		}
		for _, kv := range keys {
			w.buffer.Add(kv.Field, kv.Key, rec.Offset)
		}
		w.segRecords++
	}
	w.indexIncomplete = false
	return nil
}

// Start creates an initial active segment if recovery didn't provide one, then
// launches the writer goroutine. If ctx is non-nil, cancelling it triggers a clean
// shutdown (equivalent to Close).
func (w *Writer) Start(ctx context.Context) error {
	if w.currSeg == nil {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("creating initial segment: %w", err)
		}
	}
	if w.buffer == nil { // recovered-segment path: rotateSegment didn't run
		w.buffer = index.NewBuffer(w.schema)
	}
	if w.labelBuf == nil {
		w.labelBuf = label.NewBuilder()
	}
	w.resetPage()
	w.flushTicker = time.NewTicker(w.opts.FlushInterval)
	if w.opts.Retention > 0 {
		w.retentionTicker = time.NewTicker(retentionInterval)
		w.retentionC = w.retentionTicker.C
	}

	w.wg.Add(1)
	go w.writerLoop()

	if ctx != nil {
		// Watch for ctx cancellation, but also exit when the writer is closed
		// explicitly (closeCh closes inside shutdown) so this goroutine never leaks
		// when ctx outlives the Writer.
		go func() {
			select {
			case <-ctx.Done():
				_ = w.Close()
			case <-w.closeCh:
			}
		}()
	}
	return nil
}

// InternService resolves a service name to its interned uint16 ID. This is how the
// (future) ingest layer stamps LogEntry.ServiceID before enqueuing; tests may also
// use it. The LookupTable is concurrency-safe.
func (w *Writer) InternService(name string) uint16 {
	return w.serviceLT.GetOrCreate(name)
}

// Write enqueues an entry with no extracted index keys or labels (convenience wrapper).
func (w *Writer) Write(e model.LogEntry) error {
	return w.WriteExtracted(e, nil, nil)
}

// WriteExtracted enqueues an entry with the pre-extracted typed index keys and the
// allowlisted label set the ingest layer produced for it (design §7.4: extraction and
// label derivation run in handler goroutines; the writer interns the labels and pairs
// keys with the record's on-disk offset). Non-blocking: a full queue drops and errors.
func (w *Writer) WriteExtracted(e model.LogEntry, keys []index.KeyedValue, labels label.Set) error {
	select {
	case w.writeCh <- record{entry: e, keys: keys, labels: labels}:
		return nil
	default:
		return fmt.Errorf("write queue full, dropping entry")
	}
}

// WriteExtractedCtx is WriteExtracted, but it WAITS for queue space instead of dropping.
//
// The distinction matters for any caller ingesting a multi-entry batch on behalf of a
// client that will retry. WriteExtracted's non-blocking drop means a batch can be accepted
// halfway and then fail, and because the wire protocols logd speaks (Loki push, and every
// shipper built on it) have no way to say "I took the first 412 of your 1000", the client
// retries the WHOLE batch and the accepted prefix is stored twice. That is silent
// duplication, not data loss, so nothing surfaces it — it was found by a corpus with known
// ground truth returning 169,692 rows for 50,000 pushed lines.
//
// Blocking turns backpressure into latency, which is what Loki and VictoriaLogs do and
// what shippers already expect. The caller's context bounds the wait, and a closed writer
// is reported rather than deadlocking on a channel nobody is draining.
func (w *Writer) WriteExtractedCtx(ctx context.Context, e model.LogEntry, keys []index.KeyedValue, labels label.Set) error {
	select {
	case w.writeCh <- record{entry: e, keys: keys, labels: labels}:
		return nil
	case <-w.closeCh:
		return fmt.Errorf("writer is closed")
	case <-ctx.Done():
		return fmt.Errorf("waiting for write queue: %w", ctx.Err())
	}
}

// Close drains the queue, flushes the final partial page, seals the active segment,
// and persists all metadata. Idempotent and safe to call concurrently: the whole
// cleanup (not just closing the channel) runs under closeOnce, so an explicit Close
// racing the ctx-watcher's Close cannot double-flush/double-seal shared page state.
func (w *Writer) Close() error {
	w.closeOnce.Do(func() { w.shutdown(true) })
	return nil
}

// stop simulates an abrupt process stop (a crash) for tests: it halts the writer
// goroutine after draining queued writes — so already-flushed whole pages stay on
// disk — but performs NO final partial-page flush, NO seal, and NO clean manifest
// update. The segment therefore remains "active" on disk with stale bounds, exactly
// the state NewWriter's recovery must repair. It closes the file descriptors only to
// avoid leaking them across many tests (close does not fsync, so it changes nothing
// a crash wouldn't).
//
// Caveat exploited by callers: draining the channel flushes any page that fills, so
// stop() only drops the final partial page, not channel-buffered records — it tests
// "already-flushed data survives," which is the recovery property under test.
func (w *Writer) stop() {
	w.closeOnce.Do(func() { w.shutdown(false) })
}

// shutdown halts the writer goroutine and finalizes. graceful=true is the normal
// Close path (flush final page, seal, persist); graceful=false leaves on-disk state
// exactly as a crash would (test-only). Runs exactly once via closeOnce at both call
// sites, so cleanup never races itself.
func (w *Writer) shutdown(graceful bool) {
	close(w.closeCh)
	w.wg.Wait()
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	if w.retentionTicker != nil {
		w.retentionTicker.Stop()
	}
	w.subs.closeAll() // tailers see channel close (EOF); no more notify() runs after wg.Wait
	// Release the shard flock LAST — only after the final flush/seal/persist below — so a
	// second writer can never open the folder while we're still writing it (§10).
	defer releaseShardLock(w.lockFile)

	if !graceful {
		if w.currSeg != nil {
			w.currSeg.Close()
		}
		if w.idxWriter != nil {
			w.idxWriter.Close()
		}
		return
	}

	// The goroutine has exited, so page state is ours to touch without a race.
	if w.hdr.EntryCount > 0 {
		if err := w.flushPage(); err != nil {
			log.Printf("storage: flushing final page: %v", err)
		}
	}
	if w.currSeg != nil {
		if err := w.sealCurrentSegment(); err != nil {
			log.Printf("storage: sealing segment on close: %v", err)
		}
	}
	w.persistLookup()
}

// ---- writer goroutine ----

func (w *Writer) writerLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.closeCh:
			// Drain everything still queued, then exit. Final flush/seal is done
			// by Close after Wait (the goroutine no longer touches page state).
			for {
				select {
				case rec := <-w.writeCh:
					w.processEntry(rec)
				default:
					return
				}
			}
		case rec := <-w.writeCh:
			w.processEntry(rec)
		case <-w.flushTicker.C:
			if w.hdr.EntryCount > 0 {
				if err := w.flushPage(); err != nil {
					log.Printf("storage: flushing page on tick: %v", err)
				}
				w.resetPage()
			}
			// Also the group-commit deadline check, so a page written just before the
			// stream went quiet cannot sit unsynced indefinitely. The effective loss
			// window is therefore bounded by SyncInterval + FlushInterval.
			if err := w.syncIfDue(time.Now()); err != nil {
				log.Printf("storage: group-commit sync: %v", err)
			}
		case <-w.retentionC:
			// Runs on the writer goroutine so its manifest Save serializes with seal/
			// flush (no concurrent Save race on the manifest tmp file).
			w.enforceRetention(time.Now())
		}
	}
}

func (w *Writer) processEntry(rec record) {
	e := rec.entry
	if e.IngestedAt.IsZero() {
		e.IngestedAt = time.Now()
	}
	// A missing event time defaults to arrival time. Without this, a zero TS would
	// be rejected by EncodeEntry (unrepresentable) and the entry dropped; defaulting
	// keeps such entries and orders them by when logd saw them.
	if e.TS.IsZero() {
		e.TS = e.IngestedAt
	}

	// Decide fit/rotation using the encoded SIZE (independent of StreamID's value) BEFORE
	// interning labels. Interning yields a per-segment StreamID; if a rotation happened
	// between interning and posting, the posting would land in the new segment's builder
	// under an id from the old segment's dictionary (a wrong-stream bug the .tidx buffer
	// doesn't have because its keys carry no per-segment id).
	size := EncodedSize(e)
	maxUsable := PageSize - PageHeaderSize
	if size > maxUsable {
		log.Printf("storage: entry too large for a page: %d bytes (max %d)", size, maxUsable)
		return
	}
	if size > PageSize-int(w.hdr.FreeSpaceOffset) {
		if err := w.flushPage(); err != nil {
			log.Printf("storage: flushing full page: %v", err)
			return
		}
		w.resetPage()
		// Seal + rotate at a page boundary when the segment reaches its size OR its
		// in-RAM index exceeds the memory budget — the latter caps index RAM without
		// spill-and-merge, staying true to the per-segment (no global merge) design.
		if w.currSeg.Size >= w.opts.SegmentSizeBytes || w.indexMemExceeded() {
			if err := w.rotateSegment(); err != nil {
				log.Printf("storage: rotating segment: %v", err)
				return
			}
		}
	}

	// Target segment settled: intern into THIS segment's dictionary, stamp StreamID,
	// then encode (skipped for a recovered scan-only segment).
	indexing := !w.indexIncomplete
	var streamID uint32
	if indexing && w.labelBuf != nil {
		// Enforce the per-key value-cardinality cap: over-cap pairs are dropped from the
		// INDEXED set (they remain in the record's Extra, found by scan) so the label
		// index can't explode on a high-cardinality key. The reduced set is what we intern,
		// track for discovery, and post — keeping index and scan consistent.
		rec.labels = w.card.Apply(rec.labels)
		streamID = w.labelBuf.Intern(rec.labels)
		e.StreamID = streamID
	}

	data, err := EncodeEntry(e)
	if err != nil {
		log.Printf("storage: encoding entry: %v", err)
		return
	}

	// Append the record into the in-RAM page and update page + segment bounds. The
	// record's segment-relative BYTE offset is the offset of the page it will be
	// flushed to (currSeg.PageNum) plus its position within that page — known now
	// because this page targets currSeg.PageNum.
	off := w.hdr.FreeSpaceOffset
	segRelOffset := uint64(PageOffset(w.currSeg.PageNum) + int64(off))
	copy(w.page[off:], data)
	w.hdr.FreeSpaceOffset += uint16(len(data))
	w.hdr.EntryCount++

	w.segRecords++
	if indexing && len(rec.labels) > 0 {
		// Track the active segment's label streams for immediate discovery.
		w.liveMu.Lock()
		w.liveStreams[rec.labels.Canonical()] = rec.labels
		w.liveMu.Unlock()
	}

	tsNano := e.TS.UnixNano()
	if tsNano < w.hdr.MinTS {
		w.hdr.MinTS = tsNano
	}
	if tsNano > w.hdr.MaxTS {
		w.hdr.MaxTS = tsNano
	}
	if tsNano < w.currSeg.MinTS {
		w.currSeg.MinTS = tsNano
	}
	if tsNano > w.currSeg.MaxTS {
		w.currSeg.MaxTS = tsNano
	}

	// Buffer this record's typed-range keys and its label posting against its offset,
	// unless this is a recovered segment whose index is left incomplete (scan-only).
	if indexing {
		if w.buffer != nil {
			for _, kv := range rec.keys {
				w.buffer.Add(kv.Field, kv.Key, segRelOffset)
			}
		}
		if w.labelBuf != nil {
			w.labelBuf.AddPosting(streamID, segRelOffset)
		}
	}

	w.notify(e) // live-tail fan-out (design §11).
}

// notify is the live-tail fan-out point (§11): after a record is appended, offer it to
// matching subscribers. Delivery is non-blocking so a slow tailer never stalls writes.
func (w *Writer) notify(e model.LogEntry) { w.subs.offer(e) }

// Subscribe registers a live-tail subscriber: it receives every subsequently-written
// record for which match returns true, on the returned channel (buffered by `buffer`;
// records dropped if full). The returned func unsubscribes and closes the channel.
func (w *Writer) Subscribe(match func(model.LogEntry) bool, buffer int) (<-chan model.LogEntry, func()) {
	s, ch := w.subs.add(match, buffer)
	return ch, func() { w.subs.remove(s) }
}

// Manifest returns the live in-memory manifest so the query path sees just-flushed
// active-segment data with current bounds (the on-disk manifest lags between seals).
// The manifest is concurrency-safe.
func (w *Writer) Manifest() *Manifest { return w.manifest }

// ServiceLookup returns the service-name interning table (concurrency-safe), for
// resolving ServiceID → name during query-time label matching.
func (w *Writer) ServiceLookup() *LookupTable { return w.serviceLT }

// LiveStreams returns a snapshot of the ACTIVE segment's label streams, so label
// discovery sees them immediately (before the segment seals into its .lidx).
func (w *Writer) LiveStreams() []label.Set {
	w.liveMu.Lock()
	defer w.liveMu.Unlock()
	out := make([]label.Set, 0, len(w.liveStreams))
	for _, s := range w.liveStreams {
		out = append(out, s)
	}
	return out
}

// indexMemExceeded reports whether the active segment's in-RAM typed-range index has
// grown past the configured memory budget (the dominant index-RAM consumer; the label
// dictionary is bounded by stream cardinality). Estimated as entries × EntryBytes.
func (w *Writer) indexMemExceeded() bool {
	if w.opts.IndexMemBudget <= 0 || w.buffer == nil {
		return false
	}
	return int64(w.buffer.EntryCount())*index.EntryBytes >= w.opts.IndexMemBudget
}

const retentionInterval = time.Hour // how often the writer sweeps for retention-expired segments

// enforceRetention deletes every SEALED segment whose data is entirely older than the
// retention window (relative to now). Per-segment files make this a plain delete — no
// compaction. Runs on the writer goroutine (see writerLoop). The active segment is never
// touched. A no-op when retention is disabled.
func (w *Writer) enforceRetention(now time.Time) {
	if w.opts.Retention <= 0 {
		return
	}
	cutoff := now.Add(-w.opts.Retention).UnixNano()
	expired := w.manifest.SealedBefore(cutoff)
	if len(expired) == 0 {
		return
	}
	removed := false
	for _, meta := range expired {
		// Delete the files FIRST; only drop the manifest entry if that succeeded. A
		// failed delete (e.g. transient EBUSY) keeps the entry so the next cycle retries,
		// rather than orphaning the file (a disk leak that would undermine retention).
		if err := w.deleteSegmentFiles(meta.Path); err != nil {
			log.Printf("storage: retention deferring %s (will retry): %v", meta.Path, err)
			continue
		}
		w.manifest.Remove(meta.ID)
		removed = true
	}
	if removed {
		if err := w.manifest.Save(w.dir); err != nil {
			log.Printf("storage: saving manifest after retention: %v", err)
		}
	}
}

// syncIfDue fsyncs the active segment when the group-commit interval has elapsed. With
// SyncInterval == 0 that is every page, which is the old behaviour.
//
// Durability: pages written since the last sync may be lost on a machine crash, bounded by
// SyncInterval. That is safe by construction rather than by luck — a partially written page
// is caught by the full-page CRC on recovery and truncated, which is the same mechanism
// that already handled a torn trailing page. The loss window widens; nothing becomes
// corrupt, and nothing is silently half-read.
func (w *Writer) syncIfDue(now time.Time) error {
	if !w.needsSync {
		return nil
	}
	if w.opts.SyncInterval > 0 && now.Sub(w.lastSync) < w.opts.SyncInterval {
		return nil
	}
	return w.syncNow(now)
}

// syncNow forces the active segment durable. Called on the group-commit deadline and
// unconditionally at seal, rotate and Close, where the protocol requires it.
func (w *Writer) syncNow(now time.Time) error {
	if w.currSeg == nil {
		return nil
	}
	if err := w.currSeg.Sync(); err != nil {
		return fmt.Errorf("syncing segment: %w", err)
	}
	w.needsSync = false
	w.lastSync = now
	return nil
}

// deleteSegmentFiles removes a segment's .log and every sidecar (<base>.idx, per-field
// .tidx, .labels.lidx) — all matched by the single glob <base>.*. Returns the first
// non-ENOENT error (an already-absent file is fine).
func (w *Writer) deleteSegmentFiles(segPath string) error {
	base := strings.TrimSuffix(segPath, ".log")
	matches, _ := filepath.Glob(base + ".*")
	var firstErr error
	for _, m := range matches {
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			log.Printf("storage: retention removing %s: %v", m, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// ---- page + durability ----

func (w *Writer) resetPage() {
	w.page = make([]byte, PageSize)
	w.hdr = NewPageHeader()
}

// flushPage finalizes the current page, writes it, appends its sparse-index entry,
// group-commits (fsync) so the page is durable, and publishes the active segment's
// bounds to the manifest. fsync-per-flushed-page (rather than per-write) means a
// crash loses at most the in-RAM partial page plus whatever is still buffered in the
// write channel — not any page already flushed here.
func (w *Writer) flushPage() error {
	if w.hdr.EntryCount == 0 {
		return nil
	}

	// Make any newly-interned service names durable BEFORE the page that references
	// their IDs. Otherwise a crash could leave a durable entry whose ServiceID has
	// no persisted name, and on restart that ID would be reused for a different
	// service — silently misattributing already-stored logs.
	if err := w.persistLookupIfGrown(); err != nil {
		return fmt.Errorf("persisting service lookup: %w", err)
	}

	// Publish the segment's bounds BEFORE the page becomes readable on disk. The
	// segment's MinTS/MaxTS already include this page's records (updated per record in
	// processEntry), so publishing first guarantees a concurrent query can never see
	// the new page via the file while the manifest still excludes the segment by stale
	// bounds — which would be a transient false negative for edge-of-window queries.
	// (Bounds slightly ahead of the on-disk page is harmless: the segment is merely
	// scanned and the not-yet-written record simply isn't found until it lands.)
	w.manifest.SetBounds(w.currSeg.ID, w.currSeg.MinTS, w.currSeg.MaxTS)

	FinalizePage(w.page, w.hdr) // stamps header + full-page checksum; must be last mutation
	pageNum := w.currSeg.PageNum
	if err := w.currSeg.WritePage(pageNum, w.page); err != nil {
		return fmt.Errorf("writing page %d: %w", pageNum, err)
	}
	w.currSeg.PageNum++

	if err := w.idxWriter.WriteEntry(w.hdr.MinTS, pageNum); err != nil {
		return fmt.Errorf("writing index entry: %w", err)
	}
	// The sparse .idx is deliberately NOT fsynced here. It is a derived accelerator that
	// nothing reads today (the query path prunes on page-header bounds) and that recovery
	// rebuilds from the segment's pages anyway, so paying an fsync per page for it bought
	// nothing — it just doubled the syscall that dominates this path. It is synced at seal.
	w.needsSync = true
	return w.syncIfDue(time.Now())
}

// ---- segment lifecycle ----

func (w *Writer) rotateSegment() error {
	if w.currSeg != nil {
		if err := w.sealCurrentSegment(); err != nil {
			return err
		}
	}

	// Register first to obtain the ID under the manifest lock, then name the file
	// from that ID (never read manifest.nextID outside the lock).
	meta := &SegmentMeta{State: SegmentActive, MinTS: emptyMinTS, MaxTS: emptyMaxTS, Schema: []FieldSchema{}}
	w.manifest.Add(meta)
	meta.Path = fmt.Sprintf("%s/seg-%s.log", w.segDir, meta.ID)

	seg, err := CreateSegment(meta.Path)
	if err != nil {
		return err
	}
	seg.ID = meta.ID

	iw, err := OpenIndexWriter(indexPathFor(meta.Path))
	if err != nil {
		seg.Close()
		return fmt.Errorf("opening index writer: %w", err)
	}

	w.currSeg = seg
	w.idxWriter = iw
	// A brand-new segment indexes completely from its first record.
	w.buffer = index.NewBuffer(w.schema)
	w.labelBuf = label.NewBuilder()
	w.card = label.NewCardinality(w.opts.MaxLabelCardinality)
	w.indexIncomplete = false
	w.segRecords = 0
	// The just-sealed segment's labels are now served from its .lidx; start fresh so
	// liveStreams stays bounded by the active segment's cardinality.
	w.liveMu.Lock()
	w.liveStreams = make(map[string]label.Set)
	w.liveMu.Unlock()

	// Make the new .log/.idx directory entries durable before the manifest starts
	// referencing them; otherwise a crash could leave a manifest naming a file whose
	// dirent was never synced, and recovery would fail to open it.
	if err := fsyncDir(w.segDir); err != nil {
		return fmt.Errorf("fsync segments dir: %w", err)
	}

	// Persist the manifest now so a crash right after rotation still knows this
	// active segment exists (recovery can then reopen and repair it).
	if err := w.manifest.Save(w.dir); err != nil {
		return fmt.Errorf("saving manifest after rotate: %w", err)
	}
	return nil
}

func (w *Writer) sealCurrentSegment() error {
	if w.currSeg == nil {
		return nil
	}
	if err := w.syncNow(time.Now()); err != nil {
		return fmt.Errorf("syncing segment on seal: %w", err)
	}
	// The sparse .idx is synced here rather than per page (see flushPage): once at seal is
	// enough for a derived structure, and it keeps the sealed segment self-consistent.
	if w.idxWriter != nil {
		if err := w.idxWriter.Sync(); err != nil {
			return fmt.Errorf("syncing sparse index on seal: %w", err)
		}
	}
	if err := w.persistLookupIfGrown(); err != nil {
		return fmt.Errorf("persisting service lookup on seal: %w", err)
	}

	// Flush the typed-range index to per-field .tidx files (durable) BEFORE the
	// manifest advertises a schema for them. A recovered segment (indexIncomplete)
	// gets no .tidx and an empty schema, so the planner scans it — never a false
	// negative from a partial index (design §7.5, §9.1).
	var schema []FieldSchema
	if !w.indexIncomplete {
		segBase := strings.TrimSuffix(w.currSeg.Path, ".log")
		if w.buffer != nil {
			written, err := w.buffer.Flush(segBase, 0) // segment id in the .tidx header is vestigial
			if err != nil {
				return fmt.Errorf("flushing tidx: %w", err)
			}
			schema = schemaToManifest(written)
		}
		if w.labelBuf != nil {
			if _, err := w.labelBuf.Flush(label.IndexPath(segBase)); err != nil {
				return fmt.Errorf("flushing label index: %w", err)
			}
		}
	}

	// Record any label keys whose value-cardinality cap was breached, so the planner
	// scans them for this segment (their over-cap values live only in Extra).
	var cappedKeys []string
	if w.card != nil {
		cappedKeys = w.card.CappedKeys()
		if b := w.card.Breaches(); len(b) > 0 {
			log.Printf("storage: segment %s label-cardinality cap breached: %v (over-cap values indexed via scan only)", w.currSeg.ID, b)
		}
	}

	// Mutate the segment's bounds/state/schema under the manifest lock (never touch the
	// shared *SegmentMeta fields directly — a concurrent reader could see a torn write).
	// indexed=false for a recovered segment marks it scan-only for the planner.
	w.manifest.SealSegment(w.currSeg.ID, w.currSeg.MinTS, w.currSeg.MaxTS, schema, !w.indexIncomplete, w.segRecords, cappedKeys)
	if err := w.manifest.Save(w.dir); err != nil {
		return fmt.Errorf("saving manifest after seal: %w", err)
	}

	if w.idxWriter != nil {
		w.idxWriter.Close()
		w.idxWriter = nil
	}
	segPath := w.currSeg.Path
	if err := w.currSeg.Close(); err != nil {
		return err
	}

	// Compress LAST, after the manifest already describes a complete sealed segment. The
	// rewrite is atomic (write + fsync + rename, then unlink the raw file), so a crash
	// during it leaves either the raw file or the compressed one — never neither, and the
	// reader prefers whichever exists. A failure here is logged and left raw rather than
	// failing the seal: the segment is already durable and queryable either way.
	if ok, err := CompressSegment(segPath, w.opts.BlockPages); err != nil {
		log.Printf("storage: compressing %s (left uncompressed): %v", segPath, err)
	} else if ok {
		w.compressed++
	}
	return nil
}

// persistLookupIfGrown atomically persists lookup.bin when new services have been
// interned since the last durable write. Bounded by the (small) number of distinct
// services, so under steady state it is a no-op.
func (w *Writer) persistLookupIfGrown() error {
	n := w.serviceLT.Size()
	if n <= w.servicesPersisted {
		return nil
	}
	if err := w.writeLookupAtomic(); err != nil {
		return err
	}
	w.servicesPersisted = n
	return nil
}

// persistLookup persists lookup.bin unconditionally (best-effort, used on graceful
// close). Errors are logged, not returned — close should not fail on a derived file.
func (w *Writer) persistLookup() {
	if err := w.writeLookupAtomic(); err != nil {
		log.Printf("storage: persisting lookup.bin: %v", err)
		return
	}
	w.servicesPersisted = w.serviceLT.Size()
}

// writeLookupAtomic serializes the service table and writes it durably: tmp file →
// fsync → rename → dir fsync. A crash can then never observe a torn lookup.bin
// (which DeserializeLookupTable would reject, resetting IDs and misattributing
// already-stored entries).
func (w *Writer) writeLookupAtomic() error {
	data, err := w.serviceLT.Serialize()
	if err != nil {
		return fmt.Errorf("serializing lookup table: %w", err)
	}
	tmp := w.dir + "/lookup.bin.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.dir+"/lookup.bin"); err != nil {
		return err
	}
	return fsyncDir(w.dir)
}

// indexPathFor returns the sparse time-index path for a segment's .log path.
func indexPathFor(segPath string) string {
	return strings.TrimSuffix(segPath, ".log") + ".idx"
}

// schemaToManifest converts the index-layer field types into the manifest's
// serializable schema records (the fields a segment actually has a .tidx for).
func schemaToManifest(fields []index.FieldType) []FieldSchema {
	// Always non-nil so the manifest carries an explicit (possibly empty) schema — an
	// empty schema means "this segment has no typed-range index, scan it".
	out := make([]FieldSchema, len(fields))
	for i, f := range fields {
		out[i] = FieldSchema{Name: f.Name, Type: kindString(f.Kind)}
	}
	return out
}

func kindString(k index.ValueKind) string {
	switch k {
	case index.KindInt:
		return "int"
	case index.KindFloat:
		return "float"
	case index.KindStr:
		return "str"
	case index.KindUUID:
		return "uuid"
	default:
		return "unknown"
	}
}
