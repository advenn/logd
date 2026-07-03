package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/advenn/logd/internal/config"
	"github.com/advenn/logd/internal/template"
)

// FileStorage implements the Storage interface using the local filesystem.
// It uses a single background writer goroutine to serialize all I/O, eliminating
// file locking. HTTP handlers push entries to a channel; the writer goroutine
// is the sole reader of that channel and the sole writer to disk.
type FileStorage struct {
	cfg     *config.Config
	dataDir string

	// Write channel — HTTP handlers push here, writer goroutine consumes.
	writeCh chan LogEntry
	closeCh chan struct{}
	doneCh  chan struct{}
	writeWG sync.WaitGroup

	// Current write state (only touched by writer goroutine).
	mu          sync.Mutex // guards manifest + labelIndex reads from query path
	manifest    *Manifest
	currSeg     *Segment
	currPage    []byte              // 4KB buffer
	pageHdr     PageHeader          // header being built
	pageEntries [][]byte            // raw entry bytes in current page
	pageMatches []map[string]string // template matches per entry (field→value)
	idxWriter   *IndexWriter
	flushTicker *time.Ticker

	// Template engine and index building.
	tmplEngine   *template.Engine
	indexBuilder *template.IndexBuilder
	indexDir     string

	// Lookup tables and label tracking.
	serviceLT *LookupTable
	labelKeys map[string]struct{}
	labelVals map[string]map[string]struct{} // key → set of values

	// Shutdown coordination.
	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileStorage creates a FileStorage backed by cfg.DataDir.
// The template engine is used for field extraction and index building during
// ingestion. It may be nil if no templates are configured.
// The caller must call Start() to begin the writer goroutine.
func NewFileStorage(cfg *config.Config, tmplEngine *template.Engine) (*FileStorage, error) {
	if err := os.MkdirAll(cfg.SegmentsDir(), 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	indexDir := cfg.DataDir + "/index"
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, fmt.Errorf("creating index dir: %w", err)
	}

	manifest, err := LoadManifest(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("loading manifest: %w", err)
	}

	// Load or create lookup table.
	lt := NewLookupTable()
	ltPath := cfg.DataDir + "/lookup.bin"
	if data, err := os.ReadFile(ltPath); err == nil {
		if restored, err := DeserializeLookupTable(data); err == nil {
			lt = restored
		}
	}

	var indexBuilder *template.IndexBuilder
	if tmplEngine != nil {
		indexBuilder = template.NewIndexBuilder(tmplEngine.IndexedFields())
	}

	fs := &FileStorage{
		cfg:          cfg,
		dataDir:      cfg.DataDir,
		writeCh:      make(chan LogEntry, 4096),
		closeCh:      make(chan struct{}),
		doneCh:       make(chan struct{}),
		manifest:     manifest,
		serviceLT:    lt,
		labelKeys:    make(map[string]struct{}),
		labelVals:    make(map[string]map[string]struct{}),
		currPage:     make([]byte, PageSize),
		tmplEngine:   tmplEngine,
		indexBuilder: indexBuilder,
		indexDir:     indexDir,
	}

	// Restore the label key/value sets persisted by the last graceful shutdown
	// so /labels and /label/*/values reflect on-disk data after a restart.
	fs.loadLabels()

	return fs, nil
}

// Start launches the writer goroutine. The provided context controls shutdown.
func (fs *FileStorage) Start(ctx context.Context) error {
	fs.ctx, fs.cancel = context.WithCancel(ctx)

	// Create initial segment.
	if err := fs.rotateSegment(); err != nil {
		return fmt.Errorf("creating initial segment: %w", err)
	}

	fs.resetPage()
	fs.flushTicker = time.NewTicker(time.Duration(fs.cfg.FlushIntervalMs) * time.Millisecond)

	fs.writeWG.Add(1)
	go fs.writerLoop()

	return nil
}

// Write enqueues a log entry. Non-blocking; if the channel is full, the entry
// is dropped and an error is returned.
func (fs *FileStorage) Write(entry LogEntry) error {
	select {
	case fs.writeCh <- entry:
		return nil
	default:
		return fmt.Errorf("write queue full, dropping entry")
	}
}

// Query returns log entries matching the filter. See reader.go for implementation.
func (fs *FileStorage) Query(filter QueryFilter) ([]LogEntry, error) {
	return querySegments(fs.manifest, filter, fs.serviceLT, fs.indexDir)
}

// Labels returns all known label keys.
func (fs *FileStorage) Labels() ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	keys := make([]string, 0, len(fs.labelKeys))
	for k := range fs.labelKeys {
		keys = append(keys, k)
	}
	return keys, nil
}

// LabelValues returns all known values for a label key.
func (fs *FileStorage) LabelValues(label string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	vals, ok := fs.labelVals[label]
	if !ok {
		return nil, nil
	}
	result := make([]string, 0, len(vals))
	for v := range vals {
		result = append(result, v)
	}
	return result, nil
}

// Close shuts down the writer goroutine, flushes remaining data, and persists
// metadata.
func (fs *FileStorage) Close() error {
	close(fs.closeCh)
	fs.writeWG.Wait()

	if fs.flushTicker != nil {
		fs.flushTicker.Stop()
	}

	if fs.cancel != nil {
		fs.cancel()
	}

	// Flush final partial page.
	if fs.pageHdr.EntryCount > 0 {
		if err := fs.flushPage(); err != nil {
			log.Printf("error flushing final page: %v", err)
		}
	}

	// Seal current segment.
	if fs.currSeg != nil {
		if err := fs.sealCurrentSegment(); err != nil {
			log.Printf("error sealing segment: %v", err)
		}
	}

	// Persist lookup table.
	ltData, err := fs.serviceLT.Serialize()
	if err == nil {
		os.WriteFile(fs.dataDir+"/lookup.bin", ltData, 0644)
	}

	// Persist manifest.
	if err := fs.manifest.Save(fs.dataDir); err != nil {
		log.Printf("error saving manifest: %v", err)
	}

	// Persist label sets so they survive a restart.
	if err := fs.persistLabels(); err != nil {
		log.Printf("error saving labels: %v", err)
	}

	if fs.currSeg != nil {
		fs.currSeg.Close()
	}
	if fs.idxWriter != nil {
		fs.idxWriter.Close()
	}

	close(fs.doneCh)
	return nil
}

// ---- writer goroutine ----

func (fs *FileStorage) writerLoop() {
	defer fs.writeWG.Done()

	for {
		select {
		case <-fs.closeCh:
			// Drain remaining entries.
			for {
				select {
				case entry := <-fs.writeCh:
					fs.processEntry(entry)
				default:
					return
				}
			}

		case entry := <-fs.writeCh:
			fs.processEntry(entry)

		case <-fs.flushTicker.C:
			if fs.pageHdr.EntryCount > 0 {
				if err := fs.flushPage(); err != nil {
					log.Printf("error flushing page: %v", err)
				}
				fs.resetPage()
			}
		}
	}
}

func (fs *FileStorage) processEntry(entry LogEntry) {
	// Stamp ingested_at if not set.
	if entry.IngestedAt.IsZero() {
		entry.IngestedAt = time.Now()
	}

	// Resolve service ID.
	entry.ServiceID = fs.serviceLT.GetOrCreate(fs.ExtraLabels(entry, "service", ""))

	// Track labels.
	fs.trackLabels(entry)

	data, err := entry.Encode()
	if err != nil {
		log.Printf("error encoding entry: %v", err)
		return
	}

	entrySize := len(data)

	// Reject entries that can never fit in a page.
	maxUsableSpace := PageSize - PageHeaderSize
	if entrySize > maxUsableSpace {
		log.Printf("entry too large for page: %d bytes (max %d)", entrySize, maxUsableSpace)
		return
	}

	// If entry doesn't fit in current page, flush and start a new page.
	freeSpace := PageSize - int(fs.pageHdr.FreeSpaceOffset)
	if entrySize > freeSpace {
		if err := fs.flushPage(); err != nil {
			log.Printf("error flushing page: %v", err)
			return
		}
		fs.resetPage()

		// Check if segment needs rotation.
		if fs.currSeg.Size >= fs.cfg.SegmentSizeBytes() {
			if err := fs.rotateSegment(); err != nil {
				log.Printf("error rotating segment: %v", err)
				return
			}
		}
	}

	// Append to page buffer.
	offset := fs.pageHdr.FreeSpaceOffset
	copy(fs.currPage[offset:], data)

	// Update page header stats.
	tsNano := entry.TS.UnixNano()
	if tsNano < fs.pageHdr.MinTS {
		fs.pageHdr.MinTS = tsNano
	}
	if tsNano > fs.pageHdr.MaxTS {
		fs.pageHdr.MaxTS = tsNano
	}
	fs.pageHdr.EntryCount++
	fs.pageHdr.FreeSpaceOffset += uint16(entrySize)

	// Also track segment-level stats.
	if tsNano < fs.currSeg.MinTS {
		fs.currSeg.MinTS = tsNano
	}
	if tsNano > fs.currSeg.MaxTS {
		fs.currSeg.MaxTS = tsNano
	}

	// Run template matching for index building.
	var matches map[string]string
	if fs.tmplEngine != nil {
		m := fs.tmplEngine.Match(entry.Message)
		if len(m) > 0 {
			matches = make(map[string]string, len(m))
			for _, fm := range m {
				matches[fm.FieldName] = fm.Value
			}
		}
	}

	// Store entry bytes and template matches for index building at flush time.
	fs.pageEntries = append(fs.pageEntries, data)
	fs.pageMatches = append(fs.pageMatches, matches)
}

// ---- page management ----

func (fs *FileStorage) resetPage() {
	fs.currPage = make([]byte, PageSize)
	fs.pageHdr = NewPageHeader()
	fs.pageEntries = nil
	fs.pageMatches = nil
}

func (fs *FileStorage) flushPage() error {
	if fs.pageHdr.EntryCount == 0 {
		return nil
	}

	// Finalize header.
	fs.pageHdr.Checksum = fs.pageHdr.CalculateChecksum()
	encodePageHeader(fs.currPage, fs.pageHdr)

	// Write page to segment.
	pageNum := fs.currSeg.PageNum
	if err := fs.currSeg.WritePage(pageNum, fs.currPage); err != nil {
		return fmt.Errorf("writing page %d: %w", pageNum, err)
	}
	fs.currSeg.PageNum++

	// Append to index (use page MinTS as the index key).
	if err := fs.idxWriter.WriteEntry(fs.pageHdr.MinTS, pageNum); err != nil {
		return fmt.Errorf("writing index entry: %w", err)
	}

	// Record template index entries for this page.
	if fs.indexBuilder != nil {
		segID := fs.currSeg.ID
		for _, matches := range fs.pageMatches {
			if matches == nil {
				continue
			}
			for field, value := range matches {
				fs.indexBuilder.Add(field, value, segID, pageNum)
			}
		}
	}

	// Update manifest with current segment bounds so queries can find the
	// active segment. Without this, the active segment has MinTS=MaxTS=0
	// and is excluded from all time-range queries.
	meta := fs.manifest.Get(fs.currSeg.ID)
	if meta != nil {
		meta.MinTS = fs.currSeg.MinTS
		meta.MaxTS = fs.currSeg.MaxTS
	}

	return nil
}

// ---- segment management ----

func (fs *FileStorage) rotateSegment() error {
	// Seal current segment if one exists.
	if fs.currSeg != nil {
		if err := fs.sealCurrentSegment(); err != nil {
			return err
		}
	}

	// Create new segment.
	now := time.Now()
	name := fmt.Sprintf("%s_%d.log", now.Format("20060102-150405"), fs.manifest.nextID+1)
	path := fs.cfg.SegmentsDir() + "/" + name

	seg, err := CreateSegment(path)
	if err != nil {
		return err
	}

	meta := &SegmentMeta{
		Path:  path,
		State: SegmentActive,
	}
	fs.manifest.Add(meta)
	seg.ID = meta.ID

	// Open index writer for this segment.
	idxPath := path[:len(path)-4] + ".idx"
	idxWriter, err := OpenIndexWriter(idxPath)
	if err != nil {
		seg.Close()
		return fmt.Errorf("opening index writer: %w", err)
	}

	if fs.idxWriter != nil {
		fs.idxWriter.Close()
	}

	fs.currSeg = seg
	fs.idxWriter = idxWriter
	return nil
}

func (fs *FileStorage) sealCurrentSegment() error {
	if fs.currSeg == nil {
		return nil
	}

	if err := fs.currSeg.Sync(); err != nil {
		return fmt.Errorf("syncing segment: %w", err)
	}

	// Flush template indexes for this segment.
	if fs.indexBuilder != nil {
		if err := fs.indexBuilder.Flush(fs.indexDir); err != nil {
			log.Printf("error flushing template indexes: %v", err)
		}
		// Create a fresh index builder for the next segment.
		fs.indexBuilder = template.NewIndexBuilder(fs.tmplEngine.IndexedFields())
	}

	// Update manifest with final stats.
	meta := fs.manifest.Get(fs.currSeg.ID)
	if meta != nil {
		meta.MinTS = fs.currSeg.MinTS
		meta.MaxTS = fs.currSeg.MaxTS
		meta.State = SegmentSealed
		fs.manifest.Update(meta)
	}

	if err := fs.manifest.Save(fs.dataDir); err != nil {
		return fmt.Errorf("saving manifest after seal: %w", err)
	}

	return fs.currSeg.Close()
}

// ---- label tracking ----

// ExtraLabels extracts a label from the Extra JSON field if present, falling
// back to def. Used for label indexing and query filtering.
func (fs *FileStorage) ExtraLabels(entry LogEntry, key, def string) string {
	var extra map[string]interface{}
	if err := json.Unmarshal([]byte(entry.Extra), &extra); err != nil {
		return def
	}
	if v, ok := extra[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

// persistLabels writes the label key/value sets to disk (labels.json) so they
// survive a graceful restart. Labels are derived data; after an unclean crash
// they are simply absent until re-ingested — full crash-time rebuild from
// segments is a Phase 8 concern, and the manifest has the same property.
func (fs *FileStorage) persistLabels() error {
	fs.mu.Lock()
	out := make(map[string][]string, len(fs.labelVals))
	for k, vals := range fs.labelVals {
		list := make([]string, 0, len(vals))
		for v := range vals {
			list = append(list, v)
		}
		out[k] = list
	}
	fs.mu.Unlock()

	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	return os.WriteFile(fs.dataDir+"/labels.json", data, 0644)
}

// loadLabels restores label sets persisted by persistLabels. Missing or corrupt
// files are ignored (labels repopulate as new entries arrive).
func (fs *FileStorage) loadLabels() {
	data, err := os.ReadFile(fs.dataDir + "/labels.json")
	if err != nil {
		return
	}
	var in map[string][]string
	if err := json.Unmarshal(data, &in); err != nil {
		return
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	for k, vals := range in {
		fs.labelKeys[k] = struct{}{}
		if fs.labelVals[k] == nil {
			fs.labelVals[k] = make(map[string]struct{})
		}
		for _, v := range vals {
			fs.labelVals[k][v] = struct{}{}
		}
	}
}

func (fs *FileStorage) trackLabels(entry LogEntry) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Stream labels from Extra JSON. Extra may be empty (e.g. raw container
	// logs) — that must not prevent tracking level/service below. Empty-valued
	// and non-string labels are skipped so they don't show up as empty options.
	if entry.Extra != "" {
		var extra map[string]interface{}
		if err := json.Unmarshal([]byte(entry.Extra), &extra); err == nil {
			for k, v := range extra {
				s, ok := v.(string)
				if !ok || s == "" {
					continue
				}
				fs.addLabelValueLocked(k, s)
			}
		}
	}

	// Always track "level" — every entry has one.
	fs.addLabelValueLocked("level", entry.Level.String())

	// Track "service" only when it resolves to a non-empty name; logs without a
	// service (raw container logs) must not produce an empty service option.
	if name, ok := fs.serviceLT.GetName(entry.ServiceID); ok && name != "" {
		fs.addLabelValueLocked("service", name)
	}
}

// addLabelValueLocked records a label key/value. Caller must hold fs.mu.
func (fs *FileStorage) addLabelValueLocked(key, val string) {
	fs.labelKeys[key] = struct{}{}
	if fs.labelVals[key] == nil {
		fs.labelVals[key] = make(map[string]struct{})
	}
	fs.labelVals[key][val] = struct{}{}
}
