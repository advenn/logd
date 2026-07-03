package template

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
)

// PageRef identifies a specific page within a specific segment.
type PageRef struct {
	SegmentID uint64
	PageNum   uint64
}

// indexMagic is the file header for inverted index files.
var indexMagic = [4]byte{'L', 'D', 'X', 'I'}

// IndexBuilder accumulates field→value→page references during an active segment's
// lifetime. On Flush, entries are sorted and merged into global .tidx files.
type IndexBuilder struct {
	mu sync.Mutex

	uint32Fields map[string]map[uint32][]PageRef
	uint64Fields map[string]map[uint64][]PageRef
	stringFields map[string]map[string][]PageRef
	enumFields   map[string]map[string][]PageRef

	// Field metadata for writing file headers.
	fieldTypes map[string]FieldType
}

// NewIndexBuilder creates an IndexBuilder pre-configured for the given indexed fields.
func NewIndexBuilder(fields []FieldDescriptor) *IndexBuilder {
	ib := &IndexBuilder{
		uint32Fields: make(map[string]map[uint32][]PageRef),
		uint64Fields: make(map[string]map[uint64][]PageRef),
		stringFields: make(map[string]map[string][]PageRef),
		enumFields:   make(map[string]map[string][]PageRef),
		fieldTypes:   make(map[string]FieldType),
	}

	for _, fd := range fields {
		ib.fieldTypes[fd.Name] = fd.Type
		switch fd.Type {
		case FieldTypeUint32:
			ib.uint32Fields[fd.Name] = make(map[uint32][]PageRef)
		case FieldTypeUint64:
			ib.uint64Fields[fd.Name] = make(map[uint64][]PageRef)
		case FieldTypeString:
			ib.stringFields[fd.Name] = make(map[string][]PageRef)
		case FieldTypeEnum:
			ib.enumFields[fd.Name] = make(map[string][]PageRef)
		}
	}

	return ib
}

// Add records a field value → page reference. The value is parsed according to the
// field's declared type. Non-numeric values for uint32/uint64 fields are silently
// dropped (they won't match any query anyway).
func (ib *IndexBuilder) Add(fieldName, value string, segID, pageNum uint64) {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	ft, ok := ib.fieldTypes[fieldName]
	if !ok {
		return // field not configured for indexing
	}

	ref := PageRef{SegmentID: segID, PageNum: pageNum}

	switch ft {
	case FieldTypeUint32:
		m := ib.uint32Fields[fieldName]
		if m == nil {
			return
		}
		v, err := parseUint32(value)
		if err != nil {
			return
		}
		m[v] = appendRef(m[v], ref)
	case FieldTypeUint64:
		m := ib.uint64Fields[fieldName]
		if m == nil {
			return
		}
		v, err := parseUint64(value)
		if err != nil {
			return
		}
		m[v] = appendRef(m[v], ref)
	case FieldTypeString, FieldTypeEnum:
		m := ib.stringFields[fieldName]
		if ib.enumFields[fieldName] != nil {
			m = ib.enumFields[fieldName]
		}
		if m == nil {
			return
		}
		m[value] = appendRef(m[value], ref)
	}
}

// appendRef appends a PageRef to a slice, deduplicating by (SegmentID, PageNum).
func appendRef(refs []PageRef, ref PageRef) []PageRef {
	for _, r := range refs {
		if r.SegmentID == ref.SegmentID && r.PageNum == ref.PageNum {
			return refs // already present
		}
	}
	return append(refs, ref)
}

// Flush sorts buffered entries per field and merges them into global .tidx files.
// Called on segment seal. The indexDir is typically data/index/.
func (ib *IndexBuilder) Flush(indexDir string) error {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("creating index dir: %w", err)
	}

	// Flush uint32 fields.
	for name, m := range ib.uint32Fields {
		if err := flushUint32Index(indexDir, name, m); err != nil {
			return fmt.Errorf("flushing uint32 index %s: %w", name, err)
		}
	}

	// Flush uint64 fields.
	for name, m := range ib.uint64Fields {
		if err := flushUint64Index(indexDir, name, m); err != nil {
			return fmt.Errorf("flushing uint64 index %s: %w", name, err)
		}
	}

	// Flush string and enum fields.
	for name, m := range ib.stringFields {
		if err := flushStringIndex(indexDir, name, m); err != nil {
			return fmt.Errorf("flushing string index %s: %w", name, err)
		}
	}
	for name, m := range ib.enumFields {
		if err := flushStringIndex(indexDir, name, m); err != nil {
			return fmt.Errorf("flushing enum index %s: %w", name, err)
		}
	}

	return nil
}

// ---- uint32 index write ----

func flushUint32Index(indexDir, name string, m map[uint32][]PageRef) error {
	// Load existing entries if file exists.
	existing := make(map[uint32][]PageRef)
	path := indexDir + "/" + name + ".tidx"
	if data, err := os.ReadFile(path); err == nil {
		existing = readUint32Index(data)
	}

	// Merge new entries into existing.
	for v, refs := range m {
		existing[v] = mergeRefs(existing[v], refs)
	}

	// Sort values.
	values := make([]uint32, 0, len(existing))
	for v := range existing {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	return writeUint32IndexFile(path, values, existing)
}

func writeUint32IndexFile(path string, values []uint32, m map[uint32][]PageRef) error {
	var buf []byte
	// Header: magic(4) + field_type(1) + entry_count(8)
	buf = append(buf, indexMagic[:]...)
	buf = append(buf, byte(FieldTypeUint32))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(values)))

	for _, v := range values {
		refs := m[v]
		buf = binary.BigEndian.AppendUint32(buf, v)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(refs)))
		for _, r := range refs {
			buf = binary.BigEndian.AppendUint64(buf, r.SegmentID)
			buf = binary.BigEndian.AppendUint64(buf, r.PageNum)
		}
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func readUint32Index(data []byte) map[uint32][]PageRef {
	if len(data) < 13 {
		return nil
	}
	result := make(map[uint32][]PageRef)
	entryCount := binary.BigEndian.Uint64(data[5:13])
	offset := 13
	for i := uint64(0); i < entryCount && offset+8 <= len(data); i++ {
		v := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		refCount := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		var refs []PageRef
		for j := uint32(0); j < refCount && offset+16 <= len(data); j++ {
			refs = append(refs, PageRef{
				SegmentID: binary.BigEndian.Uint64(data[offset:]),
				PageNum:   binary.BigEndian.Uint64(data[offset+8:]),
			})
			offset += 16
		}
		result[v] = refs
	}
	return result
}

// ---- uint64 index write ----

func flushUint64Index(indexDir, name string, m map[uint64][]PageRef) error {
	existing := make(map[uint64][]PageRef)
	path := indexDir + "/" + name + ".tidx"
	if data, err := os.ReadFile(path); err == nil {
		existing = readUint64Index(data)
	}

	for v, refs := range m {
		existing[v] = mergeRefs(existing[v], refs)
	}

	values := make([]uint64, 0, len(existing))
	for v := range existing {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	return writeUint64IndexFile(path, values, existing)
}

func writeUint64IndexFile(path string, values []uint64, m map[uint64][]PageRef) error {
	var buf []byte
	buf = append(buf, indexMagic[:]...)
	buf = append(buf, byte(FieldTypeUint64))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(values)))

	for _, v := range values {
		refs := m[v]
		buf = binary.BigEndian.AppendUint64(buf, v)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(refs)))
		for _, r := range refs {
			buf = binary.BigEndian.AppendUint64(buf, r.SegmentID)
			buf = binary.BigEndian.AppendUint64(buf, r.PageNum)
		}
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func readUint64Index(data []byte) map[uint64][]PageRef {
	if len(data) < 13 {
		return nil
	}
	result := make(map[uint64][]PageRef)
	entryCount := binary.BigEndian.Uint64(data[5:13])
	offset := 13
	for i := uint64(0); i < entryCount && offset+12 <= len(data); i++ {
		v := binary.BigEndian.Uint64(data[offset:])
		offset += 8
		refCount := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		var refs []PageRef
		for j := uint32(0); j < refCount && offset+16 <= len(data); j++ {
			refs = append(refs, PageRef{
				SegmentID: binary.BigEndian.Uint64(data[offset:]),
				PageNum:   binary.BigEndian.Uint64(data[offset+8:]),
			})
			offset += 16
		}
		result[v] = refs
	}
	return result
}

// ---- string/enum index write ----

func flushStringIndex(indexDir, name string, m map[string][]PageRef) error {
	existing := make(map[string][]PageRef)
	path := indexDir + "/" + name + ".tidx"
	if data, err := os.ReadFile(path); err == nil {
		existing = readStringIndex(data)
	}

	for v, refs := range m {
		existing[v] = mergeRefs(existing[v], refs)
	}

	// Sort keys.
	keys := make([]string, 0, len(existing))
	for k := range existing {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return writeStringIndexFile(path, keys, existing)
}

func writeStringIndexFile(path string, keys []string, m map[string][]PageRef) error {
	var buf []byte
	buf = append(buf, indexMagic[:]...)
	buf = append(buf, byte(FieldTypeString))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(keys)))

	for _, k := range keys {
		refs := m[k]
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, []byte(k)...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(refs)))
		for _, r := range refs {
			buf = binary.BigEndian.AppendUint64(buf, r.SegmentID)
			buf = binary.BigEndian.AppendUint64(buf, r.PageNum)
		}
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func readStringIndex(data []byte) map[string][]PageRef {
	if len(data) < 13 {
		return nil
	}
	result := make(map[string][]PageRef)
	entryCount := binary.BigEndian.Uint64(data[5:13])
	offset := 13
	for i := uint64(0); i < entryCount && offset+2 <= len(data); i++ {
		keyLen := binary.BigEndian.Uint16(data[offset:])
		offset += 2
		if offset+int(keyLen) > len(data) {
			break
		}
		k := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)
		if offset+4 > len(data) {
			break
		}
		refCount := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		var refs []PageRef
		for j := uint32(0); j < refCount && offset+16 <= len(data); j++ {
			refs = append(refs, PageRef{
				SegmentID: binary.BigEndian.Uint64(data[offset:]),
				PageNum:   binary.BigEndian.Uint64(data[offset+8:]),
			})
			offset += 16
		}
		result[k] = refs
	}
	return result
}

// ---- merge helpers ----

func mergeRefs(existing, new []PageRef) []PageRef {
	seen := make(map[PageRef]bool)
	for _, r := range existing {
		seen[r] = true
	}
	for _, r := range new {
		if !seen[r] {
			existing = append(existing, r)
			seen[r] = true
		}
	}
	return existing
}

// ---- IndexReader ----

// IndexReader reads an inverted index file and performs lookups. For v1, the
// entire index is loaded into memory, consistent with the "in-memory index
// buffers (no spilling)" design decision.
type IndexReader struct {
	fieldType FieldType

	uint32Data map[uint32][]PageRef
	uint64Data map[uint64][]PageRef
	stringData map[string][]PageRef

	// Sorted keys for binary search.
	uint32Keys []uint32
	uint64Keys []uint64
	stringKeys []string
}

// OpenIndexReader opens a .tidx file and loads it into memory.
func OpenIndexReader(indexDir, fieldName string) (*IndexReader, error) {
	path := indexDir + "/" + fieldName + ".tidx"
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 13 {
		return nil, fmt.Errorf("index file %s too short: %d bytes", path, len(data))
	}

	// Validate magic.
	if string(data[:4]) != string(indexMagic[:]) {
		return nil, fmt.Errorf("index file %s has invalid magic", path)
	}

	ft := FieldType(data[4])
	entryCount := binary.BigEndian.Uint64(data[5:13])

	ir := &IndexReader{fieldType: ft}

	switch ft {
	case FieldTypeUint32:
		ir.uint32Data = readUint32Index(data)
		ir.uint32Keys = make([]uint32, 0, entryCount)
		for k := range ir.uint32Data {
			ir.uint32Keys = append(ir.uint32Keys, k)
		}
		sort.Slice(ir.uint32Keys, func(i, j int) bool { return ir.uint32Keys[i] < ir.uint32Keys[j] })
	case FieldTypeUint64:
		ir.uint64Data = readUint64Index(data)
		ir.uint64Keys = make([]uint64, 0, entryCount)
		for k := range ir.uint64Data {
			ir.uint64Keys = append(ir.uint64Keys, k)
		}
		sort.Slice(ir.uint64Keys, func(i, j int) bool { return ir.uint64Keys[i] < ir.uint64Keys[j] })
	case FieldTypeString, FieldTypeEnum:
		ir.stringData = readStringIndex(data)
		ir.stringKeys = make([]string, 0, entryCount)
		for k := range ir.stringData {
			ir.stringKeys = append(ir.stringKeys, k)
		}
		sort.Strings(ir.stringKeys)
	}

	return ir, nil
}

// LookupString returns page references for a string field value. Uses binary
// search over the sorted keys.
func (ir *IndexReader) LookupString(value string) ([]PageRef, error) {
	if ir.stringData == nil {
		return nil, fmt.Errorf("index is not a string/string index")
	}
	idx := sort.SearchStrings(ir.stringKeys, value)
	if idx < len(ir.stringKeys) && ir.stringKeys[idx] == value {
		return ir.stringData[value], nil
	}
	return nil, nil
}

// LookupUint32 returns page references for a uint32 field value.
func (ir *IndexReader) LookupUint32(value uint32) ([]PageRef, error) {
	if ir.uint32Data == nil {
		return nil, fmt.Errorf("index is not a uint32 index")
	}
	idx := sort.Search(len(ir.uint32Keys), func(i int) bool {
		return ir.uint32Keys[i] >= value
	})
	if idx < len(ir.uint32Keys) && ir.uint32Keys[idx] == value {
		return ir.uint32Data[value], nil
	}
	return nil, nil
}

// LookupUint64 returns page references for a uint64 field value.
func (ir *IndexReader) LookupUint64(value uint64) ([]PageRef, error) {
	if ir.uint64Data == nil {
		return nil, fmt.Errorf("index is not a uint64 index")
	}
	idx := sort.Search(len(ir.uint64Keys), func(i int) bool {
		return ir.uint64Keys[i] >= value
	})
	if idx < len(ir.uint64Keys) && ir.uint64Keys[idx] == value {
		return ir.uint64Data[value], nil
	}
	return nil, nil
}

// Lookup is a convenience method that accepts a string value and routes to the
// correct typed lookup based on the index's field type.
func (ir *IndexReader) Lookup(value string) ([]PageRef, error) {
	switch ir.fieldType {
	case FieldTypeUint32:
		v, err := parseUint32(value)
		if err != nil {
			return nil, nil
		}
		return ir.LookupUint32(v)
	case FieldTypeUint64:
		v, err := parseUint64(value)
		if err != nil {
			return nil, nil
		}
		return ir.LookupUint64(v)
	case FieldTypeString, FieldTypeEnum:
		return ir.LookupString(value)
	default:
		return nil, fmt.Errorf("unsupported field type: %s", ir.fieldType)
	}
}

// ---- parse helpers ----

func parseUint32(s string) (uint32, error) {
	var v uint32
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid uint32: %q", s)
		}
		v = v*10 + uint32(ch-'0')
	}
	return v, nil
}

func parseUint64(s string) (uint64, error) {
	var v uint64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid uint64: %q", s)
		}
		v = v*10 + uint64(ch-'0')
	}
	return v, nil
}
