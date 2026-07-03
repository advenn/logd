package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// LookupTable provides a bidirectional mapping between string names and uint16 IDs.
// It is used to avoid repeating service names and other string values in every
// log entry — only the 2-byte ID is stored on disk. A single lookup.bin file
// persists the current mappings so IDs remain stable across restarts.
//
// Must be created via NewLookupTable. Safe for concurrent use.
type LookupTable struct {
	mu       sync.RWMutex
	nameToID map[string]uint16
	idToName map[uint16]string
	nextID   uint16
}

// NewLookupTable creates an empty LookupTable with ID assignment starting from 1.
// ID 0 is reserved (sentinel value for "no service").
func NewLookupTable() *LookupTable {
	return &LookupTable{
		nameToID: make(map[string]uint16),
		idToName: make(map[uint16]string),
		nextID:   1,
	}
}

// GetOrCreate returns the ID for the given name. If the name is not yet known,
// it assigns a new ID and returns it. Subsequent calls with the same name return
// the same ID.
func (lt *LookupTable) GetOrCreate(name string) uint16 {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if id, ok := lt.nameToID[name]; ok {
		return id
	}
	id := lt.nextID
	lt.nextID++
	lt.nameToID[name] = id
	lt.idToName[id] = name
	return id
}

// GetName returns the name for the given ID. The second return value is false
// if the ID is not found.
func (lt *LookupTable) GetName(id uint16) (string, bool) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	name, ok := lt.idToName[id]
	return name, ok
}

// Serialize encodes the LookupTable to a deterministic binary format suitable
// for writing to lookup.bin. IDs are written in ascending order.
//
// Binary format (big-endian):
//
//	[2 bytes: entry_count]
//	For each entry:
//	  [2 bytes: id]
//	  [2 bytes: name_len]
//	  [N bytes: name]
func (lt *LookupTable) Serialize() ([]byte, error) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	// Calculate total size: 2 bytes for count + per-entry overhead.
	size := 2
	for _, name := range lt.idToName {
		size += 2 + 2 + len(name)
	}
	buf := make([]byte, size)

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(lt.idToName)))
	offset := 2
	// Iterate in ID order for deterministic output.
	for id := uint16(1); id < lt.nextID; id++ {
		name, ok := lt.idToName[id]
		if !ok {
			continue
		}
		binary.BigEndian.PutUint16(buf[offset:offset+2], id)
		offset += 2
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(name)))
		offset += 2
		copy(buf[offset:], name)
		offset += len(name)
	}

	return buf, nil
}

// DeserializeLookupTable reconstructs a LookupTable from its binary representation.
// The returned table is ready for use and preserves all ID assignments from the
// serialized data.
func DeserializeLookupTable(data []byte) (*LookupTable, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("lookup table data too short: need at least 2 bytes for entry count, got %d", len(data))
	}

	lt := NewLookupTable()
	count := binary.BigEndian.Uint16(data[0:2])
	offset := 2

	for i := uint16(0); i < count; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("malformed lookup table: entry %d header truncated", i)
		}

		id := binary.BigEndian.Uint16(data[offset : offset+2])
		nameLen := binary.BigEndian.Uint16(data[offset+2 : offset+4])
		offset += 4

		if offset+int(nameLen) > len(data) {
			return nil, fmt.Errorf("malformed lookup table: entry %d name extends past data", i)
		}

		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		if id == 0 {
			return nil, fmt.Errorf("malformed lookup table: entry %d has reserved ID 0", i)
		}

		lt.nameToID[name] = id
		lt.idToName[id] = name
		if id >= lt.nextID {
			lt.nextID = id + 1
		}
	}

	return lt, nil
}
