! WARNING. THIS PROJECT'S MAIN PURPOSE IS ME LEARNING GOLANG AND OTHER LOWER LEVEL ENGINEERING DEEPER
! THIS IS A COPY PROJECT OF ORIGINAL LOGD FOLDER. YOU WILL WRITE DETAILED, COMMENTED CODE HERE, I WILL COPY IT 
! TO ORIGINAL PROJECT. BY THIS I WILL HAVE MORE CHANCE TO LEARN THE CODE. 


# Build Phase 1: On-disk storage format for `logd`

## Context

I'm building `logd`, a lightweight Loki-compatible log storage daemon in Go. This is a learning project to deepen my Go skills and understand database internals. I've completed Phase 0 (project scaffolding, config loading, graceful shutdown skeleton, storage interface stubs).

The full design document is at `docs/logd-project.md`. Please read it before starting. Key sections for this phase: "Log Entry Schema", "Page-oriented I/O", and the Postgres internals lessons at the bottom.

## What I need you to build

Implement the on-disk binary format: page headers with checksums, binary log record encoding/decoding, and lookup table serialization. This is the foundation everything else rests on.

## Architecture constraints

- **Single process, single writer goroutine** — file I/O is done by one goroutine only, fed by channels. But for this phase, just implement the types and helper functions; don't implement the writer goroutine yet.
- **Append-only** — never mutate written records.
- **Fixed 4KB pages** — unit of I/O is always one page.
- **Checksums** — every page header has a CRC32 checksum to detect corruption.
- **All I/O goes through the `Storage` interface** defined in `internal/storage/storage.go`. The concrete implementation will be `FileStorage`.

## Files to create/modify

### 1. `internal/storage/record.go` — Binary LogEntry encoding

Define the in-memory `LogEntry` struct and functions to serialize/deserialize it to binary format.

**LogEntry struct:**
```go
type LogEntry struct {
    TS         time.Time  // event timestamp
    IngestedAt time.Time  // arrival timestamp (set by logd)
    Level      LogLevel   // DEBUG=0, INFO=1, WARN=2, ERROR=3
    ServiceID  uint16     // lookup table ID
    Message    string
    Extra      string     // raw JSON with trace_id, span_id, etc
}

```

Binary format (exactly as spec):
```
[8 bytes:  ts unix nano]           ← time.Time.UnixNano()
[8 bytes:  ingested_at unix nano]
[1 byte:   level]                  ← uint8 enum: 0x00=DEBUG, 0x01=INFO, 0x02=WARN, 0x03=ERROR
[2 bytes:  service_id]             ← big endian
[2 bytes:  msg_len]                ← big endian
[N bytes:  msg]                    ← UTF-8, not null-terminated
[2 bytes:  extra_len]              ← big endian
[M bytes:  extra]                  ← UTF-8 JSON
```


Functions to implement:

    func (e *LogEntry) Encode() ([]byte, error) — serialize to binary

    func DecodeLogEntry(data []byte) (LogEntry, error) — deserialize, return error on malformed data

    func (e *LogEntry) EncodedSize() int — return exact byte count without encoding (for space checks)

    func (l LogLevel) String() string and func ParseLogLevel(s string) (LogLevel, error)

Use encoding/binary with binary.BigEndian (or LittleEndian, pick one and be consistent everywhere — document your choice).

Edge cases to handle:

    Empty message (msg_len=0, zero bytes for msg)

    Empty extra (extra_len=0)

    Very long messages (define a reasonable max, like 64KB, return error if exceeded)

    Malformed byte slices that are too short


2. internal/storage/page.go — Page header and page-level operations
   Page constants:
   const PageSize = 4096        // 4KB
   const PageHeaderSize = 32    // bytes
   const PageMagic uint32 = 0x4C30474E  // "L0GD" in hex


PageHeader struct exactly matching the design doc:
```go
type PageHeader struct {
    Magic           uint32  // 4 bytes — always PageMagic
    MinTS           int64   // 8 bytes — earliest event timestamp in this page (unix nano)
    MaxTS           int64   // 8 bytes — latest event timestamp in this page
    EntryCount      uint16  // 2 bytes — number of log entries in this page
    FreeSpaceOffset uint16  // 2 bytes — offset from page start where next entry would be written
    Checksum        uint32  // 4 bytes — CRC32 of the header itself (with Checksum field set to 0 during calculation)
    Reserved        uint32  // 4 bytes — future use, always 0
}
// Total: exactly 32 bytes
```


Functions to implement:

    func WritePageHeader(w io.Writer, h PageHeader) error

    func ReadPageHeader(r io.Reader) (PageHeader, error)

    func ReadPageHeaderAt(r io.ReaderAt, offset int64) (PageHeader, error) — for reading from specific page offsets during queries

    func (h *PageHeader) CalculateChecksum() uint32 — CRC32 over the header struct with Checksum field zeroed

    func (h *PageHeader) Validate() error — check magic number, verify checksum

Checksum calculation:
Use hash/crc32 with the IEEE polynomial. Write the header bytes to a buffer with the Checksum field set to 0, compute CRC32, then set the field.

Page-level helpers:

    func PageOffset(pageNumber uint64) int64 — returns pageNumber * PageSize

    func CanFitEntry(pageFreeSpace uint16, entrySize int) bool — checks if an entry fits in remaining page space

    func NewPageHeader() PageHeader — returns a header with Magic set, timestamps initialized to max/min, counts at 0, FreeSpaceOffset at PageHeaderSize


3. internal/storage/lookup.go — Service ID and level lookup tables
LookupTable struct:
```go
type LookupTable struct {
    mu        sync.RWMutex
    nameToID  map[string]uint16
    idToName  map[uint16]string
    nextID    uint16
}
```

Methods:

    func NewLookupTable() *LookupTable

    func (lt *LookupTable) GetOrCreate(name string) uint16 — returns existing ID or assigns new one

    func (lt *LookupTable) GetName(id uint16) (string, bool)

    func (lt *LookupTable) Serialize() ([]byte, error) — binary format for persistence

    func DeserializeLookupTable(data []byte) (*LookupTable, error)

Binary format for persistence:

```
[2 bytes: entry_count]
For each entry:
  [2 bytes: id]          (big endian)
  [2 bytes: name_len]
  [N bytes: name]
```

4. Update internal/storage/storage.go (if needed)

Make sure the LogEntry, LogLevel, PageHeader, and any other exported types are accessible. Add any new error types if needed:

```
var (
    ErrPageCorrupted   = errors.New("page corruption detected: invalid magic or checksum")
    ErrEntryTooLarge   = errors.New("log entry exceeds maximum size")
    ErrMalformedRecord = errors.New("malformed binary record")
)
```

Tests to write
internal/storage/record_test.go

    Test encode/decode roundtrip with all fields populated

    Test encode/decode with empty message

    Test encode/decode with empty extra

    Test decode with truncated byte slice (should error)

    Test EncodedSize() matches actual encoded length

    Test edge case: message at max allowed size

    Test edge case: message exceeding max size should error

    Test all LogLevel values encode/decode correctly

internal/storage/page_test.go

    Write page header, read it back, verify all fields

    Test ReadPageHeaderAt reads from correct offset

    Test checksum validation: valid header passes, corrupted header fails

    Test CanFitEntry for various remaining space amounts

    Test PageOffset calculation

internal/storage/lookup_test.go

    New table is empty

    GetOrCreate with new name returns sequential IDs starting from 1

    GetOrCreate with existing name returns same ID

    GetName with valid ID returns name

    GetName with invalid ID returns false

    Serialize then Deserialize produces equivalent table

    Roundtrip: create entries, serialize, deserialize, verify

Go standards and conventions

    Use gofmt formatting style

    All exported functions/types have doc comments (starting with the name)

    Error messages use lowercase, not ending with punctuation

    Use errors.New or fmt.Errorf with %w for wrapping

    Tests use table-driven style where appropriate

    Use t.TempDir() for tests that need files

    No external dependencies beyond standard library + gopkg.in/yaml.v3 (already in go.mod)

What NOT to do

    Do NOT implement the writer goroutine, channels, or HTTP ingestion

    Do NOT implement the segment file management

    Do NOT implement indexes or querying

    Do NOT modify main.go or cmd/logd/main.go

    Do NOT add any new dependencies

    Do NOT implement compression or encryption

Verification

After implementation, the following should compile and run successfully:
bash

go build ./...
go test ./internal/storage/... -v
go vet ./internal/storage/...

The internal/storage package should export these public symbols:
LogEntry, LogLevel, PageSize, PageHeaderSize, PageMagic, PageHeader, LookupTable
Plus their methods and the error sentinels.