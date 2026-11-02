<!-- LOGD_FAULTMATRIX=1 go test ./core/query/ -run TestFaultMatrix -v -count=1, commit 4b884d2, 2026-11-02 -->

### Fault-injection matrix (E-6)

Corpus: 3000 records in 4 compressed + 4 uncompressed sealed segments and one active segment; battery of 40 queries per fault, each run as Execute and ExecuteScan and checked against the ground truth.
Sidecar faults hit one compressed and one uncompressed sealed segment. Explain: per-segment access paths summed over the battery, change against the baseline.

| fault | queries | divergences | ≠ truth | mode | records lost on all paths | Explain |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| none (baseline) | 40 | 0 | 0 | none | 0 | unchanged |
| .tidx deleted | 40 | 0 | 0 | degrade: scan | 0 | indexed -49, cost_guard -2, index_missing +51 |
| .tidx truncated to half | 40 | 0 | 0 | degrade: scan | 0 | indexed -49, cost_guard -2, index_missing +51 |
| .tidx one random byte flipped | 40 | 0 | 0 | degrade: scan | 0 | indexed -49, cost_guard -2, index_missing +51 |
| .tidx zero-length | 40 | 0 | 0 | degrade: scan | 0 | indexed -49, cost_guard -2, index_missing +51 |
| .lidx deleted | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .lidx truncated to half | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .lidx one random byte flipped | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .logz one block corrupted | 40 | 0 | 0 | skip: records lost | 102 | unchanged |
| .logz block directory corrupted | 40 | 3 | 0 | error (index 31/40, scan 34/40 queries) | n/a (the scan fails) | unchanged |
| sealed .log page corrupted | 40 | 0 | 0 | skip: records lost | 25 | unchanged |
| active .log page corrupted | 40 | 0 | 0 | skip: records lost | 26 | unchanged |
| active .log torn last page | 40 | 0 | 0 | skip: records lost | 20 | unchanged |
| manifest lists a sealed segment whose files are gone | 40 | 0 | 0 | skip: records lost; degrade: scan | 380 | indexed -27, cost_guard -1, index_missing +28 |
| manifest lists the active segment, its files gone | 0 | 0 | 0 | error: shard does not open | all (shard does not open) | – |
| drift: template pattern edited | 40 | 0 | 0 | degrade: scan | 0 | indexed -61, cost_guard -9, no_pushdown +70 |
| drift: template removed | 40 | 0 | 0 | degrade: scan | 0 | indexed -61, cost_guard -9, no_pushdown +70 |
| drift: allowlist grown (+env) | 40 | 0 | 0 | none | 0 | unchanged |
| drift: allowlist shrunk (−region) | 40 | 0 | 0 | degrade: scan | 0 | indexed -13, cost_guard +5, no_pushdown +8 |
| interrupted compression: .logz and .log, as found | 40 | 0 | 0 | none | 0 | unchanged |
| interrupted compression: after restart | 40 | 0 | 0 | none | 0 | unchanged |

- **.logz block directory corrupted** (known): the segment cannot be opened, so every query over it fails rather than skipping it; and when its .tidx has no candidates there, Execute never opens it and answers while ExecuteScan fails (TestCorruptSegzDirectorySameOnBothPaths)
- .logz block directory corrupted — lat == 777: index err=<nil>, scan err=segz: directory checksum mismatch
- .logz block directory corrupted — trace == known: index err=<nil>, scan err=segz: directory checksum mismatch
- .logz block directory corrupted — trace == absent: index err=<nil>, scan err=segz: directory checksum mismatch
- **manifest lists the active segment, its files gone**: the shard does not open: `recovering active segment: opening segment <shard>/segments/seg-01M6CS84D2ZFDKV6Z9DZ47Q1TB.log: open <shard>/segments/seg-01M6CS84D2ZFDKV6Z9DZ47Q1TB.log: no such file or directory`
