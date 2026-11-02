### Fault-injection matrix (E-6), final run 2026-11-02

Corpus: 3000 records in 4 compressed + 4 uncompressed sealed segments and one active segment; battery of 40 queries per fault, each run as Execute and ExecuteScan and checked against the ground truth.
Sidecar faults hit one compressed and one uncompressed sealed segment. Explain: per-segment access paths summed over the battery, change against the baseline.

| fault | queries | divergences | ≠ truth | mode | records lost on all paths | Explain |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| none (baseline) | 40 | 0 | 0 | none | 0 | unchanged |
| .tidx deleted | 40 | 0 | 0 | degrade: scan | 0 | indexed -47, cost_guard -4, index_missing +51 |
| .tidx truncated to half | 40 | 0 | 0 | degrade: scan | 0 | indexed -47, cost_guard -4, index_missing +51 |
| .tidx one random byte flipped | 40 | 0 | 0 | degrade: scan | 0 | indexed -47, cost_guard -4, index_missing +51 |
| .tidx zero-length | 40 | 0 | 0 | degrade: scan | 0 | indexed -47, cost_guard -4, index_missing +51 |
| .lidx deleted | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .lidx truncated to half | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .lidx one random byte flipped | 40 | 0 | 0 | degrade: scan | 0 | indexed -10, index_missing +10 |
| .logz one block corrupted | 40 | 0 | 0 | skip: records lost | 102 | unchanged |
| .logz block directory corrupted | 40 | 0 | 0 | skip: records lost | 389 | unchanged |
| sealed .log page corrupted | 40 | 0 | 0 | skip: records lost | 25 | unchanged |
| active .log page corrupted | 40 | 0 | 0 | skip: records lost | 26 | unchanged |
| active .log torn last page | 40 | 0 | 0 | skip: records lost | 20 | unchanged |
| manifest lists a sealed segment whose files are gone | 40 | 0 | 0 | skip: records lost; degrade: scan | 380 | indexed -26, cost_guard -2, index_missing +28 |
| manifest lists the active segment, its files gone | 40 | 0 | 0 | skip: records lost | 300 | cost_guard -2, no_pushdown -11 |
| drift: template pattern edited | 40 | 0 | 0 | degrade: scan | 0 | indexed -63, cost_guard -8, no_pushdown +70, indexed_in_memory +1 |
| drift: template removed | 40 | 0 | 0 | degrade: scan | 0 | indexed -63, cost_guard -8, no_pushdown +79, indexed_in_memory -8 |
| drift: allowlist grown (+env) | 40 | 0 | 0 | none | 0 | no_pushdown -1, indexed_in_memory +1 |
| drift: allowlist shrunk (−region) | 40 | 0 | 0 | degrade: scan | 0 | indexed -8, no_pushdown +9, indexed_in_memory -1 |
| interrupted compression: .logz and .log, as found | 40 | 0 | 0 | none | 0 | cost_guard -2, no_pushdown -11, not_indexed +39 |
| interrupted compression: after restart | 40 | 0 | 0 | none | 0 | unchanged |

Recovered from faultmatrix.log: the run's own write of this file failed on a relative path in final-run.sh, fixed afterwards. Every row ran; the test failed only on writing this file.
