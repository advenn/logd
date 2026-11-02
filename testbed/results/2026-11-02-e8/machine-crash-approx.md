<!-- LOGD_MACHINECRASH=1 LOGD_BENCH_DIR=$HOME/.cache/logd-bench go test ./core/storage/ -run TestMachineCrashLossApproximation -v -count=1; 2026-11-02, on a loaded machine (load average ~7) -->
### E-8 machine-crash approximation (power cut at the last fsync)

median (max) over 10 runs; the cut lands uniformly in [1 s, 2 s) after the first record

| rate (lines/s) | sync_interval_ms | flush_interval_ms | lost: open page only (process crash) | lost: + unsynced pages (machine crash) | machine-crash loss, ms of data | since last fsync, ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10000 | 50 | 500 | 32 (47) | 172 (478) | 17.1 (47.8) | 17.4 (49.7) |
| 10000 | 10 | 50 | 34 (48) | 78 (120) | 7.8 (12.0) | 8.2 (12.3) |
| 200 | 50 | 500 | 38 (49) | 38 (49) | 192.5 (245.0) | 190.8 (242.6) |
| 200 | 10 | 50 | 4 (6) | 4 (6) | 17.5 (30.0) | 17.3 (28.3) |
