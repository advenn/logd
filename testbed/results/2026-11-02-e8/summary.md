
### E-8: acknowledged lines lost to a process crash (kill -9)

median (min–max) over n runs; "lost span" is the event time the missing lines cover at the push rate;
"oldest lost age" is how long before the kill the first missing line had been acknowledged

| rate (lines/s) | sync_interval_ms | flush_interval_ms | n | acked lines | lost acked lines | runs with loss | lost span ms | oldest lost age ms | all losses a tail | restart to /ready ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 10000 | 50 | 500 | 10 | 40550 (20200–58500) | 30 (12–44) | 10/10 | 3.0 (1.2–4.4) | 5.4 (1.5–8.1) | yes | 220 (89–303) |
| 10000 | 10 | 50 | 10 | 43450 (29000–59500) | 10 (0–42) | 8/10 | 1.2 (0.5–4.2) | 5.2 (1.5–10.2) | yes | 178 (120–388) |
| 200 | 50 | 500 | 10 | 767 (564–1068) | 22 (6–42) | 10/10 | 110.0 (30.0–210.0) | 101.8 (26.7–208.0) | yes | 48 (26–68) |
| 200 | 10 | 50 | 10 | 590 (446–936) | 4 (2–8) | 10/10 | 20.0 (10.0–40.0) | 16.4 (4.1–34.2) | yes | 46 (19–50) |

Duplicates across all runs: 0. Lines stored whose push was never acknowledged (the request in
flight at the kill): median 0, max 0 per run.

Machine-crash loss (unsynced page cache) is not measured: a kill -9 leaves the page cache intact.
It adds up to sync_interval_ms plus one flush tick of written-but-unsynced pages on top of these figures;
TestMachineCrashLossApproximation in core/storage approximates it in process.
