
### E-8: acknowledged lines lost to a process crash (kill -9)

median (min–max) over n runs; "lost span" is the event time the missing lines cover at the push rate;
"oldest lost age" is how long before the kill the first missing line had been acknowledged

| rate (lines/s) | sync_interval_ms | flush_interval_ms | n | acked lines | lost acked lines | runs with loss | lost span ms | oldest lost age ms | all losses a tail | restart to /ready ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 10000 | 50 | 500 | 10 | 40550 (20200–58500) | 16 (4–44) | 10/10 | 1.6 (0.4–4.4) | 4.5 (0.7–8.8) | yes | 72 (40–108) |
| 10000 | 10 | 50 | 10 | 43500 (29100–59500) | 20 (5–40) | 10/10 | 2.0 (0.5–4.0) | 2.5 (-0.1–5.7) | yes | 72 (71–104) |
| 200 | 50 | 500 | 10 | 767 (564–1066) | 21 (4–36) | 10/10 | 105.0 (20.0–180.0) | 96.1 (11.7–176.0) | yes | 39 |
| 200 | 10 | 50 | 10 | 590 (446–934) | 6 (2–10) | 10/10 | 30.0 (10.0–50.0) | 25.6 (5.1–40.7) | yes | 39 (39–42) |

Duplicates across all runs: 0. Lines stored whose push was never acknowledged (the request in
flight at the kill): median 0, max 0 per run.

Machine-crash loss (unsynced page cache) is not measured: a kill -9 leaves the page cache intact.
It adds up to sync_interval_ms plus one flush tick of written-but-unsynced pages on top of these figures;
TestMachineCrashLossApproximation in core/storage approximates it in process.
