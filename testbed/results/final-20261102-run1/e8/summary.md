
### E-8: acknowledged lines lost to a process crash (kill -9)

median (min–max) over n runs; "lost span" is the event time the missing lines cover at the push rate;
"oldest lost age" is how long before the kill the first missing line had been acknowledged

| rate (lines/s) | sync_interval_ms | flush_interval_ms | n | acked lines | lost acked lines | runs with loss | lost span ms | oldest lost age ms | all losses a tail | restart to /ready ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 10000 | 50 | 500 | 10 | 40550 (20200–58500) | 18 (4–88) | 10/10 | 1.8 (0.4–8.8) | 4.8 (0.4–9.0) | yes | 74 (40–106) |
| 10000 | 10 | 50 | 10 | 43450 (29100–59500) | 33 (0–95) | 9/10 | 3.6 (0.5–9.5) | 3.7 (0.3–6.2) | yes | 74 (72–106) |
| 200 | 50 | 500 | 10 | 767 (564–1066) | 21 (4–36) | 10/10 | 105.0 (20.0–180.0) | 96.3 (11.8–176.2) | yes | 40 (39–44) |
| 200 | 10 | 50 | 10 | 590 (446–934) | 5 (0–8) | 9/10 | 30.0 (10.0–40.0) | 23.4 (5.6–40.1) | yes | 40 (39–41) |

Duplicates across all runs: 0. Lines stored whose push was never acknowledged (the request in
flight at the kill): median 0, max 0 per run.

Machine-crash loss (unsynced page cache) is not measured: a kill -9 leaves the page cache intact.
It adds up to sync_interval_ms plus one flush tick of written-but-unsynced pages on top of these figures;
TestMachineCrashLossApproximation in core/storage approximates it in process.
