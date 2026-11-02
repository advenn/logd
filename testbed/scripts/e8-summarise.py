#!/usr/bin/env python3
"""Summarise E-8 crash-loss runs: median and min-max per setting and rate.

    ./scripts/e8-summarise.py results/<date>-e8/runs.csv
"""
import csv, statistics, sys


def stat(vals, fmt="{:.0f}"):
    if not vals:
        return "–"
    med = fmt.format(statistics.median(vals))
    if min(vals) == max(vals):
        return med
    return f"{med} ({fmt.format(min(vals))}–{fmt.format(max(vals))})"


rows = list(csv.DictReader(open(sys.argv[1])))
if not rows:
    sys.exit("no runs")
groups = {}
for r in rows:
    groups.setdefault((int(r["rate"]), int(r["sync_ms"]), int(r["flush_ms"])), []).append(r)

print("\n### E-8: acknowledged lines lost to a process crash (kill -9)\n")
print("median (min–max) over n runs; \"lost span\" is the event time the missing lines cover at the push rate;")
print("\"oldest lost age\" is how long before the kill the first missing line had been acknowledged\n")
print("| rate (lines/s) | sync_interval_ms | flush_interval_ms | n | acked lines | lost acked lines | runs with loss "
      "| lost span ms | oldest lost age ms | all losses a tail | restart to /ready ms |")
print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |")
for (rate, sync, flush), rs in sorted(groups.items(), key=lambda kv: (-kv[0][0], -kv[0][1])):
    f = lambda k: [float(r[k]) for r in rs]
    lossy = [r for r in rs if int(r["lost_acked"]) > 0]
    tails = all(r["lost_tail"] == "1" for r in lossy)
    span = [float(r["lost_span_ms"]) for r in lossy]
    age = [float(r["oldest_lost_age_ms"]) for r in lossy]
    print(f"| {rate} | {sync} | {flush} | {len(rs)} | {stat(f('acked'))} | {stat(f('lost_acked'))} | {len(lossy)}/{len(rs)} "
          f"| {stat(span, '{:.1f}')} | {stat(age, '{:.1f}')} | {'yes' if tails else 'NO'} | {stat(f('restart_ready_ms'))} |")
dups = sum(int(r["duplicates"]) for r in rows)
unacked = [int(r["unacked_found"]) for r in rows]
print(f"\nDuplicates across all runs: {dups}. Lines stored whose push was never acknowledged (the request in")
print(f"flight at the kill): median {statistics.median(unacked):.0f}, max {max(unacked)} per run.")
print("\nMachine-crash loss (unsynced page cache) is not measured: a kill -9 leaves the page cache intact.")
print("It adds up to sync_interval_ms plus one flush tick of written-but-unsynced pages on top of these figures;")
print("TestMachineCrashLossApproximation in core/storage approximates it in process.")
