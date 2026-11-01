#!/usr/bin/env python3
"""Turn the raw per-cell samples into the tables chapter 6 needs, plus one combined raw CSV."""
import csv, math, os, statistics, sys


def pct(v, p):
    """Nearest-rank percentile of the sorted samples v: the value at rank ceil(p/100 * n).

    round(p/100*n + 0.5) was one rank too high whenever p/100*n is a whole number, because
    Python rounds halves to even: with 20 samples p95 is rank 19, but round(19.5) is 20, so
    "p95" reported the maximum.
    """
    return v[max(0, math.ceil(p * len(v) / 100) - 1)]


def self_test():
    v = list(range(1, 21))  # 20 samples: p95 is the 19th, p99 the 20th
    assert pct(v, 95) == 19 and pct(v, 99) == 20 and pct(v, 50) == 10, (pct(v, 95), pct(v, 99))
    v = list(range(1, 31))  # 30 samples: ceil(28.5) = 29, ceil(29.7) = 30
    assert pct(v, 95) == 29 and pct(v, 99) == 30
    v = list(range(1, 101))  # 100 samples: p95 is exactly the 95th
    assert pct(v, 95) == 95 and pct(v, 99) == 99
    assert pct([7.0], 95) == 7.0
    print("pct self-test ok")


if sys.argv[1:] == ["--self-test"]:
    self_test()
    sys.exit(0)

R = sys.argv[1]
THRESHOLDS = [200, 1000, 3000, 5000]
SEL = {200: "27,089 (5.42%)", 1000: "3,861 (0.77%)", 3000: "1,066 (0.21%)", 5000: "469 (0.094%)"}
ENGINES = [
    ("logd__typed_index__", "logd (index)"),
    ("logd__forced_scan__", "logd (forced scan)"),
    ("Loki____pattern__", "Loki (| pattern)"),
    ("Loki____regexp__", "Loki (| regexp)"),
    ("VictoriaLogs__extract__", "VictoriaLogs"),
]

def load(layout, t, key):
    path = os.path.join(R, f"samples-{layout}", f"t{t}_{key}.csv")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return sorted(float(r["ms"]) for r in csv.DictReader(f))

combined = [("layout", "threshold", "engine", "ms")]
for layout in sys.argv[2:]:
    print(f"\n### Layout: {layout}\n")
    print("| `latency_ms >` | rows | engine | median | p95 | p99 | min | max | n |")
    print("| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for t in THRESHOLDS:
        for key, label in ENGINES:
            v = load(layout, t, key)
            if not v:
                continue
            combined.extend((layout, t, label, f"{ms:.2f}") for ms in v)
            print(f"| {t} | {SEL[t]} | {label} | {statistics.median(v):.1f} | {pct(v,95):.1f} | "
                  f"{pct(v,99):.1f} | {v[0]:.1f} | {v[-1]:.1f} | {len(v)} |")
    print("\n| `latency_ms >` | index vs own scan | vs Loki (pattern) | vs VictoriaLogs |")
    print("| ---: | ---: | ---: | ---: |")
    for t in THRESHOLDS:
        med = {}
        for key, label in ENGINES:
            v = load(layout, t, key)
            if v:
                med[label] = statistics.median(v)
        idx = med.get("logd (index)")
        if not idx:
            continue
        f = lambda lbl: f"{med[lbl]/idx:.2f}x" if lbl in med else "-"
        print(f"| {t} | {f('logd (forced scan)')} | {f('Loki (| pattern)')} | {f('VictoriaLogs')} |")

with open(os.path.join(R, "latency-samples.csv"), "w", newline="") as f:
    csv.writer(f).writerows(combined)
print(f"\nraw samples: {len(combined)-1} rows -> latency-samples.csv")
