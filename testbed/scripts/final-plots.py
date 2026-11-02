#!/usr/bin/env python3
"""Draw the final evaluation's figures from a final-run.sh results directory.

    python3 scripts/final-plots.py results/final-20261102 <out-dir> [results/final-20261102-run1]

Needs matplotlib (the testbed's other scripts need only the standard library), for example:
    uv venv /tmp/plotenv && VIRTUAL_ENV=/tmp/plotenv uv pip install matplotlib
    /tmp/plotenv/bin/python scripts/final-plots.py ...

Every figure is drawn from the raw per-sample CSVs, so a rerun rebuilds them. Each series has
its own color, marker AND line style: the figures must survive a grayscale print and
color-vision deficiency, and three of the palette's colors sit below 3:1 against white, so
color is never the only cue. Series are also labelled directly at their right end. The
numbers behind every figure are in summary.md, as tables.
"""
import csv, glob, math, os, re, statistics, sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker

R, OUT = sys.argv[1], sys.argv[2]
R1 = sys.argv[3] if len(sys.argv) > 3 else None
os.makedirs(OUT, exist_ok=True)

# Categorical palette in fixed order (validated: adjacent-pair CVD dE >= 9.1, normal >= 19.6).
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]
INK, MUTED, GRID = "#1f1f1e", "#6b6a63", "#e4e3dc"

# label in bench-vs.sh -> (display name, color, marker, linestyle). Color follows the engine in
# every figure, never its rank.
ENGINES = {
    "logd (typed index)": ("logd (index)", C[0], "o", "-"),
    "logd v1 (typed index)": ("logd v1 (thesis-eval-1)", C[6], "s", "--"),
    "Loki (| pattern)": ("Loki | pattern", C[1], "^", "-"),
    "Loki (2× | pattern)": ("Loki 2× | pattern", C[1], "^", "-"),
    "VictoriaLogs (extract)": ("VictoriaLogs", C[2], "D", "-"),
    "VictoriaLogs (extract_regexp)": ("VictoriaLogs", C[2], "D", "-"),
    "PostgreSQL (generated col + B-tree)": ("PostgreSQL, B-tree", C[3], "v", "-"),
    "PostgreSQL (extract in WHERE)": ("PostgreSQL, extract in WHERE", C[4], "P", ":"),
    "logd (forced scan)": ("logd, forced scan", C[5], "X", ":"),
}


def slug(label):
    return re.sub(rb"[^a-zA-Z0-9]", b"_", (label + "\n").encode()).decode()


def samples(run, d):
    out = {}
    for f in glob.glob(os.path.join(run, d, "t*_*.csv")):
        m = re.match(r"t(\d+)_(.*)\.csv$", os.path.basename(f))
        with open(f) as fh:
            out[(int(m.group(1)), m.group(2))] = [float(x) for x in fh.read().split()[1:] if x.strip()]
    return out


def rows(run, sweepfile):
    got, t = {}, None
    for line in open(os.path.join(run, sweepfile)):
        m = re.match(r"=+ (?:threshold|size_bytes <) (\d+)", line)
        if m:
            t = int(m.group(1))
        m = re.match(r"\s+logd \(typed index\)\s+(\d+) rows", line)
        if m and t is not None and t not in got:
            got[t] = int(m.group(1))
    return got


def style(ax, xlabel, ylabel):
    ax.set_xlabel(xlabel, color=INK)
    ax.set_ylabel(ylabel, color=INK)
    ax.tick_params(colors=MUTED, labelsize=9)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
    ax.grid(True, which="major", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def save(fig, name):
    for ext in ("svg", "png"):
        fig.savefig(os.path.join(OUT, f"{name}.{ext}"), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", name)


def latency_figure(run, d, sweepfile, total, labels, name, title):
    data, rws = samples(run, d), rows(run, sweepfile)
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    for label in labels:
        disp, col, mk, ls = ENGINES[label]
        pts = []
        for t, n in rws.items():
            v = data.get((t, slug(label)))
            if v:
                pts.append((100 * n / total, statistics.median(v)))
        if not pts:
            continue
        pts.sort()
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=col, marker=mk, linestyle=ls, linewidth=2, markersize=7, label=disp,
                markeredgecolor="white", markeredgewidth=1)
    ax.set_xscale("log")
    ax.set_yscale("log")
    style(ax, "selectivity: share of lines matching (%, log scale)", "median latency (ms, log scale)")
    ax.set_title(title, color=INK, fontsize=10, loc="left")
    # Six or seven series: a legend under the plot, not labels at the line ends, which collide
    # where two engines finish close together. Marker and line style still separate the series
    # without color.
    ax.legend(frameon=False, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3)
    save(fig, name)


APP = ["logd (typed index)", "logd v1 (typed index)", "Loki (| pattern)", "VictoriaLogs (extract)",
       "PostgreSQL (generated col + B-tree)", "PostgreSQL (extract in WHERE)", "logd (forced scan)"]
latency_figure(R, "samples-app-sealed", "sweep-app-sealed.txt", 500000, APP, "fig-latency-app-sealed",
               "Synthetic corpus, 500k lines, all logd segments sealed")
latency_figure(R, "samples-app-live", "sweep-app-live.txt", 500000, APP, "fig-latency-app-live",
               "Synthetic corpus, 500k lines, logd's last segment still active")
HDFS = ["logd (typed index)", "Loki (2× | pattern)", "VictoriaLogs (extract_regexp)",
        "PostgreSQL (generated col + B-tree)", "PostgreSQL (extract in WHERE)", "logd (forced scan)"]
latency_figure(R, "samples-hdfs-sealed", "sweep-hdfs-sealed.txt", 2000000, HDFS, "fig-latency-hdfs",
               "Loghub HDFS, 2M lines, sealed")

# The available tools alone, for the initial-benchmark chapter: no logd series. Loki gets both
# syntaxes, since presenting only one would flatter or penalise it by choice of syntax.
ENGINES["Loki (| regexp)"] = ("Loki | regexp", C[1], "<", "--")
TOOLS_APP = ["Loki (| pattern)", "Loki (| regexp)", "VictoriaLogs (extract)",
             "PostgreSQL (generated col + B-tree)", "PostgreSQL (extract in WHERE)"]
latency_figure(R, "samples-app-sealed", "sweep-app-sealed.txt", 500000, TOOLS_APP, "fig-tools-app",
               "Available tools, synthetic corpus, 500k lines")
ENGINES["Loki (| regexp)"] = ("Loki | regexp", C[1], "<", "--")
TOOLS_HDFS = ["Loki (2× | pattern)", "Loki (| regexp)", "VictoriaLogs (extract_regexp)",
              "PostgreSQL (generated col + B-tree)", "PostgreSQL (extract in WHERE)"]
latency_figure(R, "samples-hdfs-sealed", "sweep-hdfs-sealed.txt", 2000000, TOOLS_HDFS, "fig-tools-hdfs",
               "Available tools, Loghub HDFS, 2M lines")

# Live vs sealed, current logd and v1: what indexing the active segment changed.
fig, ax = plt.subplots(figsize=(7.2, 4.2))
for run_dir, layout, ls in (("samples-app-sealed", "sealed", "-"), ("samples-app-live", "last segment active", "--")):
    data, rws = samples(R, run_dir), rows(R, "sweep-app-sealed.txt")
    for label in ("logd (typed index)", "logd v1 (typed index)"):
        disp, col, mk, _ = ENGINES[label]
        pts = sorted((100 * n / 500000, statistics.median(data[(t, slug(label))])) for t, n in rws.items()
                     if (t, slug(label)) in data)
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=col, marker=mk, linestyle=ls, linewidth=2, markersize=7,
                markeredgecolor="white", markeredgewidth=1, label=f"{disp}, {layout}")
ax.set_xscale("log")
ax.set_yscale("log")
style(ax, "selectivity (%, log scale)", "median latency (ms, log scale)")
ax.set_title("Active-segment index: logd vs logd v1, sealed and live layouts", color=INK, fontsize=10, loc="left")
ax.legend(frameon=False, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2)
save(fig, "fig-active-segment")

# Dispersion at the most selective app threshold.
data = samples(R, "samples-app-sealed")
t = 5000
fig, ax = plt.subplots(figsize=(7.2, 3.8))
labs = [l for l in APP if (t, slug(l)) in data]
vals = [data[(t, slug(l))] for l in labs]
bp = ax.boxplot(vals, orientation="horizontal", widths=0.5, patch_artist=True, showfliers=True,
                medianprops=dict(color=INK, linewidth=1.5), flierprops=dict(marker=".", markersize=3, markeredgecolor=MUTED))
for patch, l in zip(bp["boxes"], labs):
    patch.set_facecolor(ENGINES[l][1])
    patch.set_alpha(0.55)
    patch.set_edgecolor(INK)
ax.set_yticks(range(1, len(labs) + 1), [ENGINES[l][0] for l in labs], fontsize=8)
ax.set_xscale("log")
style(ax, "latency (ms, log scale); box = quartiles, whiskers = 1.5 IQR", "")
ax.set_title(f"latency_ms > {t} (469 rows, 0.094%): {len(vals[0])} interleaved samples per engine",
             color=INK, fontsize=10, loc="left")
save(fig, "fig-dispersion-app-t5000")

# Ingest, disk and memory as three panels: three measures, three scales, never one dual axis.
ing = list(csv.DictReader(open(os.path.join(R, "ingest.csv"))))
lps = {}
for r in ing:
    lps.setdefault(r["engine"], []).append(float(r["lines_per_s"]))
peak = {}
for r in ing:
    if r["peak_mb"]:
        peak.setdefault(r["engine"], []).append(int(r["peak_mb"]))
disk = {r["volume"]: int(r["bytes"]) / 1e6 for r in csv.DictReader(open(os.path.join(R, "disk.csv")))}
pgsize = {}
pgj = sorted(glob.glob(os.path.join(R, "pgload-run*.json")))
if pgj:
    import json
    for v in json.load(open(pgj[-1])):
        pgsize[v["variant"]] = v["total_bytes"] / 1e6
names = [("logd", "logd", "logd-data", C[0]), ("logd-v1", "logd v1", "logd-v1-data", C[6]),
         ("loki", "Loki", "loki-data", C[1]), ("victorialogs", "VictoriaLogs", "vlogs-data", C[2]),
         ("postgres-structured", "PostgreSQL, B-tree", None, C[3])]
fig, axes = plt.subplots(1, 3, figsize=(10, 3.4))
panels = [
    ("ingest, lines/s (median of 5)", lambda k, vol: statistics.median(lps[k]) if k in lps else None),
    ("disk after flush, MB", lambda k, vol: disk.get(vol) if vol else pgsize.get("structured")),
    ("peak memory during ingest, MB", lambda k, vol: statistics.median(peak[k]) if k in peak else None),
]
for ax, (title, fn) in zip(axes, panels):
    ys, labs, cols = [], [], []
    for key, disp, vol, col in names:
        v = fn(key, vol)
        if v is not None:
            ys.append(v); labs.append(disp); cols.append(col)
    bars = ax.barh(range(len(ys)), ys, color=cols, height=0.6, edgecolor="white", linewidth=2)
    ax.set_yticks(range(len(ys)), labs, fontsize=8)
    ax.invert_yaxis()
    for b, v in zip(bars, ys):
        ax.annotate(f"{v:,.0f}" if v >= 100 else f"{v:.1f}", (b.get_width(), b.get_y() + b.get_height() / 2),
                    xytext=(3, 0), textcoords="offset points", va="center", fontsize=8, color=INK)
    style(ax, "", "")
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
        lambda v, _: f"{v / 1000:.0f}k" if v >= 1000 else f"{v:.0f}"))
    ax.set_title(title, color=INK, fontsize=9, loc="left")
fig.suptitle("Synthetic corpus, 500k lines. PostgreSQL: disk = the B-tree variant's relations; no ingest memory (loaded by COPY)",
             color=INK, fontsize=9, x=0.01, ha="left")
fig.tight_layout()
save(fig, "fig-ingest-disk-memory")

# Run 1 vs run 2 on HDFS: the planner fix the first final run motivated.
if R1:
    d1, d2 = samples(R1, "samples-hdfs-sealed"), samples(R, "samples-hdfs-sealed")
    rw = rows(R, "sweep-hdfs-sealed.txt")
    ts = sorted(rw, reverse=True)
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    w = 0.38
    for i, (dd, lab, col) in enumerate(((d1, "logd, run 1 (524d941)", C[6]), (d2, "logd, run 2 (075e3df)", C[0]))):
        ys = [statistics.median(dd[(t, slug("logd (typed index)"))]) for t in ts]
        bars = ax.bar([x + (i - 0.5) * w for x in range(len(ts))], ys, width=w, color=col, label=lab,
                      edgecolor="white", linewidth=2, hatch="//" if i == 0 else None)
        for b, v in zip(bars, ys):
            ax.annotate(f"{v:.0f}", (b.get_x() + b.get_width() / 2, v), xytext=(0, 2), textcoords="offset points",
                        ha="center", fontsize=8, color=INK)
    # VictoriaLogs did not change between the runs, so its own shift shows how much of logd's
    # change is the session (load, cache) rather than the code. Both runs are drawn.
    vl1 = [statistics.median(d1[(t, slug("VictoriaLogs (extract_regexp)"))]) for t in ts]
    vl2 = [statistics.median(d2[(t, slug("VictoriaLogs (extract_regexp)"))]) for t in ts]
    ax.plot(range(len(ts)), vl1, color=C[2], marker="D", linewidth=2, linestyle="--",
            label="VictoriaLogs, run 1 (unchanged engine)")
    ax.plot(range(len(ts)), vl2, color=C[2], marker="D", linewidth=2, label="VictoriaLogs, run 2 (unchanged engine)")
    ax.set_xticks(range(len(ts)), [f"< {t:,}\n{rw[t]:,} rows" for t in ts], fontsize=8)
    style(ax, "size_bytes threshold", "median latency (ms)")
    ax.set_title("HDFS, run 1 vs run 2: the planner fix (08f8ed5, f63c51f) and the session shift", color=INK, fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8)
    save(fig, "fig-hdfs-run1-run2")
