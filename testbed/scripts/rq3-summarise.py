#!/usr/bin/env python3
"""Summarise the E-4 / RQ3 benchmark output: median and min-max per configuration.

    ./scripts/rq3-summarise.py results/<date>-rq3

Reads extract.txt, ingest.txt and indexram.txt (raw `go test -bench` output), prints the
markdown tables, and writes samples.csv (one row per benchmark line and metric) for plotting.
"""
import csv, os, re, statistics, sys

LINE = re.compile(r"^Benchmark(Template\w+)/rate=(\d+)/templates=(\w+)-\d+\s+(\d+)\s+(.*)$")
ORDER = ["0", "1", "5", "20", "5never", "20never"]


def parse(path):
    """{(bench, rate, templates): {unit: [values]}} from one go test output file."""
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        m = LINE.match(line.strip())
        if not m:
            continue
        bench, rate, tmpl, _n, rest = m.groups()
        fields = rest.split()
        metrics = out.setdefault((bench, int(rate), tmpl), {})
        for val, unit in zip(fields[0::2], fields[1::2]):
            metrics.setdefault(unit, []).append(float(val))
    return out


def stat(vals, fmt="{:.0f}"):
    if not vals:
        return "–"
    med = statistics.median(vals)
    if len(vals) == 1 or min(vals) == max(vals):
        return fmt.format(med)
    return f"{fmt.format(med)} ({fmt.format(min(vals))}–{fmt.format(max(vals))})"


def med(d, key, unit):
    v = d.get(key, {}).get(unit)
    return statistics.median(v) if v else None


def keyorder(k):
    bench, rate, tmpl = k
    return (bench, -rate, ORDER.index(tmpl) if tmpl in ORDER else 99)


def main():
    R = sys.argv[1]
    data = {}
    for f in ("extract.txt", "ingest.txt", "indexram.txt"):
        data.update(parse(os.path.join(R, f)))
    if not data:
        sys.exit(f"no benchmark lines found in {R}")

    with open(os.path.join(R, "samples.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["bench", "rate", "templates", "metric", "value"])
        for k in sorted(data, key=keyorder):
            for unit, vals in data[k].items():
                for v in vals:
                    w.writerow([k[0], k[1], k[2], unit, v])

    for bench, title in (("TemplateExtract", "Extraction alone (Engine.Extract per line)"),
                         ("TemplateWrite", "Writer alone (keys extracted before the timer)"),
                         ("TemplateIngest", "Full native ingest path (extract + labels + writer, until sealed)")):
        keys = sorted((k for k in data if k[0] == bench), key=keyorder)
        if not keys:
            continue
        n = max(len(data[k].get("ns/op", [])) for k in keys)
        print(f"\n### {title}\n\nmedian (min–max) of n = {n} runs; one op = one line\n")
        print("| match rate | templates | keys/line | lines matched | ns/line | lines/s | B/line | allocs/line |")
        print("| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for k in keys:
            d = data[k]
            print(f"| {k[1]}% | {k[2]} | {stat(d.get('keys/line', []), '{:.2f}')} | {stat(d.get('match%', []), '{:.1f}')}% "
                  f"| {stat(d.get('ns/op', []))} | {stat(d.get('lines/s', []))} | {stat(d.get('B/op', []))} "
                  f"| {stat(d.get('allocs/op', []))} |")

    # Decomposition and slowdown against the zero-template baseline, from the medians.
    rows = sorted({(k[1], k[2]) for k in data if k[0] == "TemplateIngest"}, key=lambda r: (-r[0], ORDER.index(r[1]) if r[1] in ORDER else 99))
    if rows:
        print("\n### Cost split and slowdown vs 0 templates (medians, ns/line)\n")
        print("| match rate | templates | extract | writer | ingest | ingest Δ vs 0 | ingest lines/s vs 0 |")
        print("| ---: | --- | ---: | ---: | ---: | ---: | ---: |")
        for rate, tmpl in rows:
            ex = med(data, ("TemplateExtract", rate, tmpl), "ns/op")
            wr = med(data, ("TemplateWrite", rate, tmpl), "ns/op")
            ig = med(data, ("TemplateIngest", rate, tmpl), "ns/op")
            base = med(data, ("TemplateIngest", rate, "0"), "ns/op")
            lps = med(data, ("TemplateIngest", rate, tmpl), "lines/s")
            lps0 = med(data, ("TemplateIngest", rate, "0"), "lines/s")
            fmt = lambda v: "–" if v is None else f"{v:.0f}"
            delta = "–" if ig is None or base is None else f"{ig - base:+.0f}"
            ratio = "–" if not lps or not lps0 else f"{lps / lps0:.2f}×"
            print(f"| {rate}% | {tmpl} | {fmt(ex)} | {fmt(wr)} | {fmt(ig)} | {delta} | {ratio} |")

    keys = sorted((k for k in data if k[0] == "TemplateIndexRAM"), key=keyorder)
    if keys:
        print("\n### Active-segment index RAM for one 64 MB segment (RQ4)\n")
        print("estimate = entries × 24 B, as indexMemExceeded computes it; heap = live heap of the typed-range buffer;")
        print("label = the label builder (one posting per record), which the estimate does not count\n")
        print("| match rate | templates | records/seg | entries/seg | estimate MB | heap MB | B/entry (heap) | label MB |")
        print("| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for k in keys:
            d = data[k]
            g = lambda u, f="{:.0f}": stat(d.get(u, []), f)
            print(f"| {k[1]}% | {k[2]} | {g('records/seg')} | {g('entries/seg')} | {g('estMB/seg', '{:.1f}')} "
                  f"| {g('heapMB/seg', '{:.1f}')} | {g('B/entry', '{:.1f}')} | {g('labelMB/seg', '{:.1f}')} |")


if __name__ == "__main__":
    main()
