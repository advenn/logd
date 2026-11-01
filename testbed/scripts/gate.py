#!/usr/bin/env python3
"""Correctness gate against GROUND TRUTH: every engine's row count must equal the manifest's.

bench-vs.sh checks that the engines agree with EACH OTHER before it times them. That catches a
broken query, but not a broken corpus or a broken extraction shared by all of them: two engines
can agree and both be wrong. This script computes the expected answer from the manifest alone,
with no backend involved, and compares every engine to that.

It also holds the exact query each engine runs, per dataset, so the queries a chapter quotes and
the queries that were verified are the same strings:

    gate.py --dataset app  --manifest corpus/app-500000.jsonl
    gate.py --dataset hdfs --manifest ~/.cache/logd-bench/hdfs-2000000.jsonl
    gate.py --dataset hdfs --manifest ... --print-queries

Exit status is non-zero if any engine returned a different count, or zero rows, for any threshold.
"""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request

LOGD = "http://localhost:7100"
LOKI = "http://localhost:3100"
VL = "http://localhost:9428"
PG = "http://localhost:7300"

# Per dataset: the field, the operator and thresholds of the selectivity sweep, and each
# engine's (name, url, query, response format). {app}, {op} and {t} are filled in per engine;
# the operator is spelled the way each language spells it (see OPS).
#
# Loki's line filter (|= "took=", |= "size ") is deliberately included: leaving it out would
# sandbag Loki, and it is also what keeps lines WITHOUT the field out of the numeric filter,
# where Loki would otherwise keep them with an __error__ label instead of dropping them.
DATASETS = {
    "app": {
        "field": "latency_ms",
        "op": ">",
        "thresholds": [200, 1000, 3000, 5000],
        "engines": [
            ("logd (typed index)", f"{LOGD}/loki/api/v1/query_range",
             '{{app="{app}"}} | latency_ms {op} {t}', "loki"),
            ("logd (forced scan)", f"{LOGD}/logd/api/v1/query_scan",
             '{{app="{app}"}} | latency_ms {op} {t}', "loki"),
            ("Loki (| pattern)", f"{LOKI}/loki/api/v1/query_range",
             '{{app="{app}"}} |= "took=" | pattern "<_>took=<latency_ms>ms<_>" | latency_ms {op} {t}', "loki"),
            ("Loki (| regexp)", f"{LOKI}/loki/api/v1/query_range",
             '{{app="{app}"}} |= "took=" | regexp "took=(?P<latency_ms>[0-9]+)ms" | latency_ms {op} {t}', "loki"),
            ("VictoriaLogs (extract)", f"{VL}/select/logsql/query",
             '_stream:{{app="{app}"}} "took=" | extract "took=<latency_ms>ms" | latency_ms:{op}{t}', "jsonl"),
            ("PostgreSQL (structured)", f"{PG}/loki/api/v1/query_range?variant=structured",
             "latency_ms {op} {t}", "loki"),
            ("PostgreSQL (unstructured)", f"{PG}/loki/api/v1/query_range?variant=unstructured",
             "latency_ms {op} {t}", "loki"),
        ],
    },
    # Loghub HDFS_v1, first 2,000,000 lines. 95% of the sizes are exactly 67108864 (a full
    # 64 MiB block), so the selective end of the sweep is SMALL blocks: size_bytes < T. The
    # thresholds give 1.58% / 1.02% / 0.106% / 0.0091% of all lines. 5% is unreachable with a
    # single threshold: everything below 64 MiB is 1.58%, and >= 64 MiB jumps to 30.1%.
    "hdfs": {
        "field": "size_bytes",
        "op": "<",
        "thresholds": [67108864, 28490000, 3535000, 90000],
        "engines": [
            ("logd (typed index)", f"{LOGD}/loki/api/v1/query_range",
             '{{app="{app}"}} | size_bytes {op} {t}', "loki"),
            ("logd (forced scan)", f"{LOGD}/logd/api/v1/query_scan",
             '{{app="{app}"}} | size_bytes {op} {t}', "loki"),
            # One `| pattern` cannot express this field: in two of the five line forms the number
            # is followed by more text ("of size N from /10.x"), in three it ends the line, and a
            # capture runs to the next literal or to the end of the line. `pattern "<_> size
            # <size_bytes>"` alone captures "N from /10.x" on the first kind, the numeric filter
            # then fails, and Loki KEEPS those lines with an __error__ label instead of dropping
            # them: measured, 200,000 rows (the limit) of which 199,740 carry __error__, against
            # 2,123 expected. Two stages are exact: the first captures N when text follows, the
            # second when N ends the line, and on a line the first stage matched the second does
            # not overwrite size_bytes (Loki 3.7: the first parser to extract a label wins).
            ("Loki (| pattern)", f"{LOKI}/loki/api/v1/query_range",
             '{{app="{app}"}} |= "size " | pattern "<_> size <size_bytes> <_>" | pattern "<_> size <size_bytes>" | size_bytes {op} {t}', "loki"),
            ("Loki (| regexp)", f"{LOKI}/loki/api/v1/query_range",
             '{{app="{app}"}} |= "size " | regexp "size (?P<size_bytes>[0-9]+)" | size_bytes {op} {t}', "loki"),
            ("VictoriaLogs (extract_regexp)", f"{VL}/select/logsql/query",
             '_stream:{{app="{app}"}} "size" | extract_regexp "size (?P<size_bytes>[0-9]+)" | size_bytes:{op}{t}', "jsonl"),
            # The same two-layout problem as Loki's pattern, and the same fix. A single
            # `extract "size <size_bytes>"` returns 1,136 of the 2,123 rows at T=3535000 (the
            # "of size N from" form yields a non-number, which the range filter drops), and
            # `extract "size <size_bytes> "` returns 987 (it never matches at end of line). The
            # second stage runs only where the first extracted nothing.
            ("VictoriaLogs (extract)", f"{VL}/select/logsql/query",
             '_stream:{{app="{app}"}} "size" | extract "size <size_bytes> " | extract if (size_bytes:"") "size <size_bytes>" | size_bytes:{op}{t}', "jsonl"),
            ("PostgreSQL (structured)", f"{PG}/loki/api/v1/query_range?variant=structured&dataset=hdfs",
             "size_bytes {op} {t}", "loki"),
            ("PostgreSQL (unstructured)", f"{PG}/loki/api/v1/query_range?variant=unstructured&dataset=hdfs",
             "size_bytes {op} {t}", "loki"),
        ],
    },
}

# How each query language spells a comparison. LogQL label filters use == for equality;
# LogsQL writes field:>N; the shim takes the bare operator.
OPS = {
    ">": {"loki": ">", "logsql": ">", "pg": ">"},
    ">=": {"loki": ">=", "logsql": ">=", "pg": ">="},
    "<": {"loki": "<", "logsql": "<", "pg": "<"},
    "<=": {"loki": "<=", "logsql": "<=", "pg": "<="},
    "=": {"loki": "==", "logsql": "=", "pg": "="},
}
CMP = {
    ">": lambda v, t: v > t, ">=": lambda v, t: v >= t,
    "<": lambda v, t: v < t, "<=": lambda v, t: v <= t, "=": lambda v, t: v == t,
}


def dialect(url):
    if url.startswith(PG):
        return "pg"
    if url.startswith(VL):
        return "logsql"
    return "loki"


def scan_manifest(path, field):
    """One pass: the query window, the app label, and every value the field takes.

    The window is padded a second either side, as window.sh does, so a boundary record is
    never clipped by an off-by-one in some engine's interval convention."""
    first = last = None
    app = None
    values = []
    with open(path, "rb") as f:
        for raw in f:
            r = json.loads(raw)
            if first is None:
                first = r["ts_ns"]
                app = r.get("labels", {}).get("app", "checkout")
            last = r["ts_ns"]
            v = r.get(field)
            if v is None and r.get("value_name") == field:
                v = r.get("value")
            if v is not None:
                values.append(v)
    if first is None:
        sys.exit(f"{path}: empty manifest")
    return first - 10**9, last + 10**9, app, values


def count_rows(body, fmt):
    if fmt == "jsonl":  # LogsQL: one JSON object per matching record
        return sum(1 for line in body.splitlines() if line.strip())
    d = json.loads(body)
    if "data" not in d:
        raise ValueError(body[:300].decode(errors="replace"))
    return sum(len(r["values"]) for r in d["data"]["result"])


def fetch(url, query, start, end, limit):
    params = urllib.parse.urlencode({"query": query, "start": start, "end": end, "limit": limit})
    full = url + ("&" if "?" in url else "?") + params
    with urllib.request.urlopen(full, timeout=600) as resp:
        return resp.read()


def render(engine, app, op, t):
    name, url, tmpl, fmt = engine
    return tmpl.format(app=app, op=OPS[op][dialect(url)], t=t)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True, choices=sorted(DATASETS))
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--thresholds", help="comma-separated; default per dataset")
    ap.add_argument("--op", choices=sorted(OPS), help="default per dataset")
    ap.add_argument("--limit", type=int, default=200000, help="as bench-vs.sh asks for (default 200000)")
    ap.add_argument("--engines", help="comma-separated substrings of engine names to run (default all)")
    ap.add_argument("--print-queries", action="store_true", help="print each engine's URL and query and exit")
    a = ap.parse_args()

    ds = DATASETS[a.dataset]
    op = a.op or ds["op"]
    thresholds = [int(x) for x in a.thresholds.split(",")] if a.thresholds else ds["thresholds"]
    engines = ds["engines"]
    if a.engines:
        want = [w.strip().lower() for w in a.engines.split(",")]
        engines = [e for e in engines if any(w in e[0].lower() for w in want)]

    start, end, app, values = scan_manifest(a.manifest, ds["field"])

    if a.print_queries:
        print(f"dataset {a.dataset}: {len(values):,} values of {ds['field']}; window [{start}, {end}); app={app}")
        for e in engines:
            print(f"\n{e[0]}\n  GET {e[1]}\n  query={render(e, app, op, 'T')}")
        return 0

    total = None
    with open(a.manifest, "rb") as f:
        total = sum(1 for _ in f)
    print(f"dataset {a.dataset}, manifest {a.manifest}")
    print(f"  {total:,} lines, {len(values):,} carry {ds['field']}; window [{start}, {end}), app={app}, limit={a.limit}")

    failed = False
    for t in thresholds:
        expected = sum(1 for v in values if CMP[op](v, t))
        print(f"\n{ds['field']} {op} {t}: expected {expected:,} ({expected / total:.4%} of lines)")
        if expected > a.limit:
            print(f"  SKIP: expected count exceeds --limit {a.limit}; every engine would truncate")
            failed = True
            continue
        for e in engines:
            q = render(e, app, op, t)
            t0 = time.monotonic()
            try:
                n = count_rows(fetch(e[1], q, start, end, a.limit), e[3])
                err = None
            except Exception as ex:  # an error body is a failed gate, not a crash
                n, err = None, str(ex)[:200]
            ms = (time.monotonic() - t0) * 1000
            ok = n == expected and n != 0
            failed |= not ok
            status = "ok" if ok else ("ERROR " + err if err else "MISMATCH")
            print(f"  {e[0]:<30} {'' if n is None else f'{n:,}':>10}  {status}  ({ms:.0f} ms, one unwarmed sample)")
    print("\nGATE " + ("FAILED" if failed else "PASSED"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
