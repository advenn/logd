#!/usr/bin/env bash
# Ingest measurement: N runs on fresh volumes, identical corpus pushed to all three backends
# closed-loop, recording accept time, lines/s and peak RSS (cgroup memory.peak) per engine.
#   ./scripts/protocol-ingest.sh results/<run-id>
# Phase 1: five ingest runs on fresh volumes. Each run recreates every backend, pushes the same
# 500k-line corpus to all three closed-loop, and records accept time and peak memory per engine.
set -euo pipefail
cd "$(dirname "$0")/.."   # every command below is relative to testbed/
R="$1"
mkdir -p "$R"
echo "run,engine,accept_sec,lines_per_s,requests,retries,rate_limited,peak_mb" > "$R/ingest.csv"
for run in 1 2 3 4 5; do
  docker compose down -v >/dev/null 2>&1
  docker compose up -d >/dev/null 2>&1
  for i in $(seq 1 60); do
    curl -sf localhost:7100/ready >/dev/null && curl -sf localhost:3100/ready >/dev/null && curl -sf localhost:9428/health >/dev/null && break
    timeout 1 tail -f /dev/null || true
  done
  # loggen directly, not through make: make echoes its recipe to stdout and corrupts the JSON.
  go run ./cmd/loggen -corpus -count=500000 -seed=1001 -base-ts=auto -progress=0 \
    -manifest=corpus/app-500000.jsonl \
    -push=http://localhost:7100/loki/api/v1/push \
    -push=http://localhost:3100/loki/api/v1/push \
    -push='http://localhost:9428/insert/loki/api/v1/push?_stream_fields=app,env,region' \
    -label app=checkout -label env=prod -label region=eu \
    > "$R/load-run$run.json" 2>"$R/load-run$run.log"
  python3 - "$R" "$run" <<'PY'
import json, subprocess, sys
R, run = sys.argv[1], sys.argv[2]
rep = json.load(open(f"{R}/load-run{run}.json"))
names = {"7100": ("logd", "tb-logd"), "3100": ("loki", "tb-loki"), "9428": ("victorialogs", "tb-victorialogs")}
rows = []
for url, st in rep["endpoints"].items():
    port = next(p for p in names if p in url)
    engine, container = names[port]
    cid = subprocess.run(["docker", "inspect", "--format", "{{.Id}}", container], capture_output=True, text=True).stdout.strip()
    peak = 0
    for base in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope", f"/sys/fs/cgroup/docker/{cid}"):
        try:
            peak = int(open(f"{base}/memory.peak").read()) // 1048576
            break
        except OSError:
            pass
    lps = st["entries"] / st["elapsed_sec"]
    rows.append((engine, st["elapsed_sec"], lps, st["requests"], st["retries"], st["rate_limited"], peak))
with open(f"{R}/ingest.csv", "a") as f:
    for r in sorted(rows):
        f.write(f"{run},{r[0]},{r[1]:.3f},{r[2]:.0f},{r[3]},{r[4]},{r[5]},{r[6]}\n")
        print(f"run {run} {r[0]:13} {r[1]:6.2f}s {r[2]:7.0f} lines/s retries={r[4]} peak={r[6]}MB")
PY
done
echo "phase 1 done; load: $(cat /proc/loadavg)"
