#!/usr/bin/env bash
# Warm latency sweep for ONE segment layout, over four selectivity levels, with the correctness
# gate and per-cell raw samples. Run it once as loaded and once after `docker compose restart logd`
# (which seals the active segment): the two layouts differ by up to 5x.
#   ./scripts/protocol-sweep.sh results/<run-id> <layout-label> <samples>
# Phases 2-3: warm latency sweep over four selectivity levels, for one segment layout.
# Usage: sweep.sh <results-dir> <layout-label> <runs>
set -euo pipefail
R="$1"; LAYOUT="$2"; N="${3:-60}"
mkdir -p "$R"
cd "$(dirname "$0")/.."
eval "$(./scripts/window.sh corpus/app-500000.jsonl env)"
export START END APP
{
  echo "layout: $LAYOUT"
  echo "samples per cell: $N (warm, interleaved), 5 warm-ups discarded"
  echo "load at start: $(cat /proc/loadavg)"
  echo "explain (latency_ms > 5000):"
  curl -sS -G localhost:7100/logd/api/v1/explain --data-urlencode "query={app=\"$APP\"} | latency_ms > 5000" \
    --data-urlencode "start=$START" --data-urlencode "end=$END" \
  | python3 "$(dirname "$0")/explain.py"
} | tee "$R/sweep-$LAYOUT.txt"
for t in 200 1000 3000 5000; do
  echo "=========== latency_ms > $t" | tee -a "$R/sweep-$LAYOUT.txt"
  THRESHOLD=$t RUNS=$N WARMUP=5 RESULTS_DIR="$R/samples-$LAYOUT" ./scripts/bench-vs.sh | tee -a "$R/sweep-$LAYOUT.txt"
done
echo "load at end: $(cat /proc/loadavg)" | tee -a "$R/sweep-$LAYOUT.txt"
for c in tb-logd tb-loki tb-victorialogs; do
  id=$(docker inspect --format '{{.Id}}' $c)
  for base in /sys/fs/cgroup/system.slice/docker-$id.scope /sys/fs/cgroup/docker/$id; do
    [ -f $base/memory.peak ] && echo "$c memory.peak=$(( $(cat $base/memory.peak)/1048576 ))MB current=$(( $(cat $base/memory.current)/1048576 ))MB" | tee -a "$R/sweep-$LAYOUT.txt" && break
  done
done
