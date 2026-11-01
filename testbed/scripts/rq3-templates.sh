#!/usr/bin/env bash
# E-4 / RQ3: what typed extraction and indexing cost at ingest, as the template count grows.
#
#   ./scripts/rq3-templates.sh [results-dir]       (default results/<date>-rq3)
#
# Runs the in-process benchmarks in core/ingest/bench_templates_test.go and keeps their raw
# output, then summarises it (median and min–max per configuration):
#
#   extract.txt    Engine.Extract per line, nothing else
#   ingest.txt     the writer alone (pre-extracted keys) and the full native ingest path,
#                  each to a real writer on a real disk, timed until the last seal
#   indexram.txt   the active segment's in-RAM index for one 64 MB segment (RQ4)
#   summary.md     the tables, from scripts/rq3-summarise.py
#
# Configurations: 0, 1, 5 and 20 templates, plus 5 and 20 whose anchors never occur, at
# match rates of ~80% and ~5% of lines. See the benchmark file for the line shapes.
#
# Knobs (environment):
#   COUNT=5             passes over every configuration (interleaved, see below)
#   LINES=1000000       lines per Write/Ingest run (fixed, so every configuration rotates
#                       through the same number of 64 MB segments)
#   EXTRACT_TIME=2s     -benchtime for the extraction benchmark
#   LOGD_BENCH_DIR      where the writer's segments go; default $HOME/.cache/logd-bench.
#                       Must be a real disk: on tmpfs fsync is free and the result is
#                       meaningless, so the script refuses tmpfs.
#
# Quiesce the machine first (load average < 1) and say in the write-up if it was not.
set -euo pipefail
cd "$(dirname "$0")/.."   # testbed/
ROOT=$(cd .. && pwd)
R=${1:-results/$(date +%F)-rq3}
COUNT=${COUNT:-5}
LINES=${LINES:-1000000}
EXTRACT_TIME=${EXTRACT_TIME:-2s}
BENCH=${LOGD_BENCH_DIR:-$HOME/.cache/logd-bench}
mkdir -p "$R" "$BENCH"
R=$(cd "$R" && pwd)

fstype=$(df --output=fstype "$BENCH" | tail -1 | tr -d " ")
if [[ "$fstype" == tmpfs || "$fstype" == ramfs ]]; then
  echo "LOGD_BENCH_DIR=$BENCH is on $fstype; point it at a real disk" >&2
  exit 2
fi
RUNDIR=$(mktemp -d "$BENCH/rq3.XXXXXX")
trap 'rm -rf "$RUNDIR"' EXIT

{
  echo "date: $(date -Is)"
  echo "commit: $(git -C "$ROOT" rev-parse HEAD)$([[ -z $(git -C "$ROOT" status --porcelain) ]] || echo " (uncommitted changes)")"
  echo "go: $(go version)"
  echo "uname: $(uname -a)"
  echo "loadavg (start): $(cat /proc/loadavg)"
  echo "bench dir: $BENCH ($fstype)"
  echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "COUNT=$COUNT LINES=$LINES EXTRACT_TIME=$EXTRACT_TIME"
  lscpu | grep -E '^(Model name|CPU\(s\)|Thread|Core)' || true
  free -g | head -2
  df -T "$BENCH" | tail -1
} > "$R/env.txt"
cat "$R/env.txt"

cd "$ROOT"
go test -c -o "$RUNDIR/ingest.test" ./core/ingest/   # build once, so every pass runs the same binary
# One pass runs every configuration once; passes repeat COUNT times. -count would instead run
# each configuration COUNT times back to back, so drift over the run (thermal state, a
# neighbouring process) would land on whichever configurations happened to run then.
: > "$R/extract.txt"; : > "$R/ingest.txt"
for pass in $(seq "$COUNT"); do
  echo "pass $pass/$COUNT, loadavg $(cut -d' ' -f1-3 /proc/loadavg)" >&2
  "$RUNDIR/ingest.test" -test.run '^$' -test.bench '^BenchmarkTemplateExtract$' \
    -test.benchtime "$EXTRACT_TIME" -test.timeout 0 | tee -a "$R/extract.txt"
  LOGD_BENCH_DIR=$RUNDIR "$RUNDIR/ingest.test" -test.run '^$' -test.bench '^BenchmarkTemplate(Write|Ingest)$' \
    -test.benchtime "${LINES}x" -test.timeout 0 | tee -a "$R/ingest.txt"
done
"$RUNDIR/ingest.test" -test.run '^$' -test.bench '^BenchmarkTemplateIndexRAM$' \
  -test.benchtime 1x -test.timeout 0 | tee "$R/indexram.txt"
echo "loadavg (end): $(cat /proc/loadavg)" >> "$R/env.txt"

python3 "$ROOT/testbed/scripts/rq3-summarise.py" "$R" | tee "$R/summary.md"
echo "raw output and summary in $R"
