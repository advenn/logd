#!/usr/bin/env bash
# Loghub HDFS replay, end to end (experiment E-2): fresh volumes, the first COUNT lines of
# HDFS.log pushed to logd, Loki and VictoriaLogs, the same manifest loaded into PostgreSQL, a
# correctness gate per threshold against the manifest's ground truth, and the exact query
# each engine ran.
#
#   ./scripts/hdfs-run.sh
#   TEARDOWN=1 ./scripts/hdfs-run.sh      # and `docker compose down -v` afterwards
#
# It does NOT time anything beyond the ingest report. The stack is left up (unless TEARDOWN=1)
# so the latency sweep can run on it next, on a quiet machine. Loki only answers from its
# ingesters for data within query_ingesters_within (3h), so run the sweep soon after.
#
# Environment:
#   DATASET     HDFS.log (Loghub HDFS_v1), default ~/.cache/logd-datasets/HDFS.log
#   COUNT       lines to replay from the start of the file (default 2000000)
#   MANIFEST    ground-truth manifest, default ~/.cache/logd-bench/hdfs-$COUNT.jsonl. ~530 MB at
#               2M lines, which is why the default is not corpus/ under a /tmp worktree:
#               /tmp is often a small tmpfs.
#   THRESHOLDS  comma-separated size_bytes thresholds for `size_bytes < T`, default per gate.py
#   RESULTS_DIR where the reports go (default results/hdfs-<timestamp>)
#   SEAL=1      restart logd after the load, which seals its active segment, before the gate
#   TEARDOWN=1  docker compose down -v at the end
#
# DESTRUCTIVE: `docker compose down -v` at the start removes this compose project's volumes,
# including a previously loaded synthetic corpus. Every ingest and disk figure needs fresh
# stores; set COMPOSE_PROJECT_NAME to keep another project's volumes.

set -euo pipefail
cd "$(dirname "$0")/.."

DATASET="${DATASET:-$HOME/.cache/logd-datasets/HDFS.log}"
COUNT="${COUNT:-2000000}"
MANIFEST="${MANIFEST:-$HOME/.cache/logd-bench/hdfs-$COUNT.jsonl}"
R="${RESULTS_DIR:-results/hdfs-$(date +%Y%m%d-%H%M%S)}"
# Must stay in step with gate.py's PostgreSQL dataset and with loggen's ground truth: the
# same regular expression is the manifest's value, PostgreSQL's generated column and the
# Loki/VictoriaLogs extraction.
VALUE_REGEX='size ([0-9]+)'

if [[ ! -f "$DATASET" ]]; then
  echo "no dataset at $DATASET — fetch Loghub HDFS_v1 (HDFS.log) first" >&2
  exit 1
fi
mkdir -p "$R" "$(dirname "$MANIFEST")"
export LOGD_CONFIG=./stack/logd/logd-hdfs.yaml

pause() { timeout "$1" tail -f /dev/null || true; }
wait_ready() {
  for _ in $(seq 1 120); do
    if curl -sf localhost:7100/ready >/dev/null && curl -sf localhost:3100/ready >/dev/null \
       && curl -sf localhost:9428/health >/dev/null && curl -sf localhost:7300/ready >/dev/null; then
      return 0
    fi
    pause 1
  done
  echo "backends not ready after 120s" >&2
  docker compose ps >&2
  exit 1
}

echo "== fresh stack (LOGD_CONFIG=$LOGD_CONFIG)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build logd loki victorialogs postgres pgshim >"$R/compose-up.log" 2>&1
wait_ready
{
  echo "git: $(git rev-parse HEAD)"
  echo "dataset: $DATASET ($(stat -c %s "$DATASET") bytes), first $COUNT lines"
  echo "load at start: $(cat /proc/loadavg)"
  for c in tb-logd tb-loki tb-victorialogs tb-postgres tb-pgshim; do
    echo "$c $(docker inspect --format '{{.Config.Image}} {{.Image}}' "$c")"
  done
} | tee "$R/env.txt"

echo "== replay $COUNT lines to logd, Loki and VictoriaLogs (closed loop, 1000 per request)"
# loggen directly, not through make, so stdout is pure JSON. _stream_fields mirrors the labels
# every engine sees (app + component); without it VictoriaLogs derives stream identity
# differently from the others.
go run ./cmd/loggen -replay="$DATASET" -replay-count="$COUNT" \
  -replay-value-regex="$VALUE_REGEX" -replay-value-name=size_bytes \
  -base-ts=auto -progress=0 -label app=hdfs -manifest="$MANIFEST" \
  -push=http://localhost:7100/loki/api/v1/push \
  -push=http://localhost:3100/loki/api/v1/push \
  -push='http://localhost:9428/insert/loki/api/v1/push?_stream_fields=app,component' \
  >"$R/push.json" 2>"$R/push.log"
python3 - "$R/push.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
rp = r["replay"]
print(f"  {r['lines']:,} lines, {rp['with_value']:,} carry {rp['value_name']}, "
      f"{rp['multi_value']} with more than one occurrence, components {rp['components']}")
for url, st in sorted(r["endpoints"].items()):
    print(f"  {url.split('/')[2]:<16} {st['elapsed_sec']:7.2f} s  {st['entries'] / st['elapsed_sec']:9.0f} lines/s"
          f"  requests={st['requests']} retries={st['retries']} failures={st['failures']}")
if rp["multi_value"]:
    print("  WARNING: lines with several sizes: logd's any-occurrence semantics may differ from the ground truth")
PY

echo "== load the same manifest into PostgreSQL"
go run ./cmd/pgload -manifest "$MANIFEST" -dataset hdfs -json "$R/pgload.json" | tee "$R/pgload.txt"

if [[ "${SEAL:-0}" == "1" ]]; then
  echo "== restart logd (seals the active segment)"
  docker compose restart logd >/dev/null
  wait_ready
fi
# VictoriaLogs makes ingested rows searchable after its in-memory flush (1 s here); logd's
# writer flushes pages every 500 ms. Give both a moment so the gate is not racing them.
pause 3

echo "== correctness gate (ground truth from the manifest)"
gate_args=(--dataset hdfs --manifest "$MANIFEST")
[[ -n "${THRESHOLDS:-}" ]] && gate_args+=(--thresholds "$THRESHOLDS")
set +e
python3 scripts/gate.py "${gate_args[@]}" | tee "$R/gate.txt"
gate=${PIPESTATUS[0]}
set -e

echo "== queries"
python3 scripts/gate.py --dataset hdfs --manifest "$MANIFEST" --print-queries | tee "$R/queries.txt"
eval "$(./scripts/window.sh "$MANIFEST" env)"
echo
echo "window for bench-vs.sh-style runs: START=$START END=$END APP=$APP"
echo "explain (size_bytes < 90000):"
curl -sS -G localhost:7100/logd/api/v1/explain --data-urlencode "query={app=\"$APP\"} | size_bytes < 90000" \
  --data-urlencode "start=$START" --data-urlencode "end=$END" | python3 scripts/explain.py | tee "$R/explain.txt"
echo "reports in $R"

if [[ "${TEARDOWN:-0}" == "1" ]]; then
  echo "== teardown"
  docker compose down -v >/dev/null 2>&1
fi
exit "$gate"
