#!/usr/bin/env bash
# The final evaluation, end to end: every engine, both datasets, both segment layouts, the
# in-process experiments, written to one results directory with the environment it ran in.
#
#   ./scripts/final-run.sh                       # all phases
#   PHASES="sweep-live sweep-sealed" ./scripts/final-run.sh results/<existing-run>
#
# Engines, all on one machine:
#   logd (this checkout)       :7100
#   logd v1 (thesis-eval-1)    :7101   docker-compose.v1.yml, build with scripts/build-v1.sh
#   Loki 3.7.7                 :3100
#   VictoriaLogs v1.52.0       :9428
#   PostgreSQL 17 via pgshim   :7300   two variants: generated column + B-tree, and
#                                      extraction in the WHERE clause
#
# Phases, in order (PHASES selects a subset, in this order):
#   env           host, kernel, CPU governor, docker, image digests, git commit, load
#   ingest        RUNS fresh-volume runs. The app corpus is pushed closed-loop to logd, logd
#                 v1, Loki and VictoriaLogs, and loaded into PostgreSQL with COPY. Records accept
#                 time, lines/s and cgroup memory.peak per engine. The last run's stack stays up.
#   sweep-live    warm latency sweep on the layout as loaded: logd's last segment is still
#                 active. Thresholds 200/1000/3000/5000, interleaved, every result set gated.
#   disk          volume sizes after flushing every engine (Loki /flush, VictoriaLogs
#                 force_flush, logd restart = seal)
#   sweep-sealed  the same sweep once logd's active segment is sealed
#   cold          COLD_ROUNDS rounds of "restart every engine, then one sample each"
#   hdfs          Loghub HDFS replay (hdfs-run.sh, sealed), then a sweep over its four thresholds
#   inproc        the in-process experiments: RQ3 template sweep, cost-guard sweep, fault
#                 matrix, kill -9 loss (E-8) and the power-cut approximation
#
# The sweeps take SAMPLES interleaved samples per engine per threshold after 5 warm-ups.
# Close every other workload first. The script records the load average at the start and end
# of each phase, so a noisy run is visible in its own output.
set -euo pipefail
cd "$(dirname "$0")/.."

R="${1:-results/final-$(date +%Y%m%d-%H%M%S)}"
PHASES="${PHASES:-env ingest sweep-live disk sweep-sealed cold hdfs inproc}"
RUNS="${RUNS:-5}"
SAMPLES="${SAMPLES:-60}"
HDFS_SAMPLES="${HDFS_SAMPLES:-20}"
COLD_ROUNDS="${COLD_ROUNDS:-5}"
CORPUS="${CORPUS:-corpus/app-500000.jsonl}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.v1.yml)
ENGINES=(tb-logd tb-logd-v1 tb-loki tb-victorialogs tb-postgres tb-pgshim)
mkdir -p "$R"
R=$(cd "$R" && pwd) # absolute: several phases cd elsewhere and pass paths into tests

has() { [[ " $PHASES " == *" $1 "* ]]; }
pause() { timeout "$1" tail -f /dev/null || true; }
note() { echo "== $(date +%H:%M:%S) $* (load $(cut -d' ' -f1-3 /proc/loadavg))" | tee -a "$R/phases.log"; }

wait_ready() {
  for _ in $(seq 1 180); do
    if curl -sf localhost:7100/ready >/dev/null && curl -sf localhost:7101/ready >/dev/null \
       && curl -sf localhost:3100/ready >/dev/null && curl -sf localhost:9428/health >/dev/null \
       && curl -sf localhost:7300/ready >/dev/null; then
      return 0
    fi
    pause 1
  done
  echo "backends not ready after 180s" >&2
  "${COMPOSE[@]}" ps >&2
  exit 1
}

peak_mb() { # container -> cgroup memory.peak in MB (0 if unavailable)
  local id base
  id=$(docker inspect --format '{{.Id}}' "$1")
  for base in /sys/fs/cgroup/system.slice/docker-$id.scope /sys/fs/cgroup/docker/$id; do
    if [[ -f $base/memory.peak ]]; then echo $(( $(cat "$base/memory.peak") / 1048576 )); return; fi
  done
  echo 0
}
current_mb() {
  local id base
  id=$(docker inspect --format '{{.Id}}' "$1")
  for base in /sys/fs/cgroup/system.slice/docker-$id.scope /sys/fs/cgroup/docker/$id; do
    if [[ -f $base/memory.current ]]; then echo $(( $(cat "$base/memory.current") / 1048576 )); return; fi
  done
  echo 0
}
memory_snapshot() { # label
  for c in "${ENGINES[@]}"; do
    echo "$1,$c,$(peak_mb "$c"),$(current_mb "$c")"
  done >> "$R/memory.csv"
}

push_app_corpus() { # out-prefix
  go run ./cmd/loggen -corpus -count=500000 -seed=1001 -base-ts=auto -progress=0 \
    -manifest="$CORPUS" \
    -push=http://localhost:7100/loki/api/v1/push \
    -push=http://localhost:7101/loki/api/v1/push \
    -push=http://localhost:3100/loki/api/v1/push \
    -push='http://localhost:9428/insert/loki/api/v1/push?_stream_fields=app,env,region' \
    -label app=checkout -label env=prod -label region=eu \
    > "$1.json" 2> "$1.log"
}

sweep() { # layout-label dataset samples thresholds...
  local layout="$1" dataset="$2" n="$3"; shift 3
  local out="$R/sweep-$dataset-$layout.txt"
  {
    echo "layout: $layout   dataset: $dataset   samples per cell: $n (interleaved), 5 warm-ups"
    echo "load at start: $(cat /proc/loadavg)"
    for url in http://localhost:7100 http://localhost:7101; do
      echo "explain $url:"
      if [[ $dataset == app ]]; then q="{app=\"$APP\"} | latency_ms > 5000"; else q="{app=\"hdfs\"} | size_bytes < 90000"; fi
      curl -sS -G "$url/logd/api/v1/explain" --data-urlencode "query=$q" \
        --data-urlencode "start=$START" --data-urlencode "end=$END" | python3 scripts/explain.py || true
    done
  } | tee "$out"
  for t in "$@"; do
    echo "=========== threshold $t" | tee -a "$out"
    DATASET=$dataset THRESHOLD=$t RUNS=$n WARMUP=5 WITH_PG=1 LOGD_V1_URL=http://localhost:7101 \
      WITH_LOGQL_STAGES=$([[ $dataset == app ]] && echo 1 || echo "") \
      RESULTS_DIR="$R/samples-$dataset-$layout" ./scripts/bench-vs.sh | tee -a "$out"
  done
  echo "load at end: $(cat /proc/loadavg)" | tee -a "$out"
  memory_snapshot "sweep-$dataset-$layout"
}

seal_logd() {
  "${COMPOSE[@]}" restart logd logd-v1 >/dev/null
  wait_ready
  pause 5 # let the background compression of the just-sealed segment finish
}

flush_others() {
  curl -sS -X POST localhost:3100/flush >/dev/null || true
  curl -sS localhost:9428/internal/force_flush >/dev/null || true
  pause 10
}

volume_bytes() { # volume -> bytes
  docker run --rm -v "$1":/v:ro alpine:3.20 du -sb /v 2>/dev/null | cut -f1
}

# ---------------------------------------------------------------------------------------
if has env; then
  note "env"
  {
    echo "date: $(date -Is)"
    echo "git: $(git rev-parse HEAD) ($(git describe --always --dirty))"
    echo "host: $(uname -a)"
    grep -m1 'model name' /proc/cpuinfo
    echo "cpus: $(nproc)   memory: $(free -g | awk '/Mem:/{print $2}') GB"
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
    echo "disk: $(df -T . | tail -1)"
    docker version --format 'docker: {{.Server.Version}}'
    go version
    echo "load: $(cat /proc/loadavg)"
    for img in logd:testbed logd:thesis-eval-1 grafana/loki:3.7.7 victoriametrics/victoria-logs:v1.52.0 \
               postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3 pgshim:testbed; do
      echo "image $img $(docker image inspect --format '{{.Id}}' "$img" 2>/dev/null || echo missing)"
    done
    echo "running containers:"; docker ps --format '  {{.Names}} {{.Image}}'
  } | tee "$R/env.txt"
fi

if has ingest; then
  note "ingest ($RUNS runs on fresh volumes)"
  echo "run,engine,accept_sec,lines_per_s,requests,retries,rate_limited,peak_mb" > "$R/ingest.csv"
  for run in $(seq 1 "$RUNS"); do
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${COMPOSE[@]}" up -d --build logd logd-v1 loki victorialogs postgres pgshim >"$R/compose-up.log" 2>&1
    wait_ready
    push_app_corpus "$R/load-run$run"
    python3 - "$R" "$run" <<'PY'
import json, subprocess, sys
R, run = sys.argv[1], sys.argv[2]
rep = json.load(open(f"{R}/load-run{run}.json"))
names = {":7100": ("logd", "tb-logd"), ":7101": ("logd-v1", "tb-logd-v1"),
         ":3100": ("loki", "tb-loki"), ":9428": ("victorialogs", "tb-victorialogs")}
def peak(container):
    cid = subprocess.run(["docker", "inspect", "--format", "{{.Id}}", container], capture_output=True, text=True).stdout.strip()
    for base in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope", f"/sys/fs/cgroup/docker/{cid}"):
        try:
            return int(open(f"{base}/memory.peak").read()) // 1048576
        except OSError:
            pass
    return 0
with open(f"{R}/ingest.csv", "a") as f:
    for url, st in sorted(rep["endpoints"].items()):
        engine, container = next(v for k, v in names.items() if k in url)
        lps = st["entries"] / st["elapsed_sec"]
        f.write(f"{run},{engine},{st['elapsed_sec']:.3f},{lps:.0f},{st['requests']},{st['retries']},{st['rate_limited']},{peak(container)}\n")
        print(f"run {run} {engine:13} {st['elapsed_sec']:6.2f}s {lps:8.0f} lines/s retries={st['retries']} peak={peak(container)}MB")
PY
    JSON="$R/pgload-run$run.json" ./scripts/pg-load.sh "$CORPUS" app > "$R/pgload-run$run.txt" 2>&1
    python3 - "$R/pgload-run$run.json" "$run" "$R/ingest.csv" <<'PY'
import json, sys
rows, run, out = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
with open(out, "a") as f:
    for v in rows:
        name, secs, lps = v["variant"], v["load_sec"], v["lines_per_s"]
        f.write(f"{run},postgres-{name},{secs:.3f},{lps:.0f},,,,\n")
        print(f"run {run} postgres-{name:12} {secs:6.2f}s {lps:8.0f} lines/s (COPY) total={v['total_bytes']} index={v['index_bytes']}")
PY
    memory_snapshot "ingest-run$run"
  done
  note "ingest done"
fi

eval "$(./scripts/window.sh "$CORPUS" env)"
export START END APP

if has sweep-live; then
  note "sweep, layout as loaded (logd's last segment active)"
  pause 5
  sweep live app "$SAMPLES" 200 1000 3000 5000
fi

if has disk; then
  note "disk after flushing every engine"
  flush_others
  seal_logd
  {
    echo "volume,bytes"
    for v in logd-data logd-v1-data loki-data vlogs-data pg-data; do
      echo "$v,$(volume_bytes "logd-testbed_$v")"
    done
  } | tee "$R/disk.csv"
  docker run --rm -v logd-testbed_logd-data:/v:ro alpine:3.20 sh -c 'cd /v && find . -type f | sed "s/.*\.//" | sort | uniq -c; du -ab . | sort -n | tail -20' > "$R/disk-logd-composition.txt"
  docker exec tb-postgres psql -U logd -d logs -Atc "select relname, pg_total_relation_size(c.oid), pg_indexes_size(c.oid) from pg_class c where relkind='r' and relname like 'logs%'" > "$R/disk-postgres.txt" 2>&1 || true
  memory_snapshot "at-rest"
fi

if has sweep-sealed; then
  note "sweep, all logd segments sealed"
  has disk || seal_logd
  sweep sealed app "$SAMPLES" 200 1000 3000 5000
fi

if has cold; then
  note "cold process caches ($COLD_ROUNDS rounds)"
  echo "round,threshold,engine,ms" > "$R/cold.csv"
  for r in $(seq 1 "$COLD_ROUNDS"); do
    "${COMPOSE[@]}" restart logd logd-v1 loki victorialogs postgres pgshim >/dev/null
    wait_ready
    for t in 200 5000; do
      # Engines are listed as parallel arrays: the names and queries are full of '|', so no
      # single-character separator inside one string is safe.
      names=("logd" "logd v1" "Loki (| pattern)" "VictoriaLogs" "PostgreSQL (B-tree)")
      urls=(http://localhost:7100/loki/api/v1/query_range http://localhost:7101/loki/api/v1/query_range
            http://localhost:3100/loki/api/v1/query_range http://localhost:9428/select/logsql/query
            "http://localhost:7300/loki/api/v1/query_range?variant=structured")
      queries=("{app=\"$APP\"} | latency_ms > $t" "{app=\"$APP\"} | latency_ms > $t"
               "{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > $t"
               "_stream:{app=\"$APP\"} \"took=\" | extract \"took=<latency_ms>ms\" | latency_ms:>$t"
               "latency_ms > $t")
      for i in "${!names[@]}"; do
        name="${names[$i]}"; url="${urls[$i]}"; q="${queries[$i]}"
        t0=$(date +%s%N)
        code=$(curl -sS -G "$url" --data-urlencode "query=$q" --data-urlencode "start=$START" \
          --data-urlencode "end=$END" --data-urlencode "limit=200000" -o /dev/null -w '%{http_code}')
        t1=$(date +%s%N)
        [[ "$code" == 200 ]] || { echo "ABORT: cold round $r, $name answered $code" >&2; exit 1; }
        printf "%d,%s,%s,%.1f\n" "$r" "$t" "$name" "$(echo "scale=2; ($t1-$t0)/1000000" | bc)" | tee -a "$R/cold.csv"
      done
    done
  done
fi

if has hdfs; then
  note "HDFS replay (fresh volumes, sealed)"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  SEAL=1 RESULTS_DIR="$R/hdfs" ./scripts/hdfs-run.sh
  # hdfs-run.sh brings up the default compose project without the v1 overlay; the HDFS sweep
  # compares the current logd with the other engines, and v1 with them on the app corpus only.
  HDFS_MANIFEST="${MANIFEST:-$HOME/.cache/logd-bench/hdfs-2000000.jsonl}"
  eval "$(./scripts/window.sh "$HDFS_MANIFEST" env)"
  export START END
  for t in 67108864 28490000 3535000 90000; do
    echo "=========== size_bytes < $t" | tee -a "$R/sweep-hdfs-sealed.txt"
    DATASET=hdfs THRESHOLD=$t RUNS="$HDFS_SAMPLES" WARMUP=3 WITH_PG=1 \
      RESULTS_DIR="$R/samples-hdfs-sealed" ./scripts/bench-vs.sh | tee -a "$R/sweep-hdfs-sealed.txt"
  done
  for c in tb-logd tb-loki tb-victorialogs tb-postgres; do echo "hdfs,$c,$(peak_mb "$c"),$(current_mb "$c")"; done >> "$R/memory.csv"
  {
    echo "volume,bytes"
    for v in logd-data loki-data vlogs-data pg-data; do echo "$v,$(volume_bytes "logd-testbed_$v")"; done
  } > "$R/hdfs/disk.csv"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi

if has inproc; then
  note "in-process experiments (stack down)"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  export LOGD_BENCH_DIR="${LOGD_BENCH_DIR:-$HOME/.cache/logd-bench}"
  (COUNT=5 LINES=1000000 ./scripts/rq3-templates.sh "$R/rq3") 2>&1 | tail -40 | tee "$R/rq3.log"
  (cd .. && LOGD_SWEEP=1 LOGD_BENCH_DIR="$LOGD_BENCH_DIR" go test ./core/query/ -run TestCostGuardSelectivitySweep -v -count=1 -timeout 60m) > "$R/costguard-sweep.txt" 2>&1 || true
  (cd .. && LOGD_FAULTMATRIX=1 LOGD_FAULTMATRIX_OUT="$R/faultmatrix.md" go test ./core/query/ -run TestFaultMatrix -v -count=1) > "$R/faultmatrix.log" 2>&1 || true
  (RATES="10000 200" ./scripts/e8-crashloss.sh "$R/e8") > "$R/e8.log" 2>&1 || true
  (cd .. && LOGD_MACHINECRASH=1 go test ./core/storage/ -run TestMachineCrashLossApproximation -v -count=1) > "$R/e8-powercut.txt" 2>&1 || true
fi

note "done: $R"
