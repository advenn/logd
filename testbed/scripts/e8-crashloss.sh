#!/usr/bin/env bash
# E-8: how many ACKNOWLEDGED lines a process crash loses, and how much time they cover.
#
#   ./scripts/e8-crashloss.sh [results-dir]       (default results/<date>-e8)
#
# Per run: build and start the logd daemon locally (not in docker) on a free port, with a
# fresh data directory under $LOGD_BENCH_DIR; push sequence-numbered lines at a fixed rate
# in batches, one request at a time (cmd/crashpush), recording which batches got HTTP 204;
# kill -9 the daemon at a random moment; restart it on the same data; query every line back;
# count the acknowledged sequence numbers that are missing and the time span they cover.
# Every setting is repeated N times.
#
# What this measures: PROCESS-crash loss. The kernel survives a kill -9, so every page logd
# had written is still in the page cache and reaches disk; what is lost is what logd still
# held in RAM — the queue (up to 4,096 records per shard) and the page being filled, which
# is written when it fills or on the flush tick (flush_interval_ms).
#
# What it does NOT measure: MACHINE-crash loss (power cut, kernel panic), which also loses
# pages written but not yet fsynced: up to sync_interval_ms plus one flush tick of data on
# top of the above. Measuring that needs a VM that can be reset (echo b > /proc/sysrq-trigger
# inside it) or a block device that drops unsynced writes (dm-flakey / dm-log-writes);
# neither is available here, and a kill -9 cannot approximate it, because the page cache
# outlives the process. Do not read these numbers as the machine-crash window.
# TestMachineCrashLossApproximation (core/storage/machinecrash_test.go) approximates it in
# process instead, by cutting a copy of the shard at its last fsync and recovering it:
#   LOGD_MACHINECRASH=1 go test ./core/storage/ -run TestMachineCrashLossApproximation -v
#
# Knobs (environment):
#   N=10                       runs per setting
#   SETTINGS="50/500 10/50"    sync_interval_ms/flush_interval_ms pairs (default, aggressive)
#   RATES="10000"              lines/s; add a low rate (e.g. 200) to see flush_interval_ms
#                              bound the loss when pages no longer fill between ticks
#   BATCH=auto                 lines per push request; auto = rate/100, so every rate sends
#                              100 requests/s and a batch never outlasts a flush tick
#   KILL_MIN=2 KILL_MAX=6      the kill lands uniformly in [KILL_MIN, KILL_MAX) s after the
#                              first push
#   SEED=8                     seeds the kill times, so a rerun kills at the same moments
#   LOGD_BENCH_DIR             default $HOME/.cache/logd-bench (a real disk)
set -euo pipefail
cd "$(dirname "$0")/.."   # testbed/
ROOT=$(cd .. && pwd)
R=${1:-results/$(date +%F)-e8}
N=${N:-10}
SETTINGS=${SETTINGS:-"50/500 10/50"}
RATES=${RATES:-10000}
BATCH=${BATCH:-auto}
KILL_MIN=${KILL_MIN:-2}
KILL_MAX=${KILL_MAX:-6}
SEED=${SEED:-8}
BENCH=${LOGD_BENCH_DIR:-$HOME/.cache/logd-bench}
mkdir -p "$R" "$BENCH"
R=$(cd "$R" && pwd)
WORK=$(mktemp -d "$BENCH/e8.XXXXXX")
PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then kill -9 "$PID" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

(cd "$ROOT" && go build -o "$WORK/logd" ./cmd/logd)
go build -o "$WORK/crashpush" ./cmd/crashpush
{
  echo "date: $(date -Is)"
  echo "commit: $(git -C "$ROOT" rev-parse HEAD)$([[ -z $(git -C "$ROOT" status --porcelain) ]] || echo " (uncommitted changes)")"
  echo "go: $(go version)"
  echo "uname: $(uname -a)"
  echo "loadavg (start): $(cat /proc/loadavg)"
  echo "data dir: $BENCH ($(df --output=fstype "$BENCH" | tail -1 | tr -d ' '))"
  echo "N=$N SETTINGS=$SETTINGS RATES=$RATES BATCH=$BATCH KILL=[$KILL_MIN,$KILL_MAX)s SEED=$SEED"
} > "$R/env.txt"
cat "$R/env.txt"

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'; }
now_ns() { date +%s%N; }

# start_logd <config> <log>: start the daemon and wait for /ready. Sets PID, and READY_MS to
# how long that took (on a restart, the crash recovery of the active segment).
start_logd() {
  local t0
  t0=$(now_ns)
  "$WORK/logd" -config "$1" >>"$2" 2>&1 &
  PID=$!
  for _ in $(seq 1 400); do
    if curl -sf "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1; then
      READY_MS=$(( ($(now_ns) - t0) / 1000000 ))
      return 0
    fi
    kill -0 "$PID" 2>/dev/null || { echo "logd exited during start; see $2" >&2; return 1; }
    sleep 0.025
  done
  echo "logd did not become ready; see $2" >&2
  return 1
}

RANDOM=$SEED
echo "setting,sync_ms,flush_ms,rate,run,kill_after_ms,acked,found,lost_acked,lost_tail,lost_span_ms,lost_ack_span_ms,oldest_lost_age_ms,unacked_found,duplicates,restart_ready_ms" > "$R/runs.csv"
for rate in $RATES; do
  for setting in $SETTINGS; do
    sync=${setting%/*}; flush=${setting#*/}
    for run in $(seq 1 "$N"); do
      id="s${sync}-f${flush}-r${rate}-${run}"
      dir="$WORK/$id"
      mkdir -p "$dir"
      PORT=$(free_port)
      cat > "$dir/config.yaml" <<EOF
port: $PORT
data_dir: $dir/data
segment_size_mb: 64
flush_interval_ms: $flush
sync_interval_ms: $sync
compress_block_pages: 8
retention_days: 0
index:
  templates:
    - name: seq
      pattern: "seq={n:int}"
labels:
  - job
  - run
EOF
      start_logd "$dir/config.yaml" "$R/$id.logd.log"
      kill_ms=$(( KILL_MIN * 1000 + RANDOM % ((KILL_MAX - KILL_MIN) * 1000) ))
      batch=$BATCH; [[ "$batch" == auto ]] && batch=$(( rate / 100 > 0 ? rate / 100 : 1 ))
      "$WORK/crashpush" push -url "http://127.0.0.1:$PORT" -run "$id" -rate "$rate" -batch "$batch" \
        -duration "$((KILL_MAX + 30))s" -out "$R/$id.push.json" 2>>"$R/$id.push.log" &
      pusher=$!
      sleep "$(awk -v m="$kill_ms" 'BEGIN { printf "%.3f", m / 1000 }')"
      kill_ns=$(now_ns)
      kill -9 "$PID"
      wait "$PID" 2>/dev/null || true
      wait "$pusher"
      echo "--- restart after kill -9 ---" >> "$R/$id.logd.log"
      start_logd "$dir/config.yaml" "$R/$id.logd.log"
      "$WORK/crashpush" verify -url "http://127.0.0.1:$PORT" -push "$R/$id.push.json" -kill-ns "$kill_ns" -out "$R/$id.verify.json"
      kill -TERM "$PID"; wait "$PID" 2>/dev/null || true; PID=""
      python3 - "$R/$id.verify.json" "$setting" "$sync" "$flush" "$rate" "$run" "$READY_MS" >> "$R/runs.csv" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))
setting, sync, flush, rate, run, ready = sys.argv[2:]
print(",".join(str(x) for x in (setting, sync, flush, rate, run, f"{v['KillAfterMS']:.0f}", v["Acked"], v["Found"],
      v["LostAcked"], int(v["LostTail"]), f"{v['LostSpanMS']:.1f}", f"{v['LostAckSpanMS']:.1f}",
      f"{v['OldestLostAgeMS']:.1f}", v["UnackedFound"], v["Duplicates"], ready)))
PY
      tail -1 "$R/runs.csv"
      rm -rf "$dir"
    done
  done
done
echo "loadavg (end): $(cat /proc/loadavg)" >> "$R/env.txt"
python3 "$ROOT/testbed/scripts/e8-summarise.py" "$R/runs.csv" | tee "$R/summary.md"
echo "raw results in $R"
