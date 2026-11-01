#!/usr/bin/env bash
# Cold-PROCESS-cache condition: restart every engine so its in-process caches start empty, then
# take one sample each. The OS page cache stays warm (dropping it needs root).
#   ./scripts/protocol-cold.sh results/<run-id> <threshold> <rounds>
# Cold-PROCESS-cache condition, no privileges needed: each round restarts all three engines so
# their in-process caches start empty (logd's index-reader cache, Loki's and VictoriaLogs' own),
# then takes ONE sample per engine. The OS page cache is left warm — dropping it needs root — so
# this measures "first query after a restart", not a cold disk.
set -euo pipefail
R="$1"; T="${2:-200}"; ROUNDS="${3:-5}"
mkdir -p "$R"
cd "$(dirname "$0")/.."
eval "$(./scripts/window.sh corpus/app-500000.jsonl env)"
out="$R/cold-t$T.csv"; echo "round,engine,ms" > "$out"
q_logd="{app=\"$APP\"} | latency_ms > $T"
q_loki="{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > $T"
q_vl="_stream:{app=\"$APP\"} \"took=\" | extract \"took=<latency_ms>ms\" | latency_ms:>$T"
for r in $(seq 1 "$ROUNDS"); do
  docker compose restart logd loki victorialogs >/dev/null 2>&1
  for i in $(seq 1 60); do
    curl -sf localhost:7100/ready >/dev/null 2>&1 && curl -sf localhost:3100/ready >/dev/null 2>&1 \
      && curl -sf localhost:9428/health >/dev/null 2>&1 && break
    timeout 1 tail -f /dev/null || true
  done
  sample() { # engine url query
    local t0 t1 code
    t0=$(date +%s%N)
    curl -sS -G "$2" --data-urlencode "query=$3" --data-urlencode "start=$START" \
      --data-urlencode "end=$END" --data-urlencode "limit=200000" -o /dev/null -w '%{http_code}' > "$R/.status"
    t1=$(date +%s%N)
    # The body is discarded, so an error answer would otherwise be recorded as a fast sample.
    code=$(<"$R/.status")
    if [[ "$code" != "200" ]]; then
      echo "ABORT: round $r, $1 answered HTTP $code. Refusing to record it as a sample." >&2
      exit 1
    fi
    printf "%d,%s,%.1f\n" "$r" "$1" "$(echo "scale=2; ($t1-$t0)/1000000" | bc)" | tee -a "$out"
  }
  sample "logd (index)"       "http://localhost:7100/loki/api/v1/query_range" "$q_logd"
  sample "logd (forced scan)" "http://localhost:7100/logd/api/v1/query_scan"  "$q_logd"
  sample "Loki (| pattern)"   "http://localhost:3100/loki/api/v1/query_range" "$q_loki"
  sample "VictoriaLogs"       "http://localhost:9428/select/logsql/query"     "$q_vl"
done
