#!/usr/bin/env bash
# Interleaved A/B latency comparison across engines.
#
# Interleaved on purpose: running all of engine A's samples and then all of engine B's
# lets any drift over the run (page-cache warming, another container waking up, thermal
# behaviour) land entirely on one engine and masquerade as a difference between them.
# Alternating one sample at a time means drift hits both roughly equally.
#
# Reports median and the full min-max range. The range is not decoration: on a shared box
# it is usually the difference between a real result and noise, and two engines whose
# ranges overlap have not been shown to differ.

set -euo pipefail

RUNS="${RUNS:-30}"
WARMUP="${WARMUP:-5}"

if [[ -z "${START:-}" || -z "${END:-}" ]]; then
  echo "START/END unset — run: eval \$(./scripts/window.sh corpus/<m>.jsonl env)" >&2
  exit 2
fi

# Fields are joined with ASCII Unit Separator (0x1f), NOT '|'. LogQL queries are full of
# '|' and so are the engine labels ("Loki (| pattern)"), so a '|' delimiter splits in the
# wrong place and silently hands curl a garbage URL.
SEP=$'\x1f'
ENGINES=()
add() { ENGINES+=("$1${SEP}$2${SEP}$3${SEP}$4"); } # name, url, query, response format

APP="${APP:-checkout}"
# The threshold is a variable so the same script can walk a selectivity sweep: on the shipped
# corpus > 200 matches 5.4% of lines and > 5000 matches 0.09%, which is the range where the
# answer changes (an index wins on the selective end, a scan-based store is flat).
T="${THRESHOLD:-200}"
# Each engine carries the response format its row count has to be read from: Loki's JSON for
# logd and Loki, one JSON object per line for VictoriaLogs' LogsQL endpoint.
add "logd (typed index)"  "http://localhost:7100/loki/api/v1/query_range" "{app=\"$APP\"} | latency_ms > $T" loki
add "logd (forced scan)"  "http://localhost:7100/logd/api/v1/query_scan"  "{app=\"$APP\"} | latency_ms > $T" loki
add "Loki (| pattern)"    "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > $T" loki
add "Loki (| regexp)"     "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | regexp \"took=(?P<latency_ms>[0-9]+)ms\" | latency_ms > $T" loki
# VictoriaLogs speaks the Loki PUSH protocol but not the Loki QUERY API, so it is measured on
# its own endpoint in LogsQL. Without this row the comparison quietly omits the engine that the
# earlier measurements found to be the real competitor.
add "VictoriaLogs (extract)" "http://localhost:9428/select/logsql/query" "_stream:{app=\"$APP\"} \"took=\" | extract \"took=<latency_ms>ms\" | latency_ms:>$T" jsonl

hit() { # url query outfile
  curl -sS -G "$1" \
    --data-urlencode "query=$2" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=200000" -o "$3"
}

rows() { # file format
  case "$2" in
    jsonl) grep -c . "$1" || true ;;  # one JSON object per matching record
    *) python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(sum(len(r['values']) for r in d['data']['result']))" "$1" ;;
  esac
}

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
# RESULTS_DIR keeps every raw sample, so percentiles can be recomputed and plots rebuilt from the
# same run instead of re-measuring (the thesis methodology asks for the raw output).
if [[ -n "${RESULTS_DIR:-}" ]]; then mkdir -p "$RESULTS_DIR"; fi
save_samples() { # label file
  [[ -z "${RESULTS_DIR:-}" ]] && return 0
  local out="$RESULTS_DIR/t${T}_$(echo "$1" | tr -c 'a-zA-Z0-9' '_').csv"
  { echo "ms"; cat "$2"; } > "$out"
}

# Correctness gate. A latency number next to a different answer is meaningless, and a
# latency number next to an EMPTY answer is worse: it looks plausible and inverts the
# conclusion. (Observed: a query that silently matched nothing reported Loki at 13ms and
# logd at 26ms, i.e. Loki "winning" purely by returning nothing faster.) Abort on either.
echo "Row counts (must all agree, and be non-zero):"
gate_rows=""
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  hit "$url" "$q" "$tmp/check"
  n=$(rows "$tmp/check" "$fmt")
  printf "  %-22s %s rows\n" "$name" "$n"
  if [[ "$n" == "0" ]]; then
    echo >&2; echo "ABORT: '$name' returned 0 rows — nothing to measure. Check the query, the app label and the time window." >&2
    exit 1
  fi
  if [[ -n "$gate_rows" && "$n" != "$gate_rows" ]]; then
    echo >&2; echo "ABORT: engines disagree ($gate_rows vs $n rows) — fix correctness before timing." >&2
    exit 1
  fi
  gate_rows="$n"
done
echo

for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  for ((i=0; i<WARMUP; i++)); do hit "$url" "$q" /dev/null; done
  : > "$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
done

echo "Collecting $RUNS interleaved samples per engine..."
for ((r=0; r<RUNS; r++)); do
  for e in "${ENGINES[@]}"; do
    IFS="$SEP" read -r name url q fmt <<<"$e"
    f="$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
    t0=$(date +%s%N); hit "$url" "$q" /dev/null; t1=$(date +%s%N)
    echo "scale=2; ($t1-$t0)/1000000" | bc >> "$f"
  done
done
echo

printf "%-24s %8s %8s %8s %8s %8s %6s\n" "engine" "median" "p95" "p99" "min" "max" "n"
printf "%-24s %8s %8s %8s %8s %8s %6s\n" "------------------------" "--------" "--------" "--------" "--------" "--------" "------"
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  f="$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
  save_samples "$name" "$f"
  python3 - "$name" "$f" <<'PY'
import statistics, sys
name, path = sys.argv[1], sys.argv[2]
v = sorted(float(x) for x in open(path) if x.strip())
def pct(p):  # nearest-rank; with few samples p99 is effectively the max, which is why n is printed
    return v[min(len(v) - 1, int(round(p / 100 * len(v) + 0.5)) - 1)]
print(f"{name:<24} {statistics.median(v):8.1f} {pct(95):8.1f} {pct(99):8.1f} {v[0]:8.1f} {v[-1]:8.1f} {len(v):6d}")
PY
done
