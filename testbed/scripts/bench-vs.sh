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
add() { ENGINES+=("$1${SEP}$2${SEP}$3"); }

APP="${APP:-checkout}"
add "logd (typed index)"  "http://localhost:7100/loki/api/v1/query_range" "{app=\"$APP\"} | latency_ms > 200"
add "logd (forced scan)"  "http://localhost:7100/logd/api/v1/query_scan"  "{app=\"$APP\"} | latency_ms > 200"
add "Loki (| pattern)"    "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > 200"
add "Loki (| regexp)"     "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | regexp \"took=(?P<latency_ms>[0-9]+)ms\" | latency_ms > 200"

hit() { # url query outfile
  curl -sS -G "$1" \
    --data-urlencode "query=$2" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=200000" -o "$3"
}

rows() { python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(sum(len(r['values']) for r in d['data']['result']))" "$1"; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

# Correctness gate. A latency number next to a different answer is meaningless, and a
# latency number next to an EMPTY answer is worse: it looks plausible and inverts the
# conclusion. (Observed: a query that silently matched nothing reported Loki at 13ms and
# logd at 26ms, i.e. Loki "winning" purely by returning nothing faster.) Abort on either.
echo "Row counts (must all agree, and be non-zero):"
gate_rows=""
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q <<<"$e"
  hit "$url" "$q" "$tmp/check"
  n=$(rows "$tmp/check")
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
  IFS="$SEP" read -r name url q <<<"$e"
  for ((i=0; i<WARMUP; i++)); do hit "$url" "$q" /dev/null; done
  : > "$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
done

echo "Collecting $RUNS interleaved samples per engine..."
for ((r=0; r<RUNS; r++)); do
  for e in "${ENGINES[@]}"; do
    IFS="$SEP" read -r name url q <<<"$e"
    f="$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
    t0=$(date +%s%N); hit "$url" "$q" /dev/null; t1=$(date +%s%N)
    echo "scale=2; ($t1-$t0)/1000000" | bc >> "$f"
  done
done
echo

printf "%-22s %9s %9s %9s\n" "engine" "median" "min" "max"
printf "%-22s %9s %9s %9s\n" "----------------------" "---------" "---------" "---------"
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q <<<"$e"
  f="$tmp/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').samples"
  python3 - "$name" "$f" <<'PY'
import statistics, sys
name, path = sys.argv[1], sys.argv[2]
v = sorted(float(x) for x in open(path) if x.strip())
print(f"{name:<22} {statistics.median(v):9.1f} {v[0]:9.1f} {v[-1]:9.1f}")
PY
done
