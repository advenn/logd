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
# Optional rows, all off by default so the default run is the one earlier results came from:
#   WITH_PG=1          PostgreSQL through the shim on :7300, both variants (structured =
#                      generated column + B-tree; unstructured = extraction in the WHERE clause)
#   LOGD_V1_URL=...    a second logd built from an older tag (e.g. thesis-eval-1 on :7101), so
#                      one interleaved run measures what the fixes and features changed
#   WITH_LOGQL_STAGES=1  logd answering the Loki query text itself (| pattern / | regexp), which
#                      needs a logd with parser stages; shows whether the same text reaches the index
# DATASET selects the corpus the queries are written for: app (the synthetic took=<n>ms corpus,
# predicate latency_ms > T) or hdfs (Loghub HDFS replay, predicate size_bytes < T). The HDFS
# forms are the ones scripts/gate.py verified against ground truth; a single Loki | pattern or
# a single VictoriaLogs extract is NOT exact on HDFS (two line shapes carry the size), so the
# two-stage forms are used.
DATASET="${DATASET:-app}"
LOGD_URL="${LOGD_URL:-http://localhost:7100}"
# Each engine carries the response format its row count has to be read from: Loki's JSON for
# logd, Loki and the PostgreSQL shim; one JSON object per line for VictoriaLogs' LogsQL endpoint.
case "$DATASET" in
app)
  add "logd (typed index)"  "$LOGD_URL/loki/api/v1/query_range" "{app=\"$APP\"} | latency_ms > $T" loki
  add "logd (forced scan)"  "$LOGD_URL/logd/api/v1/query_scan"  "{app=\"$APP\"} | latency_ms > $T" loki
  if [[ -n "${LOGD_V1_URL:-}" ]]; then
    add "logd v1 (typed index)" "$LOGD_V1_URL/loki/api/v1/query_range" "{app=\"$APP\"} | latency_ms > $T" loki
  fi
  if [[ -n "${WITH_LOGQL_STAGES:-}" ]]; then
    add "logd (Loki | pattern text)" "$LOGD_URL/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > $T" loki
    add "logd (Loki | regexp text)"  "$LOGD_URL/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | regexp \"took=(?P<latency_ms>[0-9]+)ms\" | latency_ms > $T" loki
  fi
  add "Loki (| pattern)"    "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | pattern \"<_>took=<latency_ms>ms<_>\" | latency_ms > $T" loki
  add "Loki (| regexp)"     "http://localhost:3100/loki/api/v1/query_range" "{app=\"$APP\"} |= \"took=\" | regexp \"took=(?P<latency_ms>[0-9]+)ms\" | latency_ms > $T" loki
  # VictoriaLogs speaks the Loki PUSH protocol but not the Loki QUERY API, so it is measured on
  # its own endpoint in LogsQL. Without this row the comparison quietly omits the engine that the
  # earlier measurements found to be the real competitor.
  add "VictoriaLogs (extract)" "http://localhost:9428/select/logsql/query" "_stream:{app=\"$APP\"} \"took=\" | extract \"took=<latency_ms>ms\" | latency_ms:>$T" jsonl
  if [[ -n "${WITH_PG:-}" ]]; then
    add "PostgreSQL (generated col + B-tree)" "http://localhost:7300/loki/api/v1/query_range?variant=structured"   "latency_ms > $T" loki
    add "PostgreSQL (extract in WHERE)"       "http://localhost:7300/loki/api/v1/query_range?variant=unstructured" "latency_ms > $T" loki
  fi
  ;;
hdfs)
  add "logd (typed index)"  "$LOGD_URL/loki/api/v1/query_range" "{app=\"hdfs\"} | size_bytes < $T" loki
  add "logd (forced scan)"  "$LOGD_URL/logd/api/v1/query_scan"  "{app=\"hdfs\"} | size_bytes < $T" loki
  if [[ -n "${LOGD_V1_URL:-}" ]]; then
    add "logd v1 (typed index)" "$LOGD_V1_URL/loki/api/v1/query_range" "{app=\"hdfs\"} | size_bytes < $T" loki
  fi
  add "Loki (| regexp)"     "http://localhost:3100/loki/api/v1/query_range" "{app=\"hdfs\"} |= \"size \" | regexp \"size (?P<size_bytes>[0-9]+)\" | size_bytes < $T" loki
  add "Loki (2× | pattern)" "http://localhost:3100/loki/api/v1/query_range" "{app=\"hdfs\"} |= \"size \" | pattern \"<_> size <size_bytes> <_>\" | pattern \"<_> size <size_bytes>\" | size_bytes < $T" loki
  add "VictoriaLogs (extract_regexp)" "http://localhost:9428/select/logsql/query" "_stream:{app=\"hdfs\"} \"size\" | extract_regexp \"size (?P<size_bytes>[0-9]+)\" | size_bytes:<$T" jsonl
  if [[ -n "${WITH_PG:-}" ]]; then
    add "PostgreSQL (generated col + B-tree)" "http://localhost:7300/loki/api/v1/query_range?variant=structured&dataset=hdfs"   "size_bytes < $T" loki
    add "PostgreSQL (extract in WHERE)"       "http://localhost:7300/loki/api/v1/query_range?variant=unstructured&dataset=hdfs" "size_bytes < $T" loki
  fi
  ;;
*) echo "unknown DATASET=$DATASET (app|hdfs)" >&2; exit 2 ;;
esac

here="$(cd "$(dirname "$0")" && pwd)"

hit() { # url query outfile -> prints "<http status> <bytes received>"
  curl -sS -G "$1" \
    --data-urlencode "query=$2" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=200000" -o "$3" -w '%{http_code} %{size_download}\n'
}

slug() { echo "$1" | tr -c 'a-zA-Z0-9' '_'; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
# RESULTS_DIR keeps every raw sample, so percentiles can be recomputed and plots rebuilt from the
# same run instead of re-measuring (the thesis methodology asks for the raw output).
if [[ -n "${RESULTS_DIR:-}" ]]; then mkdir -p "$RESULTS_DIR"; fi
save_samples() { # label file
  [[ -z "${RESULTS_DIR:-}" ]] && return 0
  local out="$RESULTS_DIR/t${T}_$(slug "$1").csv"
  { echo "ms"; cat "$2"; } > "$out"
}

# Correctness gate. A latency number next to a different answer is meaningless, and a
# latency number next to an EMPTY answer is worse: it looks plausible and inverts the
# conclusion. (Observed: a query that silently matched nothing reported Loki at 13ms and
# logd at 26ms, i.e. Loki "winning" purely by returning nothing faster.) Abort on either.
#
# The engines must return the same RESULT SET, not merely as many rows: each response is
# reduced to its sorted (timestamp, line) pairs (scripts/resultset.py, which reads Loki's
# JSON and VictoriaLogs' JSONL) and those are compared byte for byte.
echo "Result sets (must all be identical, and non-empty):"
gate_set=""
gate_name=""
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  key=$(slug "$name")
  read -r code size <<<"$(hit "$url" "$q" "$tmp/check")"
  if [[ "$code" != "200" ]]; then
    echo >&2; echo "ABORT: '$name' answered HTTP $code: $(head -c 300 "$tmp/check")" >&2
    exit 1
  fi
  if ! python3 "$here/resultset.py" "$fmt" "$tmp/check" > "$tmp/$key.set"; then
    echo >&2; echo "ABORT: could not read '$name''s response as $fmt." >&2
    exit 1
  fi
  echo "$size" > "$tmp/$key.size" # what every timed sample of this engine must match
  n=$(wc -l < "$tmp/$key.set")
  printf "  %-22s %8s rows  sha256 %s\n" "$name" "$n" "$(sha256sum < "$tmp/$key.set" | cut -c1-16)"
  if [[ "$n" == "0" ]]; then
    echo >&2; echo "ABORT: '$name' returned 0 rows — nothing to measure. Check the query, the app label and the time window." >&2
    exit 1
  fi
  if [[ -n "$gate_set" ]] && ! cmp -s "$gate_set" "$tmp/$key.set"; then
    echo >&2
    echo "ABORT: '$name' and '$gate_name' return different result sets — fix correctness before timing." >&2
    echo "  only in '$gate_name': $(LC_ALL=C comm -23 "$gate_set" "$tmp/$key.set" | wc -l) rows, e.g. $(LC_ALL=C comm -23 "$gate_set" "$tmp/$key.set" | head -1 | cut -c1-160)" >&2
    echo "  only in '$name': $(LC_ALL=C comm -13 "$gate_set" "$tmp/$key.set" | wc -l) rows, e.g. $(LC_ALL=C comm -13 "$gate_set" "$tmp/$key.set" | head -1 | cut -c1-160)" >&2
    exit 1
  fi
  gate_set="$tmp/$key.set"
  gate_name="$name"
done
echo

# Every timed sample is checked too. Its body goes to /dev/null, so without this an error
# response (a 4xx/5xx, a timeout's partial body) would be recorded as a fast sample. For a
# fixed window the response is deterministic, so its size must match the gate response's;
# the tolerance only absorbs Loki's `stats` block, whose timing figures change length from
# run to run.
check_sample() { # name key what
  local code size want tol
  read -r code size < "$tmp/w"
  want=$(<"$tmp/$2.size")
  tol=$(( want / 200 )); (( tol < 1024 )) && tol=1024
  if [[ "$code" != "200" ]] || (( size < want - tol || size > want + tol )); then
    echo >&2
    echo "ABORT: $3 of '$1' answered HTTP $code with $size bytes; the gate response was $want bytes. Refusing to report timings that may include a failed response." >&2
    exit 1
  fi
}

for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  key=$(slug "$name")
  for ((i=0; i<WARMUP; i++)); do
    hit "$url" "$q" /dev/null > "$tmp/w"
    check_sample "$name" "$key" "warm-up $((i+1))"
  done
  : > "$tmp/$key.samples"
done

echo "Collecting $RUNS interleaved samples per engine..."
for ((r=0; r<RUNS; r++)); do
  for e in "${ENGINES[@]}"; do
    IFS="$SEP" read -r name url q fmt <<<"$e"
    key=$(slug "$name")
    t0=$(date +%s%N); hit "$url" "$q" /dev/null > "$tmp/w"; t1=$(date +%s%N)
    check_sample "$name" "$key" "sample $((r+1))"
    echo "scale=2; ($t1-$t0)/1000000" | bc >> "$tmp/$key.samples"
  done
done
echo

printf "%-24s %8s %8s %8s %8s %8s %6s\n" "engine" "median" "p95" "p99" "min" "max" "n"
printf "%-24s %8s %8s %8s %8s %8s %6s\n" "------------------------" "--------" "--------" "--------" "--------" "--------" "------"
for e in "${ENGINES[@]}"; do
  IFS="$SEP" read -r name url q fmt <<<"$e"
  f="$tmp/$(slug "$name").samples"
  save_samples "$name" "$f"
  python3 - "$name" "$f" <<'PY'
import math, statistics, sys
name, path = sys.argv[1], sys.argv[2]
v = sorted(float(x) for x in open(path) if x.strip())
def pct(p):  # nearest-rank, rank ceil(p/100*n), as protocol-analyse.py; with few samples p99 is the max, hence n
    return v[max(0, math.ceil(p * len(v) / 100) - 1)]
print(f"{name:<24} {statistics.median(v):8.1f} {pct(95):8.1f} {pct(99):8.1f} {v[0]:8.1f} {v[-1]:8.1f} {len(v):6d}")
PY
done
