#!/usr/bin/env bash
# logd's index path vs logd's own forced scan.
#
# This is the fairest comparison logd can offer, and it is why it runs before any
# cross-product benchmark. An index-vs-scan result against another product always invites
# "you configured it wrong"; a comparison against the same binary, over the same data,
# with the same page cache, has exactly one variable — whether the .tidx is consulted.
#
# It also double-checks the engine's core contract: both paths MUST return identical rows.
# A speedup with a different answer is not a speedup.

set -euo pipefail

LOGD_URL="${LOGD_URL:-http://localhost:7100}"
# NOTE: assigned in two steps on purpose. Writing ${QUERY:-{app="checkout"} | ...}
# does not work: the '}' inside the default value closes the parameter expansion early,
# silently truncating the query to `{app="checkout"` and producing a LogQL parse error.
: "${QUERY:=}"
if [[ -z "$QUERY" ]]; then
  # APP is exported by scripts/window.sh from the corpus manifest. Defaulting it blindly
  # would query a stream that may not exist in this corpus and return nothing, which reads
  # as a fast engine rather than a mistake.
  QUERY="{app=\"${APP:-checkout}\"} | latency_ms > 200"
fi
LIMIT="${LIMIT:-1000000}"

# START/END must come from the corpus manifest (scripts/window.sh), never from a default.
# The corpus base timestamp is "auto" — it moves with every generation — so a hardcoded
# fallback would silently query an empty window and report a meaningless "0 rows, very
# fast" result rather than an error.
if [[ -z "${START:-}" || -z "${END:-}" ]]; then
  echo "START/END are not set. Run via 'make compare', or:" >&2
  echo "  eval \$(./scripts/window.sh corpus/<manifest>.jsonl env) && ./scripts/compare-self.sh" >&2
  exit 2
fi
WARMUP="${WARMUP:-5}"
RUNS="${RUNS:-20}"

fetch() { # endpoint outfile -> prints "<http status> <bytes received>"
  curl -sS -G "$LOGD_URL/$1" \
    --data-urlencode "query=$QUERY" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=$LIMIT" \
    -o "$2" -w '%{http_code} %{size_download}\n'
}

rows() { python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(sum(len(r['values']) for r in d['data']['result']))" "$1"; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

read -r _ idx_size <<<"$(fetch "loki/api/v1/query_range" "$tmp/index.json")"
read -r _ scn_size <<<"$(fetch "logd/api/v1/query_scan"  "$tmp/scan.json")"

# Fail clearly on an error body rather than letting the comparator die on a missing key.
for f in "$tmp/index.json" "$tmp/scan.json"; do
  if ! python3 -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if 'data' in d else 1)" "$f"; then
    echo "logd returned an error rather than a result set:" >&2
    head -c 400 "$f" >&2; echo >&2
    exit 1
  fi
done

# Correctness gate first. If the two paths disagree, the timings are meaningless and the
# engine has a real bug, so stop rather than print a number.
if ! python3 - "$tmp/index.json" "$tmp/scan.json" <<'PY'
import json, sys
def norm(p):
    d = json.load(open(p))
    return sorted(v[0] + "|" + v[1] for r in d["data"]["result"] for v in r["values"])
a, b = norm(sys.argv[1]), norm(sys.argv[2])
if a != b:
    print(f"MISMATCH: index returned {len(a)} rows, scan returned {len(b)}", file=sys.stderr)
    sys.exit(1)
PY
then
  echo "FAIL: index path and forced scan disagree — refusing to report timings." >&2
  exit 1
fi

n=$(rows "$tmp/index.json")
if [[ "$n" == "0" ]]; then
  echo "ABORT: query matched 0 rows — nothing to measure. Check the app label and window." >&2
  exit 1
fi

# Interleaved, one sample per path at a time, for the reason bench-vs.sh gives: drift over
# the run (page cache, a neighbour waking up, thermals) then lands on both paths instead of
# masquerading as a difference between them. Reported as median and min-max, not a mean,
# which one slow outlier can move.
#
# Every sample is checked: its body goes to /dev/null, so an error response would otherwise
# be timed as a fast answer. Both paths are logd and the window is fixed, so each response
# must be the gate response byte for byte in size.
sample() { # endpoint gate-size -> elapsed ms
  local t0 t1 code size
  t0=$(date +%s%N); fetch "$1" /dev/null > "$tmp/w"; t1=$(date +%s%N)
  read -r code size < "$tmp/w"
  if [[ "$code" != "200" || "$size" != "$2" ]]; then
    echo "ABORT: $1 answered HTTP $code with $size bytes; the gate response was $2 bytes. Refusing to report timings." >&2
    exit 1
  fi
  echo "scale=2; ($t1 - $t0) / 1000000" | bc
}

for ((i=0; i<WARMUP; i++)); do
  sample "loki/api/v1/query_range" "$idx_size" > /dev/null
  sample "logd/api/v1/query_scan"  "$scn_size" > /dev/null
done
: > "$tmp/idx.samples"; : > "$tmp/scn.samples"
for ((i=0; i<RUNS; i++)); do
  sample "loki/api/v1/query_range" "$idx_size" >> "$tmp/idx.samples"
  sample "logd/api/v1/query_scan"  "$scn_size" >> "$tmp/scn.samples"
done

echo
echo "query:  $QUERY"
echo "rows:   $n  (identical on both paths)"
echo "runs:   $RUNS interleaved per path, $WARMUP warm-ups each discarded, warm page cache"
echo
python3 - "$tmp/idx.samples" "$tmp/scn.samples" <<'PY'
import statistics, sys
def load(p):
    return sorted(float(x) for x in open(p) if x.strip())
idx, scn = load(sys.argv[1]), load(sys.argv[2])
print(f"  {'':26} {'median':>9} {'min':>9} {'max':>9}")
for name, v in (("logd (typed index)", idx), ("logd (forced scan)", scn)):
    print(f"  {name:<26} {statistics.median(v):9.2f} {v[0]:9.2f} {v[-1]:9.2f}  ms")
mi, ms = statistics.median(idx), statistics.median(scn)
if mi > 0:
    print(f"  {'speedup (median)':<26} {ms / mi:8.2f}x")
PY
echo
echo "Access path per segment:"
curl -sS -G "$LOGD_URL/logd/api/v1/explain" \
  --data-urlencode "query=$QUERY" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" | python3 -m json.tool
