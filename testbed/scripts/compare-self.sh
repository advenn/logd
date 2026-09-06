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
  QUERY='{app="checkout"} | latency_ms > 200'
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

fetch() { # endpoint outfile
  curl -sS -G "$LOGD_URL/$1" \
    --data-urlencode "query=$QUERY" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=$LIMIT" \
    -o "$2"
}

rows() { python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(sum(len(r['values']) for r in d['data']['result']))" "$1"; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

fetch "loki/api/v1/query_range" "$tmp/index.json"
fetch "logd/api/v1/query_scan"  "$tmp/scan.json"

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

bench() { # endpoint -> mean ms
  local ep="$1" i t0 t1
  for ((i=0; i<WARMUP; i++)); do fetch "$ep" /dev/null; done
  t0=$(date +%s%N)
  for ((i=0; i<RUNS; i++)); do fetch "$ep" /dev/null; done
  t1=$(date +%s%N)
  echo "scale=2; ($t1 - $t0) / $RUNS / 1000000" | bc
}

idx=$(bench "loki/api/v1/query_range")
scn=$(bench "logd/api/v1/query_scan")

echo
echo "query:  $QUERY"
echo "rows:   $n  (identical on both paths)"
echo "runs:   $RUNS measured, $WARMUP warm-ups discarded, warm page cache"
echo
printf "  %-26s %8s ms\n" "logd (typed index)" "$idx"
printf "  %-26s %8s ms\n" "logd (forced scan)" "$scn"
echo "$idx $scn" | awk '{ if ($1 > 0) printf "  %-26s %8.2fx\n", "speedup", $2/$1 }'
echo
echo "Access path per segment:"
curl -sS -G "$LOGD_URL/logd/api/v1/explain" \
  --data-urlencode "query=$QUERY" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" | python3 -m json.tool
