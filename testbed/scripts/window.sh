#!/usr/bin/env bash
# Derive the query time window from a corpus manifest.
#
# The window must come from the manifest rather than being hardcoded, because the corpus
# base timestamp defaults to "auto" (it ends just before now, so Loki's ingester will
# actually serve it). Hardcoding nanoseconds would silently query an empty window.
#
# Usage:
#   window.sh <manifest> env       print START=... END=... for eval
#   window.sh <manifest> explain   call logd's explain endpoint over that window

set -euo pipefail

manifest="${1:?usage: window.sh <manifest> <env|explain>}"
mode="${2:-env}"
LOGD_URL="${LOGD_URL:-http://localhost:7100}"
# NOTE: assigned in two steps on purpose. Writing ${QUERY:-{app="checkout"} | ...}
# does not work: the '}' inside the default value closes the parameter expansion early,
# silently truncating the query to `{app="checkout"` and producing a LogQL parse error.
: "${QUERY:=}"
if [[ -z "$QUERY" ]]; then
  QUERY='{app="checkout"} | latency_ms > 200'
fi

if [[ ! -f "$manifest" ]]; then
  echo "no manifest at $manifest — run 'make corpus' or 'make load' first" >&2
  exit 1
fi

# One pass, first and last line only: manifests are large and fully parsing a million-line
# JSONL file to learn two numbers would dominate the command's runtime.
read -r start end < <(python3 - "$manifest" <<'PY'
import json, sys, collections
path = sys.argv[1]
with open(path, "rb") as f:
    first = f.readline()
    # Cheap tail read rather than scanning the whole file.
    try:
        f.seek(max(0, f.seek(0, 2) - 65536))
    except OSError:
        f.seek(0)
    last = collections.deque(f, maxlen=1)[0]
a = json.loads(first)["ts_ns"]
b = json.loads(last)["ts_ns"]
# Pad by a second on each side so boundary records are never clipped by an off-by-one.
print(a - 10**9, b + 10**9)
PY
)

case "$mode" in
  env)
    echo "export START=$start END=$end"
    ;;
  explain)
    curl -sS -G "$LOGD_URL/logd/api/v1/explain" \
      --data-urlencode "query=$QUERY" \
      --data-urlencode "start=$start" \
      --data-urlencode "end=$end" | python3 -m json.tool
    ;;
  *)
    echo "unknown mode $mode" >&2; exit 2
    ;;
esac
