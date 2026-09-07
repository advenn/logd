#!/usr/bin/env bash
# Derive the query parameters for a corpus from its manifest.
#
# Both the time window AND the app label come from the manifest rather than from defaults.
# The corpus base timestamp defaults to "auto" (it ends just before now, so Loki's ingester
# will actually serve it) and different corpora are generated under different app labels,
# so hardcoding either one silently queries an empty stream — which does not look like an
# error, it looks like a very fast engine returning nothing.
#
# Usage:
#   window.sh <manifest> env       print START=... END=... APP=... for eval
#   window.sh <manifest> explain   call logd's explain endpoint over that window

set -euo pipefail

manifest="${1:?usage: window.sh <manifest> <env|explain>}"
mode="${2:-env}"
LOGD_URL="${LOGD_URL:-http://localhost:7100}"

if [[ ! -f "$manifest" ]]; then
  echo "no manifest at $manifest — run 'make corpus' or 'make load' first" >&2
  exit 1
fi

# First and last line only: manifests run to hundreds of MB and fully parsing one to learn
# three values would dominate this command's runtime.
read -r start end app < <(python3 - "$manifest" <<'PY'
import collections, json, sys

with open(sys.argv[1], "rb") as f:
    first = f.readline()
    try:
        f.seek(max(0, f.seek(0, 2) - 65536))
    except OSError:
        f.seek(0)
    last = collections.deque(f, maxlen=1)[0]

head = json.loads(first)
start, end = head["ts_ns"], json.loads(last)["ts_ns"]
app = head.get("labels", {}).get("app", "checkout")
# Pad a second either side so boundary records are never clipped by an off-by-one.
print(start - 10**9, end + 10**9, app)
PY
)

case "$mode" in
  env)
    echo "export START=$start END=$end APP=$app"
    ;;
  explain)
    query="${QUERY:-}"
    if [[ -z "$query" ]]; then
      query="{app=\"$app\"} | latency_ms > 200"
    fi
    curl -sS -G "$LOGD_URL/logd/api/v1/explain" \
      --data-urlencode "query=$query" \
      --data-urlencode "start=$start" \
      --data-urlencode "end=$end" | python3 -m json.tool
    ;;
  *)
    echo "unknown mode $mode" >&2
    exit 2
    ;;
esac
