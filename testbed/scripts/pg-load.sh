#!/usr/bin/env bash
# Load a corpus manifest into PostgreSQL, both variants, and print load seconds, lines/s, WAL
# written, and table and index sizes -- the PostgreSQL row of the ingest and disk comparison.
#
#   ./scripts/pg-load.sh [manifest] [dataset]
#   ./scripts/pg-load.sh corpus/app-500000.jsonl app
#
# The manifest is the one loggen wrote while pushing to the log stores, so PostgreSQL holds the
# same lines, timestamps and labels, and the same ground truth applies to its answers.
#
# Environment:
#   PG_DSN    connection string (default postgres://logd:logd@localhost:5432/logs)
#   BATCH     rows per COPY transaction (default 1000, the push driver's batch; 0 = one COPY)
#   VARIANTS  default structured,unstructured
#   JSON      also write the results as JSON to this path
#
# Every load truncates the variant's table first, so rerunning measures a load into an empty
# table rather than an append. It does not shrink the WAL directory or reset the cluster;
# `docker compose down -v` does that, and a series of ingest runs should start from it.

set -euo pipefail
cd "$(dirname "$0")/.."

MANIFEST="${1:-corpus/app-500000.jsonl}"
DATASET="${2:-app}"
if [[ ! -f "$MANIFEST" ]]; then
  echo "no manifest at $MANIFEST — run 'make corpus' (or 'make load') first" >&2
  exit 1
fi

args=(-manifest "$MANIFEST" -dataset "$DATASET" -batch "${BATCH:-1000}" -variants "${VARIANTS:-structured,unstructured}")
[[ -n "${JSON:-}" ]] && args+=(-json "$JSON")

echo "PostgreSQL load: $MANIFEST (dataset $DATASET), batch ${BATCH:-1000}"
echo "load at start: $(cat /proc/loadavg)"
go run ./cmd/pgload "${args[@]}"
