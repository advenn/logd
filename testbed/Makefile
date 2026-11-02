# Head-to-head testbed: logd vs Loki vs VictoriaLogs.
#
# Typical session:
#   make up          bring the stack up (logd, Loki, VictoriaLogs, Grafana)
#   make load        generate a corpus and push identical bytes to all three
#   make explain     show which access path logd chose
#   make compare     time logd's index path against its own forced scan
#   make pg-load     load the same corpus into PostgreSQL (both variants), print cost and size
#   make gate        every engine's row count against the manifest's ground truth
#   make hdfs        the Loghub HDFS replay end to end (fresh volumes, push, load, gate)
#   make down        tear everything down

COMPOSE  ?= docker compose
# 500,000 lines, the corpus the protocol-*.sh scripts and final-run.sh push and read back
# (corpus/app-500000.jsonl), so `make load` produces the manifest they expect.
COUNT    ?= 500000
SEED     ?= 1001
CORPUS   ?= corpus/app-$(COUNT).jsonl
# "auto" ends the corpus just before now. A corpus dated in the distant past is silently
# invisible to Loki (it stays in the ingester, which is not consulted for old data).
BASE_TS  ?= auto

LOGD_URL ?= http://localhost:7100
LOKI_URL ?= http://localhost:3100
VL_URL   ?= http://localhost:9428

# VictoriaLogs needs _stream_fields or it derives stream identity from every label,
# including high-cardinality ones, and its stream count stops matching the others.
VL_PUSH  ?= $(VL_URL)/insert/loki/api/v1/push?_stream_fields=app,env,region

.PHONY: up down clean ps logs corpus load pg-load gate hdfs explain compare bench verify-loki test alloy

up:
	$(COMPOSE) up -d --build
	@echo "Grafana: http://localhost:3000  (anonymous admin; Explore -> pick a datasource)"

down:
	$(COMPOSE) down

# Removes volumes too: every benchmark run must start from empty stores, or the disk
# figures include the previous run's data.
clean:
	$(COMPOSE) down -v
	rm -rf corpus results

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail=100 -f

# Generate a deterministic corpus WITHOUT pushing it. The manifest is the ground truth
# the parity check compares against, so it is worth keeping around.
corpus:
	@mkdir -p corpus
	go run ./cmd/loggen -corpus -count=$(COUNT) -seed=$(SEED) -base-ts=$(BASE_TS) \
		-manifest=$(CORPUS) \
		-label app=checkout -label env=prod -label region=eu

# Push identical encoded bytes to all three backends, closed-loop.
load:
	@mkdir -p corpus
	go run ./cmd/loggen -corpus -count=$(COUNT) -seed=$(SEED) -base-ts=$(BASE_TS) \
		-manifest=$(CORPUS) \
		-push=$(LOGD_URL)/loki/api/v1/push \
		-push=$(LOKI_URL)/loki/api/v1/push \
		-push='$(VL_PUSH)' \
		-label app=checkout -label env=prod -label region=eu

# Load the same manifest into PostgreSQL: COPY in the push driver's 1000-line batches, both
# the pre-structured and the unstructured variant, then load time, lines/s and sizes. Run it
# after `make load` so PostgreSQL holds exactly what the log stores received.
pg-load:
	@./scripts/pg-load.sh $(CORPUS) app

# Correctness against GROUND TRUTH, not just agreement: the expected count per threshold is
# computed from the manifest alone, and every engine -- logd (index and forced scan), Loki
# (pattern and regexp), VictoriaLogs and both PostgreSQL variants -- must return exactly it.
gate:
	@python3 scripts/gate.py --dataset app --manifest $(CORPUS)

# Loghub HDFS_v1 replay (experiment E-2). DESTRUCTIVE: starts from `docker compose down -v`.
# The ~530 MB manifest goes to ~/.cache/logd-bench, not corpus/. See scripts/hdfs-run.sh.
hdfs:
	@./scripts/hdfs-run.sh

# Show logd's chosen access path. "mode":"index" is the differentiator working: on sealed
# segments from their .tidx/.lidx, on the active (unsealed) segment from the writer's
# in-memory index (reason indexed_in_memory).
explain:
	@./scripts/window.sh $(CORPUS) explain

# The fairest single number logd can produce: its own index path vs its own forced scan.
# Same binary, same data, same page cache — the only variable is whether the index is
# consulted, so "you misconfigured the other one" cannot apply.
compare:
	@win=$$(./scripts/window.sh $(CORPUS) env) || exit 1; eval "$$win"; ./scripts/compare-self.sh

# Interleaved A/B latency across every engine. Interleaved rather than sequential so drift
# over the run (cache warming, a neighbouring container waking up) lands on all engines
# equally instead of on whichever one happened to go last. Aborts unless every engine
# returned the same non-empty result set.
bench:
	@win=$$(./scripts/window.sh $(CORPUS) env) || exit 1; eval "$$win"; ./scripts/bench-vs.sh

# How the index config is doing against live traffic. candidates==0 means the anchor never
# appears; matches<candidates means the pattern does not align after the anchor;
# failures>0 means it aligns but a capture will not parse as the declared type.
index-stats:
	@curl -s $(LOGD_URL)/logd/api/v1/index_stats | python3 -m json.tool

# Loki config keys drift between minor releases. Re-run this on every image bump.
verify-loki:
	docker run --rm -v "$(PWD)/stack/loki:/etc/loki:ro" grafana/loki:3.7.7 \
		-config.file=/etc/loki/loki.yaml -verify-config && echo "loki config OK"

test:
	go test ./...

# Compatibility demo only — never during a measured run. See docker-compose.yml.
alloy:
	$(COMPOSE) --profile alloy up -d alloy
