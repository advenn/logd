# Head-to-head testbed: logd vs Loki vs VictoriaLogs.
#
# Typical session:
#   make up          bring the stack up (logd, Loki, VictoriaLogs, Grafana)
#   make load        generate a corpus and push identical bytes to all three
#   make explain     show which access path logd chose
#   make compare     time logd's index path against its own forced scan
#   make down        tear everything down

COMPOSE  ?= docker compose
COUNT    ?= 200000
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

.PHONY: up down clean ps logs corpus load explain compare bench verify-loki test alloy

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

# Show logd's chosen access path. "mode":"index" on sealed segments is the differentiator
# working; the active (unsealed) segment always scans by design.
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
# returned the same non-zero row count.
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
