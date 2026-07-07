# logd — build & run helpers. Run `make` (or `make help`) for the target list.

IMAGE   ?= logd:latest
NETWORK ?= logd-net
COMPOSE ?= docker compose
EXAMPLE := deploy/consumer-example/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help
help: ## List targets
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## --- Docker ---

.PHONY: build
build: ## Build the logd Docker image
	docker build -t $(IMAGE) .

.PHONY: net
net: ## Create the shared external network (idempotent)
	@docker network create $(NETWORK) 2>/dev/null || true

.PHONY: up
up: net build ## Build the image + start logd on the shared network
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop logd (keeps the data volume)
	$(COMPOSE) down

.PHONY: logs
logs: ## Tail logd's logs
	$(COMPOSE) logs -f logd

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: example
example: net ## Start the consumer example (Grafana + a seed push)
	$(COMPOSE) -f $(EXAMPLE) up

.PHONY: clean
clean: ## Stop everything, remove the data volume + shared network
	-$(COMPOSE) down -v
	-$(COMPOSE) -f $(EXAMPLE) down
	-docker network rm $(NETWORK)

## --- Local (no Docker) ---

.PHONY: bin
bin: ## Build the static binary to ./logd
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o logd ./cmd/logd

.PHONY: test
test: ## Run the test suite with the race detector
	go test -race ./...

.PHONY: vet
vet: ## go vet ./...
	go vet ./...

.PHONY: run
run: ## Run logd locally against config.yaml
	go run ./cmd/logd -config config.yaml
