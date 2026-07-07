# Running logd in Docker (and using it from other containers/projects)

logd ships as a single ~27 MB static image. It listens on `:7100` (Loki-compatible HTTP)
and stores data under `/data`. The one thing that makes it reachable **from containers in
other compose projects / other folders** is a *shared external network*: every container
that joins `logd-net` can reach the daemon at `http://logd:7100` by name.

## 1. Start logd

```sh
# once: create the shared network other projects will join
docker network create logd-net

# from this folder: build + run logd on that network
docker compose up -d --build

# check it
docker compose ps
curl -s localhost:7100/ready        # -> ready   (only if you kept the published port)
```

logd is now reachable two ways:

- **From other containers on `logd-net`:** `http://logd:7100`  ← use this in a cluster
- **From the host / host tools:** `http://localhost:7100` (the published `7100:7100` port)

Data persists in the `logd-data` volume across restarts.

## 2. Use it from ANOTHER project (another folder)

Any other `docker-compose.yml` just declares `logd-net` as external and joins it; then it
talks to `http://logd:7100`. Minimal example:

```yaml
# some-other-project/docker-compose.yml
services:
  my-app:
    image: my-app:latest
    networks: [logd-net]
    environment:
      LOKI_URL: http://logd:7100      # your app pushes/queries here

networks:
  logd-net:
    external: true                     # the network logd created above
```

A complete, runnable example (Grafana pre-wired to logd + a container that pushes a line
by name) is in [`deploy/consumer-example/`](deploy/consumer-example/):

```sh
cd deploy/consumer-example
docker compose up            # seeds a log line, starts Grafana on http://localhost:3000
# in Grafana → Explore → logd →   {app="demo"}
```

Quick one-off proof from any container on the network:

```sh
docker run --rm --network logd-net curlimages/curl -s \
  -XPOST http://logd:7100/loki/api/v1/push -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"app":"demo"},"values":[["'"$(date +%s)"'000000000","hello took 42ms"]]}]}'
```

## 3. Ship every container's logs to logd (Grafana Alloy)

To auto-collect the Docker daemon's container logs and push them to logd, add an Alloy
service on `logd-net` with the Docker socket mounted:

```yaml
# docker-compose.yml (in any project)
services:
  alloy:
    image: grafana/alloy:latest
    command: ["run", "/etc/alloy/config.alloy"]
    volumes:
      - ./config.alloy:/etc/alloy/config.alloy:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks: [logd-net]

networks:
  logd-net: { external: true }
```

```river
// config.alloy — discover docker containers and forward their logs to logd
discovery.docker "all" {
  host = "unix:///var/run/docker.sock"
}
loki.source.docker "all" {
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.docker.all.targets
  forward_to = [loki.write.logd.receiver]
}
loki.write "logd" {
  endpoint { url = "http://logd:7100/loki/api/v1/push" }
}
```

(Promtail works the same way: point its `clients[].url` at `http://logd:7100/loki/api/v1/push`.)

## 4. Query it

- **Grafana:** add a **Loki** datasource with URL `http://logd:7100` (the example
  provisions this for you). Log queries and metric queries both work:
  `count_over_time({app="demo"}[5m])`.
- **API / curl:** `GET /loki/api/v1/query_range`, `/query`, `/labels`,
  `/label/{name}/values`, `/series`, and a WebSocket `/loki/api/v1/tail`.

## 5. Configure

The image bakes [`deploy/logd.yaml`](deploy/logd.yaml) at `/etc/logd/config.yaml`. To
change extraction templates, the label allowlist, retention, sharding, etc. **without
rebuilding**, bind-mount your own over it — uncomment in `docker-compose.yml`:

```yaml
    volumes:
      - logd-data:/data
      - ./deploy/logd.yaml:/etc/logd/config.yaml:ro
```

Key knobs (see the file for all): `index.templates` (range-queryable fields), `labels`
(indexed label allowlist), `retention_days`, `shards`, `multitenancy`.

**Multitenancy:** set `multitenancy: true` and callers send `X-Scope-OrgID: <tenant>` on
push and query — each tenant sees only its own data. Point Grafana's datasource header
`X-Scope-OrgID` at the tenant (see the example datasource file).

## 6. No shared network? (fallback)

If you can't put everything on `logd-net` (e.g. a container on the host's default bridge),
keep the published `7100:7100` port and reach logd via the host:

- Docker Desktop (Mac/Windows): `http://host.docker.internal:7100`
- Linux: the bridge gateway, usually `http://172.17.0.1:7100`

The shared network (service-name DNS) is preferred — it's portable and needs no host ports.

## Operations

```sh
docker compose logs -f logd      # tail the daemon's own logs
docker compose down              # stop (keeps the data volume)
docker compose down -v           # stop AND delete the data volume
```
