# syntax=docker/dockerfile:1

# ---- build: static binary, no CGO ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/logd ./cmd/logd

# ---- runtime: minimal, non-root ----
FROM alpine:3.20
RUN apk add --no-cache wget \
 && adduser -D -H -u 10001 logd \
 && mkdir -p /data /etc/logd \
 && chown logd:logd /data
COPY --from=build /out/logd /usr/local/bin/logd
COPY deploy/logd.yaml /etc/logd/config.yaml

USER logd
VOLUME ["/data"]
EXPOSE 7100

# /ready is a lightweight liveness probe.
HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=5s \
  CMD wget -qO- http://localhost:7100/ready || exit 1

# Mount your own config over /etc/logd/config.yaml to customize.
ENTRYPOINT ["/usr/local/bin/logd", "-config", "/etc/logd/config.yaml"]
