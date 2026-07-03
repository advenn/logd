# Build logd from source into a small static-ish image.
# Used by docker-compose.local.yaml (local builds) and as the basis for
# the registry image.

FROM golang:1.26.2-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/logd ./cmd/logd

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /var/lib/logd

COPY --from=build /out/logd /usr/local/bin/logd
# Baked default config so the image is runnable standalone (registry use).
# Local compose overrides this by mounting ./config.yaml on top.
COPY config.yaml ./config.yaml
RUN mkdir -p ./data

EXPOSE 3100
ENTRYPOINT ["/usr/local/bin/logd"]
CMD ["-config", "config.yaml"]
