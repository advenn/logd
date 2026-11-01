# syntax=docker/dockerfile:1
#
# pgshim: PostgreSQL behind a Loki-shaped HTTP endpoint, for the testbed harness.
# Build context is testbed/ (see docker-compose.yml).

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
# The testbed module replaces github.com/advenn/logd with ../ so loggen can import the real
# daemon packages. The shim imports none of them, and ../ is outside this build context (the
# repository's own .dockerignore keeps testbed/ out of the daemon's context, so the context
# cannot simply be widened). Dropping the requirement here builds the same shim source
# without the daemon tree.
RUN go mod edit -dropreplace=github.com/advenn/logd -droprequire=github.com/advenn/logd \
 && go mod download github.com/jackc/pgx/v5
COPY internal/pgdata ./internal/pgdata
COPY cmd/pgshim ./cmd/pgshim
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pgshim ./cmd/pgshim

FROM alpine:3.20
RUN apk add --no-cache wget && adduser -D -H -u 10001 pgshim
COPY --from=build /out/pgshim /usr/local/bin/pgshim
USER pgshim
EXPOSE 7300
ENTRYPOINT ["/usr/local/bin/pgshim"]
