# Builds the NetBird components from the local ../netbird checkout, with the
# management-integrations module replaced by the local
# ../management-integrations-flow-enabled/integrations checkout.
#
# Build context:          ../netbird
# Named context:          integrations -> ../management-integrations-flow-enabled/integrations
# Targets:                management | signal | relay | client

FROM golang:1.25-bookworm AS build
WORKDIR /src/netbird

COPY --from=integrations / /src/integrations
COPY go.mod go.sum ./
# Point at the local integrations checkout instead of the pinned GitHub version.
RUN go mod edit -replace github.com/netbirdio/management-integrations/integrations=/src/integrations
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# go mod edit again: COPY . . overwrote go.mod with the repo's version.
RUN go mod edit -replace github.com/netbirdio/management-integrations/integrations=/src/integrations

ARG LDFLAGS="-X github.com/netbirdio/netbird/version.version=0.53.0"

FROM build AS build-management
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -mod=mod -ldflags "${LDFLAGS}" -o /out/netbird-mgmt ./management

FROM build AS build-signal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=mod -ldflags "${LDFLAGS}" -o /out/netbird-signal ./signal

FROM build AS build-relay
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=mod -ldflags "${LDFLAGS}" -o /out/netbird-relay ./relay

FROM build AS build-client
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=mod -ldflags "${LDFLAGS}" -o /out/netbird ./client

FROM ubuntu:24.04 AS management
# sqlite3: scripts/seed*.sh read peer public keys from the store.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl sqlite3 && rm -rf /var/lib/apt/lists/*
COPY --from=build-management /out/netbird-mgmt /go/bin/netbird-mgmt
ENTRYPOINT ["/go/bin/netbird-mgmt", "management"]
CMD ["--log-file", "console"]

FROM gcr.io/distroless/base:debug AS signal
COPY --from=build-signal /out/netbird-signal /go/bin/netbird-signal
ENTRYPOINT ["/go/bin/netbird-signal", "run"]
CMD ["--log-file", "console"]

FROM gcr.io/distroless/base:debug AS relay
ENV NB_LOG_FILE=console
COPY --from=build-relay /out/netbird-relay /go/bin/netbird-relay
ENTRYPOINT ["/go/bin/netbird-relay"]

# Same as netbird's client/Dockerfile, plus curl/busybox-extras so peers can
# generate TCP/HTTP traffic for flow events.
FROM alpine:3.22 AS client
RUN apk add --no-cache bash ca-certificates ip6tables iproute2 iptables curl busybox-extras jq socat
ENV NETBIRD_BIN="/usr/local/bin/netbird" \
    NB_LOG_FILE="console,/var/log/netbird/client.log" \
    NB_DAEMON_ADDR="unix:///var/run/netbird.sock" \
    NB_ENTRYPOINT_SERVICE_TIMEOUT="5" \
    NB_ENTRYPOINT_LOGIN_TIMEOUT="1"
COPY client/netbird-entrypoint.sh /usr/local/bin/netbird-entrypoint.sh
COPY --from=build-client /out/netbird /usr/local/bin/netbird
ENTRYPOINT ["/usr/local/bin/netbird-entrypoint.sh"]
