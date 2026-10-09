FROM alpine:3.20 AS health-probe
RUN apk --no-cache add curl && \
    curl -fsSL https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.28/grpc_health_probe-linux-amd64 \
         -o /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
# Only copy packages the flow server imports — changes to go-etl/, client/, etc. won't bust this layer.
COPY database/ ./database/
COPY *.go ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o /flow-server .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=health-probe /usr/local/bin/grpc_health_probe /usr/local/bin/grpc_health_probe
COPY --from=builder /flow-server /flow-server
ENTRYPOINT ["/flow-server"]
