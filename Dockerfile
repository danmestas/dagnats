# Multi-stage build for dagnats.
# Stage 1: build static binaries inside golang:alpine.
# Stage 2: minimal alpine runtime with just the binaries + ca-certs + tini.

FROM golang:1.26-alpine AS builder

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

# Cache module downloads in their own layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binaries. -trimpath strips local paths for reproducibility.
# -s -w drops the symbol/debug info; the version is injected via ldflags
# so `dagnats --version` reports the build's git describe output.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w -X github.com/danmestas/dagnats/cli.Version=${VERSION}" \
    -o /out/dagnats ./cmd/dagnats && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w -X github.com/danmestas/dagnats/cli.Version=${VERSION}" \
    -o /out/dagnats-api ./cmd/dagnats-api && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w -X github.com/danmestas/dagnats/cli.Version=${VERSION}" \
    -o /out/dagnats-engine ./cmd/dagnats-engine

# ---- runtime stage ----

FROM alpine:3.19

# ca-certificates: outbound TLS from triggers, observability exporters, etc.
# tini: PID-1 signal handler so SIGTERM reaches the binary in k8s/compose.
RUN apk add --no-cache ca-certificates tini && \
    addgroup -S dagnats && \
    adduser -S -G dagnats -h /home/dagnats dagnats

COPY --from=builder /out/dagnats         /usr/local/bin/dagnats
COPY --from=builder /out/dagnats-api     /usr/local/bin/dagnats-api
COPY --from=builder /out/dagnats-engine  /usr/local/bin/dagnats-engine

USER dagnats
WORKDIR /home/dagnats

# 4222: NATS client port (embedded server in `dagnats serve`)
# 8222: NATS HTTP monitor
# 8080: dagnats HTTP control plane
EXPOSE 4222 8222 8080

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/dagnats"]
CMD ["serve"]
