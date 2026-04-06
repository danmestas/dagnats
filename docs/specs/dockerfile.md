# Dockerfile and Docker Compose

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (additive deployment artifact)

## Problem

There is no containerized deployment path. Production deployment is
undefined — no Dockerfile, no docker-compose, no systemd unit. This
blocks adoption by teams that use containers for deployment and by
anyone who wants to try dagnats without installing Go.

## Design

### 1. Dockerfile (Multi-Stage)

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/dagnats ./cmd/dagnats

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/dagnats /usr/local/bin/dagnats

# Default data directory inside container.
ENV DAGNATS_DATA_DIR=/data
VOLUME /data

EXPOSE 4222 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD wget -q --spider http://localhost:8080/health/telemetry || exit 1

ENTRYPOINT ["dagnats"]
CMD ["serve"]
```

Key decisions:
- **Alpine base**: Small image (~15MB compressed). No glibc dependency
  since CGO is disabled.
- **`/data` volume**: NATS JetStream storage. Must be persistent.
- **Health check**: Uses the existing `/health/telemetry` endpoint.
  Needs adjustment — see section 3.

### 2. Docker Compose (Development)

`docker-compose.yml` at repo root:

```yaml
services:
  dagnats:
    build: .
    ports:
      - "4222:4222"
      - "8080:8080"
    volumes:
      - dagnats-data:/data
    environment:
      DAGNATS_HTTP_ADDR: ":8080"
      DAGNATS_NATS_PORT: "4222"

volumes:
  dagnats-data:
```

### 3. Health Check Endpoint Adjustment

The current `GET /health/telemetry` always returns 200 with a JSON body
containing `"status": "healthy"` or `"status": "degraded"`. For
container health checks and load balancer probes, it should return:
- **200** when healthy
- **503** when degraded

Change in `internal/api/rest.go`:

```go
// Current: always 200
w.WriteHeader(http.StatusOK)

// Proposed: status-aware
if response.Status == "degraded" {
    w.WriteHeader(http.StatusServiceUnavailable)
} else {
    w.WriteHeader(http.StatusOK)
}
```

Also add a lightweight `GET /health` that returns just `{"status":"ok"}`
with 200 for simple liveness probes (no JetStream inspection). The
Dockerfile health check should use this simpler endpoint.

### 4. .dockerignore

```
.git
bin/
*.test
coverage.out
hello-world
ui/
docs/site/public/
.agents/
.claude/
```

### 5. Files Changed

| File | Change |
|------|--------|
| `Dockerfile` (new) | Multi-stage build |
| `docker-compose.yml` (new) | Dev compose with volume |
| `.dockerignore` (new) | Exclude build artifacts |
| `internal/api/rest.go` | Add `GET /health`, make `/health/telemetry` return 503 on degraded |
| `Makefile` | Add `docker-build` and `docker-run` targets |

### 6. Makefile Targets

```makefile
docker-build:
	docker build -t dagnats:latest .

docker-run:
	docker run -p 4222:4222 -p 8080:8080 -v dagnats-data:/data dagnats:latest
```
