# Observability Guide

DagNats has three observability modes. Pick the one that fits your
environment -- you can change modes without modifying application
code because all three consume the same OTLP telemetry.

## How It Works

Every DagNats component (engine, API, workers) emits traces,
metrics, and logs via the OpenTelemetry SDK. Telemetry always
flows to the internal NATS `TELEMETRY` stream. The question is
what _consumes_ that telemetry.

```
                  +-----------+
                  |  DagNats  |
                  |  (OTel    |
                  |   SDK)    |
                  +-----+-----+
                        |
                   OTLP/HTTP
                        |
          +-------------+-------------+
          |             |             |
     [Embedded]    [Distributed]  [External]
     Local Parquet  S3 Parquet    Production
     + DuckDB MCP   + DuckDB     OTel Collector
```

## Choosing a Mode

| | Embedded | Distributed | External |
|---|---|---|---|
| **Setup** | `sidecar install && sidecar start` | sidecar + S3 config | Set one env var |
| **Storage** | Local Parquet files | S3 Parquet files | Backend-managed |
| **Query** | DuckDB MCP (AI) | DuckDB CLI/WASM/MCP | Backend UI |
| **Cost** | Free | S3 storage only | Backend license |
| **Best for** | Dev, testing, AI debug | Team clusters | Production |
| **Retention** | Disk space | S3 lifecycle rules | Backend config |

---

## Mode 1: Embedded (dev, testing, AI analysis)

Run the sidecar alongside DagNats. It writes Parquet files to
disk and exposes them to AI tools via a DuckDB MCP server.

### Architecture

The sidecar supervises three child processes in dependency order:

1. **otlp2parquet** -- receives OTLP/HTTP, writes Parquet files
2. **otelcol** -- OpenTelemetry Collector that routes telemetry
   to otlp2parquet (and optionally to a backend)
3. **dagnats-mcp-duckdb** -- DuckDB over the Parquet files,
   exposed as an MCP server for AI tools

### Quick Start

```bash
# Install external binaries (otelcol, otlp2parquet)
dagnats sidecar install

# Start the sidecar (zero-config defaults)
dagnats sidecar start

# In another terminal, start DagNats pointed at the sidecar
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dagnats serve
```

Telemetry lands in `./telemetry-data/` as Parquet files. The
DuckDB MCP server reads them automatically.

### AI Analysis via MCP

The `dagnats-mcp-duckdb` server exposes seven query tools:

| Tool | Purpose |
|------|---------|
| `query_traces` | Search traces by service, operation, status, time range |
| `get_trace` | All spans for a specific trace ID |
| `query_logs` | Search logs by service, severity, body content |
| `query_metrics` | Search across gauge, sum, and histogram metrics |
| `get_errors` | Find error traces (StatusCode = 2) |
| `latency_percentiles` | Compute p50/p90/p95/p99 for a span name |
| `query_sql` | Raw SQL for ad-hoc queries |

Connect it to Claude Code, Cursor, or any MCP-compatible tool.
The AI can then analyze traces, find slow spans, correlate
errors, and compute latency distributions directly from your
telemetry.

#### Wire up the MCP server

The `dagnats-mcp-duckdb` binary runs over stdio. Install it and
register it with your client.

```bash
go install github.com/danmestas/dagnats/cmd/dagnats-mcp-duckdb@latest
```

**Claude Code:**

```bash
claude mcp add dagnats-telemetry -- dagnats-mcp-duckdb --data-dir ./telemetry-data
```

**Generic MCP client** (Cursor, Continue, anything that reads an
`mcp.json`-style config):

```json
{
  "mcpServers": {
    "dagnats-telemetry": {
      "command": "dagnats-mcp-duckdb",
      "args": ["--data-dir", "./telemetry-data"]
    }
  }
}
```

Point `--data-dir` at the directory the sidecar writes Parquet
files to (default `./telemetry-data`).

### Sidecar Config

```yaml
# dagnats-sidecar.yaml (optional -- defaults work out of the box)
listen: "0.0.0.0:4318"
storage:
  type: local
  local_path: ./telemetry-data
mcp:
  listen: ""  # empty = stdio transport
```

---

## Mode 2: Distributed (team, cluster, S3)

Multiple DagNats nodes write Parquet to shared S3 storage.
Anyone on the team can query the telemetry with DuckDB -- either
via the MCP server, a local DuckDB CLI, or DuckDB-WASM in the
browser.

### Architecture

```
  Node A          Node B          Node C
  (engine)        (engine)        (workers)
     |               |               |
  sidecar         sidecar         sidecar
     |               |               |
     +-------+-------+-------+-------+
             |               |
          S3 bucket      S3 bucket
        (traces/)       (metrics/)
             |               |
         +---+---+       +---+---+
         | DuckDB |      | DuckDB |
         | (any   |      | WASM   |
         | node)  |      | (browser)|
         +---------+     +---------+
```

### Config (each node)

```yaml
listen: "0.0.0.0:4318"
storage:
  type: s3
  s3:
    endpoint: https://s3.amazonaws.com
    bucket: my-team-telemetry
    region: us-east-1
```

### Querying

**From any node with the MCP server:**
```bash
dagnats sidecar start --config=sidecar-s3.yaml
# The DuckDB MCP server reads Parquet from S3
```

**From a local DuckDB CLI:**
```sql
SELECT TraceId, SpanName, Duration
FROM read_parquet('s3://my-team-telemetry/traces/*.parquet')
WHERE StatusCode = 2
ORDER BY Timestamp DESC
LIMIT 20;
```

**From DuckDB-WASM (browser):**
Point DuckDB-WASM at the same S3 bucket with appropriate CORS
and credentials. Full distributed tracing in the browser with
no backend server.

---

## Mode 3: External Collector (production)

Skip the sidecar entirely. Point DagNats at your production
OTel Collector (SigNoz, Grafana Tempo, Jaeger, Datadog, etc.).

### Config

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 dagnats serve
```

Or in `dagnats.yaml`:

```yaml
otlp_endpoint: http://collector:4318
```

### Hybrid: External + Sidecar

You can run both. The sidecar's OTel Collector forwards to your
production backend while also writing Parquet locally:

```yaml
# sidecar config
listen: "0.0.0.0:4318"
storage:
  type: local
  local_path: ./telemetry-data
backend:
  endpoint: http://production-collector:4318
  headers:
    Authorization: "Bearer <token>"
```

DagNats points at the sidecar (`localhost:4318`), which fans out
to both Parquet storage and the production backend.

---

## CLI Commands

```bash
dagnats sidecar start              # Start with defaults
dagnats sidecar start --config=X   # Start with config file
dagnats sidecar install            # Download otelcol + otlp2parquet
dagnats sidecar status             # Check binary availability
```

## Internal Telemetry Stream

Regardless of which mode you pick, all telemetry also flows
through the NATS `TELEMETRY` JetStream stream (7-day retention,
1 GB cap). You can always consume it directly:

```bash
nats sub "telemetry.spans.>"
nats sub "telemetry.metrics.>"
nats sub "telemetry.logs.>"
```

See [architecture/observability.md](architecture/observability.md)
for signal types, instrumentation points, and trace propagation
details.
