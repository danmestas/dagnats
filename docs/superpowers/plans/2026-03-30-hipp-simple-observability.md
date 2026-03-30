# Hipp-Simple Observability Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ~400 LOC of self-contained observability (tracing, metrics, logs) to DagNats using NATS JetStream as the sole backend, with an optional embedded Jaeger exporter.

**Architecture:** Extend the existing `observe/` interfaces with a `Tracer` + noop defaults, then implement all interfaces in `observe/simple/` using pure Go + nats.go. Telemetry publishes as JSON to a single `TELEMETRY` stream. An embedded Jaeger OTLP/HTTP goroutine optionally exports spans. Component constructors migrate from separate logger/metrics params to a single `*Telemetry` struct.

**Tech Stack:** Go, NATS JetStream, stdlib `net/http`, `crypto/rand`, `encoding/json`

**Spec:** `docs/superpowers/specs/2026-03-30-otel-nats-observability-design.md`

**Coding rules (from CLAUDE.md):**
- Red-green TDD: failing test first, minimal code to pass, refactor
- Minimum 2 assertions per test (positive + negative space)
- All errors handled, no `_ = err`
- Minimum 2 assertions per function for programmer errors (panic on invariant violations)
- Functions must not exceed 70 lines
- 100-column line limit
- Each test file opens with methodology comment
- Bounded timeouts on all test waits
- No shared NATS servers between tests

---

## File Structure

### New Files

| File | Responsibility | LOC |
|------|---------------|-----|
| `observe/tracer.go` | Tracer, Span interfaces + SpanOption, Attribute types | ~70 |
| `observe/simple/types.go` | SpanRecord, MetricPoint, LogRecord data structs | ~50 |
| `observe/simple/trace_collector.go` | TraceCollector implements observe.Tracer | ~80 |
| `observe/simple/metrics_collector.go` | MetricsCollector implements observe.Metrics | ~60 |
| `observe/simple/log_collector.go` | LogCollector implements observe.Logger | ~50 |
| `observe/simple/error_reporter.go` | ErrorReporter implements observe.ErrorReporter | ~30 |
| `observe/simple/propagation.go` | W3C traceparent inject/extract for NATS + Event | ~40 |
| `observe/simple/jaeger.go` | Embedded Jaeger OTLP/HTTP exporter | ~70 |
| `observe/simple/monitor.go` | StorageMonitor — stream stats + advisories | ~40 |
| `observe/simple/setup.go` | SetupTelemetry + Telemetry struct | ~30 |
| `observe/tracer_test.go` | Tests for Tracer noop + Attribute constructors | ~40 |
| `observe/simple/types_test.go` | JSON round-trip tests for data structs | ~50 |
| `observe/simple/trace_collector_test.go` | Unit + integration tests for TraceCollector | ~80 |
| `observe/simple/metrics_collector_test.go` | Unit + integration tests for MetricsCollector | ~60 |
| `observe/simple/log_collector_test.go` | Unit + integration tests for LogCollector | ~50 |
| `observe/simple/propagation_test.go` | W3C traceparent inject/extract round-trip tests | ~50 |
| `observe/simple/jaeger_test.go` | Jaeger exporter tests with mock HTTP server | ~70 |
| `observe/simple/error_reporter_test.go` | ErrorReporter span/logger fallback tests | ~40 |
| `observe/simple/monitor_test.go` | StorageMonitor advisory tests | ~40 |
| `observe/simple/setup_test.go` | SetupTelemetry zero-config + noop fallback tests | ~40 |

### Modified Files

| File | Changes |
|------|---------|
| `observe/noop.go` | Add `noopTracer`, `noopSpan`, `NewNoopTracer()` |
| `protocol/protocol.go` | Add `TraceParent`, `TraceState` fields to `Event` |
| `natsutil/conn.go` | Add `SetupTelemetryStream`, call from `SetupAll` |
| `engine/orchestrator.go` | Change constructor to accept `*observe.Telemetry` |
| `worker/worker.go` | Change constructor to accept `*observe.Telemetry` |
| `api/service.go` | Change constructor to accept `*observe.Telemetry` |
| `cmd/dagnats-engine/main.go` | Call `SetupTelemetry`, pass to orchestrator |
| `cmd/dagnats-api/main.go` | Call `SetupTelemetry`, pass to service |
| `engine/orchestrator_test.go` | Update constructor calls |
| `worker/worker_test.go` | Update constructor calls |
| `api/service_test.go` | Update constructor calls |
| `api/rest_test.go` | Update constructor calls |
| `api/natsapi_test.go` | Update constructor calls |
| `e2e_test.go` | Update constructor calls |

---

## Chunk 1: Foundation — Interfaces, Noop, Protocol, NATS Stream

### Task 1: Add Tracer Interface to observe/

**Files:**
- Create: `observe/tracer.go`
- Create: `observe/tracer_test.go`
- Modify: `observe/noop.go`

- [ ] **Step 1: Write the failing test for Tracer noop**

```go
// observe/tracer_test.go
// Tests for Tracer interface, Span interface, noop implementations, and
// Attribute constructors. Methodology: verify compile-time interface
// satisfaction, runtime safety of noop, and typed attribute construction.
package observe

import (
	"context"
	"testing"
)

func TestNoopTracerSatisfiesInterface(t *testing.T) {
	var tracer Tracer = NewNoopTracer()
	if tracer == nil {
		t.Fatal("NewNoopTracer returned nil")
	}
	ctx, span := tracer.Start(context.Background(), "test-span")
	if ctx == nil {
		t.Fatal("Start returned nil context")
	}
	if span == nil {
		t.Fatal("Start returned nil span")
	}
	// Span methods must not panic on noop
	span.SetStatus(StatusOK, "")
	span.SetAttributes(StringAttr("key", "val"))
	span.RecordError(nil)
	span.AddEvent("evt")
	span.End()
}

func TestAttributeConstructors(t *testing.T) {
	s := StringAttr("k", "v")
	if s.Key != "k" || s.Value != "v" {
		t.Fatalf("StringAttr = %+v, want key=k val=v", s)
	}
	i := Int64Attr("n", 42)
	if i.Key != "n" || i.Value != int64(42) {
		t.Fatalf("Int64Attr = %+v, want key=n val=42", i)
	}
	f := Float64Attr("f", 3.14)
	if f.Key != "f" || f.Value != 3.14 {
		t.Fatalf("Float64Attr = %+v, want key=f val=3.14", f)
	}
	b := BoolAttr("ok", true)
	if b.Key != "ok" || b.Value != true {
		t.Fatalf("BoolAttr = %+v, want key=ok val=true", b)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/ -run TestNoopTracer -v`
Expected: FAIL — `NewNoopTracer` undefined

- [ ] **Step 3: Write the Tracer interface and types**

Create `observe/tracer.go` with:
- `Tracer` interface (Start method)
- `Span` interface (End, SetStatus, SetAttributes, RecordError, AddEvent)
- `StatusCode` type with `StatusOK`, `StatusError` constants
- `SpanKind` type with `SpanKindInternal`, `SpanKindServer`, `SpanKindClient`
- `SpanOption` interface (sealed)
- `spanKindOption` and `attrsOption` private types implementing `SpanOption`
- `WithSpanKind(kind)` and `WithAttributes(attrs...)` constructors
- `Attribute` struct with `Key string`, `Value any`
- `StringAttr`, `Int64Attr`, `Float64Attr`, `BoolAttr` constructors

All in `package observe`. stdlib only — no external imports.

- [ ] **Step 4: Add noop implementations**

Add to `observe/noop.go`:
- `noopTracer` struct implementing `Tracer`
- `noopSpan` struct implementing `Span`
- `NewNoopTracer() Tracer`
- `noopTracer.Start` returns the same context + `&noopSpan{}`
- All `noopSpan` methods are empty

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./observe/ -v`
Expected: ALL PASS (existing tests still pass too)

- [ ] **Step 6: Commit**

```bash
git add observe/tracer.go observe/tracer_test.go observe/noop.go
git commit -m "feat(observe): add Tracer/Span interfaces with noop defaults"
```

---

### Task 2: Add TraceParent/TraceState to protocol.Event

**Files:**
- Modify: `protocol/protocol.go`
- Modify: `protocol/protocol_test.go`

- [ ] **Step 1: Write the failing test**

Add to `protocol/protocol_test.go`:

```go
func TestEventTraceContextOmitEmpty(t *testing.T) {
	// Events without trace context should serialize identically to before
	evt := NewWorkflowEvent(EventWorkflowStarted, "run-1", nil)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// trace_parent and trace_state should NOT appear in JSON
	if bytes.Contains(data, []byte("trace_parent")) {
		t.Fatal("empty TraceParent should be omitted from JSON")
	}

	// Events WITH trace context should include both fields
	evt.TraceParent = "00-abc-def-01"
	evt.TraceState = "vendor=value"
	data, err = evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal with trace: %v", err)
	}
	if !bytes.Contains(data, []byte(`"trace_parent"`)) {
		t.Fatal("non-empty TraceParent should appear in JSON")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./protocol/ -run TestEventTraceContext -v`
Expected: FAIL — `evt.TraceParent` undefined

- [ ] **Step 3: Add fields to Event struct**

In `protocol/protocol.go`, add to the `Event` struct:

```go
TraceParent string `json:"trace_parent,omitempty"`
TraceState  string `json:"trace_state,omitempty"`
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./protocol/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add protocol/protocol.go protocol/protocol_test.go
git commit -m "feat(protocol): add TraceParent/TraceState to Event"
```

---

### Task 3: Add TELEMETRY stream to natsutil

**Files:**
- Modify: `natsutil/conn.go`
- Modify: `natsutil/conn_test.go`

- [ ] **Step 1: Write the failing test**

Add to `natsutil/conn_test.go`:

```go
func TestSetupTelemetryStream(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	err = SetupTelemetryStream(js)
	if err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}
	info, err := js.StreamInfo("TELEMETRY")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.Config.MaxAge != 7*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 7d", info.Config.MaxAge)
	}
	if info.Config.MaxBytes != 1<<30 {
		t.Fatalf("MaxBytes = %d, want 1GB", info.Config.MaxBytes)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./natsutil/ -run TestSetupTelemetryStream -v`
Expected: FAIL — `SetupTelemetryStream` undefined

- [ ] **Step 3: Write SetupTelemetryStream**

Add to `natsutil/conn.go`:

```go
// SetupTelemetryStream creates the TELEMETRY stream for all observability
// signals (spans, metrics, logs). 7-day retention, 1GB cap, 5s dedup window.
func SetupTelemetryStream(js nats.JetStreamContext) error {
	if js == nil {
		panic("SetupTelemetryStream: js must not be nil")
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TELEMETRY",
		Subjects:   []string{"telemetry.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		MaxAge:     7 * 24 * time.Hour,
		MaxBytes:   1 << 30,
		Duplicates: 5 * time.Second,
	})
	return err
}
```

Add `"time"` import. Call `SetupTelemetryStream(js)` at the end of `SetupAll`,
after `SetupKVBuckets`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./natsutil/ -v`
Expected: ALL PASS

- [ ] **Step 5: Verify SetupAll includes telemetry**

Run: `go test ./natsutil/ -run TestSetupAll -v`
Expected: PASS (SetupAll now creates 4 streams + 2 KV buckets)

- [ ] **Step 6: Commit**

```bash
git add natsutil/conn.go natsutil/conn_test.go
git commit -m "feat(natsutil): add TELEMETRY stream to SetupAll"
```

---

### Task 4: Add Telemetry struct to observe/

The `Telemetry` struct bundles all observe interfaces into a single argument
for component constructors. It lives in `observe/` (not `observe/simple/`)
so components can reference it without importing the simple adapter.

**NOTE:** The spec places `Telemetry` in `observe/simple/setup.go`. We move it
to `observe/observe.go` to prevent `engine/`, `worker/`, `api/` from importing
`observe/simple/` directly — which would violate the spec's own dependency rule
(spec line 718-719). The spec should be updated to reflect this.

**Files:**
- Modify: `observe/observe.go`
- Modify: `observe/observe_test.go`

- [ ] **Step 1: Write the failing test**

Add to `observe/observe_test.go`:

```go
func TestNewNoopTelemetry(t *testing.T) {
	tel := NewNoopTelemetry()
	if tel == nil {
		t.Fatal("NewNoopTelemetry returned nil")
	}
	if tel.Tracer == nil || tel.Logger == nil ||
		tel.Metrics == nil || tel.Errors == nil {
		t.Fatal("NewNoopTelemetry has nil fields")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/ -run TestNewNoopTelemetry -v`
Expected: FAIL — `NewNoopTelemetry` undefined

- [ ] **Step 3: Add the Telemetry struct**

Add to the end of `observe/observe.go`:

```go
// Telemetry bundles all observability interfaces. Passed as a single
// argument to component constructors instead of separate parameters.
// All fields must be non-nil — use Noop constructors for safe defaults.
type Telemetry struct {
	Tracer  Tracer
	Logger  Logger
	Metrics Metrics
	Errors  ErrorReporter
}

// NewNoopTelemetry returns a Telemetry with all noop implementations.
// Safe default for tests and environments where telemetry is not needed.
func NewNoopTelemetry() *Telemetry {
	return &Telemetry{
		Tracer:  NewNoopTracer(),
		Logger:  NewNoopLogger(),
		Metrics: NewNoopMetrics(),
		Errors:  NewNoopErrorReporter(),
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./observe/ -v`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add observe/observe.go observe/observe_test.go
git commit -m "feat(observe): add Telemetry struct bundling all interfaces"
```

---

## Chunk 2: Simple Implementation — Data Types, Collectors, Propagation

### Task 5: Add telemetry data types

**Files:**
- Create: `observe/simple/types.go`
- Create: `observe/simple/types_test.go`

- [ ] **Step 1: Write the failing test**

```go
// observe/simple/types_test.go
// Tests for telemetry wire-format types. Methodology: verify JSON
// round-trip fidelity for SpanRecord, MetricPoint, LogRecord.
// Each test asserts both successful deserialization and field correctness.
package simple

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSpanRecordJSONRoundTrip(t *testing.T) {
	original := SpanRecord{
		TraceID:    "abc123",
		SpanID:     "def456",
		ParentID:   "parent1",
		Name:       "test.span",
		Service:    "engine",
		Kind:       "internal",
		StartTime:  time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 3, 30, 0, 0, 1, 0, time.UTC),
		DurationMS: 1000,
		Status:     "ok",
		Attributes: map[string]any{"run_id": "r1"},
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded SpanRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.TraceID != original.TraceID {
		t.Fatalf("TraceID = %q, want %q", decoded.TraceID, original.TraceID)
	}
	if decoded.DurationMS != 1000 {
		t.Fatalf("DurationMS = %d, want 1000", decoded.DurationMS)
	}
}

func TestMetricPointJSONRoundTrip(t *testing.T) {
	original := MetricPoint{
		Name:    "step.duration_ms",
		Type:    "histogram",
		Value:   42.5,
		Tags:    map[string]string{"task": "llm-coder"},
		Service: "worker",
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded MetricPoint
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Name != "step.duration_ms" {
		t.Fatalf("Name = %q, want step.duration_ms", decoded.Name)
	}
	if decoded.Value != 42.5 {
		t.Fatalf("Value = %f, want 42.5", decoded.Value)
	}
}

func TestLogRecordJSONRoundTrip(t *testing.T) {
	original := LogRecord{
		Level:   "error",
		Message: "step failed",
		Service: "engine",
		TraceID: "trace1",
		SpanID:  "span1",
		Error:   "connection refused",
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded LogRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Level != "error" {
		t.Fatalf("Level = %q, want error", decoded.Level)
	}
	if decoded.Error != "connection refused" {
		t.Fatalf("Error = %q, want connection refused", decoded.Error)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestSpanRecord -v`
Expected: FAIL — package does not exist yet

- [ ] **Step 3: Write the data types**

Create `observe/simple/types.go` with `SpanRecord`, `SpanEvent`, `MetricPoint`,
`LogRecord` structs exactly matching the spec (lines 80-135 of spec). Include
JSON tags with `omitempty` for optional fields. Add `Timestamp` fields with
`time.Time` type. All structs in `package simple`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./observe/simple/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add observe/simple/types.go observe/simple/types_test.go
git commit -m "feat(observe/simple): add telemetry data types"
```

---

### Task 6: W3C trace context propagation

**Files:**
- Create: `observe/simple/propagation.go`
- Create: `observe/simple/propagation_test.go`

- [ ] **Step 1: Write the failing test**

```go
// observe/simple/propagation_test.go
// Tests for W3C traceparent propagation. Methodology: verify inject/extract
// round-trip for both NATS headers and protocol.Event fields. Assert both
// locations are written atomically.
package simple

import (
	"testing"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestGenerateTraceID(t *testing.T) {
	id := generateTraceID()
	if len(id) != 32 {
		t.Fatalf("trace ID len = %d, want 32 hex chars", len(id))
	}
	id2 := generateTraceID()
	if id == id2 {
		t.Fatal("two generated trace IDs should not be equal")
	}
}

func TestGenerateSpanID(t *testing.T) {
	id := generateSpanID()
	if len(id) != 16 {
		t.Fatalf("span ID len = %d, want 16 hex chars", len(id))
	}
	id2 := generateSpanID()
	if id == id2 {
		t.Fatal("two generated span IDs should not be equal")
	}
}

func TestFormatTraceparent(t *testing.T) {
	tp := formatTraceparent("aaa", "bbb")
	if tp != "00-aaa-bbb-01" {
		t.Fatalf("traceparent = %q, want 00-aaa-bbb-01", tp)
	}
}

func TestParseTraceparent(t *testing.T) {
	traceID, spanID, ok := parseTraceparent("00-abc123-def456-01")
	if !ok {
		t.Fatal("parseTraceparent returned false for valid input")
	}
	if traceID != "abc123" || spanID != "def456" {
		t.Fatalf("got trace=%q span=%q, want abc123/def456",
			traceID, spanID)
	}
	_, _, ok = parseTraceparent("invalid")
	if ok {
		t.Fatal("parseTraceparent should return false for invalid")
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	msg := &nats.Msg{Header: nats.Header{}}
	evt := &protocol.Event{RunID: "run-1", Type: protocol.EventStepStarted}
	traceID := generateTraceID()
	spanID := generateSpanID()

	injectTraceparent(msg, evt, traceID, spanID)

	// Both locations should have the value
	if evt.TraceParent == "" {
		t.Fatal("TraceParent not set on event")
	}
	if msg.Header.Get("traceparent") == "" {
		t.Fatal("traceparent not set on NATS header")
	}

	// Extract should recover the IDs
	gotTrace, gotSpan, ok := extractTraceparent(msg, evt)
	if !ok {
		t.Fatal("extractTraceparent returned false")
	}
	if gotTrace != traceID || gotSpan != spanID {
		t.Fatalf("extract got %q/%q, want %q/%q",
			gotTrace, gotSpan, traceID, spanID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestGenerate -v`
Expected: FAIL — functions undefined

- [ ] **Step 3: Write propagation.go**

Create `observe/simple/propagation.go` with:
- `generateTraceID() string` — 16 bytes from crypto/rand, hex-encoded (32 chars)
- `generateSpanID() string` — 8 bytes from crypto/rand, hex-encoded (16 chars)
- `formatTraceparent(traceID, spanID string) string` — `"00-" + traceID + "-" + spanID + "-01"`
- `parseTraceparent(tp string) (traceID, spanID string, ok bool)` — split on `-`, validate 4 parts
- `injectTraceparent(msg *nats.Msg, evt *protocol.Event, traceID, spanID string)` — writes to both locations atomically
- `extractTraceparent(msg *nats.Msg, evt *protocol.Event) (traceID, spanID string, ok bool)` — reads headers first, falls back to event

All functions are package-private except `InjectTraceContext` and `ExtractTraceContext` which are the public API (wrapping context operations around the above). Those require context key types for storing span info — implement in same file.

- [ ] **Step 4: Run tests**

Run: `go test ./observe/simple/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add observe/simple/propagation.go observe/simple/propagation_test.go
git commit -m "feat(observe/simple): add W3C trace context propagation"
```

---

### Task 7: MetricsCollector

**Files:**
- Create: `observe/simple/metrics_collector.go`
- Create: `observe/simple/metrics_collector_test.go`

- [ ] **Step 1: Write the failing integration test**

```go
// observe/simple/metrics_collector_test.go
// Tests for MetricsCollector. Methodology: verify that Counter, Histogram,
// Gauge publish correct JSON MetricPoints to NATS telemetry subjects.
// Uses real embedded NATS — no mocks.
package simple

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestMetricsCollectorCounter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	mc := NewMetricsCollector(js, "test-svc")
	sub, err := js.SubscribeSync("telemetry.metrics.>",
		nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	counter := mc.Counter("api.requests", map[string]string{"ep": "/runs"})
	counter.Inc()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var point MetricPoint
	if err := json.Unmarshal(msg.Data, &point); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if point.Name != "api.requests" {
		t.Fatalf("Name = %q, want api.requests", point.Name)
	}
	if point.Value != 1.0 {
		t.Fatalf("Value = %f, want 1.0", point.Value)
	}
	if point.Type != "counter" {
		t.Fatalf("Type = %q, want counter", point.Type)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestMetricsCollectorCounter -v -timeout 10s`
Expected: FAIL — `NewMetricsCollector` undefined

- [ ] **Step 3: Write MetricsCollector**

Create `observe/simple/metrics_collector.go`:
- `MetricsCollector` struct with `js` and `serviceName`
- Implements `observe.Metrics` — `Counter()`, `Histogram()`, `Gauge()` return wrappers
- `simpleCounter` — on `Inc()` / `Add()`, marshal `MetricPoint{Type: "counter"}` and publish to `telemetry.metrics.{service}.{name}`
- `simpleHistogram` — on `Observe()`, publish `MetricPoint{Type: "histogram"}`
- `simpleGauge` — tracks value in atomic, on `Set()` / `Inc()` / `Dec()`, publish `MetricPoint{Type: "gauge"}`
- On publish error: log to stderr, continue (never propagate)

Assertions: `NewMetricsCollector` panics on nil `js` or empty `serviceName`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./observe/simple/ -run TestMetricsCollector -v -timeout 10s`
Expected: PASS

- [ ] **Step 5: Add histogram and gauge tests**

Write `TestMetricsCollectorHistogram` and `TestMetricsCollectorGauge` following
the same pattern as counter. Each asserts correct `Type` and `Value` in the
published MetricPoint.

- [ ] **Step 6: Run all tests**

Run: `go test ./observe/simple/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add observe/simple/metrics_collector.go observe/simple/metrics_collector_test.go
git commit -m "feat(observe/simple): add MetricsCollector"
```

---

### Task 8: TraceCollector

**Files:**
- Create: `observe/simple/trace_collector.go`
- Create: `observe/simple/trace_collector_test.go`

- [ ] **Step 1: Write the failing unit test**

Test that `LiveSpan.End()` produces a correct `SpanRecord`:

```go
// observe/simple/trace_collector_test.go
// Tests for TraceCollector. Methodology: unit tests for span lifecycle
// without NATS, integration tests with real embedded NATS for publish
// verification. Each test asserts positive result + negative space.
package simple

// ... imports ...

func TestLiveSpanProducesSpanRecord(t *testing.T) {
	records := make(chan SpanRecord, 10)
	span := &LiveSpan{
		traceID:   "trace-1",
		spanID:    "span-1",
		name:      "test.op",
		service:   "engine",
		kind:      "internal",
		startTime: time.Now(),
		records:   records,
	}
	span.SetAttributes(observe.StringAttr("run_id", "r1"))
	span.SetStatus(observe.StatusOK, "")
	span.End()

	select {
	case rec := <-records:
		if rec.TraceID != "trace-1" {
			t.Fatalf("TraceID = %q, want trace-1", rec.TraceID)
		}
		if rec.DurationMS <= 0 {
			t.Fatal("DurationMS should be > 0")
		}
		if rec.Attributes["run_id"] != "r1" {
			t.Fatalf("run_id attr = %v, want r1", rec.Attributes["run_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("no SpanRecord received within 1s")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestLiveSpan -v`
Expected: FAIL — `LiveSpan` undefined

- [ ] **Step 3: Write TraceCollector and LiveSpan**

Create `observe/simple/trace_collector.go`:
- `TraceCollector` struct: `js`, `metrics`, `serviceName`, `records chan SpanRecord` (cap 1024)
- `NewTraceCollector(js, serviceName, metrics)` — panics on nil js/empty name, starts background publish goroutine
- `Start(ctx, name, opts...)` — generates trace/span IDs, extracts parent from context, returns `(ctxWithSpan, &LiveSpan{...})`
- `Flush()` — blocks until channel drained or 5s timeout
- `LiveSpan` struct: stores trace/span IDs, name, service, kind, startTime, attributes, events, status, error, `records chan`
- `LiveSpan.End()` — calculates duration, builds `SpanRecord`, sends to `records` channel. If channel full, increment `telemetry.spans.dropped` counter.
- `LiveSpan.SetAttributes()`, `SetStatus()`, `RecordError()`, `AddEvent()` — mutate internal state
- Background goroutine: reads from `records`, marshals to JSON, publishes to `telemetry.spans.{service}.{run_id}` (extracts run_id from attributes, falls back to "no-run")

Context key: private type `spanContextKey` to store/retrieve active span from context.

- [ ] **Step 4: Run unit test**

Run: `go test ./observe/simple/ -run TestLiveSpan -v`
Expected: PASS

- [ ] **Step 5: Write integration test with real NATS**

```go
func TestTraceCollectorPublishesToNATS(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	metrics := observe.NewNoopMetrics()

	tc := NewTraceCollector(js, "test-svc", metrics)
	defer tc.Flush()

	sub, err := js.SubscribeSync("telemetry.spans.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	_, span := tc.Start(ctx, "integration.test",
		observe.WithAttributes(observe.StringAttr("run_id", "r42")))
	span.End()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var rec SpanRecord
	json.Unmarshal(msg.Data, &rec)
	if rec.Name != "integration.test" {
		t.Fatalf("Name = %q, want integration.test", rec.Name)
	}
	// Verify subject includes run_id
	if msg.Subject != "telemetry.spans.test-svc.r42" {
		t.Fatalf("Subject = %q, want telemetry.spans.test-svc.r42",
			msg.Subject)
	}
}
```

- [ ] **Step 6: Write dedup integration test**

```go
func TestTraceCollectorDedup(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// Publish same span twice (same Nats-Msg-Id)
	rec := SpanRecord{
		TraceID: "t1", SpanID: "s1", Name: "dup-test",
		Service: "test", Status: "ok",
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	msgID := rec.TraceID + "." + rec.SpanID
	js.Publish("telemetry.spans.test.r1", data, nats.MsgId(msgID))
	js.Publish("telemetry.spans.test.r1", data, nats.MsgId(msgID))

	// Only one message should be in the stream
	info, err := js.StreamInfo("TELEMETRY")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream has %d msgs, want 1 (dedup failed)",
			info.State.Msgs)
	}
}
```

- [ ] **Step 7: Run all tests**

Run: `go test ./observe/simple/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add observe/simple/trace_collector.go observe/simple/trace_collector_test.go
git commit -m "feat(observe/simple): add TraceCollector with async NATS publish"
```

---

### Task 9: LogCollector

**Files:**
- Create: `observe/simple/log_collector.go`
- Create: `observe/simple/log_collector_test.go`

- [ ] **Step 1: Write the failing integration test**

```go
// observe/simple/log_collector_test.go
// Tests for LogCollector. Methodology: verify that Info/Error calls
// publish correct JSON LogRecords to NATS telemetry subjects.
// Uses real embedded NATS. Asserts field correctness + correct subject.
package simple

// ... imports ...

func TestLogCollectorInfo(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	lc := NewLogCollector(js, "test-svc")
	sub, err := js.SubscribeSync("telemetry.logs.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	lc.Info("step completed", observe.String("step_id", "s1"))

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var rec LogRecord
	json.Unmarshal(msg.Data, &rec)
	if rec.Level != "info" {
		t.Fatalf("Level = %q, want info", rec.Level)
	}
	if rec.Message != "step completed" {
		t.Fatalf("Message = %q, want 'step completed'", rec.Message)
	}
	if msg.Subject != "telemetry.logs.test-svc.info" {
		t.Fatalf("Subject = %q, want telemetry.logs.test-svc.info",
			msg.Subject)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestLogCollector -v -timeout 10s`
Expected: FAIL — `NewLogCollector` undefined

- [ ] **Step 3: Write LogCollector**

Create `observe/simple/log_collector.go`:
- `LogCollector` struct: `js`, `serviceName`, `fields []observe.Field` (for With)
- `NewLogCollector(js, serviceName)` — panics on nil js/empty name
- `Info(msg, fields...)` — build `LogRecord{Level: "info"}`, marshal, publish to `telemetry.logs.{service}.info`
- `Error(msg, err, fields...)` — same with `Level: "error"`, set `Error` field
- `With(fields...)` — returns new `LogCollector` with prepended fields (for trace context correlation)
- On publish error: write to stderr, never propagate

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./observe/simple/ -run TestLogCollector -v -timeout 10s`
Expected: PASS

- [ ] **Step 5: Add test for With + Error**

Test that `logger.With(traceID, spanID).Error(...)` produces a LogRecord with
trace context fields and the error string. Assert subject is `telemetry.logs.{svc}.error`.

- [ ] **Step 6: Commit**

```bash
git add observe/simple/log_collector.go observe/simple/log_collector_test.go
git commit -m "feat(observe/simple): add LogCollector with NATS publish"
```

---

### Task 10: ErrorReporter

**Files:**
- Create: `observe/simple/error_reporter.go`
- Create: `observe/simple/error_reporter_test.go`

- [ ] **Step 1: Write the failing test**

```go
// observe/simple/error_reporter_test.go
// Tests for ErrorReporter. Methodology: verify span-aware error capture
// falls back to logger when no active span exists. Uses TraceCollector
// to create real spans and verifies both code paths.
package simple

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/observe"
)

func TestErrorReporterWithActiveSpan(t *testing.T) {
	records := make(chan SpanRecord, 10)
	tc := &TraceCollector{
		serviceName: "test", records: records,
		metrics: observe.NewNoopMetrics(),
	}
	reporter := NewErrorReporter(tc, observe.NewNoopLogger())

	ctx, span := tc.Start(context.Background(), "test.op")
	reporter.CaptureError(ctx, errors.New("boom"),
		map[string]string{"step": "s1"})
	span.End()

	select {
	case rec := <-records:
		if rec.Error != "boom" {
			t.Fatalf("Error = %q, want boom", rec.Error)
		}
		if rec.Status != "error" {
			t.Fatalf("Status = %q, want error", rec.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("no SpanRecord received")
	}
}

func TestErrorReporterWithoutSpanFallsBackToLogger(t *testing.T) {
	reporter := NewErrorReporter(
		observe.NewNoopTracer(), observe.NewNoopLogger())
	// Should not panic when no active span in context
	reporter.CaptureError(context.Background(),
		errors.New("no-span"), nil)
	reporter.CaptureMessage(context.Background(),
		"test msg", observe.LevelError)
	// Positive: no panic occurred
	// Negative: error was handled (via logger fallback, not lost)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestErrorReporter -v`
Expected: FAIL — `NewErrorReporter` undefined

- [ ] **Step 3: Write ErrorReporter**

Create `observe/simple/error_reporter.go`:
- `errorReporter` struct: `tracer observe.Tracer`, `logger observe.Logger`
- `NewErrorReporter(tracer, logger)` — panics on nil tracer/logger
- `CaptureError(ctx, err, tags)` — extract span from ctx. If found: `span.RecordError(err)` + `span.SetStatus(StatusError, err.Error())`. If not found: `logger.Error(err.Error(), err, tagsAsFields...)`.
- `CaptureMessage(ctx, msg, level)` — extract span from ctx. If found: `span.AddEvent(msg)`. If not found: `logger.Info(msg)` or `logger.Error(msg, nil)` based on level.

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add observe/simple/error_reporter.go observe/simple/error_reporter_test.go
git commit -m "feat(observe/simple): add ErrorReporter with span/logger fallback"
```

---

## Chunk 3: Export, Monitor, Setup

### Task 11: Jaeger exporter

**Files:**
- Create: `observe/simple/jaeger.go`
- Create: `observe/simple/jaeger_test.go`

- [ ] **Step 1: Write the failing test with mock HTTP server**

```go
// observe/simple/jaeger_test.go
// Tests for Jaeger OTLP/HTTP exporter. Methodology: real embedded NATS +
// mock HTTP server. Verify batching, error handling, and shutdown behavior.
package simple

// ... imports (net/http/httptest, sync/atomic) ...

func TestExportToJaegerHappyPath(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received.Add(1)
			w.WriteHeader(http.StatusOK)
		},
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	logger := observe.NewNoopLogger()
	go ExportToJaeger(ctx, js, srv.URL, logger)

	// Publish a span to the TELEMETRY stream
	rec := SpanRecord{
		TraceID: "t1", SpanID: "s1", Name: "test",
		Service: "engine", Status: "ok",
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish("telemetry.spans.engine.r1", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for exporter to batch and send
	deadline := time.After(10 * time.Second)
	for received.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("exporter did not POST within 10s")
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()

	if received.Load() < 1 {
		t.Fatal("expected at least 1 POST to Jaeger")
	}
}

func TestExportToJaegerHandlesFailure(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	// Server returns 500 but tracks requests
	var requestCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		},
	))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 6*time.Second)
	defer cancel()

	logger := observe.NewNoopLogger()
	go ExportToJaeger(ctx, js, srv.URL, logger)

	rec := SpanRecord{
		TraceID: "t1", SpanID: "s1", Name: "test",
		Service: "engine", Status: "ok",
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish("telemetry.spans.engine.r1", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for exporter to attempt the POST
	deadline := time.After(5 * time.Second)
	for requestCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("exporter did not attempt POST within 5s")
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()

	// Positive: exporter attempted the export despite 500
	if requestCount.Load() < 1 {
		t.Fatal("exporter should have attempted at least 1 POST")
	}
	// Negative: no panic occurred — exporter handled error gracefully
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./observe/simple/ -run TestExportToJaeger -v -timeout 30s`
Expected: FAIL — `ExportToJaeger` undefined

- [ ] **Step 3: Write Jaeger exporter**

Create `observe/simple/jaeger.go`:
- `ExportToJaeger(ctx, js, endpoint, logger)` function
- Subscribe to `telemetry.spans.>` on TELEMETRY stream with `nats.DeliverNew()`
- Batch: collect up to 100 spans or 5s timeout
- Convert batch to OTLP JSON traces format (simplified: array of resource spans)
- POST to `{endpoint}/v1/traces` with `Content-Type: application/json`
- On HTTP error: `logger.Error(...)`, drop batch, continue
- On ctx cancel: flush remaining batch, return
- HTTP client with 10s timeout (bounded)
- Assertions: panic on empty endpoint, nil js, nil logger

- [ ] **Step 4: Run tests**

Run: `go test ./observe/simple/ -run TestExportToJaeger -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add observe/simple/jaeger.go observe/simple/jaeger_test.go
git commit -m "feat(observe/simple): add embedded Jaeger OTLP/HTTP exporter"
```

---

### Task 12: StorageMonitor

**Files:**
- Create: `observe/simple/monitor.go`
- Create: `observe/simple/monitor_test.go`

- [ ] **Step 1: Write the failing test**

Test that StorageMonitor publishes an advisory when stream exceeds threshold.

```go
// observe/simple/monitor_test.go
// Tests for StorageMonitor. Methodology: real embedded NATS with a tiny
// stream to trigger threshold quickly. Asserts advisory message content.
package simple

// ... imports ...

func TestStorageMonitorPublishesAdvisory(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	// Create a tiny TELEMETRY stream (1KB max)
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "TELEMETRY", Subjects: []string{"telemetry.>"},
		MaxBytes: 1024, Storage: nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("AddStream: %v", err)
	}

	// Subscribe to advisories
	sub, err := nc.SubscribeSync("alerts.storage.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Fill the stream past 80%
	bigPayload := make([]byte, 900)
	js.Publish("telemetry.spans.test.r1", bigPayload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon := NewStorageMonitor(js, 100*time.Millisecond, 0.8)
	go mon.Start(ctx)

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no advisory received: %v", err)
	}
	if !bytes.Contains(msg.Data, []byte("TELEMETRY")) {
		t.Fatal("advisory should mention TELEMETRY stream")
	}
	cancel()
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write StorageMonitor**

Create `observe/simple/monitor.go`:
- `StorageMonitor` struct: `js`, `interval`, `warnRatio float64`
- `NewStorageMonitor(js, interval, warnRatio)` — panics on nil js
- `Start(ctx)` — ticker loop, calls `js.StreamInfo("TELEMETRY")`, if `usage/max > warnRatio`, publishes JSON advisory to `alerts.storage.TELEMETRY`. Exits on ctx cancel.
- `Status() TelemetryHealth` — returns current stats (thread-safe via atomic/mutex)

- [ ] **Step 4: Run tests**

Run: `go test ./observe/simple/ -run TestStorageMonitor -v -timeout 10s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add observe/simple/monitor.go observe/simple/monitor_test.go
git commit -m "feat(observe/simple): add StorageMonitor with NATS advisories"
```

---

### Task 13: SetupTelemetry

**Files:**
- Create: `observe/simple/setup.go`
- Create: `observe/simple/setup_test.go`

- [ ] **Step 1: Write the failing test**

```go
// observe/simple/setup_test.go
// Tests for SetupTelemetry. Methodology: verify zero-config defaults
// produce working collectors with real NATS. Verify noop fallback when
// JetStream is unavailable.
package simple

// ... imports ...

func TestSetupTelemetryWithNATS(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)

	tel, shutdown := SetupTelemetry(nc)
	defer shutdown()

	if tel == nil {
		t.Fatal("SetupTelemetry returned nil")
	}
	if tel.Tracer == nil || tel.Logger == nil ||
		tel.Metrics == nil || tel.Errors == nil {
		t.Fatal("Telemetry has nil fields")
	}

	// Verify tracer works (produces spans)
	_, span := tel.Tracer.Start(context.Background(), "setup.test")
	span.End()
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write SetupTelemetry**

Create `observe/simple/setup.go` matching spec lines 366-406:
- Get `serviceName` from `filepath.Base(os.Args[0])`
- Get `js` from `nc.JetStream()` — on error, log to stderr, return `NewNoopTelemetry()`
- Create `MetricsCollector`, `TraceCollector`, `LogCollector`, `ErrorReporter` in order
- If `JAEGER_ENDPOINT` env var is set, start `ExportToJaeger` goroutine
- Return `*observe.Telemetry` and `shutdown func()`

- [ ] **Step 4: Run tests**

Run: `go test ./observe/simple/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 5: Run full test suite**

Run: `go test ./... -timeout 60s`
Expected: ALL PASS (existing tests unchanged)

- [ ] **Step 6: Commit**

```bash
git add observe/simple/setup.go observe/simple/setup_test.go
git commit -m "feat(observe/simple): add SetupTelemetry entry point"
```

---

## Chunk 4: Migration & Wiring

### Task 14: Migrate engine.Orchestrator constructor

**Files:**
- Modify: `engine/orchestrator.go`
- Modify: `engine/orchestrator_test.go`

- [ ] **Step 1: Update constructor signature**

Change `NewOrchestrator(nc *nats.Conn, logger observe.Logger, metrics observe.Metrics)`
to `NewOrchestrator(nc *nats.Conn, tel *observe.Telemetry)`.

Update the struct to store `tel *observe.Telemetry` and replace `o.logger` with
`o.tel.Logger` and `o.metrics` with `o.tel.Metrics` throughout the file.

Add assertion: `if tel == nil { panic("NewOrchestrator: tel must not be nil") }`

Remove old `logger` and `metrics` assertions.

- [ ] **Step 2: Update orchestrator_test.go**

Replace all `engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())`
calls with `engine.NewOrchestrator(nc, observe.NewNoopTelemetry())`.

- [ ] **Step 3: Run tests**

Run: `go test ./engine/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "refactor(engine): migrate to *Telemetry constructor"
```

---

### Task 15: Migrate worker.Worker constructor

**Files:**
- Modify: `worker/worker.go`
- Modify: `worker/worker_test.go`

- [ ] **Step 1: Update constructor signature**

Change `NewWorker(nc *nats.Conn, logger observe.Logger)` to
`NewWorker(nc *nats.Conn, tel *observe.Telemetry)`.

Store `tel` on the struct, use `w.tel.Logger` everywhere.

- [ ] **Step 2: Update worker_test.go**

Replace all `worker.NewWorker(nc, observe.NewNoopLogger())` with
`worker.NewWorker(nc, observe.NewNoopTelemetry())`.

- [ ] **Step 3: Run tests**

Run: `go test ./worker/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add worker/worker.go worker/worker_test.go
git commit -m "refactor(worker): migrate to *Telemetry constructor"
```

---

### Task 16: Migrate api.Service constructor

**Files:**
- Modify: `api/service.go`
- Modify: `api/service_test.go`
- Modify: `api/rest_test.go`
- Modify: `api/natsapi.go`
- Modify: `api/natsapi_test.go`

- [ ] **Step 1: Update constructor signature**

Change `NewService(nc *nats.Conn, logger observe.Logger)` to
`NewService(nc *nats.Conn, tel *observe.Telemetry)`.

Store `tel` on the struct, use `s.tel.Logger` everywhere.

- [ ] **Step 2: Update all test files**

Replace all `api.NewService(nc, observe.NewNoopLogger())` with
`api.NewService(nc, observe.NewNoopTelemetry())` in:
- `api/service_test.go`
- `api/rest_test.go`
- `api/natsapi_test.go`

Also check if `api/natsapi.go` constructs a Service and update if needed.

**Note:** `api/rest.go` does NOT need changes — it receives a `*Service` and
doesn't reference logger/metrics directly. `cmd/dagnats/main.go` (CLI) also
does NOT need changes — it doesn't create component constructors.

- [ ] **Step 3: Run tests**

Run: `go test ./api/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add api/service.go api/service_test.go api/rest_test.go \
    api/natsapi.go api/natsapi_test.go
git commit -m "refactor(api): migrate to *Telemetry constructor"
```

---

### Task 17: Migrate e2e_test.go

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Update constructor calls**

Replace:
```go
orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
w := worker.NewWorker(nc, observe.NewNoopLogger())
svc := api.NewService(nc, observe.NewNoopLogger())
```
With:
```go
tel := observe.NewNoopTelemetry()
orch := engine.NewOrchestrator(nc, tel)
w := worker.NewWorker(nc, tel)
svc := api.NewService(nc, tel)
```

- [ ] **Step 2: Run e2e tests**

Run: `go test -run TestE2E -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add e2e_test.go
git commit -m "refactor(e2e): migrate tests to *Telemetry"
```

---

### Task 18: Wire cmd binaries

**Files:**
- Modify: `cmd/dagnats-engine/main.go`
- Modify: `cmd/dagnats-api/main.go`

- [ ] **Step 1: Update dagnats-engine**

Replace:
```go
orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
```
With:
```go
tel, shutdown := simple.SetupTelemetry(nc)
defer shutdown()
orch := engine.NewOrchestrator(nc, tel)
```

Add import: `"github.com/danmestas/dagnats/observe/simple"`
Remove unused import: `"github.com/danmestas/dagnats/observe"`

- [ ] **Step 2: Update dagnats-api**

Replace:
```go
svc := api.NewService(nc, observe.NewNoopLogger())
```
With:
```go
tel, shutdown := simple.SetupTelemetry(nc)
defer shutdown()
svc := api.NewService(nc, tel)
```

- [ ] **Step 3: Build both binaries**

Run: `go build ./cmd/dagnats-engine && go build ./cmd/dagnats-api`
Expected: Both compile cleanly

- [ ] **Step 4: Run full test suite**

Run: `go test ./... -timeout 60s`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/dagnats-engine/main.go cmd/dagnats-api/main.go
git commit -m "feat(cmd): wire SetupTelemetry into engine and API binaries"
```

---

### Task 19: Run full test suite and verify

- [ ] **Step 1: Run all tests**

Run: `go test ./... -timeout 60s -count=1`
Expected: ALL PASS

- [ ] **Step 2: Run go vet**

Run: `go vet ./...`
Expected: No issues

- [ ] **Step 3: Run staticcheck**

Run: `staticcheck ./...`
Expected: No issues (or only pre-existing)

- [ ] **Step 4: Verify build**

Run: `go build ./...`
Expected: All packages compile

- [ ] **Step 5: Final commit if any cleanup needed**

---

## Deferred Work (Not In This Plan)

These are explicitly out of scope per the spec. Track as future work:

- **Add instrumentation spans** to engine/worker/api (spec Instrumentation Points section) — this is a follow-up plan after the foundation is in place
- **Add instrumentation metrics** (workflow.runs.active, step.duration_ms, etc.)
- **Health endpoint** extension with telemetry status
- **E2E telemetry test** — trace propagation across components
