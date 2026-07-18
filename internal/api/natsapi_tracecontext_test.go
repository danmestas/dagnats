// api/natsapi_tracecontext_test.go
// Methodology: real embedded NATS, send a request carrying a W3C
// traceparent header to api.runs.start, then read the run back over
// api.runs.get and assert the persisted trace_id equals the inbound
// trace ID (positive) and that a request without the header yields an
// empty trace_id (negative). All waits are bounded.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// inboundTraceParent is the W3C example traceparent; its trace ID is
// what api.runs.get must surface after a traced api.runs.start.
const inboundTraceParent = "00-0af7651916cd43dd8448eb211c80319c-" +
	"b7ad6b7169203331-01"

const inboundTraceID = "0af7651916cd43dd8448eb211c80319c"

// installAPIPropagator sets the global OTel propagator and returns a
// restore func, so this test cannot leak propagator state into others.
func installAPIPropagator() func() {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	return func() { otel.SetTextMapPropagator(prev) }
}

func TestNATSAPIStartRunPropagatesTraceContext(t *testing.T) {
	restore := installAPIPropagator()
	defer restore()

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()

	wb := dag.NewWorkflow("trace-ctx-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if _, err := nc.Request(
		"api.workflows.register", mustMarshal(t, wfDef), 5*time.Second,
	); err != nil {
		t.Fatalf("register request failed: %v", err)
	}

	traced := startRunWithHeader(t, nc, inboundTraceParent)
	if traced == "" {
		t.Fatal("traced start returned empty run_id")
	}
	if got := waitForRunTraceID(t, nc, traced, true); got != inboundTraceID {
		t.Fatalf("trace_id = %q, want %q", got, inboundTraceID)
	}

	// Negative space: no traceparent header -> no trace linkage.
	untraced := startRunWithHeader(t, nc, "")
	if untraced == "" {
		t.Fatal("untraced start returned empty run_id")
	}
	if got := waitForRunTraceID(t, nc, untraced, false); got != "" {
		t.Fatalf("untraced trace_id = %q, want empty", got)
	}
}

// startRunWithHeader starts a run over api.runs.start, setting the
// traceparent header when tp is non-empty. Returns the run ID.
func startRunWithHeader(
	t *testing.T, nc *nats.Conn, tp string,
) string {
	t.Helper()
	msg := nats.NewMsg("api.runs.start")
	msg.Data = mustMarshal(t,
		startRunRequest{Workflow: "trace-ctx-test"},
	)
	if tp != "" {
		msg.Header.Set("traceparent", tp)
	}
	reply, err := nc.RequestMsg(msg, 5*time.Second)
	if err != nil {
		t.Fatalf("start request failed: %v", err)
	}
	var resp map[string]string
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("start reply unmarshal failed: %v", err)
	}
	if resp["error"] != "" {
		t.Fatalf("start replied error: %s", resp["error"])
	}
	return resp["run_id"]
}

// runTraceID reads trace_id off a single api.runs.get reply. It
// returns the reply's error envelope so callers can poll through the
// window where the snapshot has not been persisted yet.
func runTraceID(
	t *testing.T, nc *nats.Conn, runID string,
) (traceID, replyError string) {
	t.Helper()
	reply, err := nc.Request(
		"api.runs.get", []byte(runID), 5*time.Second,
	)
	if err != nil {
		t.Fatalf("get request failed: %v", err)
	}
	var resp struct {
		TraceID string `json:"trace_id"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("get reply unmarshal failed: %v", err)
	}
	return resp.TraceID, resp.Error
}

// waitForRunTraceID polls api.runs.get until the run snapshot exists,
// returning its trace_id. When wantTraceID is set the poll also waits
// for a non-empty trace_id: fetchRunTraceID is best-effort and can come
// back empty on a slow history read even once the snapshot is readable,
// so exiting on snapshot-readable alone would flake. Bounded on both
// iterations and wall time; fails the test on timeout.
func waitForRunTraceID(
	t *testing.T, nc *nats.Conn, runID string, wantTraceID bool,
) string {
	t.Helper()
	const attempts_max = 100
	deadline := time.Now().Add(5 * time.Second)
	var lastErr string
	for i := 0; i < attempts_max && time.Now().Before(deadline); i++ {
		traceID, replyErr := runTraceID(t, nc, runID)
		if replyErr == "" && (traceID != "" || !wantTraceID) {
			return traceID
		}
		lastErr = replyErr
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run %s never became readable: %s", runID, lastErr)
	return ""
}
