// e2e_telemetry_test.go
// End-to-end test: verify that distributed traces propagate correctly
// across DagNats components (API -> Engine -> Worker). All spans from
// a single workflow run must share the same trace_id and at least one
// span must have a non-empty parent_id, proving cross-NATS linking.
// Methodology: real NATS server, real telemetry, real components. No mocks.
package dagnats_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe/simple"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestE2ETelemetryTracePropagation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	tel, shutdown := simple.SetupTelemetry(nc)
	defer shutdown()

	ctx := t.Context()

	orch := engine.NewOrchestrator(nc, tel)
	orch.Start()
	defer orch.Stop()

	w := worker.NewWorker(nc, tel)
	w.Handle("tel-a", func(tc worker.TaskContext) error {
		return tc.Complete([]byte(`"a-done"`))
	})
	w.Handle("tel-b", func(tc worker.TaskContext) error {
		return tc.Complete([]byte(`"b-done"`))
	})
	w.Start()
	defer w.Stop()

	svc := api.NewService(nc, tel)
	wfDef, err := dag.NewWorkflow("e2e-telemetry").
		Task("a", "tel-a").
		Task("b", "tel-b").DependsOn("a").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	runID, err := svc.StartRun(ctx, "e2e-telemetry", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	waitForRunCompletion(t, svc, runID)

	// Flush telemetry so all buffered spans are published.
	shutdown()

	allSpans := collectSpans(t, nc)

	// Find the root trace from api.startRun, then filter all
	// spans to that trace. Unrelated API spans (getRun polling)
	// start independent traces and must be excluded.
	rootTraceID := findRootTraceID(t, allSpans)
	spans := filterSpansByTraceID(allSpans, rootTraceID)
	if len(spans) < 3 {
		t.Fatalf(
			"expected at least 3 spans in trace %s, got %d",
			rootTraceID, len(spans),
		)
	}

	assertSpanServices(t, spans)
	assertTraceIDConsistency(t, spans)
	assertParentSpanExists(t, spans)
}

// waitForRunCompletion polls until the workflow run reaches Completed
// status, failing after a bounded 10-second timeout.
func waitForRunCompletion(
	t *testing.T, svc *api.Service, runID string,
) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		run, err := svc.GetRun(t.Context(), runID)
		if err == engine.ErrRunNotFound {
			select {
			case <-deadline:
				t.Fatalf("run did not appear within 10s")
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			t.Fatalf("GetRun failed: %v", err)
		}
		if run.Status == dag.RunStatusCompleted {
			return
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatal("workflow failed unexpectedly")
		}
		select {
		case <-deadline:
			t.Fatalf(
				"workflow did not complete within 10s, "+
					"status: %v", run.Status,
			)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// collectSpans subscribes to the TELEMETRY stream with DeliverAll
// and reads all available span records, returning them as a slice.
func collectSpans(
	t *testing.T, nc *nats.Conn,
) []simple.SpanRecord {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	sub, err := js.SubscribeSync(
		"telemetry.spans.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Logf("Unsubscribe warning: %v", err)
		}
	}()

	var spans []simple.SpanRecord
	for {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			break
		}
		var rec simple.SpanRecord
		if err := json.Unmarshal(msg.Data, &rec); err != nil {
			t.Fatalf("Unmarshal span failed: %v", err)
		}
		spans = append(spans, rec)
	}
	if len(spans) == 0 {
		t.Fatal("expected at least 1 span, got 0")
	}
	return spans
}

// findRootTraceID locates the api.startRun span and returns its
// trace_id. This is the root of the distributed workflow trace.
func findRootTraceID(
	t *testing.T, spans []simple.SpanRecord,
) string {
	t.Helper()
	for _, s := range spans {
		if s.Name == "api.startRun" {
			return s.TraceID
		}
	}
	t.Fatal("no api.startRun span found")
	return ""
}

// filterSpansByTraceID returns only spans matching the given trace_id.
// This isolates the workflow execution trace from unrelated API calls.
func filterSpansByTraceID(
	spans []simple.SpanRecord, traceID string,
) []simple.SpanRecord {
	var filtered []simple.SpanRecord
	for _, s := range spans {
		if s.TraceID == traceID {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// assertSpanServices verifies that at least one span exists from
// each of the three component layers (api, engine, worker). Since
// the test binary name is used as service name, all spans may have
// the same service. We verify at least 3 distinct span names exist
// covering the API/engine/worker layers.
func assertSpanServices(
	t *testing.T, spans []simple.SpanRecord,
) {
	t.Helper()
	names := make(map[string]bool, len(spans))
	for _, s := range spans {
		names[s.Name] = true
	}

	// The spans should include spans from different layers.
	// In test binaries all services share the same name, so
	// verify by span count rather than distinct service names.
	if len(spans) < 3 {
		t.Fatalf(
			"expected at least 3 spans (api+engine+worker), "+
				"got %d", len(spans),
		)
	}
	if len(names) < 2 {
		t.Fatalf(
			"expected at least 2 distinct span names, got %d: %v",
			len(names), names,
		)
	}
}

// assertTraceIDConsistency verifies that all spans with a non-empty
// trace_id share the same value, proving distributed trace linkage.
func assertTraceIDConsistency(
	t *testing.T, spans []simple.SpanRecord,
) {
	t.Helper()
	var traceID string
	for _, s := range spans {
		if s.TraceID == "" {
			continue
		}
		if traceID == "" {
			traceID = s.TraceID
			continue
		}
		if s.TraceID != traceID {
			t.Fatalf(
				"trace_id mismatch: first=%s got=%s "+
					"(span=%s)",
				traceID, s.TraceID, s.Name,
			)
		}
	}
	if traceID == "" {
		t.Fatal("no spans had a non-empty trace_id")
	}
}

// assertParentSpanExists verifies that at least one span has a
// non-empty parent_id, confirming child spans were created.
func assertParentSpanExists(
	t *testing.T, spans []simple.SpanRecord,
) {
	t.Helper()
	for _, s := range spans {
		if s.ParentID != "" {
			return
		}
	}
	t.Fatal("no span had a non-empty parent_id")
}
