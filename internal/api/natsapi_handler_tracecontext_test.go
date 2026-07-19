// api/natsapi_handler_tracecontext_test.go
// Methodology: real embedded NATS plus an in-memory OTel span recorder
// installed via otel.SetTracerProvider (the pattern from
// internal/trigger/fire_test.go). This file owns the binary's TestMain
// and therefore its real TracerProvider. Each of the five micro
// handlers that TestNATSAPIStartRunPropagatesTraceContext does not
// cover is driven over its real subject with an inbound W3C
// traceparent, and the server-side span its service call records is
// asserted to carry the inbound trace ID. Only the TRACE ID is
// asserted: a non-recording remote parent reuses the parent's span ID,
// so a parentage assertion would pass for the wrong reason. Negative
// space per endpoint: the same request without a traceparent must
// record a span that does NOT carry the inbound trace ID, proving no
// ambient trace leaks in. The three runtime subjects gate on
// VerifyDispatch before reaching their observed service method, so the
// test stands up a real run and echoes its live dispatch nonce. All
// waits are bounded; the NATS server is isolated per test.
package api

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// traceWorkflowName is registered once and reused as the run's
// workflow, the runtime-register def, and the spawn child target.
const traceWorkflowName = "handler-trace-ctx-test"

// traceStepID is the single task in traceWorkflowName; with no worker
// attached it stays Queued, which is a valid dispatch state for
// VerifyDispatch, so its nonce stays usable for the whole test.
const traceStepID = "a"

// sharedSpanExporter records every span this test binary produces. It
// is installed once from TestMain, before any tracer is obtained: the
// OTel global TracerProvider hands its delegate to a previously-created
// tracer only once, process-wide (internal/global's delegateTraceOnce),
// so a provider installed mid-run would never reach a Service built
// before it. Tests isolate themselves by resetting the buffer.
var sharedSpanExporter = tracetest.NewInMemoryExporter()

// TestMain installs the real SDK TracerProvider for the whole
// internal/api binary. Sibling tests must therefore assert on trace
// PROVENANCE (this run did not inherit that caller's trace), never on
// trace ABSENCE -- under a real provider, which is what production
// installs via InitTelemetry, an untraced request legitimately mints a
// fresh root span whose trace ID is then persisted.
func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(sharedSpanExporter),
	))
	os.Exit(m.Run())
}

// installSpanRecorder returns the shared exporter with a clean buffer
// and pins the composite W3C propagator, so extraction is deterministic
// regardless of test order (NewNATSAPI calls EnsureDefaultPropagator,
// which is no-clobber, so whichever propagator is installed first wins).
func installSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		),
	)
	sharedSpanExporter.Reset()
	t.Cleanup(func() {
		sharedSpanExporter.Reset()
		otel.SetTextMapPropagator(previousPropagator)
	})
	return sharedSpanExporter
}

// tracedEndpointCase drives one micro subject and names the span its
// service call must record.
type tracedEndpointCase struct {
	name     string
	subject  string
	payload  []byte
	spanName string
}

// tracedEndpointCases builds the five uncovered endpoints. The three
// runtime subjects echo a live dispatch (run/step/nonce) so they clear
// VerifyDispatch and reach the per-operation observed method behind it;
// whether that method then succeeds or returns a typed kind is
// irrelevant -- observed records the span either way.
func tracedEndpointCases(
	t *testing.T, defJSON []byte, runID, nonce string,
) []tracedEndpointCase {
	t.Helper()
	if runID == "" || nonce == "" {
		t.Fatal("tracedEndpointCases: runID and nonce must be non-empty")
	}
	proof := func(payload map[string]any) []byte {
		payload["owner_step_id"] = traceStepID
		payload["parent_step_id"] = traceStepID
		payload["nonce"] = nonce
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload failed: %v", err)
		}
		return data
	}
	return []tracedEndpointCase{
		{
			name:     "register",
			subject:  "api.workflows.register",
			payload:  defJSON,
			spanName: "dagnats.api registerWorkflow",
		},
		{
			name:     "get",
			subject:  "api.runs.get",
			payload:  []byte("no-such-run"),
			spanName: "dagnats.api getRun",
		},
		{
			name:    "runtimes-register",
			subject: "api.runtimes.register",
			payload: proof(map[string]any{
				"def":          json.RawMessage(defJSON),
				"owner_run_id": runID,
			}),
			spanName: "dagnats.api registerRuntimeWorkflow",
		},
		{
			name:    "runs-spawn",
			subject: "api.runs.spawn",
			payload: proof(map[string]any{
				"child_workflow": traceWorkflowName,
				"parent_run_id":  runID,
			}),
			spanName: "dagnats.api spawnChildRun",
		},
		{
			name:    "runtimes-budget",
			subject: "api.runtimes.budget",
			payload: proof(map[string]any{
				"owner_run_id": runID,
			}),
			spanName: "dagnats.api budget",
		},
	}
}

func TestNATSAPIHandlersPropagateTraceContext(t *testing.T) {
	exporter := installSpanRecorder(t)

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orchestrator := engine.NewOrchestrator(nc)
	orchestrator.Start()
	defer orchestrator.Stop()

	service := NewService(nc)
	natsAPI := NewNATSAPI(service, nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()

	defJSON := buildWorkflowDefJSON(t)
	requestWithTraceParent(t, nc, "api.workflows.register", defJSON, "")
	runID := startRun(t, nc)
	nonce := waitForDispatchNonce(t, service, runID)

	for _, testCase := range tracedEndpointCases(
		t, defJSON, runID, nonce,
	) {
		t.Run(testCase.name, func(t *testing.T) {
			exporter.Reset()
			requestWithTraceParent(
				t, nc, testCase.subject, testCase.payload,
				inboundTraceParent,
			)
			if !hasSpanWithTraceID(
				exporter.GetSpans(), testCase.spanName, inboundTraceID,
			) {
				t.Fatalf("no %q span carried inbound trace %s",
					testCase.spanName, inboundTraceID)
			}

			// Negative space: no inbound header -> no ambient trace.
			exporter.Reset()
			requestWithTraceParent(
				t, nc, testCase.subject, testCase.payload, "",
			)
			spans := exporter.GetSpans()
			if countSpansNamed(spans, testCase.spanName) == 0 {
				t.Fatalf("untraced request recorded no %q span",
					testCase.spanName)
			}
			if hasSpanWithTraceID(
				spans, testCase.spanName, inboundTraceID,
			) {
				t.Fatalf("untraced %q span carried trace %s",
					testCase.spanName, inboundTraceID)
			}
		})
	}
}

// buildWorkflowDefJSON returns the marshalled single-task workflow the
// whole test reuses.
func buildWorkflowDefJSON(t *testing.T) []byte {
	t.Helper()
	builder := dag.NewWorkflow(traceWorkflowName)
	builder.Task(traceStepID, "task-a")
	workflowDef, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	defJSON, err := json.Marshal(workflowDef)
	if err != nil {
		t.Fatalf("marshal workflow def failed: %v", err)
	}
	if len(defJSON) == 0 {
		t.Fatal("marshalled workflow def is empty")
	}
	return defJSON
}

// startRun starts traceWorkflowName over api.runs.start and returns the
// run ID, failing the test on a transport or envelope error.
func startRun(t *testing.T, nc *nats.Conn) string {
	t.Helper()
	reply, err := nc.Request(
		"api.runs.start",
		[]byte(`{"workflow":"`+traceWorkflowName+`"}`),
		5*time.Second,
	)
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
	if resp["run_id"] == "" {
		t.Fatal("start returned empty run_id")
	}
	return resp["run_id"]
}

// waitForDispatchNonce polls the run snapshot until the orchestrator has
// dispatched traceStepID and stamped its DispatchNonce -- the proof the
// three runtime endpoints must echo to clear VerifyDispatch. Bounded on
// both iterations and wall time; fails the test on timeout.
func waitForDispatchNonce(
	t *testing.T, service *Service, runID string,
) string {
	t.Helper()
	const attempts_max = 100
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; i < attempts_max && time.Now().Before(deadline); i++ {
		run, err := service.GetRun(context.Background(), runID)
		if err == nil {
			if state, ok := run.Steps[traceStepID]; ok &&
				state.DispatchNonce != "" {
				return state.DispatchNonce
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run %s step %s never got a dispatch nonce",
		runID, traceStepID)
	return ""
}

// requestWithTraceParent sends payload to subject, setting the
// traceparent header when traceParent is non-empty. A transport error
// fails the test; a handler-level error envelope does not -- observed
// records the span on the error path too, so what matters here is that
// the request reached the handler.
func requestWithTraceParent(
	t *testing.T, nc *nats.Conn, subject string,
	payload []byte, traceParent string,
) {
	t.Helper()
	msg := nats.NewMsg(subject)
	msg.Data = payload
	if traceParent != "" {
		msg.Header.Set("traceparent", traceParent)
	}
	reply, err := nc.RequestMsg(msg, 5*time.Second)
	if err != nil {
		t.Fatalf("%s request failed: %v", subject, err)
	}
	if len(reply.Data) == 0 {
		t.Fatalf("%s replied with empty body", subject)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(reply.Data, &envelope); err != nil {
		t.Fatalf("%s reply unmarshal failed: %v", subject, err)
	}
}

// hasSpanWithTraceID reports whether any recorded span with the given
// name belongs to traceID. Matching on name AND trace ID keeps the
// assertion immune to spans other tests leave in the shared exporter.
func hasSpanWithTraceID(
	spans tracetest.SpanStubs, name, traceID string,
) bool {
	for _, span := range spans {
		if span.Name == name &&
			span.SpanContext.TraceID().String() == traceID {
			return true
		}
	}
	return false
}

// countSpansNamed returns how many recorded spans carry name.
func countSpansNamed(spans tracetest.SpanStubs, name string) int {
	count := 0
	for _, span := range spans {
		if span.Name == name {
			count++
		}
	}
	return count
}
