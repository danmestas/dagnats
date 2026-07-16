// e2e_trigger_resolution_test.go
// End-to-end: each trigger type fires → orchestrator resolves the
// registered WorkflowDef from workflow_defs KV → first task is
// dispatched. Methodology: real embedded NATS, real TriggerService,
// real Orchestrator. No mocks. Verifies #167 across all three trigger
// types in one place. The two trigger.fire parent-hop tests below
// (#504) reuse this file's harness with an added in-memory span
// recorder rather than a new file, since they extend the same
// no-mocks trigger+orchestrator contract this file already proves.
package dagnats_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTriggerE2E(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	return nc
}

func registerWorkflowDef(
	t *testing.T, nc *nats.Conn, name string,
) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue workflow_defs: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: name, Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-" + name, Type: dag.StepTypeNormal},
		},
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := defKV.Put(name, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}
}

func registerTriggerDef(
	t *testing.T, nc *nats.Conn, def trigger.TriggerDef,
) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	trigKV, err := js.KeyValue("triggers")
	if err != nil {
		t.Fatalf("KeyValue triggers: %v", err)
	}
	defData, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal trigger: %v", err)
	}
	if _, err := trigKV.Put(def.ID, defData); err != nil {
		t.Fatalf("put trigger: %v", err)
	}
}

func waitForTask(t *testing.T, nc *nats.Conn, taskName string) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.PullSubscribe(
		"task."+taskName+".*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(8*time.Second))
	if err != nil {
		t.Fatalf("trigger did not produce task %q: %v", taskName, err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message for %q, got %d",
			taskName, len(msgs))
	}
}

// TestE2ECronTriggerDispatchesFirstTask verifies a cron trigger fire
// results in the orchestrator dispatching the first task — the
// reproducer from #166 / #167 with a positive outcome.
func TestE2ECronTriggerDispatchesFirstTask(t *testing.T) {
	nc := setupTriggerE2E(t)

	registerWorkflowDef(t, nc, "cron-wf")
	registerTriggerDef(t, nc, trigger.TriggerDef{
		ID:         "cron-t1",
		WorkflowID: "cron-wf",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	})

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	defer svc.Stop()

	svc.TickNow()
	waitForTask(t, nc, "task-cron-wf")
}

// TestE2ESubjectTriggerDispatchesFirstTask verifies a subject trigger
// fired by an inbound NATS message results in the orchestrator
// dispatching the first task.
func TestE2ESubjectTriggerDispatchesFirstTask(t *testing.T) {
	nc := setupTriggerE2E(t)

	registerWorkflowDef(t, nc, "subj-wf")
	registerTriggerDef(t, nc, trigger.TriggerDef{
		ID:         "subj-t1",
		WorkflowID: "subj-wf",
		Enabled:    true,
		Subject: &trigger.SubjectConfig{
			Subject: "events.subj.fire",
		},
	})

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	defer svc.Stop()

	if err := nc.Flush(); err != nil {
		t.Fatalf("flush after subscribe: %v", err)
	}
	if err := nc.Publish(
		"events.subj.fire", []byte(`{"hello":"world"}`),
	); err != nil {
		t.Fatalf("publish trigger subject: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush after publish: %v", err)
	}
	waitForTask(t, nc, "task-subj-wf")
}

// TestE2EWebhookTriggerDispatchesFirstTask verifies a webhook trigger
// fired by an HTTP POST results in the orchestrator dispatching the
// first task.
func TestE2EWebhookTriggerDispatchesFirstTask(t *testing.T) {
	nc := setupTriggerE2E(t)

	registerWorkflowDef(t, nc, "hook-wf")
	registerTriggerDef(t, nc, trigger.TriggerDef{
		ID:         "hook-t1",
		WorkflowID: "hook-wf",
		Enabled:    true,
		Webhook: &trigger.WebhookConfig{
			Path: "/hooks/hook-t1",
		},
	})

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	defer svc.Stop()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost, "/hooks/hook-t1",
		strings.NewReader(`{"hello":"world"}`),
	)
	svc.WebhookHandler().ServeHTTP(rec, req)
	if rec.Code >= 400 {
		t.Fatalf("webhook POST rejected: status=%d body=%q",
			rec.Code, rec.Body.String())
	}
	waitForTask(t, nc, "task-hook-wf")
}

// spanRecorderOnce/sharedSpanExporter back installE2ESpanRecorder.
// The OTel Go SDK only ever delegates a tracer obtained before any
// SDK was installed once, process-wide (see
// go.opentelemetry.io/otel/internal/global's delegateTraceOnce).
// internal/trigger's fireTracer package var is obtained at that
// package's init time, before any test runs, so it permanently binds
// to whichever TracerProvider the first otel.SetTracerProvider call
// in this test binary installs. Both tests below must therefore
// share one provider/exporter installed exactly once and isolate
// themselves by resetting the exporter's buffer, exactly like
// internal/trigger/fire_test.go's installSpanRecorder.
var (
	spanRecorderOnce   sync.Once
	sharedSpanExporter *tracetest.InMemoryExporter
)

// installE2ESpanRecorder returns the shared in-memory span exporter,
// installing it and the composite W3C propagator on first use.
// Resets the exporter's buffer before and after the test.
func installE2ESpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	spanRecorderOnce.Do(func() {
		sharedSpanExporter = tracetest.NewInMemoryExporter()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(sharedSpanExporter),
		))
		otel.SetTextMapPropagator(
			propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{}, propagation.Baggage{},
			),
		)
	})
	sharedSpanExporter.Reset()
	t.Cleanup(sharedSpanExporter.Reset)
	return sharedSpanExporter
}

// waitForSpanNamed polls the exporter for a span with the given name,
// bounded by timeout, since span export can lag a beat behind the
// task-dispatch signal waitForTask already waited on (both run in
// the same process but on different goroutines).
func waitForSpanNamed(
	t *testing.T,
	exporter *tracetest.InMemoryExporter,
	name string,
	timeout time.Duration,
) tracetest.SpanStub {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, s := range exporter.GetSpans() {
			if s.Name == name {
				return s
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for span %q", name)
	return tracetest.SpanStub{}
}

// countFireSpansNamed counts recorded spans with the given name.
func countFireSpansNamed(
	spans tracetest.SpanStubs, name string,
) int {
	count := 0
	for _, s := range spans {
		if s.Name == name {
			count++
		}
	}
	return count
}

// TestTick_ParentsHandleEventUnderTriggerFire proves the #504 parent
// hop for the cron fire path: the engine's handleEvent span shares
// its trace ID with the trigger.fire span that started the run.
func TestTick_ParentsHandleEventUnderTriggerFire(t *testing.T) {
	exporter := installE2ESpanRecorder(t)
	nc := setupTriggerE2E(t)

	registerWorkflowDef(t, nc, "tick-parent-wf")
	def := trigger.TriggerDef{
		ID:         "tick-parent-t1",
		WorkflowID: "tick-parent-wf",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *", Timezone: "UTC",
		},
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	scheduler, err := trigger.NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger: %v", err)
	}

	tickTime := time.Now()
	if err := scheduler.Tick(tickTime); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	waitForTask(t, nc, "task-tick-parent-wf")

	fireSpan := waitForSpanNamed(t, exporter, "trigger.fire", 5*time.Second)
	handleSpan := waitForSpanNamed(
		t, exporter, "dagnats.engine handleEvent", 5*time.Second,
	)

	// Positive: the engine's handleEvent span is a child of (shares
	// the trace ID with) trigger.fire — the parent hop this issue
	// exists to prove.
	wantTraceID := fireSpan.SpanContext.TraceID()
	if handleSpan.SpanContext.TraceID() != wantTraceID {
		t.Fatalf(
			"handleEvent trace ID = %s, want %s (trigger.fire's)",
			handleSpan.SpanContext.TraceID(), wantTraceID,
		)
	}

	// Negative: regression guard on #173 at the tracing level — a
	// second Tick for the same matching minute is dedup-claimed
	// before it ever reaches Fire, so no second trigger.fire span
	// appears.
	if err := scheduler.Tick(tickTime); err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := countFireSpansNamed(
		exporter.GetSpans(), "trigger.fire",
	); got != 1 {
		t.Fatalf(
			"trigger.fire span count after dedup tick = %d, want 1",
			got,
		)
	}
}

// TestFireTrigger_ParentsUnderAPISpan proves the #504 parent hop for
// the manual fire path: a 3-level chain "dagnats.api fireTrigger" →
// "trigger.fire" → "dagnats.engine handleEvent" all share one trace,
// and that a direct StartRun (bypassing triggers) never produces a
// trigger.fire span.
func TestFireTrigger_ParentsUnderAPISpan(t *testing.T) {
	exporter := installE2ESpanRecorder(t)
	nc := setupTriggerE2E(t)

	registerWorkflowDef(t, nc, "manual-parent-wf")
	registerTriggerDef(t, nc, trigger.TriggerDef{
		ID:         "manual-parent-t1",
		WorkflowID: "manual-parent-wf",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *", Timezone: "UTC",
		},
	})

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := api.NewService(nc)
	runID, err := svc.FireTrigger(
		context.Background(), "manual-parent-t1",
	)
	if err != nil {
		t.Fatalf("FireTrigger: %v", err)
	}
	if runID == "" {
		t.Fatal("FireTrigger: expected non-empty run ID")
	}
	waitForTask(t, nc, "task-manual-parent-wf")

	apiSpan := waitForSpanNamed(
		t, exporter, "dagnats.api fireTrigger", 5*time.Second,
	)
	fireSpan := waitForSpanNamed(t, exporter, "trigger.fire", 5*time.Second)
	handleSpan := waitForSpanNamed(
		t, exporter, "dagnats.engine handleEvent", 5*time.Second,
	)

	// Positive: all three spans share one trace ID — the full
	// fireTrigger -> trigger.fire -> handleEvent chain.
	traceID := apiSpan.SpanContext.TraceID()
	if fireSpan.SpanContext.TraceID() != traceID {
		t.Fatalf(
			"trigger.fire trace ID = %s, want %s (fireTrigger's)",
			fireSpan.SpanContext.TraceID(), traceID,
		)
	}
	if handleSpan.SpanContext.TraceID() != traceID {
		t.Fatalf(
			"handleEvent trace ID = %s, want %s (fireTrigger's)",
			handleSpan.SpanContext.TraceID(), traceID,
		)
	}

	// Negative: a direct StartRun bypasses triggers entirely, so its
	// trace must contain zero trigger.fire spans.
	exporter.Reset()
	if _, err := svc.StartRun(
		context.Background(), "manual-parent-wf", nil,
	); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitForSpanNamed(t, exporter, "dagnats.api startRun", 5*time.Second)
	if got := countFireSpansNamed(
		exporter.GetSpans(), "trigger.fire",
	); got != 0 {
		t.Fatalf(
			"trigger.fire span count after direct StartRun = %d, want 0",
			got,
		)
	}
}
