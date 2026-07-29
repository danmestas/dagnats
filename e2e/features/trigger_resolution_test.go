// e2e/features/trigger_resolution_test.go
// End-to-end tests migrated from the repo-root e2e_trigger_resolution
// _test.go so they run against every enabled topology via RunE2E. Each
// trigger type fires -> the orchestrator resolves the registered
// WorkflowDef from workflow_defs KV -> the first task is dispatched
// (#167). The two trigger.fire parent-hop tests (#504) reuse this
// file's helpers plus an in-memory span recorder rather than a new
// file, since they extend the same no-mocks trigger+orchestrator
// contract this file already proves.
// Methodology: real embedded NATS, real TriggerService/Scheduler, real
// Orchestrator. No mocks.
package features

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// createTriggerBuckets provisions the triggers and trigger_state KV
// buckets the trigger subsystem needs — the harness does not create
// them by default.
func createTriggerBuckets(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if nc == nil {
		panic("createTriggerBuckets: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	for _, bucket := range []string{"triggers", "trigger_state"} {
		if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: bucket,
		}); err != nil {
			t.Fatalf("create KV %q: %v", bucket, err)
		}
	}
}

// registerWorkflowDef writes a single-normal-step workflow to the
// workflow_defs KV. Its task is named "task-<name>" so callers can wait
// for dispatch on that subject.
func registerWorkflowDef(t *testing.T, nc *nats.Conn, name string) {
	t.Helper()
	if name == "" {
		panic("registerWorkflowDef: name must not be empty")
	}
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

// registerTriggerDef writes a trigger def to the triggers KV.
func registerTriggerDef(
	t *testing.T, nc *nats.Conn, def trigger.TriggerDef,
) {
	t.Helper()
	if def.ID == "" {
		panic("registerTriggerDef: def.ID must not be empty")
	}
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

// waitForTask fetches one message for taskName off TASK_QUEUES, failing
// if none arrives within the bounded wait — the signal that a trigger
// fire reached the orchestrator and it dispatched the first task.
func waitForTask(t *testing.T, nc *nats.Conn, taskName string) {
	t.Helper()
	if taskName == "" {
		panic("waitForTask: taskName must not be empty")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.PullSubscribe(
		"task."+taskName+".*", "", nats.BindStream("TASK_QUEUES"),
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

// startTriggerService creates and starts a TriggerService, waiting until
// its KV watcher has actually loaded a trigger before returning. The
// wait replaces the root file's fire-immediately-after-Start pattern,
// which races the watcher and surfaces the miss downstream as a missing
// task (#558 rationale, shared with cron_test/subject_trigger_test).
func startTriggerService(t *testing.T, nc *nats.Conn) *trigger.TriggerService {
	t.Helper()
	ts, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("TriggerService.Start: %v", err)
	}
	t.Cleanup(func() { ts.Stop() })
	harness.WaitForPrecondition(t, "trigger loaded into scheduler",
		triggerReadyCeiling, func() bool { return ts.TriggerCount() >= 1 })
	return ts
}

// TestE2ECronTriggerDispatchesFirstTask verifies a cron trigger fire
// results in the orchestrator dispatching the first task — the
// reproducer from #166 / #167 with a positive outcome.
func TestE2ECronTriggerDispatchesFirstTask(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		createTriggerBuckets(t, nc)
		name := harness.UniqueName(t, "cron-wf")
		registerWorkflowDef(t, nc, name)
		registerTriggerDef(t, nc, trigger.TriggerDef{
			ID:         harness.UniqueName(t, "cron-t"),
			WorkflowID: name, Enabled: true,
			Cron: &trigger.CronConfig{
				Expression: "* * * * *", Timezone: "UTC",
			},
		})

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		ts := startTriggerService(t, nc)
		ts.TickNow()
		waitForTask(t, nc, "task-"+name)
	})
}

// TestE2ESubjectTriggerDispatchesFirstTask verifies a subject trigger
// fired by an inbound NATS message results in the orchestrator
// dispatching the first task.
func TestE2ESubjectTriggerDispatchesFirstTask(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		createTriggerBuckets(t, nc)
		name := harness.UniqueName(t, "subj-wf")
		subject := harness.UniqueName(t, "events.subj.fire")
		registerWorkflowDef(t, nc, name)
		registerTriggerDef(t, nc, trigger.TriggerDef{
			ID:         harness.UniqueName(t, "subj-t"),
			WorkflowID: name, Enabled: true,
			Subject: &trigger.SubjectConfig{Subject: subject},
		})

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		startTriggerService(t, nc)

		if err := nc.Flush(); err != nil {
			t.Fatalf("flush after subscribe: %v", err)
		}
		if err := nc.Publish(subject, []byte(`{"hello":"world"}`)); err != nil {
			t.Fatalf("publish trigger subject: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush after publish: %v", err)
		}
		waitForTask(t, nc, "task-"+name)
	})
}

// TestE2EWebhookTriggerDispatchesFirstTask verifies a webhook trigger
// fired by an HTTP POST results in the orchestrator dispatching the
// first task.
func TestE2EWebhookTriggerDispatchesFirstTask(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		createTriggerBuckets(t, nc)
		name := harness.UniqueName(t, "hook-wf")
		path := "/hooks/" + harness.UniqueName(t, "hook-t")
		registerWorkflowDef(t, nc, name)
		registerTriggerDef(t, nc, trigger.TriggerDef{
			ID:         harness.UniqueName(t, "hook-t"),
			WorkflowID: name, Enabled: true,
			Webhook: &trigger.WebhookConfig{Path: path},
		})

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		ts := startTriggerService(t, nc)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path,
			strings.NewReader(`{"hello":"world"}`))
		ts.WebhookHandler().ServeHTTP(rec, req)
		if rec.Code >= 400 {
			t.Fatalf("webhook POST rejected: status=%d body=%q",
				rec.Code, rec.Body.String())
		}
		waitForTask(t, nc, "task-"+name)
	})
}

// spanRecorderOnce/sharedSpanExporter back installE2ESpanRecorder. The
// OTel Go SDK only ever delegates a tracer obtained before any SDK was
// installed once, process-wide (see go.opentelemetry.io/otel/internal/
// global's delegateTraceOnce). internal/trigger's fireTracer package var
// is obtained at that package's init time, before any test runs, so it
// permanently binds to whichever TracerProvider the first
// otel.SetTracerProvider call in this test binary installs. Both tests
// below must therefore share one provider/exporter installed exactly
// once and isolate themselves by resetting the exporter's buffer,
// exactly like internal/trigger/fire_test.go's installSpanRecorder.
var (
	spanRecorderOnce   sync.Once
	sharedSpanExporter *tracetest.InMemoryExporter
)

// installE2ESpanRecorder returns the shared in-memory span exporter,
// installing it and the composite W3C propagator on first use. Resets
// the exporter's buffer before and after the test.
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

// spanPollInterval bounds installE2ESpanRecorder's consumers' poll rate.
const spanPollInterval = 50 * time.Millisecond

// waitForSpanNamed polls the exporter for a span with the given name,
// bounded by both a wall-clock budget and a poll cap, since span export
// can lag a beat behind the task-dispatch signal waitForTask waited on
// (both run in-process but on different goroutines).
func waitForSpanNamed(
	t *testing.T, exporter *tracetest.InMemoryExporter,
	name string, timeout time.Duration,
) tracetest.SpanStub {
	t.Helper()
	pollsMax := int(timeout/spanPollInterval) + 2
	for polls := 0; polls < pollsMax; polls++ {
		for _, s := range exporter.GetSpans() {
			if s.Name == name {
				return s
			}
		}
		time.Sleep(spanPollInterval)
	}
	t.Fatalf("timed out waiting for span %q", name)
	return tracetest.SpanStub{}
}

// countFireSpansNamed counts recorded spans with the given name.
func countFireSpansNamed(spans tracetest.SpanStubs, name string) int {
	count := 0
	for _, s := range spans {
		if s.Name == name {
			count++
		}
	}
	return count
}

// assertSpansShareTrace fails unless every span in others carries want's
// trace ID — the parent-hop invariant these #504 tests exist to prove.
func assertSpansShareTrace(
	t *testing.T, want tracetest.SpanStub, others ...tracetest.SpanStub,
) {
	t.Helper()
	wantID := want.SpanContext.TraceID()
	for _, s := range others {
		if s.SpanContext.TraceID() != wantID {
			t.Fatalf("span %q trace ID = %s, want %s",
				s.Name, s.SpanContext.TraceID(), wantID)
		}
	}
}

// TestTick_ParentsHandleEventUnderTriggerFire proves the #504 parent hop
// for the cron fire path: the engine's handleEvent span shares its trace
// ID with the trigger.fire span that started the run.
func TestTick_ParentsHandleEventUnderTriggerFire(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		exporter := installE2ESpanRecorder(t)
		createTriggerBuckets(t, nc)
		name := harness.UniqueName(t, "tick-parent-wf")
		registerWorkflowDef(t, nc, name)
		def := trigger.TriggerDef{
			ID:         harness.UniqueName(t, "tick-parent-t"),
			WorkflowID: name, Enabled: true,
			Cron: &trigger.CronConfig{
				Expression: "* * * * *", Timezone: "UTC",
			},
		}

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

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
		waitForTask(t, nc, "task-"+name)

		fireSpan := waitForSpanNamed(t, exporter, "trigger.fire", 5*time.Second)
		handleSpan := waitForSpanNamed(
			t, exporter, "dagnats.engine handleEvent", 5*time.Second)

		// Positive: handleEvent is a child of (shares the trace ID with)
		// trigger.fire — the parent hop this issue exists to prove.
		assertSpansShareTrace(t, fireSpan, handleSpan)

		// Negative: a second Tick for the same matching minute is
		// dedup-claimed before it ever reaches Fire (#173 at the tracing
		// level), so no second trigger.fire span ever appears.
		if err := scheduler.Tick(tickTime); err != nil {
			t.Fatalf("second Tick: %v", err)
		}
		harness.AssertHoldsForWindow(t,
			"exactly one trigger.fire span after dedup tick",
			500*time.Millisecond, spanPollInterval,
			func() (bool, string) {
				got := countFireSpansNamed(exporter.GetSpans(), "trigger.fire")
				if got != 1 {
					return false, fmt.Sprintf("span count = %d, want 1", got)
				}
				return true, ""
			})
	})
}

// TestFireTrigger_ParentsUnderAPISpan proves the #504 parent hop for the
// manual fire path: a 3-level chain "dagnats.api fireTrigger" ->
// "trigger.fire" -> "dagnats.engine handleEvent" all share one trace,
// and that a direct StartRun (bypassing triggers) never produces a
// trigger.fire span.
func TestFireTrigger_ParentsUnderAPISpan(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		exporter := installE2ESpanRecorder(t)
		createTriggerBuckets(t, nc)
		name := harness.UniqueName(t, "manual-parent-wf")
		triggerID := harness.UniqueName(t, "manual-parent-t")
		registerWorkflowDef(t, nc, name)
		registerTriggerDef(t, nc, trigger.TriggerDef{
			ID: triggerID, WorkflowID: name, Enabled: true,
			Cron: &trigger.CronConfig{
				Expression: "* * * * *", Timezone: "UTC",
			},
		})

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		svc := harness.NewTestService(t, nc)
		runID, err := svc.FireTrigger(context.Background(), triggerID)
		if err != nil {
			t.Fatalf("FireTrigger: %v", err)
		}
		if runID == "" {
			t.Fatal("FireTrigger: expected non-empty run ID")
		}
		waitForTask(t, nc, "task-"+name)

		apiSpan := waitForSpanNamed(
			t, exporter, "dagnats.api fireTrigger", 5*time.Second)
		fireSpan := waitForSpanNamed(t, exporter, "trigger.fire", 5*time.Second)
		handleSpan := waitForSpanNamed(
			t, exporter, "dagnats.engine handleEvent", 5*time.Second)

		// Positive: the full fireTrigger -> trigger.fire -> handleEvent
		// chain shares one trace ID.
		assertSpansShareTrace(t, apiSpan, fireSpan, handleSpan)

		// Negative: a direct StartRun bypasses triggers entirely, so its
		// trace must contain zero trigger.fire spans.
		exporter.Reset()
		if _, err := svc.StartRun(context.Background(), name, nil); err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		waitForSpanNamed(t, exporter, "dagnats.api startRun", 5*time.Second)
		if got := countFireSpansNamed(
			exporter.GetSpans(), "trigger.fire"); got != 0 {
			t.Fatalf(
				"trigger.fire span count after direct StartRun = %d, want 0",
				got)
		}
	})
}
