// e2e_trigger_resolution_test.go
// End-to-end: each trigger type fires → orchestrator resolves the
// registered WorkflowDef from workflow_defs KV → first task is
// dispatched. Methodology: real embedded NATS, real TriggerService,
// real Orchestrator. No mocks. Verifies #167 across all three trigger
// types in one place.
package dagnats_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
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

	svc, err := trigger.NewTriggerService(nc)
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

	svc, err := trigger.NewTriggerService(nc)
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

	svc, err := trigger.NewTriggerService(nc)
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
