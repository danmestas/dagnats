// e2e/features/agent_routing_test.go
// End-to-end test migrated from the repo-root e2e_agent_test.go: verify
// that agent steps are routed to a custom stream (AGENT_TASKS) while
// normal steps go to TASK_QUEUES. A mixed workflow (normal -> agent)
// exercises the full path: orchestrator dispatches, normal worker
// completes, agent task appears on the AGENT_TASKS stream. Runs against
// every enabled topology via RunE2E.
// Methodology: real NATS server, real orchestrator, real workers. No
// mocks.
package features

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// putMixedWorkflowDef writes a normal->agent workflow def to the
// workflow_defs KV and returns the marshaled def (reused as the
// workflow.started event payload). The agent step carries a role
// metadata so routing keys off StepTypeAgent, not the task name.
func putMixedWorkflowDef(
	t *testing.T, js nats.JetStreamContext, wfName string,
) []byte {
	t.Helper()
	if wfName == "" {
		panic("putMixedWorkflowDef: wfName must not be empty")
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue workflow_defs: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "1",
		Steps: []dag.StepDef{
			{ID: "prepare", Task: "prep-task", Type: dag.StepTypeNormal},
			{
				ID: "agent", Task: "llm-task", Type: dag.StepTypeAgent,
				DependsOn: []string{"prepare"},
				Metadata:  map[string]string{"role": "coder"},
			},
		},
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := defKV.Put(wfName, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}
	return defData
}

func TestE2EAgentStepRouting(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		// AGENT_TASKS stream is not provisioned by the harness.
		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "AGENT_TASKS",
			Subjects: []string{"agent.task.>"},
		}); err != nil {
			t.Fatalf("add AGENT_TASKS stream: %v", err)
		}

		wfName := harness.UniqueName(t, "mixed-wf")
		runID := harness.UniqueName(t, "e2e-mixed")
		defData := putMixedWorkflowDef(t, js, wfName)

		// Orchestrator routes agent steps to the agent.task subject.
		orch := engine.NewOrchestrator(nc, engine.WithStepRoutes(
			map[dag.StepType]string{dag.StepTypeAgent: "agent.task"},
		))
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Normal worker handles "prepare"; nothing handles "agent".
		harness.SubscribeWorker(t, nc, "prep-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"prepared"`))
			})

		agentSub, err := js.SubscribeSync("agent.task.>",
			nats.AckExplicit(), nats.DeliverAll())
		if err != nil {
			t.Fatalf("subscribe agent tasks: %v", err)
		}

		// Start the workflow via a history event.
		startEvt := protocol.NewWorkflowEvent(
			protocol.EventWorkflowStarted, runID, defData)
		data, err := startEvt.Marshal()
		if err != nil {
			t.Fatalf("marshal start event: %v", err)
		}
		if _, err := js.Publish(startEvt.NATSSubject(), data,
			nats.MsgId(startEvt.NATSMsgID())); err != nil {
			t.Fatalf("publish start event: %v", err)
		}

		// Positive: the agent task arrives on AGENT_TASKS after prepare
		// completes and the agent step is enqueued.
		agentMsg, err := agentSub.NextMsg(10 * time.Second)
		if err != nil {
			t.Fatalf("agent task should arrive on AGENT_TASKS: %v", err)
		}
		if agentMsg == nil {
			t.Fatalf("agent message should not be nil")
		}
		var payload protocol.TaskPayload
		if err := json.Unmarshal(agentMsg.Data, &payload); err != nil {
			t.Fatalf("unmarshal agent payload: %v", err)
		}
		// Positive: it is the agent step's payload for this run.
		if payload.StepID != "agent" {
			t.Fatalf("step id = %q, want agent", payload.StepID)
		}
		if payload.RunID != runID {
			t.Fatalf("run id = %q, want %q", payload.RunID, runID)
		}
		agentMsg.Ack()
	})
}
