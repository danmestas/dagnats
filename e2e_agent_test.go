// e2e_agent_test.go
// End-to-end test: verify that agent steps are routed to a custom stream
// while normal steps go to TASK_QUEUES. A mixed workflow (normal -> agent)
// exercises the full path: orchestrator dispatches, normal worker completes,
// agent task appears on the AGENT_TASKS stream.
// Methodology: real NATS server, real orchestrator, real workers. No mocks.
package dagnats_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestE2EAgentStepRouting(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	err := natsutil.SetupAll(nc,
		natsutil.WithStreams(natsutil.StreamConfig{
			Name:     "AGENT_TASKS",
			Subjects: []string{"agent.task.>"},
		}),
	)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Workflow: normal prepare step -> agent step
	wfDef := dag.WorkflowDef{
		Name:    "mixed-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "prepare", Task: "prep-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "agent", Task: "llm-task",
				Type:      dag.StepTypeAgent,
				DependsOn: []string{"prepare"},
				Metadata:  map[string]string{"role": "coder"},
			},
		},
	}
	defData, _ := json.Marshal(wfDef)
	if _, err := defKV.Put("mixed-wf", defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	// Start orchestrator with agent routing
	routes := map[dag.StepType]string{
		dag.StepTypeAgent: "agent.task",
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry(),
		engine.WithStepRoutes(routes))
	orch.Start()
	defer orch.Stop()

	// Normal worker handles "prepare"
	w := worker.NewWorker(nc, observe.NewNoopTelemetry())
	w.Handle("prep-task", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"prepared"`))
	})
	w.Start()
	defer w.Stop()

	// Subscribe to AGENT_TASKS to verify routing
	agentSub, err := js.SubscribeSync("agent.task.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe agent tasks: %v", err)
	}

	// Start the workflow via history event
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "e2e-mixed-1", defData)
	data, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), data,
		nats.MsgId(startEvt.NATSMsgID()))

	// Wait for agent task to arrive on AGENT_TASKS
	// (prepare step must complete first, then agent step is enqueued)
	agentMsg, err := agentSub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("agent task should arrive on AGENT_TASKS: %v", err)
	}
	if agentMsg == nil {
		t.Fatalf("agent message should not be nil")
	}

	// Verify it's the agent step's payload
	var payload protocol.TaskPayload
	if err := json.Unmarshal(agentMsg.Data, &payload); err != nil {
		t.Fatalf("unmarshal agent payload: %v", err)
	}
	if payload.StepID != "agent" {
		t.Fatalf("step id = %q, want agent", payload.StepID)
	}
	if payload.RunID != "e2e-mixed-1" {
		t.Fatalf("run id = %q, want e2e-mixed-1", payload.RunID)
	}
	agentMsg.Ack()
}
