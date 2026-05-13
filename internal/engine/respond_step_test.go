// engine/respond_step_test.go
//
// Methodology: integration tests with embedded NATS. Build a workflow
// whose terminal step is a Respond step and drive it through the
// orchestrator's normal event loop. Assert that the engine publishes
// the response envelope to httpenvelope.ResponseSubject(runID), and
// that the respond step is marked Completed in the run snapshot so
// the DAG advance machinery keeps working. Each test starts its own
// embedded NATS server (no shared state). Bounded waits — no
// unbounded polling.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// respondPayloadEnvelope mirrors the on-the-wire shape the engine
// publishes to ResponseSubject(runID). Defined here so a shape-drift
// fails the test at compile time.
type respondPayloadEnvelope struct {
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers,omitempty"`
	ContentType string            `json:"content_type"`
	Body        []byte            `json:"body,omitempty"`
}

func TestEngineRespondStepPublishesResponseWithConfiguredStatus(t *testing.T) {
	const runID = "run-respond-happy-1"
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	respSub, err := nc.SubscribeSync(httpenvelope.ResponseSubject(runID))
	if err != nil {
		t.Fatalf("SubscribeSync response: %v", err)
	}
	defer func() { _ = respSub.Unsubscribe() }()

	respCfg, err := json.Marshal(dag.RespondConfig{Status: 201})
	if err != nil {
		t.Fatalf("marshal RespondConfig: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name:    "respond-happy",
		Version: "v1",
		Steps: []dag.StepDef{
			{
				ID:     "respond",
				Type:   dag.StepTypeRespond,
				Config: respCfg,
			},
		},
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	publishStructuredStart(t, nc, runID, wfDef, []byte(`{"hello":"world"}`))

	msg, err := respSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("waiting for response publish: %v", err)
	}
	var got respondPayloadEnvelope
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("unmarshal response envelope: %v", err)
	}
	if got.Status != 201 {
		t.Fatalf("Status = %d, want 201 (configured)", got.Status)
	}
	if got.ContentType != "application/json" {
		t.Fatalf("ContentType = %q, want default", got.ContentType)
	}
	// Without an upstream step, the run's Input is forwarded as the
	// body. Programmer authors who want a different body provide an
	// explicit upstream step or a BodyFrom dotpath.
	if string(got.Body) != `{"hello":"world"}` {
		t.Fatalf("Body = %q, want forwarded input", got.Body)
	}
}

func TestEngineRespondStepDefaultsStatusAndContentType(t *testing.T) {
	const runID = "run-respond-defaults-1"
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	respSub, err := nc.SubscribeSync(httpenvelope.ResponseSubject(runID))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() { _ = respSub.Unsubscribe() }()

	respCfg, err := json.Marshal(dag.RespondConfig{})
	if err != nil {
		t.Fatalf("marshal RespondConfig: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name:    "respond-defaults",
		Version: "v1",
		Steps: []dag.StepDef{
			{
				ID:     "respond",
				Type:   dag.StepTypeRespond,
				Config: respCfg,
			},
		},
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	publishStructuredStart(t, nc, runID, wfDef, nil)

	msg, err := respSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("waiting for response: %v", err)
	}
	var got respondPayloadEnvelope
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != 200 {
		t.Fatalf("Status = %d, want default 200", got.Status)
	}
	if got.ContentType != "application/json" {
		t.Fatalf("ContentType = %q, want default", got.ContentType)
	}
}

func TestEngineRespondStepHonoursBodyFromDotpath(t *testing.T) {
	const runID = "run-respond-bodyfrom-1"
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	respSub, err := nc.SubscribeSync(httpenvelope.ResponseSubject(runID))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() { _ = respSub.Unsubscribe() }()

	respCfg, err := json.Marshal(dag.RespondConfig{
		Status:   202,
		BodyFrom: "result.value",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name:    "respond-bodyfrom",
		Version: "v1",
		Steps: []dag.StepDef{
			{
				ID:     "respond",
				Type:   dag.StepTypeRespond,
				Config: respCfg,
			},
		},
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	input := []byte(`{"result":{"value":"extracted"}}`)
	publishStructuredStart(t, nc, runID, wfDef, input)

	msg, err := respSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("waiting for response: %v", err)
	}
	var got respondPayloadEnvelope
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != 202 {
		t.Fatalf("Status = %d, want 202", got.Status)
	}
	if string(got.Body) != `"extracted"` {
		t.Fatalf("Body = %q, want extracted dotpath value", got.Body)
	}
}

// publishStructuredStart publishes a workflow.started event in the
// {workflow_def, input} shape that the orchestrator handles.
func publishStructuredStart(
	t *testing.T, nc *nats.Conn,
	runID string, wfDef dag.WorkflowDef, input []byte,
) {
	t.Helper()
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal wfDef: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workflow_def": json.RawMessage(defData),
		"input":        json.RawMessage(input),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	if _, err := js.PublishMsg(
		&nats.Msg{
			Subject: evt.NATSSubject(),
			Data:    data,
			Header:  nats.Header{nats.MsgIdHdr: []string{evt.NATSMsgID()}},
		},
	); err != nil {
		t.Fatalf("publish: %v", err)
	}
	_ = ctx
}
