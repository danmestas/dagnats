// continue_wait_test.go
// Methodology: real embedded NATS, a running orchestrator, a real bridge
// and an agent-loop step claimed over HTTP. A `continue` carrying a
// wait (ADR-027) must park the step exactly as the Go SDK's Continue
// does: the engine holds it and no task is redelivered until the delay
// elapses or an input is submitted. Both the plain-step and the
// journal-step paths run. Negative space: a wait on input while inputs
// are queued is refused 409 inputs_queued without publishing the
// continue, and the task stays claimed (heartbeat still 200). Every
// wait is bounded.
package bridge

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const waitTestTimeout = 10 * time.Second

type waitRig struct {
	nc    *nats.Conn
	js    jetstream.JetStream
	ts    *httptest.Server
	runID string
}

// newWaitRig starts the engine and bridge and one run of an agent-loop
// step on task type "echo"; the step id is "agent".
func newWaitRig(t *testing.T) *waitRig {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	ts := httptest.NewServer(newTestBridge(t, nc).Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), waitTestTimeout)
	defer cancel()
	svc := api.NewService(nc)
	wfDef := dag.WorkflowDef{
		Name: "continue-wait", Version: "1",
		Steps: []dag.StepDef{{
			ID: "agent", Task: "echo", Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 10}),
		}},
	}
	if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	runID, err := svc.StartRun(ctx, "continue-wait", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return &waitRig{nc: nc, js: js, ts: ts, runID: runID}
}

// claim polls one task, failing when none arrives within the bound.
func (r *waitRig) claim(t *testing.T, journaled bool) pollResponse {
	t.Helper()
	tasks := postPoll(t, r.ts, "echo", 1, 5000)
	if len(tasks) != 1 {
		t.Fatalf("claim: got %d tasks, want 1", len(tasks))
	}
	if journaled {
		status, body := resolveBody(t, r.ts, tasks[0].TaskID,
			`{"action":"journal.append","type":"entry","data":{"k":1}}`)
		if status != 200 {
			t.Fatalf("journal.append: %d %s", status, body)
		}
	}
	return tasks[0]
}

// submit places a queued input for the step and announces it.
func (r *waitRig) submit(t *testing.T, requestID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, r.js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: r.runID, StepID: "agent", RequestID: requestID, Mode: inputs.ModeSteer,
		Content: json.RawMessage(`"x"`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	evt := protocol.NewInputQueuedEvent(r.runID, "agent", requestID)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal input.queued: %v", err)
	}
	if _, err := r.js.Publish(ctx, evt.NATSSubject(), data, jetstream.WithMsgID(evt.NATSMsgID())); err != nil {
		t.Fatalf("publish input.queued: %v", err)
	}
}

func (r *waitRig) noTask(t *testing.T, within time.Duration) {
	t.Helper()
	ms := int(within / time.Millisecond)
	if got := postPoll(t, r.ts, "echo", 1, ms); len(got) != 0 {
		t.Fatalf("got a task while the step should be parked: %+v", got)
	}
}

func continueEventWait(t *testing.T, r *waitRig) *protocol.ContinueWait {
	t.Helper()
	evt := consumeHistoryEvent(t, r.nc, r.runID, protocol.EventStepContinue, 5*time.Second)
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode continue payload: %v", err)
	}
	return payload.Wait
}

func TestContinueWait_DelayParksThenRedelivers(t *testing.T) {
	for _, journaled := range []bool{false, true} {
		name := "plain"
		if journaled {
			name = "journal"
		}
		t.Run(name, func(t *testing.T) {
			rig := newWaitRig(t)
			task := rig.claim(t, journaled)
			start := time.Now()
			status, body := resolveBody(t, rig.ts, task.TaskID,
				`{"action":"continue","output":{"o":1},"wait":{"delay_ms":1500}}`)
			if status != 200 {
				t.Fatalf("continue: %d %s", status, body)
			}
			if wait := continueEventWait(t, rig); wait == nil || wait.DelayMs != 1500 || wait.Input {
				t.Fatalf("event wait = %+v, want delay_ms 1500 only", wait)
			}
			if journaled {
				recs, err := journal.Read(context.Background(), rig.js, rig.runID, "agent")
				if err != nil {
					t.Fatalf("journal.Read: %v", err)
				}
				var commit journal.Commit
				last := recs[len(recs)-1]
				if err := json.Unmarshal(last.Data, &commit); err != nil || commit.Wait == nil ||
					commit.Wait.DelayMs != 1500 {
					t.Fatalf("commit = %s (%v), want the wait recorded", last.Data, err)
				}
			}
			rig.noTask(t, 500*time.Millisecond)
			next := postPoll(t, rig.ts, "echo", 1, 5000)
			if len(next) != 1 || next[0].Iteration != 1 {
				t.Fatalf("after delay got %+v, want iteration 1", next)
			}
			if time.Since(start) < 1500*time.Millisecond {
				t.Errorf("redelivered after %s, before the delay elapsed", time.Since(start))
			}
		})
	}
}

func TestContinueWait_InputParksUntilSubmitted(t *testing.T) {
	for _, journaled := range []bool{false, true} {
		name := "plain"
		if journaled {
			name = "journal"
		}
		t.Run(name, func(t *testing.T) {
			rig := newWaitRig(t)
			task := rig.claim(t, journaled)
			status, body := resolveBody(t, rig.ts, task.TaskID,
				`{"action":"continue","wait":{"input":true}}`)
			if status != 200 {
				t.Fatalf("continue: %d %s", status, body)
			}
			if wait := continueEventWait(t, rig); wait == nil || !wait.Input {
				t.Fatalf("event wait = %+v, want input", wait)
			}
			rig.noTask(t, 700*time.Millisecond)
			rig.submit(t, "req-1")
			next := postPoll(t, rig.ts, "echo", 1, 5000)
			if len(next) != 1 || next[0].Iteration != 1 {
				t.Fatalf("after submit got %+v, want iteration 1", next)
			}
		})
	}
}

func TestContinueWait_InputRefusedWhileQueued(t *testing.T) {
	for _, journaled := range []bool{false, true} {
		name := "plain"
		if journaled {
			name = "journal"
		}
		t.Run(name, func(t *testing.T) {
			rig := newWaitRig(t)
			task := rig.claim(t, journaled)
			rig.submit(t, "req-1")
			status, body := resolveBody(t, rig.ts, task.TaskID,
				`{"action":"continue","wait":{"input":true}}`)
			if status != 409 {
				t.Fatalf("continue = %d %s, want 409", status, body)
			}
			var got struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal([]byte(body), &got); err != nil || got.Code != "inputs_queued" ||
				got.Error == "" {
				t.Fatalf("409 body = %s, want code inputs_queued with an error", body)
			}
			// Still claimed and nothing published: the client can carry on.
			if status, _ := resolveBody(t, rig.ts, task.TaskID, `{"action":"heartbeat"}`); status != 200 {
				t.Fatalf("heartbeat after 409 = %d, want 200 (still claimed)", status)
			}
			if n := countHistory(t, rig.nc, rig.runID, protocol.EventStepContinue); n != 0 {
				t.Fatalf("%d step.continue events after a refused wait, want 0", n)
			}
			status, body = resolveBody(t, rig.ts, task.TaskID, `{"action":"continue"}`)
			if status != 200 {
				t.Fatalf("continue without wait = %d %s, want 200", status, body)
			}
		})
	}
}

func TestContinueWait_InvalidIs400(t *testing.T) {
	rig := newWaitRig(t)
	task := rig.claim(t, false)
	for _, wait := range []string{
		`{"delay_ms":-1}`,
		`{"until":"not-a-time"}`,
		`{"until":"0001-01-01T00:00:00Z"}`,
	} {
		status, body := resolveBody(t, rig.ts, task.TaskID,
			`{"action":"continue","wait":`+wait+`}`)
		if status != 400 {
			t.Errorf("wait %s = %d %s, want 400", wait, status, body)
		}
		if !strings.Contains(body, "invalid_wait") && !strings.Contains(body, "invalid JSON") {
			t.Errorf("wait %s body = %s, want code invalid_wait or invalid JSON", wait, body)
		}
	}
	if status, _ := resolveBody(t, rig.ts, task.TaskID, `{"action":"heartbeat"}`); status != 200 {
		t.Fatalf("heartbeat after invalid waits = %d, want 200", status)
	}
}
