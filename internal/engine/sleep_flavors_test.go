// internal/engine/sleep_flavors_test.go
// Tests for dispatch-time-resolved sleep steps: the cron and
// until_input_path forms of SleepConfig.
// Uses a real embedded NATS server.
// Methodology: start a run whose def contains a sleep step in the form
// under test, poll the persisted snapshot until the orchestrator has
// dispatched it, and assert on the recorded WakeAt — that is the single
// observable the whole resolution path feeds. Each test also checks the
// negative: that the wake time is not what a different form (or a
// different input source) would have produced. All waits are bounded.
package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// sleepFlavorDef builds a one-step workflow whose only step is a sleep
// step carrying the given raw config JSON.
func sleepFlavorDef(name, cfg string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:     "nap",
				Type:   dag.StepTypeSleep,
				Config: json.RawMessage(cfg),
			},
		},
	}
}

// waitForWakeAt polls the run snapshot until the named step has a
// recorded wake time, or the bounded deadline expires.
func waitForWakeAt(
	t *testing.T, orch *Orchestrator, runID, stepID string,
) time.Time {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := orch.store.Load(context.Background(), runID)
		if err == nil {
			if state, ok := run.Steps[stepID]; ok &&
				state.WakeAt != nil {
				return *state.WakeAt
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("step %q never recorded a WakeAt within 5s", stepID)
	return time.Time{}
}

func startSleepFlavorRun(
	t *testing.T, js nats.JetStreamContext,
	wfDef dag.WorkflowDef, runID string, input []byte,
) {
	t.Helper()
	defData := mustMarshal(t, wfDef)
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs KV: %v", err)
	}
	mustPut(t, defKV, wfDef.Name, defData)
	startAdmissionRun(t, js, wfDef, runID, input)
}

func TestSleepStepCronResolvesNextOccurrence(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	dispatchedAfter := time.Now()
	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-cron-wf", `{"cron":"* * * * *"}`),
		"run-sleep-cron", []byte(`{}`))

	wakeAt := waitForWakeAt(t, orch, "run-sleep-cron", "nap")

	// Positive: the wake time is the next whole minute, which is what
	// this cron expression means.
	if !wakeAt.Equal(wakeAt.Truncate(time.Minute)) {
		t.Errorf("WakeAt %s is not on a minute boundary", wakeAt)
	}
	// Negative: strictly after dispatch, and never more than one
	// interval away — a duration-form fallthrough would wake immediately.
	if !wakeAt.After(dispatchedAfter) {
		t.Errorf("WakeAt %s must follow dispatch %s",
			wakeAt, dispatchedAfter)
	}
	if wakeAt.Sub(dispatchedAfter) > time.Minute {
		t.Errorf("WakeAt %s is more than one minute out from %s",
			wakeAt, dispatchedAfter)
	}
}

func TestSleepStepUntilInputPathResolvesRFC3339(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	deadline := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	input := []byte(
		`{"deadline":"` + deadline.Format(time.RFC3339) + `"}`)
	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-until-wf",
			`{"until_input_path":"deadline"}`),
		"run-sleep-until", input)

	wakeAt := waitForWakeAt(t, orch, "run-sleep-until", "nap")

	// Positive: wake at the deadline. The engine computes
	// now + (deadline - now), so drift is only the sub-second remainder
	// discarded by the RFC3339 round-trip.
	if drift := wakeAt.Sub(deadline); drift > time.Second ||
		drift < -time.Second {
		t.Errorf("WakeAt %s differs from deadline %s by %v",
			wakeAt, deadline, drift)
	}
	// Negative: not an immediate wake.
	if wakeAt.Before(time.Now().Add(time.Hour)) {
		t.Errorf("WakeAt %s is far too early", wakeAt)
	}
}

func TestSleepStepUntilInputPathResolvesMilliseconds(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	dispatchedAfter := time.Now()
	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-ms-wf",
			`{"until_input_path":"wait_ms"}`),
		"run-sleep-ms", []byte(`{"wait_ms":3600000}`))

	wakeAt := waitForWakeAt(t, orch, "run-sleep-ms", "nap")

	delay := wakeAt.Sub(dispatchedAfter)
	// Positive: roughly one hour out (3600000 ms).
	if delay < 59*time.Minute || delay > time.Hour+time.Minute {
		t.Errorf("delay %v, want about 1h", delay)
	}
	// Negative: the number was not read as nanoseconds or seconds.
	if delay < time.Minute {
		t.Errorf("delay %v suggests the unit was misread", delay)
	}
}

func TestSleepStepUntilInputPathPastInstantCompletes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	past := time.Now().Add(-72 * time.Hour).Format(time.RFC3339)
	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-past-wf",
			`{"until_input_path":"deadline"}`),
		"run-sleep-past", []byte(`{"deadline":"`+past+`"}`))

	// Positive: a past deadline clamps to a zero-length sleep, so the
	// run completes normally rather than erroring.
	deadline := time.Now().Add(10 * time.Second)
	var status dag.RunStatus
	for time.Now().Before(deadline) {
		run, err := orch.store.Load(
			context.Background(), "run-sleep-past")
		if err == nil {
			status = run.Status
			if status == dag.RunStatusCompleted {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if status != dag.RunStatusCompleted {
		t.Fatalf("run status = %s, want completed", status)
	}

	// Assert on the wake time, not merely on completion: the clamp is
	// what makes WakeAt land at dispatch time rather than 72 hours in
	// the past, and a completion-only check cannot tell those apart.
	run, err := orch.store.Load(context.Background(), "run-sleep-past")
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	wakeAt := run.Steps["nap"].WakeAt
	if wakeAt == nil {
		t.Fatal("sleep step recorded no WakeAt")
	}
	if drift := time.Since(*wakeAt); drift > time.Minute {
		t.Errorf(
			"WakeAt %s is %v in the past — past instants must clamp "+
				"to a zero-length sleep, not carry the stale deadline",
			wakeAt, drift)
	}
}

func TestSleepStepPastInstantSchedulesMinimumTimer(t *testing.T) {
	// Pins the durationMs <= 0 -> 1 clamp in enqueueSleepStep, which the
	// past-instant path made newly reachable with an exact zero.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	sub, err := js.SubscribeSync(
		"sleep.run-sleep-clamp.nap", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	past := time.Now().Add(-48 * time.Hour).Format(time.RFC3339)
	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-clamp-wf",
			`{"until_input_path":"deadline"}`),
		"run-sleep-clamp", []byte(`{"deadline":"`+past+`"}`))

	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("no timer message scheduled: %v", err)
	}
	var timer TimerMessage
	if err := json.Unmarshal(msg.Data, &timer); err != nil {
		t.Fatalf("unmarshal TimerMessage: %v", err)
	}

	// Positive: the clamp floors a zero-length sleep at 1ms so NATS
	// still redelivers and the step completes.
	if timer.DurationMs != 1 {
		t.Errorf("DurationMs = %d, want 1", timer.DurationMs)
	}
	// Negative: never zero or negative, which would not schedule.
	if timer.DurationMs <= 0 {
		t.Errorf("DurationMs = %d must be positive", timer.DurationMs)
	}
	if timer.Action != TimerActionSleepComplete {
		t.Errorf("Action = %q, want %q",
			timer.Action, TimerActionSleepComplete)
	}
}

func TestSleepStepResolutionFailureFailsStep(t *testing.T) {
	// until_input_path resolves against immutable run input, so a missing
	// path fails identically on every redelivery. The step must reach
	// Failed rather than sitting Queued forever.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startSleepFlavorRun(t, js,
		sleepFlavorDef("sleep-missing-wf",
			`{"until_input_path":"deadline"}`),
		"run-sleep-missing", []byte(`{"other":1}`))

	deadline := time.Now().Add(10 * time.Second)
	var state dag.StepState
	var runStatus dag.RunStatus
	for time.Now().Before(deadline) {
		run, loadErr := orch.store.Load(
			context.Background(), "run-sleep-missing")
		if loadErr == nil {
			state = run.Steps["nap"]
			runStatus = run.Status
			if state.Status == dag.StepStatusFailed {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Positive: the step is Failed with a diagnostic naming the path.
	if state.Status != dag.StepStatusFailed {
		t.Fatalf("step status = %s, want failed", state.Status)
	}
	if !strings.Contains(state.Error, "deadline") {
		t.Errorf("step error %q should name the path", state.Error)
	}
	// Negative: it did not wedge in Queued, and the run did not stay
	// Running with no terminal step.
	if state.Status == dag.StepStatusQueued {
		t.Error("step must not remain queued on a permanent failure")
	}
	if runStatus != dag.RunStatusFailed {
		t.Errorf("run status = %s, want failed", runStatus)
	}
}

func TestSleepStepUntilInputPathReadsRunInputNotStepInput(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	runDeadline := time.Now().Add(6 * time.Hour).Truncate(time.Second)
	upstreamDeadline := time.Now().Add(20 * time.Minute).
		Truncate(time.Second)

	wfDef := dag.WorkflowDef{
		Name:    "sleep-runinput-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "prep", Type: dag.StepTypeNormal},
			{
				ID:        "nap",
				Type:      dag.StepTypeSleep,
				DependsOn: []string{"a"},
				Config: json.RawMessage(
					`{"until_input_path":"deadline"}`),
			},
		},
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startSleepFlavorRun(t, js, wfDef, "run-sleep-src",
		[]byte(`{"deadline":"`+
			runDeadline.Format(time.RFC3339)+`"}`))

	// Drain the dispatched task for step "a" so the run advances.
	sub, err := js.PullSubscribe(
		"task.prep.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("subscribe task.prep: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch task.prep: %v", err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack task.prep: %v", err)
	}

	// Step "a" emits its own "deadline" — the wrong source. If the sleep
	// step resolved against its resolved (upstream) input it would wake
	// 20 minutes out instead of 6 hours out.
	comp := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-sleep-src", "a",
		[]byte(`{"deadline":"`+
			upstreamDeadline.Format(time.RFC3339)+`"}`),
	)
	compData, err := comp.Marshal()
	if err != nil {
		t.Fatalf("marshal step completed: %v", err)
	}
	if _, err := js.Publish(
		comp.NATSSubject(), compData,
		nats.MsgId(comp.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish step completed: %v", err)
	}

	wakeAt := waitForWakeAt(t, orch, "run-sleep-src", "nap")

	// Positive: resolved against the run input.
	if drift := wakeAt.Sub(runDeadline); drift > 5*time.Second ||
		drift < -5*time.Second {
		t.Errorf("WakeAt %s should track run input deadline %s",
			wakeAt, runDeadline)
	}
	// Negative: not the upstream step's output.
	if wakeAt.Before(upstreamDeadline.Add(time.Minute)) {
		t.Errorf(
			"WakeAt %s tracks the upstream output %s — "+
				"sleep must resolve against run input",
			wakeAt, upstreamDeadline)
	}
}
