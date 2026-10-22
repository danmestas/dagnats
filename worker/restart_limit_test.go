// worker/restart_limit_test.go
// The crash-loop guard (ADR-028, #807): a journal step fails with
// reason restart_limit once the configured number of deliveries have
// claimed one iteration without progress; pauses are progress and
// never trip it; the retry after the failure resumes from the
// journal; the limit is a Worker option with a default of 5.
//
// Methodology: taskContexts built from a TaskPayload the way the
// Worker builds one, against an embedded NATS server with the journal
// provisioned; crashes are deliveries that claim and are abandoned,
// and a redelivery keeps the payload (nonce, stamped attempt from
// iteration 1) and raises only the delivery count. One case runs
// real Workers behind a real orchestrator, with one Worker per
// stalled delivery because a stalled handler blocks its consumer
// loop. Every wait is bounded.
package worker

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// commitIteration0 commits iteration 0 of runID with state so later
// deliveries start at iteration 1 with the stamped attempt.
func commitIteration0(t *testing.T, nc *nats.Conn, runID string, state []byte) {
	t.Helper()
	tc := delivery(t, nc, journalPayload(runID, 0), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	if err := tc.Checkpoint(state); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := tc.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}
}

// abandonedDeliveries claims iteration 1 of runID n times without
// progress, as n crashed deliveries of one task message would.
func abandonedDeliveries(t *testing.T, nc *nats.Conn, runID string, n int) {
	t.Helper()
	for delivered := 1; delivered <= n; delivered++ {
		tc := delivery(t, nc, journalPayload(runID, 1), delivered)
		if _, err := tc.Journal(); err != nil {
			t.Fatalf("delivery %d Journal: %v", delivered, err)
		}
	}
}

func TestRestartLimit_RedeliveriesWithoutProgressTripIt(t *testing.T) {
	nc, js := journalServer(t)
	commitIteration0(t, nc, "run-rl", []byte("s0"))
	abandonedDeliveries(t, nc, "run-rl", RestartLimitDefault)
	tripped := delivery(t, nc, journalPayload("run-rl", 1), RestartLimitDefault+1)
	_, err := tripped.Journal()
	if !errors.Is(err, journal.ErrRestartLimit) {
		t.Fatalf("Journal on delivery %d err = %v, want ErrRestartLimit", RestartLimitDefault+1, err)
	}
	evt := nextHistory(t, nc, "run-rl", protocol.EventStepFailed)
	var failed protocol.StepFailedPayload
	if err := json.Unmarshal(evt.Payload, &failed); err != nil {
		t.Fatalf("decode step.failed: %v", err)
	}
	if !strings.HasPrefix(failed.Error, ReasonRestartLimit) ||
		failed.FailureType != protocol.FailureTypeRetriable || evt.AttemptNumber != 1 {
		t.Errorf("step.failed = %+v on attempt %d, want a retriable %s failure of attempt 1",
			failed, evt.AttemptNumber, ReasonRestartLimit)
	}
	records := readJournal(t, js, "run-rl")
	last := records[len(records)-1]
	if last.Type != journal.TypeCommit || evt.JournalSeq != last.Seq {
		t.Errorf("tail = %+v, event journal_seq = %d; want the fail commit named by the event",
			last, evt.JournalSeq)
	}
	if done, ack := tripped.journalOutcome(err); !done || !ack {
		t.Errorf("journalOutcome = done %v ack %v, want the task acknowledged", done, ack)
	}
	if err := tripped.Complete(nil); !errors.Is(err, journal.ErrRestartLimit) {
		t.Errorf("Complete after the limit err = %v, want ErrRestartLimit", err)
	}
	// The retry is attempt 2 of iteration 1: it resumes from the
	// journal with the state iteration 0 committed and a fresh count.
	retryPayload := journalPayload("run-rl", 1)
	retryPayload.Attempt = 2
	retry := delivery(t, nc, retryPayload, 1)
	j, err := retry.Journal()
	if err != nil {
		t.Fatalf("retry Journal: %v", err)
	}
	state, err := retry.LoadCheckpoint()
	if err != nil || string(state) != "s0" || j.Restarts() != 0 {
		t.Errorf("retry LoadCheckpoint = %q, %v, Restarts = %d; want s0 and 0", state, err, j.Restarts())
	}
}

func TestRestartLimit_BelowTheLimitRunsAndIsConfigurable(t *testing.T) {
	nc, _ := journalServer(t)
	commitIteration0(t, nc, "run-rb", []byte("s0"))
	abandonedDeliveries(t, nc, "run-rb", RestartLimitDefault-1)
	fifth := delivery(t, nc, journalPayload("run-rb", 1), RestartLimitDefault)
	if _, err := fifth.Journal(); err != nil {
		t.Errorf("delivery %d Journal err = %v, want the handler to run", RestartLimitDefault, err)
	}
	w := NewWorker(nc, WithRestartLimit(2))
	if w.restartLimit != 2 {
		t.Fatalf("restartLimit = %d, want 2", w.restartLimit)
	}
	commitIteration0(t, nc, "run-rc", []byte("s0"))
	abandonedDeliveries(t, nc, "run-rc", 2)
	third := delivery(t, nc, journalPayload("run-rc", 1), 3)
	third.restartLimit = w.restartLimit
	if _, err := third.Journal(); !errors.Is(err, journal.ErrRestartLimit) {
		t.Errorf("delivery 3 with limit 2 err = %v, want ErrRestartLimit", err)
	}
}

func TestRestartLimit_PausesAreProgress(t *testing.T) {
	nc, _ := journalServer(t)
	commitIteration0(t, nc, "run-rp", []byte("s0"))
	for delivered := 1; delivered <= RestartLimitDefault+1; delivered++ {
		tc := delivery(t, nc, journalPayload("run-rp", 1), delivered)
		if _, err := tc.Journal(); err != nil {
			t.Fatalf("delivery %d Journal err = %v, want pauses never to trip the limit", delivered, err)
		}
		if err := tc.Pause("cooldown", 10*time.Millisecond); err != nil {
			t.Fatalf("delivery %d Pause: %v", delivered, err)
		}
	}
	resumed := delivery(t, nc, journalPayload("run-rp", 1), RestartLimitDefault+2)
	j, err := resumed.Journal()
	if err != nil {
		t.Fatalf("resumed Journal: %v", err)
	}
	if name, ok := j.Resume(); !ok || name != "cooldown" || j.Restarts() != 0 {
		t.Errorf("Resume = %q %v, Restarts = %d; want cooldown and 0", name, ok, j.Restarts())
	}
}

func TestRestartLimit_RealCrashLoopFailsThenRetryResumesFromJournal(t *testing.T) {
	const taskType = "restart-limit-task"
	nc, js, jsNew, defData := startLoopEngineDef(t, loopDefWithRetry("wf-restart-limit", taskType))
	release := make(chan struct{})
	stalled := make(chan struct{}, RestartLimitDefault+1)
	fenced := make(chan error, RestartLimitDefault+1)
	handler := func(tc TaskContext) error {
		j, err := tc.Journal()
		if err != nil {
			return err
		}
		if j.Task().Iteration == 0 {
			if err := tc.Checkpoint([]byte(`"s0"`)); err != nil {
				return err
			}
			return tc.Continue(nil)
		}
		if j.Task().Attempt == 1 {
			stalled <- struct{}{}
			<-release
			_, err := j.Append(tc.Context(), journal.TypeEntry, []byte(`"late"`))
			fenced <- err
			return err
		}
		state, err := tc.LoadCheckpoint()
		if err != nil {
			return err
		}
		return tc.Complete(state)
	}
	// One Worker per stalled delivery: a stalled handler blocks its
	// consumer loop, so the redelivery must land elsewhere.
	for i := 0; i <= RestartLimitDefault; i++ {
		startFencingWorker(t, nc, taskType, handler)
	}
	startStepRun(t, js, "run-crashloop", defData)
	for i := 0; i < RestartLimitDefault; i++ {
		waitClosed(t, stalled, "stalled delivery")
	}
	run := waitStepSettled(t, jsNew, "run-crashloop", 20*time.Second)
	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted || string(step.Output) != `"s0"` || step.Attempts != 2 {
		t.Fatalf("step = %+v, want completed on attempt 2 with the journal's state", step)
	}
	evt := nextHistory(t, nc, "run-crashloop", protocol.EventStepFailed)
	if !strings.Contains(string(evt.Payload), ReasonRestartLimit) {
		t.Errorf("step.failed payload = %s, want reason %s", evt.Payload, ReasonRestartLimit)
	}
	close(release)
	for i := 0; i < RestartLimitDefault; i++ {
		select {
		case err := <-fenced:
			if !errors.Is(err, journal.ErrFenced) {
				t.Errorf("stalled delivery append err = %v, want ErrFenced", err)
			}
		case <-time.After(journalTestTimeout):
			t.Fatal("a stalled delivery never resumed")
		}
	}
	assertJournalClean(t, nc, "run-crashloop")
}

// loopDefWithRetry is the agent-loop def of startLoopEngine with one
// quick retry, so a restart_limit failure is retried.
func loopDefWithRetry(name, taskType string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: name, Version: "1",
		Steps: []dag.StepDef{{
			ID: "s", Task: taskType, Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 10}),
			Retry: &dag.RetryPolicy{
				MaxAttempts: 2, Strategy: dag.RetryFixed,
				InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond,
			},
		}},
	}
}
