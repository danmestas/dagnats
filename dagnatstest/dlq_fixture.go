// dagnatstest/dlq_fixture.go
// DLQFixture clusters DLQ-related test helpers off a shared Harness.
// Construct via NewDLQFixture(h). Helpers stay under 70 lines (TigerStyle).
//
// Concern boundary: the Harness owns embedded NATS lifecycle; the fixture
// owns DLQ-stream observation and DLQ-publish drivers. The fixture never
// mutates the Harness beyond reading its NATS connection.
package dagnatstest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DLQFixture holds DLQ-focused test helpers bound to a Harness.
// Use NewDLQFixture(h) to construct.
type DLQFixture struct {
	h *Harness
}

// NewDLQFixture binds a DLQFixture to an existing Harness. Panics if h is nil.
func NewDLQFixture(h *Harness) *DLQFixture {
	if h == nil {
		panic("NewDLQFixture: h must not be nil")
	}
	if h.NC == nil {
		panic("NewDLQFixture: h.NC must not be nil")
	}
	return &DLQFixture{h: h}
}

// Count returns the current number of entries on the DEAD_LETTERS stream.
// Reads stream info directly — no consumer needed.
func (f *DLQFixture) Count(ctx context.Context) (int, error) {
	if ctx == nil {
		panic("Count: ctx must not be nil")
	}
	if f.h == nil {
		panic("Count: fixture not bound")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return 0, err
	}
	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return int(info.State.Msgs), nil
}

// PublishDLQWithMsgID publishes a synthetic DLQ entry on subject `dead.<task>`
// with the given Nats-Msg-Id header for dedup testing. Returns the publish
// error so callers can assert success/failure.
//
// Used by tests that exercise the dedup contract directly without driving
// the full workflow-failure path.
func (f *DLQFixture) PublishDLQWithMsgID(
	ctx context.Context, subject string, msgID string, body []byte,
) error {
	if ctx == nil {
		panic("PublishDLQWithMsgID: ctx must not be nil")
	}
	if subject == "" {
		panic("PublishDLQWithMsgID: subject must not be empty")
	}
	if msgID == "" {
		panic("PublishDLQWithMsgID: msgID must not be empty")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    body,
		Header: nats.Header{
			"Nats-Msg-Id": {msgID},
		},
	}
	_, err = js.PublishMsg(ctx, msg)
	return err
}

// PublishDLQNoMsgID publishes a synthetic DLQ entry on `subject` with no
// Nats-Msg-Id header — mirrors the pre-fix production code path in
// engine.RecoveryManager.PublishDeadLetter. Used to demonstrate that
// without dedup, repeated publishes for the same logical event all land.
func (f *DLQFixture) PublishDLQNoMsgID(
	ctx context.Context, subject string, body []byte,
) error {
	if ctx == nil {
		panic("PublishDLQNoMsgID: ctx must not be nil")
	}
	if subject == "" {
		panic("PublishDLQNoMsgID: subject must not be empty")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return err
	}
	_, err = js.Publish(ctx, subject, body)
	return err
}

// PublishAndExhaustToDLQ drives a workflow whose step is failed
// non-retriably so the engine publishes a DLQ entry, and returns
// the DLQ stream sequence of that entry. The given body is used as
// the workflow input bytes; after the fix it must also appear on
// the DLQ entry's Body field (the TaskPayload bytes containing this
// input). Drives via direct step.failed publish on history —
// mirrors orchestrator_test.go's shape for deterministic exhaustion.
//
// Side effect: drains the original task message from TASK_QUEUES
// so a subsequent AwaitReplay sees only the republished delivery.
func (f *DLQFixture) PublishAndExhaustToDLQ(
	t *testing.T, body []byte,
) uint64 {
	if t == nil {
		panic("PublishAndExhaustToDLQ: t must not be nil")
	}
	if len(body) == 0 {
		panic("PublishAndExhaustToDLQ: body must not be empty")
	}
	t.Helper()
	wfName := fmt.Sprintf("dlq-fixture-wf-%d", time.Now().UnixNano())
	task := fmt.Sprintf("dlq-fixture-task-%d", time.Now().UnixNano())
	wfDef := dag.WorkflowDef{
		Name:    wfName,
		Version: "1",
		Steps: []dag.StepDef{{
			ID: "s1", Task: task, Type: dag.StepTypeNormal,
		}},
	}
	ctx := t.Context()
	if err := f.h.Svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("PublishAndExhaustToDLQ: register: %v", err)
	}
	runID, err := f.h.Svc.StartRun(ctx, wfName, body)
	if err != nil {
		t.Fatalf("PublishAndExhaustToDLQ: start: %v", err)
	}
	f.waitForStepQueued(t, runID, "s1", 5*time.Second)
	f.drainTaskQueue(t, 2*time.Second)
	before, err := f.Count(ctx)
	if err != nil {
		t.Fatalf("PublishAndExhaustToDLQ: count: %v", err)
	}
	f.publishNonRetriableStepFailed(t, runID, "s1")
	f.WaitForCount(t, before+1, 5*time.Second)
	return f.latestDLQSequence(t, task, runID)
}

// drainTaskQueue subscribes to the TASK_QUEUES workqueue and acks
// any pending messages so a follow-up AwaitReplay sees only
// freshly-published task deliveries. Bounded by timeout.
func (f *DLQFixture) drainTaskQueue(
	t *testing.T, timeout time.Duration,
) {
	if t == nil {
		panic("drainTaskQueue: t must not be nil")
	}
	if timeout <= 0 {
		panic("drainTaskQueue: timeout must be positive")
	}
	t.Helper()
	js, err := f.h.NC.JetStream()
	if err != nil {
		t.Fatalf("drainTaskQueue: jetstream: %v", err)
	}
	sub, err := js.SubscribeSync("task.>",
		nats.BindStream("TASK_QUEUES"),
		nats.DeliverAll(), nats.AckExplicit())
	if err != nil {
		t.Fatalf("drainTaskQueue: subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck
	deadline := time.Now().Add(timeout)
	const drainMax = 1024
	for i := 0; i < drainMax && time.Now().Before(deadline); i++ {
		msg, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			return
		}
		if ackErr := msg.Ack(); ackErr != nil {
			t.Fatalf("drainTaskQueue: ack: %v", ackErr)
		}
	}
}

// publishNonRetriableStepFailed publishes a synthetic step.failed
// event with FailureType=non_retriable so the engine immediately
// writes a DLQ entry — bypassing retry backoff.
func (f *DLQFixture) publishNonRetriableStepFailed(
	t *testing.T, runID, stepID string,
) {
	if runID == "" {
		panic("publishNonRetriableStepFailed: runID must not be empty")
	}
	if stepID == "" {
		panic("publishNonRetriableStepFailed: stepID must not be empty")
	}
	t.Helper()
	payload, err := json.Marshal(protocol.StepFailedPayload{
		Error:       "deterministic failure for DLQ fixture",
		FailureType: protocol.FailureTypeNonRetriable,
	})
	if err != nil {
		t.Fatalf("publishNonRetriableStepFailed: marshal: %v", err)
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepFailed, runID, stepID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("publishNonRetriableStepFailed: event: %v", err)
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		t.Fatalf("publishNonRetriableStepFailed: js: %v", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	if _, err := js.PublishMsg(t.Context(), msg); err != nil {
		t.Fatalf("publishNonRetriableStepFailed: publish: %v", err)
	}
}

// waitForStepQueued polls the run snapshot until the named step
// reaches StepStatusQueued, bounded by timeout.
func (f *DLQFixture) waitForStepQueued(
	t *testing.T, runID, stepID string, timeout time.Duration,
) {
	if runID == "" {
		panic("waitForStepQueued: runID must not be empty")
	}
	if stepID == "" {
		panic("waitForStepQueued: stepID must not be empty")
	}
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		run, err := f.h.Svc.GetRun(t.Context(), runID)
		if err == nil {
			if state, ok := run.Steps[stepID]; ok &&
				state.Status == dag.StepStatusQueued {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf(
				"waitForStepQueued: step %q never queued after %s",
				stepID, timeout,
			)
		case <-tick.C:
		}
	}
}

// latestDLQSequence reads the DEAD_LETTERS stream and returns the
// sequence of the most recent entry matching dead.<task>.<run>.*.
func (f *DLQFixture) latestDLQSequence(
	t *testing.T, task, runID string,
) uint64 {
	if task == "" {
		panic("latestDLQSequence: task must not be empty")
	}
	if runID == "" {
		panic("latestDLQSequence: runID must not be empty")
	}
	t.Helper()
	views, err := f.h.Svc.ListDeadLetters(t.Context(), 100)
	if err != nil {
		t.Fatalf("latestDLQSequence: list: %v", err)
	}
	prefix := "dead." + task + "."
	var seq uint64
	for _, dl := range views {
		if !strings.HasPrefix(dl.Subject, prefix) {
			continue
		}
		if dl.RunID != runID {
			continue
		}
		if dl.Sequence > seq {
			seq = dl.Sequence
		}
	}
	if seq == 0 {
		t.Fatalf(
			"latestDLQSequence: no DLQ entry for task=%q run=%q",
			task, runID,
		)
	}
	return seq
}

// Replay invokes the API-layer replay for the given DLQ sequence.
// Returns the error verbatim so callers can match against
// ErrDLQBodyMissing or assert nil.
func (f *DLQFixture) Replay(ctx context.Context, seq uint64) error {
	if ctx == nil {
		panic("Replay: ctx must not be nil")
	}
	if seq == 0 {
		panic("Replay: seq must be positive")
	}
	if f.h == nil {
		panic("Replay: fixture not bound")
	}
	return f.h.Svc.ReplayDeadLetter(ctx, seq)
}

// AwaitReplay subscribes to the task subject space, blocks up to d
// for the next delivery, and returns the message body via a 1-buffer
// channel. nil means timeout. Install *before* Replay so the
// subscription catches the republished message.
func (f *DLQFixture) AwaitReplay(
	t *testing.T, d time.Duration,
) <-chan []byte {
	if t == nil {
		panic("AwaitReplay: t must not be nil")
	}
	if d <= 0 {
		panic("AwaitReplay: d must be positive")
	}
	t.Helper()
	js, err := f.h.NC.JetStream()
	if err != nil {
		t.Fatalf("AwaitReplay: jetstream: %v", err)
	}
	// TASK_QUEUES is a WorkQueue stream — sync subscribers must use
	// DeliverAll. The fixture is responsible for having drained the
	// stream first via drainTaskQueue so the first message seen is
	// the replay.
	sub, err := js.SubscribeSync("task.>",
		nats.BindStream("TASK_QUEUES"),
		nats.DeliverAll(), nats.AckExplicit())
	if err != nil {
		t.Fatalf("AwaitReplay: subscribe: %v", err)
	}
	out := make(chan []byte, 1)
	go func() {
		defer sub.Unsubscribe() //nolint:errcheck
		msg, err := sub.NextMsg(d)
		if err != nil {
			out <- nil
			return
		}
		msg.Ack() //nolint:errcheck
		out <- msg.Data
	}()
	return out
}

// PublishLegacyEntry writes a synthetic DLQ entry in the pre-fix
// shape: payload is just {run_id, step_id, task} with no Body field.
// Returns the DLQ stream sequence so the caller can replay it.
func (f *DLQFixture) PublishLegacyEntry(t *testing.T) uint64 {
	if t == nil {
		panic("PublishLegacyEntry: t must not be nil")
	}
	t.Helper()
	task := fmt.Sprintf("legacy-task-%d", time.Now().UnixNano())
	runID := fmt.Sprintf("legacy-run-%d", time.Now().UnixNano())
	stepID := "s1"
	// Legacy payload: pre-fix shape with no body bytes.
	payload, err := json.Marshal(map[string]any{
		"run_id":   runID,
		"step_id":  stepID,
		"task":     task,
		"error":    "legacy entry, no body preserved",
		"attempts": 1,
	})
	if err != nil {
		t.Fatalf("PublishLegacyEntry: marshal: %v", err)
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		t.Fatalf("PublishLegacyEntry: js: %v", err)
	}
	subject := "dead." + task + "." + runID + "." + stepID
	msgID := "dlq-legacy:" + runID + ":" + stepID
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header: nats.Header{
			"Nats-Msg-Id": {msgID},
			"Error":       {"legacy entry, no body preserved"},
		},
	}
	ack, err := js.PublishMsg(t.Context(), msg)
	if err != nil {
		t.Fatalf("PublishLegacyEntry: publish: %v", err)
	}
	return ack.Sequence
}

// IsBodyMissingError reports whether err is the typed body-missing
// error returned by Replay against a legacy DLQ entry.
func (f *DLQFixture) IsBodyMissingError(err error) bool {
	if err == nil {
		return false
	}
	// Compares against the typed sentinel exposed by internal/api.
	return errors.Is(err, api.ErrDLQBodyMissing)
}

// Seed publishes n synthetic DLQ entries with deterministic body bytes
// ("seed-<i>") and a populated metadata header set so #203's CLI tests
// can assert on truncation, --all behavior, and the visibility of the
// delivery_count + consumer fields in --json output.
//
// Bounded: n must be in (0, seedMax]. Helpers stay under 70 lines so the
// publish loop delegates to seedOne.
func (f *DLQFixture) Seed(t *testing.T, n int) {
	if t == nil {
		panic("Seed: t must not be nil")
	}
	if n <= 0 {
		panic("Seed: n must be positive")
	}
	const seedMax = 2000
	if n > seedMax {
		panic("Seed: n exceeds bound")
	}
	t.Helper()
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		t.Fatalf("Seed: jetstream: %v", err)
	}
	for i := 0; i < n; i++ {
		f.seedOne(t, js, i)
	}
	f.WaitForCount(t, n, 10*time.Second)
}

// seedOne publishes one synthetic DLQ entry on dead.seed-task.run-i.s1
// with a modern-shape header set. Body is "seed-<i>" so callers can
// recognize it deterministically.
func (f *DLQFixture) seedOne(
	t *testing.T, js jetstream.JetStream, i int,
) {
	t.Helper()
	runID := fmt.Sprintf("seed-run-%d", i)
	stepID := "s1"
	task := "seed-task"
	subject := "dead." + task + "." + runID + "." + stepID
	body := []byte(fmt.Sprintf("seed-%d", i))
	msg := &nats.Msg{
		Subject: subject,
		Data:    body,
		Header: nats.Header{
			"Nats-Msg-Id":                 {"seed:" + runID},
			engine.HeaderDLQRunID:         {runID},
			engine.HeaderDLQStepID:        {stepID},
			engine.HeaderDLQTask:          {task},
			engine.HeaderDLQError:         {"seed entry"},
			engine.HeaderDLQAttempts:      {"3"},
			engine.HeaderDLQDeliveryCount: {"3"},
			engine.HeaderDLQConsumer:      {engine.DLQConsumerTaskQueues},
			engine.HeaderDLQTaskSubject:   {"task." + task + "." + runID},
		},
	}
	if _, err := js.PublishMsg(t.Context(), msg); err != nil {
		t.Fatalf("Seed: publish %d: %v", i, err)
	}
}

// WaitForCount polls Count() until it returns target or timeout.
// Bounded wait per CLAUDE.md testing rules — fails the test on timeout.
// Returns the final observed count.
func (f *DLQFixture) WaitForCount(
	t *testing.T, target int, timeout time.Duration,
) int {
	if t == nil {
		panic("WaitForCount: t must not be nil")
	}
	if timeout <= 0 {
		panic("WaitForCount: timeout must be positive")
	}
	t.Helper()
	ctx := t.Context()
	deadline := time.After(timeout)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	last := -1
	for {
		got, err := f.Count(ctx)
		if err == nil {
			last = got
			if got == target {
				return got
			}
		}
		select {
		case <-deadline:
			t.Fatalf(
				"WaitForCount: DLQ count %d != target %d "+
					"after %s", last, target, timeout,
			)
			return last
		case <-ticker.C:
		}
	}
}
