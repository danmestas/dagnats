// engine/sleeptimer.go
// Durable sleep timer using NakWithDelay on the SLEEP_TIMERS stream.
// On first delivery, NAKs with the specified duration to schedule wake.
// On redelivery, dispatches based on the action field in the payload.
// This pattern avoids in-memory timers — NATS handles persistence and
// redelivery, so the timer survives process restarts.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TimerAction identifies the action to perform when a timer fires.
// Extensible for future uses (wait-for-event timeouts, rate-limit).
type TimerAction string

const (
	TimerActionSleepComplete   TimerAction = "sleep_complete"
	TimerActionRateRetry       TimerAction = "rate_retry"
	TimerActionWaitTimeout     TimerAction = "wait_timeout"
	TimerActionRetryAfter      TimerAction = "retry_after"
	TimerActionRetryBackoff    TimerAction = "retry_backoff"
	TimerActionTaskConcurRetry TimerAction = "task_concurrency_retry"
	TimerActionDebounce        TimerAction = "debounce_fire"
	TimerActionStepTimeout     TimerAction = "step_timeout"
	// TimerActionLoopDelay re-publishes the next agent-loop iteration
	// after the step's loop_delay (#801), so the delay survives an
	// engine restart like retry backoff does.
	TimerActionLoopDelay TimerAction = "loop_delay"
	// TimerActionLoopWait fires the step.wait.elapsed event for an
	// agent-loop iteration whose continue asked for a delay or a
	// deadline (ADR-027). The engine, not the timer, sends the task:
	// the step may have been woken by an input first, and only the
	// engine's saved wait flags under the run lock can tell.
	TimerActionLoopWait TimerAction = "loop_wait"
)

// TimerMessage is the payload published to the SLEEP_TIMERS stream.
// DurationMs is the sleep duration in milliseconds.
// TaskType and Input are used by rate_retry to re-publish the task.
// TriggerID and DebounceKey are used by debounce_fire.
type TimerMessage struct {
	Action      TimerAction     `json:"action"`
	RunID       string          `json:"run_id"`
	StepID      string          `json:"step_id"`
	DurationMs  int64           `json:"duration_ms"`
	TaskType    string          `json:"task_type,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
	Attempt     int             `json:"attempt,omitempty"`
	TriggerID   string          `json:"trigger_id,omitempty"`
	DebounceKey string          `json:"debounce_key,omitempty"`
	// DispatchNonce + RequiredCapabilities carry the per-dispatch grant
	// decision (#380) across the durable timer boundary: the task-dispatch
	// caller (Publish / sticky) strips the caps via effectiveCapabilities and
	// mints the run-binding nonce at SCHEDULING time, so the timer fire can
	// re-publish a TaskPayload that still honors the grant policy WITHOUT the
	// SleepTimer needing the policy itself. A timer that re-publishes a task
	// dispatch (rate_retry / retry_after / retry_backoff / sticky-fallback)
	// must set these; non-dispatch timers leave them empty. Additive,
	// omitempty: legacy timer messages deserialize to "".
	DispatchNonce        string   `json:"dispatch_nonce,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	// WorkflowName mirrors protocol.TaskPayload.WorkflowName (#503) across
	// the durable timer boundary, so a timer-fired re-publish (rate_retry /
	// retry_after / retry_backoff / sticky-fallback) still stamps the
	// TaskPayload the worker eventually sees. Additive, omitempty: legacy
	// timer messages deserialize to "" and the re-published TaskPayload
	// simply carries no workflow_name.
	WorkflowName string `json:"workflow_name,omitempty"`
	// Subject is the resolved dispatch subject for a timer that
	// re-publishes a task (rate_retry / task_concurrency_retry /
	// retry_after / retry_backoff / sticky-fallback), computed by
	// TaskPublisher.StepSubject when the timer is SCHEDULED (#721).
	//
	// The resolved subject is carried rather than the ingredients to
	// rebuild it, so a re-dispatch cannot diverge from the first
	// dispatch. Before this, the timer rebuilt the subject as
	// "task.<Task>.<runID>" and dropped the worker group, which put every
	// retry of a grouped step on a subject no grouped consumer filters:
	// the step never ran again and its run never ended. Additive,
	// omitempty: see dispatchSubject for the legacy fallback.
	Subject string `json:"subject,omitempty"`
	// Metadata is the step's StepDef.Metadata, copied when a re-dispatch
	// timer is SCHEDULED so the retried task carries it like the first
	// dispatch did (#732). omitempty: nil for steps without metadata and
	// for timers that never re-dispatch.
	Metadata map[string]string `json:"metadata,omitempty"`
	// Iteration is the agent-loop iteration a loop_delay timer
	// re-publishes, and the iteration a step_timeout watchdog was armed
	// for (#801). It scopes the timer's msg-id (every iteration of a
	// loop shares one attempt) and the staleness check at fire time.
	Iteration int `json:"iteration,omitempty"`
}

// dispatchSubject returns the subject a dispatch timer re-publishes
// to. It is the single place that resolution happens, shared by every
// dispatch action, so the paths cannot drift apart again (#721 found
// two independent copies, both dropping the worker group).
//
// A timer scheduled before #721 carries no Subject and falls back to
// the pre-#721 ungrouped derivation. For an ungrouped step that is the
// correct subject. For a grouped step it is still the wrong one, but
// the step's timeout watchdog then schedules a fresh retry that DOES
// carry the resolved subject, so an affected run recovers on its next
// timeout cycle rather than staying stuck.
func (tm TimerMessage) dispatchSubject() string {
	if tm.RunID == "" {
		panic("TimerMessage.dispatchSubject: RunID must not be empty")
	}
	if tm.Subject != "" {
		return tm.Subject
	}
	if tm.TaskType == "" {
		panic("TimerMessage.dispatchSubject: legacy timer has no TaskType")
	}
	return fmt.Sprintf("task.%s.%s", tm.TaskType, tm.RunID)
}

// DebounceHandler is called when a debounce timer fires. The seq
// parameter is the JetStream stream sequence of the timer message,
// used for stale timer detection.
type DebounceHandler func(tm TimerMessage, seq uint64)

// StepTimeoutHandler is called when a step_timeout timer fires.
// The handler owns the staleness check (compare current run state
// vs the (runID, stepID, attempt) baked into the timer) and the
// synthetic step.failed publish. Hosting this on the orchestrator
// avoids piping a SnapshotStore into SleepTimer.
type StepTimeoutHandler func(tm TimerMessage)

// SleepTimer manages durable timers via NakWithDelay on the
// SLEEP_TIMERS stream. Subscribes to sleep.> subjects. tp wraps
// the publish path so every fire-* and re-publish carries W3C
// trace context (#334).
type SleepTimer struct {
	nc            *nats.Conn
	js            jetstream.JetStream
	tp            *natsutil.TracingPublisher
	cc            jetstream.ConsumeContext
	onDebounce    DebounceHandler
	onStepTimeout StepTimeoutHandler
	startOnce     sync.Once
	// store reads the CURRENT run snapshot at fire time so every
	// re-dispatch derives TaskPayload.Attempt AND Iteration from
	// Steps[stepID] (dispatchIdentity, #624 review rounds 2 and 4)
	// instead of trusting a value threaded through however many
	// TimerMessage hops it took to reach here. Requires
	// natsutil.SetupAll to have provisioned the workflow_runs KV
	// bucket first — every existing NewSleepTimer call site already
	// requires that (see the orchestrator.go comment above its own
	// construction), so this adds no new precondition in practice.
	store *SnapshotStore
	// loopStateMaxBytes is stamped on every re-published TaskPayload
	// (#801), mirroring TaskPublisher.
	loopStateMaxBytes int
}

// NewSleepTimer creates a SleepTimer bound to the given connection.
// Panics on nil nc, js, or tp — these are programmer errors.
func NewSleepTimer(
	nc *nats.Conn,
	js jetstream.JetStream,
	tp *natsutil.TracingPublisher,
) *SleepTimer {
	if nc == nil {
		panic("NewSleepTimer: nc must not be nil")
	}
	if js == nil {
		panic("NewSleepTimer: js must not be nil")
	}
	if tp == nil {
		panic("NewSleepTimer: tp must not be nil")
	}
	return &SleepTimer{
		nc: nc, js: js, tp: tp, store: NewSnapshotStore(js),
		loopStateMaxBytes: protocol.LoopStateMaxBytesDefault,
	}
}

// identityForRedispatch loads the CURRENT run snapshot and returns
// dispatchIdentity(run, stepID, dispatchNewAttempt) — the authoritative
// (attempt, iteration) pair every fire-* re-dispatch path in this file
// must use (#624 review round 4: renamed from attemptForRedispatch,
// which returned only attempt and left every caller to set Iteration
// by hand — the exact bug this round fixes) — with the step's committed
// loop state and dispatch nonce (#801), so a retry resumes from the
// last iteration the engine accepted and answers under the nonce the
// snapshot will check. Falls back to the timer's own nonce (minted if
// empty) and (0, 0, nil) numbering on any lookup failure: a
// missing/unreadable snapshot must never block a retry from firing.
func (st *SleepTimer) identityForRedispatch(
	ctx context.Context, tm TimerMessage,
) redispatch {
	if tm.RunID == "" {
		panic("identityForRedispatch: runID must not be empty")
	}
	if tm.StepID == "" {
		panic("identityForRedispatch: stepID must not be empty")
	}
	fallback := redispatch{nonce: nonceOrMint(tm.DispatchNonce)}
	if st.store == nil {
		return fallback
	}
	run, err := st.store.Load(ctx, tm.RunID)
	if err != nil {
		return fallback
	}
	state := run.Steps[tm.StepID]
	attempt, iteration := dispatchIdentity(run, tm.StepID, dispatchNewAttempt)
	nonce := state.DispatchNonce
	if nonce == "" {
		nonce = fallback.nonce
	}
	return redispatch{
		attempt: attempt, iteration: iteration, loopState: state.LoopState, nonce: nonce,
		stale: redispatchStale(tm, state),
	}
}

// redispatch is what a timer-fired re-publish stamps on its payload.
// stale names why the timer must not fire, or is empty.
type redispatch struct {
	attempt   int
	iteration int
	loopState []byte
	nonce     string
	stale     string
}

// redispatchStale reports why a dispatch timer no longer belongs to
// the step's current state, or "" when it may fire (#801). A retry
// timer belongs to a step still waiting on that retry: not terminal,
// on the attempt the timer was scheduled for, with the retry's
// dispatch time still pending (startAttemptClock clears it once the
// retry starts). A rate, concurrency or sticky-fallback timer belongs
// to a first send still waiting: the step not yet started (Queued, or
// Pending for a map instance the snapshot has no entry for), on the
// dispatch the timer was scheduled for. Firing a stale timer would
// publish a second task whose events the live nonce from the snapshot
// would let through the event fence.
func redispatchStale(tm TimerMessage, state dag.StepState) string {
	if tm.RunID == "" {
		panic("redispatchStale: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("redispatchStale: StepID must not be empty")
	}
	inFlight := state.Status == dag.StepStatusQueued ||
		state.Status == dag.StepStatusRunning
	switch tm.Action {
	case TimerActionRetryAfter, TimerActionRetryBackoff:
		if !inFlight {
			return "step not in flight"
		}
		if state.RetryDispatchAt == nil {
			return "no retry pending"
		}
		if tm.Attempt > 0 && tm.Attempt != state.Attempts {
			return "attempt moved on"
		}
	case TimerActionRateRetry, TimerActionTaskConcurRetry:
		if state.Status != dag.StepStatusPending &&
			state.Status != dag.StepStatusQueued {
			return "step already started"
		}
		if tm.DispatchNonce != "" && tm.DispatchNonce != state.DispatchNonce {
			return "dispatch moved on"
		}
	}
	return ""
}

// dropStaleTimer logs a timer that must not fire.
func dropStaleTimer(ctx context.Context, tm TimerMessage, reason string) {
	if reason == "" {
		panic("dropStaleTimer: reason must not be empty")
	}
	if tm.RunID == "" {
		panic("dropStaleTimer: RunID must not be empty")
	}
	slog.WarnContext(ctx, "dropping stale dispatch timer",
		"action", string(tm.Action), "reason", reason,
		"run_id", tm.RunID, "step_id", tm.StepID, "timer_attempt", tm.Attempt)
}

// Start subscribes to sleep.> on the SLEEP_TIMERS stream.
// Idempotent — safe to call multiple times. The consumer is
// also started lazily on the first call to Schedule.
func (st *SleepTimer) Start() error {
	var err error
	st.startOnce.Do(func() {
		err = st.startConsumer()
	})
	return err
}

// startConsumer creates the durable consumer on SLEEP_TIMERS.
func (st *SleepTimer) startConsumer() error {
	if st.js == nil {
		panic("SleepTimer.startConsumer: js must not be nil")
	}
	stream, err := st.js.Stream(
		context.Background(), "SLEEP_TIMERS",
	)
	if err != nil {
		return fmt.Errorf("stream SLEEP_TIMERS: %w", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			Durable:       "sleep-timer",
			FilterSubject: "sleep.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return fmt.Errorf("consumer sleep.>: %w", err)
	}
	cc, err := cons.Consume(st.handleTimerJS)
	if err != nil {
		return fmt.Errorf("consume sleep.>: %w", err)
	}
	st.cc = cc
	return nil
}

// OnDebounce sets the handler called when a debounce timer fires.
// Must be called before Start.
func (st *SleepTimer) OnDebounce(fn DebounceHandler) {
	if fn == nil {
		panic("OnDebounce: fn must not be nil")
	}
	st.onDebounce = fn
}

// OnStepTimeout sets the handler called when a step_timeout timer
// fires. Must be called before Start. The handler owns the
// staleness check and synthetic step.failed publish.
func (st *SleepTimer) OnStepTimeout(fn StepTimeoutHandler) {
	if fn == nil {
		panic("OnStepTimeout: fn must not be nil")
	}
	st.onStepTimeout = fn
}

// Stop drains the subscription. Safe to call multiple times.
func (st *SleepTimer) Stop() {
	if st.cc != nil {
		st.cc.Stop()
		// Wait for the pull-loop goroutine to fully quiesce (see
		// waitConsumeClosed's doc comment in orchestrator.go) — cc.Stop()
		// only signals it to exit.
		waitConsumeClosed(st.cc, "sleeptimer")
		st.cc = nil
	}
}

// Schedule publishes a timer message to sleep.{runID}.{stepID}.
// Uses Nats-Msg-Id for dedup so duplicate schedules are harmless.
// MsgID embeds the Action so different timer kinds for the same
// (run, step, attempt) — e.g. step_timeout AND retry_backoff for
// the same failed attempt — never collide on dedup.
func (st *SleepTimer) Schedule(ctx context.Context, msg TimerMessage) error {
	if msg.RunID == "" {
		panic("SleepTimer.Schedule: RunID must not be empty")
	}
	if msg.StepID == "" {
		panic("SleepTimer.Schedule: StepID must not be empty")
	}
	if err := st.Start(); err != nil {
		return fmt.Errorf("start sleep timer: %w", err)
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal TimerMessage: %w", err)
	}
	subject := fmt.Sprintf(
		"sleep.%s.%s", msg.RunID, msg.StepID,
	)
	action := string(msg.Action)
	if action == "" {
		action = "sleep"
	}
	msgID := fmt.Sprintf(
		"%s.%s.%s", msg.RunID, msg.StepID, action,
	)
	if msg.Attempt > 0 {
		msgID = fmt.Sprintf(
			"%s.%s.%s.%d",
			msg.RunID, msg.StepID, action, msg.Attempt,
		)
	}
	// Every iteration of a loop shares one attempt, so a timer armed
	// per iteration (loop_delay, step_timeout) is keyed by it too.
	if msg.Iteration > 0 {
		msgID = fmt.Sprintf("%s.%d", msgID, msg.Iteration)
	}
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	_, err = st.tp.JSPublishMsg(ctx, natsMsg)
	return err
}

// ScheduleDebounce publishes a debounce timer to SLEEP_TIMERS. Returns
// the stream sequence number for stale timer detection. Does not use
// dedup IDs — each debounce reset publishes a new timer message.
//
// A package function, not a SleepTimer method: the trigger service
// schedules debounce timers but must not run a sleep-timer consumer of
// its own (the durable is shared, so a second consumer would take step
// timers it cannot handle). The orchestrator's SleepTimer, given a
// handler via WithDebounceHandler, fires them.
func ScheduleDebounce(
	ctx context.Context, tp *natsutil.TracingPublisher, msg TimerMessage,
) (uint64, error) {
	if tp == nil {
		panic("ScheduleDebounce: tp must not be nil")
	}
	if msg.TriggerID == "" {
		panic("ScheduleDebounce: TriggerID must not be empty")
	}
	if msg.Action != TimerActionDebounce {
		panic("ScheduleDebounce: Action must be debounce_fire")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal TimerMessage: %w", err)
	}
	subject := fmt.Sprintf(
		"sleep.debounce.%s", msg.DebounceKey,
	)
	ack, err := tp.JSPublish(ctx, subject, data)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// handleTimerJS processes a timer message from the SLEEP_TIMERS
// stream. First delivery NAKs with the configured delay. Redelivery
// dispatches the action and ACKs.
func (st *SleepTimer) handleTimerJS(msg jetstream.Msg) {
	if msg == nil {
		panic("handleTimerJS: msg must not be nil")
	}
	if len(msg.Data()) == 0 {
		panic("handleTimerJS: msg.Data must not be empty")
	}

	meta, err := msg.Metadata()
	if err != nil {
		msg.Nak()
		return
	}

	var tm TimerMessage
	if err := json.Unmarshal(msg.Data(), &tm); err != nil {
		// Corrupt message — ack to avoid infinite redelivery.
		msg.Ack()
		return
	}

	if meta.NumDelivered == 1 {
		delay := time.Duration(tm.DurationMs) * time.Millisecond
		if delay <= 0 {
			delay = time.Millisecond
		}
		msg.NakWithDelay(delay)
		return
	}

	// Redelivery — dispatch based on action.
	switch tm.Action {
	case TimerActionSleepComplete:
		st.fireSleepComplete(tm)
	case TimerActionRateRetry:
		st.fireRateRetry(tm)
	case TimerActionWaitTimeout:
		st.fireWaitTimeout(tm)
	case TimerActionRetryAfter:
		st.fireRetryAfter(tm)
	case TimerActionRetryBackoff:
		st.fireRetryBackoff(tm)
	case TimerActionApprovalTimeout:
		st.fireApprovalTimeout(tm)
	case TimerActionTaskConcurRetry:
		st.fireRateRetry(tm) // Same re-publish logic as rate retry
	case TimerActionDebounce:
		if st.onDebounce != nil {
			st.onDebounce(tm, meta.Sequence.Stream)
		}
	case TimerActionStepTimeout:
		if st.onStepTimeout != nil {
			st.onStepTimeout(tm)
		}
	case TimerActionLoopDelay:
		st.fireLoopDelay(tm)
	case TimerActionLoopWait:
		st.fireLoopWait(tm)
	default:
		// Unknown action — ack to prevent loop.
	}
	msg.Ack()
}

// fireSleepComplete publishes an EventStepSleepCompleted event to
// the history stream for the given run. This wakes the orchestrator
// to advance downstream steps.
func (st *SleepTimer) fireSleepComplete(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireSleepComplete: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("fireSleepComplete: StepID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	evt := protocol.NewStepEvent(
		protocol.EventStepSleepCompleted,
		tm.RunID, tm.StepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	st.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// fireWaitTimeout publishes an EventStepWaitTimeout event to the
// history stream for the given run. This wakes the orchestrator to
// mark the wait step as completed with a timeout indicator.
func (st *SleepTimer) fireWaitTimeout(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireWaitTimeout: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("fireWaitTimeout: StepID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitTimeout,
		tm.RunID, tm.StepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	st.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// fireApprovalTimeout publishes an EventApprovalExpired event to
// the history stream. The orchestrator will fail the approval step
// if it has not already been resolved.
func (st *SleepTimer) fireApprovalTimeout(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireApprovalTimeout: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic(
			"fireApprovalTimeout: StepID must not be empty",
		)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	evt := protocol.NewStepEvent(
		protocol.EventApprovalExpired,
		tm.RunID, tm.StepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	st.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// fireRateRetry re-publishes a rate-limited task to the task queue.
// The orchestrator scheduled this timer when the token bucket was
// exhausted, so the task gets another chance at dispatch.
func (st *SleepTimer) fireRateRetry(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireRateRetry: RunID must not be empty")
	}
	if tm.TaskType == "" {
		panic("fireRateRetry: TaskType must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	subject := tm.dispatchSubject()
	// #624 review round 2/4: fireRateRetry used to omit Attempt
	// entirely (zero value), so every rate-retried task ran as if
	// attempt 1 (worker's NumDelivered fallback) regardless of the
	// step's real attempt count; round 4 fixed the same gap for
	// Iteration, which was still omitted even after round 2 — both
	// now come from the live snapshot via dispatchIdentity.
	rd := st.identityForRedispatch(ctx, tm)
	if rd.stale != "" {
		dropStaleTimer(ctx, tm, rd.stale)
		return
	}
	payload := protocol.TaskPayload{
		TaskID:       tm.RunID + "." + tm.StepID,
		RunID:        tm.RunID,
		StepID:       tm.StepID,
		Attempt:      rd.attempt,
		Iteration:    rd.iteration,
		Input:        tm.Input,
		WorkflowName: tm.WorkflowName,
		// Carry the grant decision (#380) so a rate-retried granted step
		// keeps its control-plane capability; the nonce is the snapshot's
		// so the dispatch passes VerifyDispatch and the event fence.
		RequiredCapabilities: tm.RequiredCapabilities,
		DispatchNonce:        rd.nonce,
		Metadata:             tm.Metadata,
		State:                rd.loopState,
		StateMaxBytes:        st.loopStateMaxBytes,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msgID := fmt.Sprintf(
		"%s.%s.rate_retry", tm.RunID, tm.StepID,
	)
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	st.tp.JSPublishMsg(ctx, msg)
}

// fireRetryAfter re-publishes a task after a worker-requested retry
// delay. Distinct MsgId suffix from rate_retry / retry_backoff so
// dedup is per-cause and concurrent re-publishes don't collide.
func (st *SleepTimer) fireRetryAfter(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireRetryAfter: RunID must not be empty")
	}
	if tm.TaskType == "" {
		panic("fireRetryAfter: TaskType must not be empty")
	}
	st.republishTask(tm, "retry_after")
}

// fireRetryBackoff re-publishes a task after a policy-driven backoff
// delay. Mirrors fireRetryAfter; the only difference is who chose the
// delay (worker for retry_after, RetryPolicy for retry_backoff). The
// MsgId suffix differs so the two fire paths never dedup against each
// other on the same step.
func (st *SleepTimer) fireRetryBackoff(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireRetryBackoff: RunID must not be empty")
	}
	if tm.TaskType == "" {
		panic("fireRetryBackoff: TaskType must not be empty")
	}
	st.republishTask(tm, "retry_backoff")
}

// republishTask is the shared task re-publish path used by retry_after
// and retry_backoff timer fires. The kind suffix scopes the dedup
// MsgId to the cause, so the two paths can fire independently for the
// same step without colliding. Including the resolved attempt in the
// MsgId keeps each retry distinct within JetStream's dedup window —
// without it, a multi-retry backoff loop would dedup attempts 2..N to
// a no-op.
//
// payload.Attempt/Iteration (and the MsgId's own attempt component)
// both come from identityForRedispatch — the live run snapshot's
// Steps[stepID].Attempts/Iterations, NOT tm.Attempt (#624 review round
// 3: a round-2 version of this comment described the OLD tm.Attempt+1
// scheme; the MsgId below used to still key on tm.Attempt even after
// the payload switched to the snapshot-derived value, which could
// silently diverge from what actually got dispatched). Round 4: the
// payload also used to leave Iteration at the Go zero value even
// though a retried loop step's Steps[stepID].Iterations is never
// reset — dispatchIdentity now derives both together so this cannot
// drift apart again. Deriving fresh here means a re-dispatch can never
// regress to a stale or dropped value the way fireRateRetry's omission
// once did, and the MsgId always reflects the SAME attempt number the
// payload carries.
func (st *SleepTimer) republishTask(
	tm TimerMessage, kind string,
) {
	if kind == "" {
		panic("republishTask: kind must not be empty")
	}
	if tm.RunID == "" {
		panic("republishTask: RunID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	subject := tm.dispatchSubject()
	rd := st.identityForRedispatch(ctx, tm)
	if rd.stale != "" {
		dropStaleTimer(ctx, tm, rd.stale)
		return
	}
	payload := protocol.TaskPayload{
		TaskID:       tm.RunID + "." + tm.StepID,
		RunID:        tm.RunID,
		StepID:       tm.StepID,
		Input:        tm.Input,
		Attempt:      rd.attempt,
		Iteration:    rd.iteration,
		WorkflowName: tm.WorkflowName,
		// Carry the grant decision (#380); the nonce is the snapshot's
		// (a timer scheduled before #801 carries none) so the retried
		// task passes VerifyDispatch and the event fence.
		RequiredCapabilities: tm.RequiredCapabilities,
		DispatchNonce:        rd.nonce,
		Metadata:             tm.Metadata,
		State:                rd.loopState,
		StateMaxBytes:        st.loopStateMaxBytes,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msgID := fmt.Sprintf(
		"%s.%s.%s.%d",
		tm.RunID, tm.StepID, kind, rd.attempt,
	)
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	st.tp.JSPublishMsg(ctx, msg)
}

// fireLoopDelay publishes the agent-loop iteration a loop_delay timer
// was scheduled for (#801). The iteration's identity (attempt, nonce)
// and committed state come from the live snapshot, as a retry's do;
// the timer is stale, and dropped, once the step left that dispatch:
// it completed, failed or was cancelled, or a resend of the same
// iteration already ran and the loop moved on. A step still Queued
// (a legacy worker that continued without reporting step.started)
// is on the same dispatch and gets its task.
func (st *SleepTimer) fireLoopDelay(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireLoopDelay: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("fireLoopDelay: StepID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	run, err := st.store.Load(ctx, tm.RunID)
	if err != nil {
		return
	}
	state := run.Steps[tm.StepID]
	inFlight := state.Status == dag.StepStatusRunning ||
		state.Status == dag.StepStatusQueued
	if !inFlight || state.Iterations != tm.Iteration ||
		state.DispatchNonce != tm.DispatchNonce {
		return
	}
	payload := protocol.TaskPayload{
		TaskID:               tm.RunID + "." + tm.StepID,
		RunID:                tm.RunID,
		StepID:               tm.StepID,
		Input:                tm.Input,
		Attempt:              state.Attempts,
		Iteration:            state.Iterations,
		WorkflowName:         tm.WorkflowName,
		RequiredCapabilities: tm.RequiredCapabilities,
		DispatchNonce:        tm.DispatchNonce,
		Metadata:             tm.Metadata,
		State:                state.LoopState,
		StateMaxBytes:        st.loopStateMaxBytes,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	// The same msg-id PublishIteration uses: a resend of this iteration
	// that raced the timer is one task inside the dedup window.
	msgID := fmt.Sprintf(
		"%s.%s.continue.%d", tm.RunID, tm.StepID, state.Iterations,
	)
	msg := &nats.Msg{
		Subject: tm.dispatchSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	st.tp.JSPublishMsg(ctx, msg)
}

// fireLoopWait publishes step.wait.elapsed for the iteration a
// loop_wait timer was armed for (ADR-027), naming that dispatch so the
// engine can tell a late fire from a live one. The msg-id is stable
// per attempt and iteration, so a redelivered timer is one event.
func (st *SleepTimer) fireLoopWait(tm TimerMessage) {
	if tm.RunID == "" || tm.StepID == "" {
		panic("fireLoopWait: run and step must not be empty")
	}
	if tm.Action != TimerActionLoopWait {
		panic("fireLoopWait: timer must be a loop_wait")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	evt := protocol.NewStepEvent(protocol.EventStepWaitElapsed, tm.RunID, tm.StepID, nil)
	// The engine compares iteration and nonce, never the attempt, so a
	// timer armed while Attempts still lagged (a continue accepted on a
	// Queued step) wakes the step like any other.
	evt.SetIdentity(tm.Iteration, max(tm.Attempt, 1), tm.DispatchNonce)
	if err := publishLifecycleEvent(ctx, st.tp, evt); err != nil {
		slog.WarnContext(ctx, "loop wait: publish step.wait.elapsed",
			"run_id", tm.RunID, "step_id", tm.StepID, "error", err)
	}
}
