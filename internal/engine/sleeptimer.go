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
	"sync"
	"time"

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
	TimerActionTaskConcurRetry TimerAction = "task_concurrency_retry"
	TimerActionDebounce        TimerAction = "debounce_fire"
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
}

// DebounceHandler is called when a debounce timer fires. The seq
// parameter is the JetStream stream sequence of the timer message,
// used for stale timer detection.
type DebounceHandler func(tm TimerMessage, seq uint64)

// SleepTimer manages durable timers via NakWithDelay on the
// SLEEP_TIMERS stream. Subscribes to sleep.> subjects.
type SleepTimer struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	cc         jetstream.ConsumeContext
	onDebounce DebounceHandler
	startOnce  sync.Once
}

// NewSleepTimer creates a SleepTimer bound to the given connection.
// Panics on nil nc or js — these are programmer errors.
func NewSleepTimer(
	nc *nats.Conn, js jetstream.JetStream,
) *SleepTimer {
	if nc == nil {
		panic("NewSleepTimer: nc must not be nil")
	}
	if js == nil {
		panic("NewSleepTimer: js must not be nil")
	}
	return &SleepTimer{nc: nc, js: js}
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

// Stop drains the subscription. Safe to call multiple times.
func (st *SleepTimer) Stop() {
	if st.cc != nil {
		st.cc.Stop()
		st.cc = nil
	}
}

// Schedule publishes a timer message to sleep.{runID}.{stepID}.
// Uses Nats-Msg-Id for dedup so duplicate schedules are harmless.
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
	msgID := fmt.Sprintf(
		"%s.%s.sleep", msg.RunID, msg.StepID,
	)
	if msg.Attempt > 0 {
		msgID = fmt.Sprintf(
			"%s.%s.sleep.%d",
			msg.RunID, msg.StepID, msg.Attempt,
		)
	}
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	_, err = st.js.PublishMsg(ctx, natsMsg)
	return err
}

// ScheduleDebounce publishes a debounce timer. Returns the stream
// sequence number for stale timer detection. Does not use dedup IDs
// — each debounce reset publishes a new timer message.
func (st *SleepTimer) ScheduleDebounce(
	ctx context.Context, msg TimerMessage,
) (uint64, error) {
	if msg.TriggerID == "" {
		panic("ScheduleDebounce: TriggerID must not be empty")
	}
	if msg.Action != TimerActionDebounce {
		panic("ScheduleDebounce: Action must be debounce_fire")
	}
	if err := st.Start(); err != nil {
		return 0, fmt.Errorf("start sleep timer: %w", err)
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal TimerMessage: %w", err)
	}
	subject := fmt.Sprintf(
		"sleep.debounce.%s", msg.DebounceKey,
	)
	ack, err := st.js.Publish(ctx, subject, data)
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
	case TimerActionApprovalTimeout:
		st.fireApprovalTimeout(tm)
	case TimerActionTaskConcurRetry:
		st.fireRateRetry(tm) // Same re-publish logic as rate retry
	case TimerActionDebounce:
		if st.onDebounce != nil {
			st.onDebounce(tm, meta.Sequence.Stream)
		}
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
	st.js.Publish(
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
	st.js.Publish(
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
	st.js.Publish(
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
	subject := fmt.Sprintf("task.%s.%s", tm.TaskType, tm.RunID)
	payload := protocol.TaskPayload{
		TaskID: tm.RunID + "." + tm.StepID,
		RunID:  tm.RunID,
		StepID: tm.StepID,
		Input:  tm.Input,
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
	st.js.PublishMsg(ctx, msg)
}

// fireRetryAfter re-publishes a task after a worker retry delay.
// Distinct from fireRateRetry to avoid MsgId collisions.
func (st *SleepTimer) fireRetryAfter(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireRetryAfter: RunID must not be empty")
	}
	if tm.TaskType == "" {
		panic("fireRetryAfter: TaskType must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	subject := fmt.Sprintf(
		"task.%s.%s", tm.TaskType, tm.RunID,
	)
	payload := protocol.TaskPayload{
		TaskID: tm.RunID + "." + tm.StepID,
		RunID:  tm.RunID,
		StepID: tm.StepID,
		Input:  tm.Input,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msgID := fmt.Sprintf(
		"%s.%s.retry_after", tm.RunID, tm.StepID,
	)
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	st.js.PublishMsg(ctx, msg)
}
