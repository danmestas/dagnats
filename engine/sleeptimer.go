// engine/sleeptimer.go
// Durable sleep timer using NakWithDelay on the SLEEP_TIMERS stream.
// On first delivery, NAKs with the specified duration to schedule wake.
// On redelivery, dispatches based on the action field in the payload.
// This pattern avoids in-memory timers — NATS handles persistence and
// redelivery, so the timer survives process restarts.
package engine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// TimerAction identifies the action to perform when a timer fires.
// Extensible for future uses (wait-for-event timeouts, rate-limit).
type TimerAction string

const (
	TimerActionSleepComplete TimerAction = "sleep_complete"
	TimerActionRateRetry     TimerAction = "rate_retry"
	TimerActionWaitTimeout   TimerAction = "wait_timeout"
)

// TimerMessage is the payload published to the SLEEP_TIMERS stream.
// DurationMs is the sleep duration in milliseconds.
// TaskType and Input are used by rate_retry to re-publish the task.
type TimerMessage struct {
	Action     TimerAction     `json:"action"`
	RunID      string          `json:"run_id"`
	StepID     string          `json:"step_id"`
	DurationMs int64           `json:"duration_ms"`
	TaskType   string          `json:"task_type,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
}

// SleepTimer manages durable timers via NakWithDelay on the
// SLEEP_TIMERS stream. Subscribes to sleep.> subjects.
type SleepTimer struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	sub *nats.Subscription
}

// NewSleepTimer creates a SleepTimer bound to the given connection.
// Panics on nil nc or js — these are programmer errors.
func NewSleepTimer(
	nc *nats.Conn, js nats.JetStreamContext,
) *SleepTimer {
	if nc == nil {
		panic("NewSleepTimer: nc must not be nil")
	}
	if js == nil {
		panic("NewSleepTimer: js must not be nil")
	}
	return &SleepTimer{nc: nc, js: js}
}

// Start subscribes to sleep.> on the SLEEP_TIMERS stream with a
// durable consumer. Panics if already started.
func (st *SleepTimer) Start() error {
	if st.js == nil {
		panic("SleepTimer.Start: js must not be nil")
	}
	if st.sub != nil {
		panic("SleepTimer.Start: already started")
	}
	sub, err := st.js.Subscribe(
		"sleep.>",
		st.handleTimer,
		nats.Durable("sleep-timer"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		return fmt.Errorf("subscribe sleep.>: %w", err)
	}
	st.sub = sub
	return nil
}

// Stop drains the subscription. Safe to call multiple times.
func (st *SleepTimer) Stop() {
	if st.sub != nil {
		st.sub.Unsubscribe()
		st.sub = nil
	}
}

// Schedule publishes a timer message to sleep.{runID}.{stepID}.
// Uses Nats-Msg-Id for dedup so duplicate schedules are harmless.
func (st *SleepTimer) Schedule(msg TimerMessage) error {
	if msg.RunID == "" {
		panic("SleepTimer.Schedule: RunID must not be empty")
	}
	if msg.StepID == "" {
		panic("SleepTimer.Schedule: StepID must not be empty")
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
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	_, err = st.js.PublishMsg(natsMsg)
	return err
}

// handleTimer processes a timer message from the SLEEP_TIMERS
// stream. First delivery NAKs with the configured delay. Redelivery
// dispatches the action and ACKs.
func (st *SleepTimer) handleTimer(msg *nats.Msg) {
	if msg == nil {
		panic("handleTimer: msg must not be nil")
	}
	if len(msg.Data) == 0 {
		panic("handleTimer: msg.Data must not be empty")
	}

	meta, err := msg.Metadata()
	if err != nil {
		msg.Nak()
		return
	}

	var tm TimerMessage
	if err := json.Unmarshal(msg.Data, &tm); err != nil {
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
	evt := protocol.NewStepEvent(
		protocol.EventStepSleepCompleted,
		tm.RunID, tm.StepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	st.js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
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
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitTimeout,
		tm.RunID, tm.StepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	st.js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
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
	st.js.PublishMsg(msg)
}
