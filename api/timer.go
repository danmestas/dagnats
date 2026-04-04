// api/timer.go
// Timer consumer that fires scheduled workflow runs when their
// timer messages redeliver from the SLEEP_TIMERS stream.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// TimerConsumer subscribes to the SLEEP_TIMERS stream for
// scheduled.> subjects and fires workflows when timers expire.
type TimerConsumer struct {
	svc *Service
	sub *nats.Subscription
}

// NewTimerConsumer creates a timer consumer. Panics on nil svc.
func NewTimerConsumer(svc *Service) *TimerConsumer {
	if svc == nil {
		panic("NewTimerConsumer: svc must not be nil")
	}
	return &TimerConsumer{svc: svc}
}

// Start subscribes to scheduled.> on the SLEEP_TIMERS stream.
func (tc *TimerConsumer) Start() error {
	sub, err := tc.svc.js.Subscribe(
		"scheduled.>",
		tc.handleTimer,
		nats.Durable("scheduled-run-timer"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		return fmt.Errorf("subscribe SLEEP_TIMERS: %w", err)
	}
	tc.sub = sub
	return nil
}

// Stop unsubscribes the timer consumer.
func (tc *TimerConsumer) Stop() {
	if tc.sub != nil {
		tc.sub.Unsubscribe()
	}
}

// handleTimer processes a timer message. On first delivery,
// NAKs with delay to schedule the actual fire time. On
// redelivery (NumDelivered > 1), fires the workflow.
func (tc *TimerConsumer) handleTimer(msg *nats.Msg) {
	if msg == nil {
		panic("handleTimer: msg must not be nil")
	}

	meta, err := msg.Metadata()
	if err != nil {
		msg.Nak()
		return
	}

	var sr ScheduledRun
	if err := json.Unmarshal(msg.Data, &sr); err != nil {
		msg.Ack()
		return
	}

	// First delivery: NAK with delay to schedule the fire.
	if meta.NumDelivered == 1 {
		delay := time.Until(sr.RunAt)
		if delay <= 0 {
			delay = time.Millisecond
		}
		msg.NakWithDelay(delay)
		return
	}

	// Redelivery: fire the scheduled run.
	current, err := tc.svc.GetScheduledRun(sr.RunID)
	if err != nil {
		msg.Ack()
		return
	}
	if current.Status != "scheduled" {
		tc.svc.scheduledKV.Delete(sr.RunID)
		msg.Ack()
		return
	}

	err = tc.fireScheduledRun(current)
	if err != nil {
		tc.svc.tel.Logger.Error(
			"fire scheduled run", err,
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}

	tc.svc.scheduledKV.Delete(sr.RunID)
	msg.Ack()
}

// fireScheduledRun publishes a workflow.started event.
func (tc *TimerConsumer) fireScheduledRun(
	sr ScheduledRun,
) error {
	if sr.RunID == "" {
		panic("fireScheduledRun: RunID must not be empty")
	}
	if sr.WorkflowID == "" {
		panic(
			"fireScheduledRun: WorkflowID must not be empty",
		)
	}

	entry, err := tc.svc.defKV.Get(sr.WorkflowID)
	if err != nil {
		return fmt.Errorf(
			"workflow %q not found: %w", sr.WorkflowID, err,
		)
	}

	payload, err := buildStartPayload(
		entry.Value(), sr.Input,
	)
	if err != nil {
		return err
	}

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, sr.RunID, payload,
	)

	_, span := tc.svc.tel.Tracer.Start(
		context.Background(), "timer.fireScheduledRun",
	)
	defer span.End()
	injectAPITraceCtx(span, &evt)

	data, err := evt.Marshal()
	if err != nil {
		return err
	}

	pubMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	injectAPIMsgTraceCtx(span, pubMsg)
	_, err = tc.svc.js.PublishMsg(pubMsg)

	span.SetAttributes(
		observe.StringAttr("run_id", sr.RunID),
		observe.StringAttr("workflow", sr.WorkflowID),
	)
	return err
}
