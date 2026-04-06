// api/timer.go
// Timer consumer that fires scheduled workflow runs when their
// timer messages redeliver from the SLEEP_TIMERS stream.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// TimerConsumer subscribes to the SLEEP_TIMERS stream for
// scheduled.> subjects and fires workflows when timers expire.
type TimerConsumer struct {
	svc *Service
	cc  jetstream.ConsumeContext
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
	stream, err := tc.svc.js.Stream(
		context.Background(), "SLEEP_TIMERS",
	)
	if err != nil {
		return fmt.Errorf("stream SLEEP_TIMERS: %w", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			Durable:       "scheduled-run-timer",
			FilterSubject: "scheduled.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return fmt.Errorf("consumer scheduled.>: %w", err)
	}
	cc, err := cons.Consume(tc.handleTimerJS)
	if err != nil {
		return fmt.Errorf("consume scheduled.>: %w", err)
	}
	tc.cc = cc
	return nil
}

// Stop unsubscribes the timer consumer.
func (tc *TimerConsumer) Stop() {
	if tc.cc != nil {
		tc.cc.Stop()
	}
}

// handleTimerJS processes a timer message. On first delivery,
// NAKs with delay to schedule the actual fire time. On
// redelivery (NumDelivered > 1), fires the workflow.
func (tc *TimerConsumer) handleTimerJS(msg jetstream.Msg) {
	if msg == nil {
		panic("handleTimerJS: msg must not be nil")
	}

	meta, err := msg.Metadata()
	if err != nil {
		msg.Nak()
		return
	}

	var sr ScheduledRun
	if err := json.Unmarshal(msg.Data(), &sr); err != nil {
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
		tc.svc.scheduledKV.Delete(
			context.Background(), sr.RunID,
		)
		msg.Ack()
		return
	}

	err = tc.fireScheduledRun(current)
	if err != nil {
		slog.Error("fire scheduled run", "error", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}

	tc.svc.scheduledKV.Delete(
		context.Background(), sr.RunID,
	)
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

	entry, err := tc.svc.defKV.Get(
		context.Background(), sr.WorkflowID,
	)
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

	_, span := tc.svc.tracer.Start(
		context.Background(),
		"dagnats.api fireScheduledRun",
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
	_, err = tc.svc.js.PublishMsg(
		context.Background(), pubMsg,
	)

	span.SetAttributes(
		attribute.String("run_id", sr.RunID),
		attribute.String("workflow", sr.WorkflowID),
	)
	return err
}
