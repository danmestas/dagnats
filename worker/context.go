package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

type taskContext struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	tel       *observe.Telemetry
	runID     string
	stepID    string
	iteration int
	attempt   int
	input     []byte
	ctx       context.Context
	span      observe.Span
}

// newTaskContext constructs a taskContext from a dispatched
// TaskPayload. The ctx and span carry trace context from the
// parent executeTask span so child spans link correctly.
func newTaskContext(
	nc *nats.Conn,
	tel *observe.Telemetry,
	js nats.JetStreamContext,
	payload protocol.TaskPayload,
	ctx context.Context,
	span observe.Span,
) *taskContext {
	if tel == nil {
		panic("newTaskContext: tel must not be nil")
	}
	if ctx == nil {
		panic("newTaskContext: ctx must not be nil")
	}
	return &taskContext{
		nc:        nc,
		js:        js,
		tel:       tel,
		runID:     payload.RunID,
		stepID:    payload.StepID,
		iteration: payload.Iteration,
		attempt:   payload.Attempt,
		input:     payload.Input,
		ctx:       ctx,
		span:      span,
	}
}

func (c *taskContext) Input() []byte    { return c.input }
func (c *taskContext) RunID() string    { return c.runID }
func (c *taskContext) StepID() string   { return c.stepID }
func (c *taskContext) RetryCount() int  { return c.attempt }

// Complete publishes a step.completed event with trace context.
func (c *taskContext) Complete(output []byte) error {
	_, span := c.tel.Tracer.Start(c.ctx,
		"worker.complete",
		observe.WithAttributes(
			observe.StringAttr("run_id", c.runID),
			observe.StringAttr("step_id", c.stepID),
			observe.Int64Attr(
				"output_size_bytes", int64(len(output)),
			),
		),
	)
	defer span.End()
	return c.publishEvent(
		protocol.EventStepCompleted, output,
	)
}

// Fail publishes a step.failed event with error status.
func (c *taskContext) Fail(err error) error {
	_, span := c.tel.Tracer.Start(c.ctx,
		"worker.fail",
		observe.WithAttributes(
			observe.StringAttr("run_id", c.runID),
			observe.StringAttr("step_id", c.stepID),
			observe.StringAttr("error", err.Error()),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(observe.StatusError, err.Error())
	payload := []byte(fmt.Sprintf("%q", err.Error()))
	return c.publishEvent(
		protocol.EventStepFailed, payload,
	)
}

// Continue publishes a step.continue event. The MsgId includes a
// nonce (UnixNano timestamp) so that if a worker crashes after
// Continue() but before acking the task message, the redelivered
// task can publish a new continue event without JetStream dedup
// swallowing it.
func (c *taskContext) Continue(output []byte) error {
	_, span := c.tel.Tracer.Start(c.ctx,
		"worker.continue",
		observe.WithAttributes(
			observe.StringAttr("run_id", c.runID),
			observe.StringAttr("step_id", c.stepID),
			observe.Int64Attr(
				"iteration", int64(c.iteration),
			),
		),
	)
	defer span.End()
	evt := protocol.NewStepEvent(
		protocol.EventStepContinue,
		c.runID, c.stepID, output,
	)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	msgID := fmt.Sprintf(
		"%s.%s.continue.%d.%s",
		c.runID, c.stepID, c.iteration, nonce,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	injectWorkerTraceCtx(c.span, &evt, msg)
	_, err = c.js.PublishMsg(msg)
	return err
}

// PutStream publishes data to a streaming subject for real-time
// consumption. Uses core NATS pub/sub (not JetStream) -- tokens are
// ephemeral, fire-and-forget. Clients subscribe to
// stream.{run_id}.{step_id} for live delivery.
func (c *taskContext) PutStream(data []byte) error {
	subject := fmt.Sprintf("stream.%s.%s", c.runID, c.stepID)
	return c.nc.Publish(subject, data)
}

// publishEvent creates, traces, and publishes a step lifecycle
// event with trace context propagation.
func (c *taskContext) publishEvent(
	eventType protocol.EventType, payload []byte,
) error {
	evt := protocol.NewStepEvent(
		eventType, c.runID, c.stepID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	injectWorkerTraceCtx(c.span, &evt, msg)
	_, err = c.js.PublishMsg(msg)
	return err
}
