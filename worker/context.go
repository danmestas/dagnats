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
	nc           *nats.Conn
	js           nats.JetStreamContext
	tel          *observe.Telemetry
	runID        string
	stepID       string
	iteration    int
	attempt      int
	input        []byte
	ctx          context.Context
	span         observe.Span
	msg          *nats.Msg
	checkpointKV nats.KeyValue
	signalKV     nats.KeyValue
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
	msg *nats.Msg,
	checkpointKV nats.KeyValue,
	signalKV nats.KeyValue,
) *taskContext {
	if tel == nil {
		panic("newTaskContext: tel must not be nil")
	}
	if ctx == nil {
		panic("newTaskContext: ctx must not be nil")
	}
	return &taskContext{
		nc:           nc,
		js:           js,
		tel:          tel,
		runID:        payload.RunID,
		stepID:       payload.StepID,
		iteration:    payload.Iteration,
		attempt:      payload.Attempt,
		input:        payload.Input,
		ctx:          ctx,
		span:         span,
		msg:          msg,
		checkpointKV: checkpointKV,
		signalKV:     signalKV,
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

// Heartbeat extends the AckWait timer on the original NATS message
// to prevent redelivery while long-running work is in progress.
// Safe to call when msg is nil.
func (c *taskContext) Heartbeat() error {
	if c.msg == nil {
		return nil
	}
	return c.msg.InProgress()
}

// Checkpoint saves arbitrary state to KV at {runID}.{stepID}.
// Returns error if checkpointKV is not configured.
func (c *taskContext) Checkpoint(state []byte) error {
	if c.checkpointKV == nil {
		return fmt.Errorf("checkpoint KV not configured")
	}
	key := c.runID + "." + c.stepID
	_, err := c.checkpointKV.Put(key, state)
	return err
}

// LoadCheckpoint retrieves saved state from KV at {runID}.{stepID}.
// Returns (nil, nil) if checkpoint does not exist or KV not configured.
func (c *taskContext) LoadCheckpoint() ([]byte, error) {
	if c.checkpointKV == nil {
		return nil, nil
	}
	key := c.runID + "." + c.stepID
	entry, err := c.checkpointKV.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return entry.Value(), nil
}

// WaitForSignal watches KV at {runID}.{name} until a value appears
// or timeout expires. Timeout is capped at 1 hour for safety.
func (c *taskContext) WaitForSignal(
	name string, timeout time.Duration,
) ([]byte, error) {
	if c.signalKV == nil {
		return nil, fmt.Errorf("signal KV not configured")
	}
	if timeout > 1*time.Hour {
		timeout = 1 * time.Hour
	}
	key := c.runID + "." + name
	watcher, err := c.signalKV.Watch(key)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			return entry.Value(), nil
		case <-timer.C:
			return nil, fmt.Errorf(
				"signal %q timed out after %s", name, timeout,
			)
		}
	}
}

// SendSignal writes data to KV at {runID}.{name} to wake up a
// waiting WaitForSignal call (possibly in another step).
func (c *taskContext) SendSignal(
	runID, name string, data []byte,
) error {
	if c.signalKV == nil {
		return fmt.Errorf("signal KV not configured")
	}
	key := runID + "." + name
	_, err := c.signalKV.Put(key, data)
	return err
}
