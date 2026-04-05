package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type taskContext struct {
	nc           *nats.Conn
	js           jetstream.JetStream
	tel          *observe.Telemetry
	runID        string
	stepID       string
	iteration    int
	attempt      int
	input        []byte
	ctx          context.Context
	span         observe.Span
	msg          jetstream.Msg
	checkpointKV jetstream.KeyValue
	signalKV     jetstream.KeyValue
	paused       bool // set by Pause() to prevent double-ack in handleMessage
}

// newTaskContext constructs a taskContext from a dispatched
// TaskPayload. The ctx and span carry trace context from the
// parent executeTask span so child spans link correctly.
func newTaskContext(
	nc *nats.Conn,
	tel *observe.Telemetry,
	js jetstream.JetStream,
	payload protocol.TaskPayload,
	ctx context.Context,
	span observe.Span,
	msg jetstream.Msg,
	checkpointKV jetstream.KeyValue,
	signalKV jetstream.KeyValue,
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

func (c *taskContext) Input() []byte   { return c.input }
func (c *taskContext) RunID() string   { return c.runID }
func (c *taskContext) StepID() string  { return c.stepID }
func (c *taskContext) RetryCount() int { return c.attempt }

// Complete publishes a step.completed event with trace context.
func (c *taskContext) Complete(output []byte) error {
	if c.msg == nil {
		panic("Complete: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Complete: runID must not be empty")
	}
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

// Fail publishes a step.failed event with retriable failure type.
func (c *taskContext) Fail(err error) error {
	if c.msg == nil {
		panic("Fail: msg already consumed or nil")
	}
	if err == nil {
		panic("Fail: err must not be nil")
	}
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
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:       err.Error(),
		FailureType: protocol.FailureTypeRetriable,
	})
}

// FailPermanent publishes a step.failed event with non-retriable
// failure type. The engine skips all retries for this step.
func (c *taskContext) FailPermanent(err error) error {
	if c.msg == nil {
		panic("FailPermanent: msg already consumed or nil")
	}
	if err == nil {
		panic("FailPermanent: err must not be nil")
	}
	_, span := c.tel.Tracer.Start(c.ctx,
		"worker.failPermanent",
		observe.WithAttributes(
			observe.StringAttr("run_id", c.runID),
			observe.StringAttr("step_id", c.stepID),
			observe.StringAttr("error", err.Error()),
			observe.StringAttr("failure_type", "non_retriable"),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(observe.StatusError, err.Error())
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:       err.Error(),
		FailureType: protocol.FailureTypeNonRetriable,
	})
}

// FailRetryAfter publishes a step.failed event with an explicit
// retry delay. The engine retries after exactly the specified
// duration, ignoring the step's backoff policy for this attempt.
func (c *taskContext) FailRetryAfter(
	err error, after time.Duration,
) error {
	if c.msg == nil {
		panic("FailRetryAfter: msg already consumed or nil")
	}
	if err == nil {
		panic("FailRetryAfter: err must not be nil")
	}
	if after <= 0 {
		panic("FailRetryAfter: after must be positive")
	}
	_, span := c.tel.Tracer.Start(c.ctx,
		"worker.failRetryAfter",
		observe.WithAttributes(
			observe.StringAttr("run_id", c.runID),
			observe.StringAttr("step_id", c.stepID),
			observe.StringAttr("error", err.Error()),
			observe.StringAttr("failure_type", "retry_after"),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(observe.StatusError, err.Error())
	afterMs := after.Milliseconds()
	if afterMs > 3_600_000 {
		afterMs = 3_600_000
	}
	if afterMs < 100 {
		afterMs = 100
	}
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:        err.Error(),
		FailureType:  protocol.FailureTypeRetryAfter,
		RetryAfterMs: afterMs,
	})
}

// publishFailedPayload marshals a StepFailedPayload and publishes
// it as a step.failed event.
func (c *taskContext) publishFailedPayload(
	payload protocol.StepFailedPayload,
) error {
	if c.runID == "" {
		panic("publishFailedPayload: runID must not be empty")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal StepFailedPayload: %w", err)
	}
	return c.publishEvent(protocol.EventStepFailed, data)
}

// Continue publishes a step.continue event. The MsgId includes a
// nonce (UnixNano timestamp) so that if a worker crashes after
// Continue() but before acking the task message, the redelivered
// task can publish a new continue event without JetStream dedup
// swallowing it.
func (c *taskContext) Continue(output []byte) error {
	if c.msg == nil {
		panic("Continue: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Continue: runID must not be empty")
	}
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
	outMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	injectWorkerTraceCtx(c.span, &evt, outMsg)
	_, err = c.js.PublishMsg(
		context.Background(), outMsg,
	)
	return err
}

// PutStream publishes data to a streaming subject for real-time
// consumption. Uses core NATS pub/sub (not JetStream) -- tokens are
// ephemeral, fire-and-forget. Clients subscribe to
// stream.{run_id}.{step_id} for live delivery.
func (c *taskContext) PutStream(data []byte) error {
	if c.msg == nil {
		panic("PutStream: msg already consumed or nil")
	}
	if c.nc == nil {
		panic("PutStream: nc must not be nil")
	}
	subject := fmt.Sprintf(
		"stream.%s.%s", c.runID, c.stepID,
	)
	return c.nc.Publish(subject, data)
}

// publishEvent creates, traces, and publishes a step lifecycle
// event with trace context propagation.
func (c *taskContext) publishEvent(
	eventType protocol.EventType, payload []byte,
) error {
	if eventType == "" {
		panic("publishEvent: eventType must not be empty")
	}
	if c.runID == "" {
		panic("publishEvent: runID must not be empty")
	}
	evt := protocol.NewStepEvent(
		eventType, c.runID, c.stepID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	outMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	injectWorkerTraceCtx(c.span, &evt, outMsg)
	_, err = c.js.PublishMsg(
		context.Background(), outMsg,
	)
	return err
}

// Heartbeat extends the AckWait timer on the original NATS message
// to prevent redelivery while long-running work is in progress.
func (c *taskContext) Heartbeat() error {
	if c.msg == nil {
		panic("Heartbeat: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Heartbeat: runID must not be empty")
	}
	return c.msg.InProgress()
}

// Checkpoint saves arbitrary state to KV at {runID}.{stepID}.
// Returns error if checkpointKV is not configured.
func (c *taskContext) Checkpoint(state []byte) error {
	if c.msg == nil {
		panic("Checkpoint: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Checkpoint: runID must not be empty")
	}
	if c.checkpointKV == nil {
		return fmt.Errorf("checkpoint KV not configured")
	}
	key := c.runID + "." + c.stepID
	_, err := c.checkpointKV.Put(
		context.Background(), key, state,
	)
	return err
}

// LoadCheckpoint retrieves saved state from KV at {runID}.{stepID}.
// Returns (nil, nil) if checkpoint does not exist or KV not configured.
func (c *taskContext) LoadCheckpoint() ([]byte, error) {
	if c.runID == "" {
		panic("LoadCheckpoint: runID must not be empty")
	}
	if c.stepID == "" {
		panic("LoadCheckpoint: stepID must not be empty")
	}
	if c.checkpointKV == nil {
		return nil, nil
	}
	key := c.runID + "." + c.stepID
	entry, err := c.checkpointKV.Get(
		context.Background(), key,
	)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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
	if name == "" {
		panic("WaitForSignal: name must not be empty")
	}
	if timeout <= 0 || timeout > 1*time.Hour {
		panic("WaitForSignal: timeout must be in (0, 1h]")
	}
	if c.signalKV == nil {
		return nil, fmt.Errorf("signal KV not configured")
	}
	key := c.runID + "." + name
	watcher, err := c.signalKV.Watch(
		context.Background(), key,
	)
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

// Pause checkpoints state with a pause marker, then NAKs with delay.
// On redeliver, the handler calls LoadCheckpoint to detect the resume.
// The engine is not involved — step stays StepStatusRunning.
func (c *taskContext) Pause(name string, duration time.Duration) error {
	if name == "" {
		panic("Pause: name must not be empty")
	}
	if duration <= 0 {
		panic("Pause: duration must be positive")
	}
	if c.msg == nil {
		panic("Pause: msg already consumed or nil")
	}
	checkpoint := map[string]any{"pause_resume": name}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal pause checkpoint: %w", err)
	}
	if err := c.Checkpoint(data); err != nil {
		return fmt.Errorf("save pause checkpoint: %w", err)
	}
	c.paused = true
	return c.msg.NakWithDelay(duration)
}

// SendSignal writes data to KV at {runID}.{name} to wake up a
// waiting WaitForSignal call (possibly in another step).
func (c *taskContext) SendSignal(
	runID, name string, data []byte,
) error {
	if runID == "" {
		panic("SendSignal: runID must not be empty")
	}
	if name == "" {
		panic("SendSignal: name must not be empty")
	}
	if c.signalKV == nil {
		return fmt.Errorf("signal KV not configured")
	}
	key := runID + "." + name
	_, err := c.signalKV.Put(
		context.Background(), key, data,
	)
	return err
}
