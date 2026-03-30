package worker

import (
	"fmt"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

type taskContext struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	runID     string
	stepID    string
	iteration int
	attempt   int
	input     []byte
}

// newTaskContext constructs a taskContext from a dispatched TaskPayload.
// iteration is the agent-loop cycle index, used to make Continue MsgIds unique
// across iterations so JetStream deduplication does not swallow subsequent cycles.
func newTaskContext(
	nc *nats.Conn,
	js nats.JetStreamContext,
	payload protocol.TaskPayload,
) *taskContext {
	return &taskContext{
		nc:        nc,
		js:        js,
		runID:     payload.RunID,
		stepID:    payload.StepID,
		iteration: payload.Iteration,
		attempt:   payload.Attempt,
		input:     payload.Input,
	}
}

func (c *taskContext) Input() []byte    { return c.input }
func (c *taskContext) RunID() string    { return c.runID }
func (c *taskContext) StepID() string   { return c.stepID }
func (c *taskContext) RetryCount() int  { return c.attempt }

func (c *taskContext) Complete(output []byte) error {
	return c.publishEvent(protocol.EventStepCompleted, output)
}

func (c *taskContext) Fail(err error) error {
	payload := []byte(fmt.Sprintf("%q", err.Error()))
	return c.publishEvent(protocol.EventStepFailed, payload)
}

// Continue publishes a step.continue event. The MsgId includes a nonce
// (UnixNano timestamp) so that if a worker crashes after Continue() but
// before acking the task message, the redelivered task can publish a new
// continue event without JetStream dedup swallowing it. Duplicate
// continues are safe because the orchestrator bounds iterations via
// MaxIterations and serializes events per-run.
func (c *taskContext) Continue(output []byte) error {
	evt := protocol.NewStepEvent(
		protocol.EventStepContinue, c.runID, c.stepID, output,
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
	_, err = c.js.Publish(evt.NATSSubject(), data, nats.MsgId(msgID))
	return err
}

// PutStream publishes data to a streaming subject for real-time consumption.
// Uses core NATS pub/sub (not JetStream) — tokens are ephemeral, fire-and-forget.
// Clients subscribe to stream.{run_id}.{step_id} for live delivery.
func (c *taskContext) PutStream(data []byte) error {
	subject := fmt.Sprintf("stream.%s.%s", c.runID, c.stepID)
	return c.nc.Publish(subject, data)
}

func (c *taskContext) publishEvent(eventType protocol.EventType, payload []byte) error {
	evt := protocol.NewStepEvent(eventType, c.runID, c.stepID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	_, err = c.js.Publish(evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
	return err
}
