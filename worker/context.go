package worker

import (
	"fmt"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

type taskContext struct {
	js        nats.JetStreamContext
	runID     string
	stepID    string
	iteration int
	input     []byte
}

// newTaskContext constructs a taskContext from a dispatched TaskPayload.
// iteration is the agent-loop cycle index, used to make Continue MsgIds unique
// across iterations so JetStream deduplication does not swallow subsequent cycles.
func newTaskContext(js nats.JetStreamContext, payload protocol.TaskPayload) *taskContext {
	return &taskContext{
		js:        js,
		runID:     payload.RunID,
		stepID:    payload.StepID,
		iteration: payload.Iteration,
		input:     payload.Input,
	}
}

func (c *taskContext) Input() []byte  { return c.input }
func (c *taskContext) RunID() string  { return c.runID }
func (c *taskContext) StepID() string { return c.stepID }

func (c *taskContext) Complete(output []byte) error {
	return c.publishEvent(protocol.EventStepCompleted, output)
}

func (c *taskContext) Fail(err error) error {
	payload := []byte(fmt.Sprintf("%q", err.Error()))
	return c.publishEvent(protocol.EventStepFailed, payload)
}

// Continue publishes a step.continue event with a per-iteration MsgId so each
// loop cycle produces a distinct dedup key — preventing JetStream from swallowing
// the second and subsequent continue signals.
func (c *taskContext) Continue(output []byte) error {
	evt := protocol.NewStepEvent(protocol.EventStepContinue, c.runID, c.stepID, output)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msgID := fmt.Sprintf("%s.%s.continue.%d", c.runID, c.stepID, c.iteration)
	_, err = c.js.Publish(evt.NATSSubject(), data, nats.MsgId(msgID))
	return err
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
