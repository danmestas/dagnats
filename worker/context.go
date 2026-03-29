package worker

import (
	"fmt"

	"github.com/danmestas/dagnats/engine"
	"github.com/nats-io/nats.go"
)

type taskContext struct {
	js     nats.JetStreamContext
	runID  string
	stepID string
	input  []byte
}

func newTaskContext(js nats.JetStreamContext, runID string, stepID string, input []byte) *taskContext {
	return &taskContext{js: js, runID: runID, stepID: stepID, input: input}
}

func (c *taskContext) Input() []byte  { return c.input }
func (c *taskContext) RunID() string  { return c.runID }
func (c *taskContext) StepID() string { return c.stepID }

func (c *taskContext) Complete(output []byte) error {
	return c.publishEvent(engine.EventStepCompleted, output)
}

func (c *taskContext) Fail(err error) error {
	payload := []byte(fmt.Sprintf("%q", err.Error()))
	return c.publishEvent(engine.EventStepFailed, payload)
}

func (c *taskContext) Continue(output []byte) error {
	return c.publishEvent(engine.EventStepContinue, output)
}

func (c *taskContext) publishEvent(eventType engine.EventType, payload []byte) error {
	evt := engine.NewStepEvent(eventType, c.runID, c.stepID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	_, err = c.js.Publish(evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
	return err
}
