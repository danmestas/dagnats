// trigger/subject_test.go
// Methodology: Integration tests with embedded NATS. Each test creates
// a SubjectTrigger with real NATS subscriptions to verify message flow
// and workflow event publishing. Bounded timeouts prevent hanging tests.
package trigger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestSubjectTriggerPublishesWorkflowStarted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "test-subject-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Subject: &SubjectConfig{
			Subject: "events.user.created",
		},
	}

	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()

	// Subscribe to workflow events
	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish message to trigger subject
	testPayload := []byte(`{"user_id": "12345", "email": "test@example.com"}`)
	err = nc.Publish("events.user.created", testPayload)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Should receive workflow.started event
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event, got timeout")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Verify event type
	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}

	// Verify trigger envelope in payload
	var envelope TriggerEnvelope
	err = json.Unmarshal(evt.Payload, &envelope)
	if err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	if envelope.Trigger != "subject" {
		t.Errorf("expected trigger=subject, got %s", envelope.Trigger)
	}
	if envelope.Source != "test-subject-trigger" {
		t.Errorf("expected source=test-subject-trigger, got %s", envelope.Source)
	}

	// Verify original payload is embedded
	var data map[string]interface{}
	err = json.Unmarshal(envelope.Data, &data)
	if err != nil {
		t.Fatalf("unmarshal data failed: %v", err)
	}
	if data["user_id"] != "12345" {
		t.Errorf("expected user_id=12345, got %v", data["user_id"])
	}
}

func TestSubjectTriggerDisabled(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "disabled-subject-trigger",
		WorkflowID: "test-workflow",
		Enabled:    false,
		Subject: &SubjectConfig{
			Subject: "events.test",
		},
	}

	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish message
	err = nc.Publish("events.test", []byte(`{"data": "test"}`))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Should NOT receive workflow event
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("disabled trigger should not fire")
	}
}

func TestSubjectTriggerWildcardSubject(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "wildcard-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Subject: &SubjectConfig{
			Subject: "events.user.*",
		},
	}

	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish to matching wildcard subject
	err = nc.Publish("events.user.created", []byte(`{"type": "created"}`))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected event for wildcard match")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}

	// Publish to another matching subject
	err = nc.Publish("events.user.deleted", []byte(`{"type": "deleted"}`))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	msg, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected event for second wildcard match")
	}

	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}
}

func TestSubjectTriggerEmptyPayload(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "empty-payload-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Subject: &SubjectConfig{
			Subject: "events.empty",
		},
	}

	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish empty message
	err = nc.Publish("events.empty", nil)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected event even with empty payload")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	var envelope TriggerEnvelope
	err = json.Unmarshal(evt.Payload, &envelope)
	if err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	// Data should be null or empty
	if len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		t.Errorf("expected empty or null data, got %s", envelope.Data)
	}
}
