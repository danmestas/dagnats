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
	"github.com/nats-io/nats.go"
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

	trigger, sub := setupSubjectTrigger(t, nc, js)
	defer trigger.Close()

	// Publish message to trigger subject
	testPayload := []byte(`{"user_id": "12345", "email": "test@example.com"}`)
	err = nc.Publish("events.user.created", testPayload)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Positive: should receive workflow.started event
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event, got timeout")
	}

	verifySubjectTriggerEvent(t, msg, "test-subject-trigger", "12345")
}

func setupSubjectTrigger(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext,
) (*SubjectTrigger, *nats.Subscription) {
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

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	return trigger, sub
}

func verifySubjectTriggerEvent(
	t *testing.T, msg *nats.Msg, expectedSource, expectedUserID string,
) {
	var evt protocol.Event
	err := json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}

	var envelope TriggerEnvelope
	err = json.Unmarshal(evt.Payload, &envelope)
	if err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	if envelope.Trigger != "subject" {
		t.Errorf("expected trigger=subject, got %s", envelope.Trigger)
	}
	if envelope.Source != expectedSource {
		t.Errorf("expected source=%s, got %s", expectedSource, envelope.Source)
	}

	var data map[string]interface{}
	err = json.Unmarshal(envelope.Data, &data)
	if err != nil {
		t.Fatalf("unmarshal data failed: %v", err)
	}
	if data["user_id"] != expectedUserID {
		t.Errorf("expected user_id=%s, got %v", expectedUserID, data["user_id"])
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

	trigger, sub := setupWildcardSubjectTrigger(t, nc, js)
	defer trigger.Close()

	// Positive: publish to matching wildcard subject
	publishAndVerifyWildcard(t, nc, sub, "events.user.created",
		`{"type": "created"}`)

	// Positive: publish to another matching subject
	publishAndVerifyWildcard(t, nc, sub, "events.user.deleted",
		`{"type": "deleted"}`)
}

func setupWildcardSubjectTrigger(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext,
) (*SubjectTrigger, *nats.Subscription) {
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

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	return trigger, sub
}

func publishAndVerifyWildcard(
	t *testing.T, nc *nats.Conn, sub *nats.Subscription,
	subject, payload string,
) {
	err := nc.Publish(subject, []byte(payload))
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
}

func TestSubjectTriggerRejectsNilSubjectConfig(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	def := TriggerDef{
		ID:         "no-subject-config",
		WorkflowID: "test-workflow",
		Enabled:    true,
		// Subject is nil
	}

	// Positive: returns error
	_, err = NewSubjectTrigger(nc, def)
	if err == nil {
		t.Fatalf("expected error for nil subject config")
	}

	// Negative: non-nil config succeeds
	def.Subject = &SubjectConfig{Subject: "test.subject"}
	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()
}

func TestSubjectTriggerRejectsEmptySubject(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	def := TriggerDef{
		ID:         "empty-subject",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Subject:    &SubjectConfig{Subject: ""},
	}

	// Positive: returns error for empty subject
	_, err = NewSubjectTrigger(nc, def)
	if err == nil {
		t.Fatalf("expected error for empty subject string")
	}

	// Negative: non-empty subject succeeds
	def.Subject.Subject = "events.test"
	trigger, err := NewSubjectTrigger(nc, def)
	if err != nil {
		t.Fatalf("NewSubjectTrigger failed: %v", err)
	}
	defer trigger.Close()
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
