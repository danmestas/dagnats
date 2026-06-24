// e2e/features/subject_trigger_test.go
// Tests subject trigger fires workflow events. Methodology: register
// trigger on a NATS subject, publish message, verify workflow.started
// event published with correct TriggerEnvelope payload.
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestSubjectTrigger(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		// Create trigger KV buckets (not provisioned by harness).
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: "triggers",
		}); err != nil {
			t.Fatalf("create triggers KV: %v", err)
		}
		if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: "trigger_state",
		}); err != nil {
			t.Fatalf("create trigger_state KV: %v", err)
		}

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "subject-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "subject-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		ts, err := trigger.NewTriggerService(nc, "1.0.0")
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		err = ts.Start()
		if err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(func() { ts.Stop() })

		subjectName := harness.UniqueName(t, "events.order")
		triggerID := harness.UniqueName(t, "subject")
		triggerDef := trigger.TriggerDef{
			ID:         triggerID,
			WorkflowID: wfName,
			Enabled:    true,
			Subject: &trigger.SubjectConfig{
				Subject: subjectName,
			},
		}
		err = svc.CreateTrigger(ctx, triggerDef)
		if err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// Subscribe to history stream to capture triggered events.
		// No orchestrator running — we test trigger event publishing.
		eventCh := make(chan protocol.Event, 1)
		sub, err := js.Subscribe("history.>",
			func(msg *nats.Msg) {
				var evt protocol.Event
				if unmarshalErr := json.Unmarshal(
					msg.Data, &evt,
				); unmarshalErr != nil {
					return
				}
				if evt.Type == protocol.EventWorkflowStarted {
					select {
					case eventCh <- evt:
					default:
					}
				}
				msg.Ack()
			},
			nats.DeliverNew(),
			nats.AckExplicit(),
		)
		if err != nil {
			t.Fatalf("Subscribe history: %v", err)
		}
		t.Cleanup(func() { sub.Unsubscribe() })

		// Allow KV watcher and subscription to establish.
		time.Sleep(1 * time.Second)

		// Publish to the trigger subject.
		err = nc.Publish(subjectName, []byte(`{"order_id":"123"}`))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}

		// Positive: workflow.started event was published.
		select {
		case evt := <-eventCh:
			if evt.Type != protocol.EventWorkflowStarted {
				t.Fatalf("expected workflow.started, got %s",
					evt.Type)
			}
			var envelope trigger.TriggerEnvelope
			if err := json.Unmarshal(
				evt.Payload, &envelope,
			); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			// Verify trigger type is subject.
			if envelope.Trigger != "subject" {
				t.Fatalf("trigger type: expected subject, got %s",
					envelope.Trigger)
			}
			// Negative: envelope contains original message data.
			if envelope.Data == nil {
				t.Fatal("envelope.Data is nil, expected payload")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for workflow.started event")
		}
	})
}
