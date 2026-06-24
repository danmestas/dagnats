// e2e/features/webhook_test.go
// Tests webhook trigger fires workflow events. Methodology: register
// webhook trigger, POST to handler via httptest, verify workflow.started
// event published with correct TriggerEnvelope payload.
package features

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestWebhookTrigger(t *testing.T) {
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

		wfName := harness.UniqueName(t, "webhook-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "webhook-task")
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

		webhookPath := "/" + harness.UniqueName(t, "hook")
		triggerID := harness.UniqueName(t, "webhook")
		triggerDef := trigger.TriggerDef{
			ID:         triggerID,
			WorkflowID: wfName,
			Enabled:    true,
			Webhook: &trigger.WebhookConfig{
				Path: webhookPath,
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

		// Allow KV watcher to pick up the new trigger.
		time.Sleep(1 * time.Second)

		// POST to webhook handler.
		handler := ts.WebhookHandler()
		req := httptest.NewRequest(
			http.MethodPost, webhookPath,
			strings.NewReader(`{"event":"test"}`),
		)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// Positive: webhook accepted with 200 OK.
		if rec.Code != http.StatusOK {
			t.Fatalf("webhook: expected 200, got %d", rec.Code)
		}

		// Negative: workflow.started event was published.
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
			// Verify trigger type is webhook.
			if envelope.Trigger != "webhook" {
				t.Fatalf("trigger type: expected webhook, got %s",
					envelope.Trigger)
			}
			// Verify source matches trigger ID.
			if envelope.Source != triggerID {
				t.Fatalf("source: expected %s, got %s",
					triggerID, envelope.Source)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for workflow.started event")
		}
	})
}
