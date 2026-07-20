// e2e/features/cron_test.go
// Tests cron trigger fires workflow events. Methodology: register cron
// trigger, force tick, verify workflow.started event published to
// JetStream history stream with correct TriggerEnvelope payload.
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

func TestCronTrigger(t *testing.T) {
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

		// Register workflow (needed for trigger association).
		wfName := harness.UniqueName(t, "cron-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "cron-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Create trigger service and register cron trigger.
		ts, err := trigger.NewTriggerService(nc, "1.0.0")
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		err = ts.Start()
		if err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(func() { ts.Stop() })

		triggerID := harness.UniqueName(t, "cron")
		triggerDef := trigger.TriggerDef{
			ID:         triggerID,
			WorkflowID: wfName,
			Enabled:    true,
			Cron: &trigger.CronConfig{
				Expression: "* * * * *",
				Timezone:   "UTC",
			},
		}
		err = svc.CreateTrigger(ctx, triggerDef)
		if err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// Subscribe to history stream to capture triggered events.
		// No orchestrator running — we test trigger event publishing only.
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

		// Ticking before the KV watcher has installed the trigger
		// fires an empty scheduler, and the miss then looks like a
		// missing workflow.started event.
		harness.WaitForPrecondition(t,
			"cron trigger "+triggerID+" registered in the scheduler",
			triggerReadyCeiling,
			func() bool { return ts.TriggerCount() >= 1 },
		)

		// Force a tick.
		ts.TickNow()

		// Positive: a workflow.started event was published.
		select {
		case evt := <-eventCh:
			if evt.Type != protocol.EventWorkflowStarted {
				t.Fatalf("expected workflow.started, got %s",
					evt.Type)
			}
			// Verify the envelope payload contains cron source.
			var envelope trigger.TriggerEnvelope
			if err := json.Unmarshal(
				evt.Payload, &envelope,
			); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			// Positive: trigger type is cron.
			if envelope.Trigger != "cron" {
				t.Fatalf("trigger type: expected cron, got %s",
					envelope.Trigger)
			}
			// Negative: source matches trigger ID.
			if envelope.Source != triggerID {
				t.Fatalf("source: expected %s, got %s",
					triggerID, envelope.Source)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for workflow.started event")
		}
	})
}
