// internal/api/list_run_events_step_test.go
// Regression guard for #142: ListRunEvents must surface step.* lifecycle
// events (queued/started/completed/failed) in addition to workflow.* events.
// Pre-PR-150, only workflow.* events were emitted; #142 was filed against
// that state. Post-#150, all step events are published to history.<runID>
// and ListRunEvents subscribes to the full prefix — this test pins that
// behavior so future regressions don't quietly hide step transitions
// from the CLI.
package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestListRunEvents_IncludesStepLifecycleEvents(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	svc := NewService(nc)
	const runID = "run-step-events-test"

	publish := func(typ protocol.EventType, stepID string) {
		evt := protocol.Event{
			Type:      typ,
			RunID:     runID,
			StepID:    stepID,
			Timestamp: time.Now().UTC(),
		}
		data, err := evt.Marshal()
		if err != nil {
			t.Fatalf("marshal %s: %v", typ, err)
		}
		if _, err := js.Publish(
			"history."+runID, data,
			nats.MsgId(string(typ)+"."+stepID),
		); err != nil {
			t.Fatalf("publish %s: %v", typ, err)
		}
	}

	publish(protocol.EventWorkflowStarted, "")
	publish(protocol.EventStepQueued, "fetch")
	publish(protocol.EventStepStarted, "fetch")
	publish(protocol.EventStepCompleted, "fetch")
	publish(protocol.EventWorkflowCompleted, "")

	events, err := svc.ListRunEvents(context.Background(), runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}

	seen := make(map[string]bool, len(events))
	for _, e := range events {
		seen[e.Type] = true
	}

	wanted := []string{
		string(protocol.EventWorkflowStarted),
		string(protocol.EventStepQueued),
		string(protocol.EventStepStarted),
		string(protocol.EventStepCompleted),
		string(protocol.EventWorkflowCompleted),
	}
	for _, w := range wanted {
		if !seen[w] {
			var got []string
			for _, e := range events {
				got = append(got, e.Type)
			}
			t.Fatalf("ListRunEvents missing %q\nseen: %s",
				w, strings.Join(got, ", "))
		}
	}
}
