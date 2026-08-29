// internal/engine/run_event_dedup_test.go
// White-box (package engine) test for event.run.* dedup: publishing the
// same finalize twice (simulating a redelivery — e.g. a handler that NAKs
// after saveFn succeeded and re-runs) must collapse to exactly one
// message in EVENTS for that run, because publishRunEvent's Msg-Id is
// keyed on RunID alone.
// Methodology: real embedded NATS/JetStream. Subscribe to the exact
// event.run.* subject BEFORE either publish. First NextMsg must succeed;
// a second NextMsg within the dedup window must time out.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishRunEvent_RedeliveredFinalize_DedupsToOneMessage(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsc, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, jsc)

	now := time.Now().UTC()
	run := dag.WorkflowRun{
		RunID:       "run-dedup-1",
		WorkflowID:  "dedup-wf",
		Status:      dag.RunStatusCompleted,
		CreatedAt:   now.Add(-time.Minute),
		CompletedAt: &now,
	}

	subject := runEventSubject(run.WorkflowID, run.RunID, "completed")
	sub, err := js.SubscribeSync(subject, nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	ctx := t.Context()
	// First publish: the original finalize.
	publishRunEvent(ctx, tp, run)
	// Second publish: simulates a redelivered finalize (e.g. the
	// handler NAK'd after saveFn succeeded but before ack, and NATS
	// redelivered) — same RunID, same Msg-Id.
	publishRunEvent(ctx, tp, run)

	first, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg (first): %v", err)
	}
	if first == nil {
		t.Fatal("first message must not be nil")
	}
	// Positive: the surviving message is the run.completed event we
	// published, on the exact subject we subscribed to — not just "a"
	// message, but the correct one, with the correct payload.
	if first.Subject != subject {
		t.Fatalf("surviving message subject = %q, want %q", first.Subject, subject)
	}
	var survivingEvt protocol.RunEvent
	if err := json.Unmarshal(first.Data, &survivingEvt); err != nil {
		t.Fatalf("unmarshal surviving message: %v", err)
	}
	if survivingEvt.Type != protocol.RunEventCompleted {
		t.Fatalf("surviving payload Type = %q, want %q",
			survivingEvt.Type, protocol.RunEventCompleted)
	}
	if survivingEvt.RunID != run.RunID {
		t.Fatalf("surviving payload RunID = %q, want %q",
			survivingEvt.RunID, run.RunID)
	}
	if survivingEvt.WorkflowID != run.WorkflowID {
		t.Fatalf("surviving payload WorkflowID = %q, want %q",
			survivingEvt.WorkflowID, run.WorkflowID)
	}

	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Fatal(
			"expected timeout on second NextMsg — dedup should " +
				"have collapsed the redelivered publish to one message",
		)
	}
}
