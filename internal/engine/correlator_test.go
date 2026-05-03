// engine/correlator_test.go
// Tests for the event correlator: KV-backed waiter index, event matching,
// and waiter cleanup. Uses real embedded NATS server with JetStream.
// Methodology: register waiters via AddWaiter, publish events to the EVENTS
// stream, then verify match events appear on the history stream. Negative
// tests confirm non-matching events are ignored.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCorrelatorMatchesEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	c := NewCorrelator(nc, jsNew)
	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	// Register a waiter for payment.completed with order_id match.
	waiter := EventWaiter{
		RunID:     "run-c1",
		StepID:    "wait-step",
		EventType: "payment.completed",
		Match: dag.ResolvedMatch{
			Left:  "order_id",
			Op:    dag.MatchOpEq,
			Right: "ord-123",
		},
	}
	if err := c.AddWaiter(context.Background(), waiter); err != nil {
		t.Fatalf("AddWaiter failed: %v", err)
	}

	// Give KV watch time to populate the in-memory index.
	time.Sleep(200 * time.Millisecond)

	// Subscribe to history for the match event.
	historySub, err := js.SubscribeSync(
		"history.run-c1",
		nats.DeliverNew(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history failed: %v", err)
	}

	// Publish a matching event.
	eventData := []byte(`{"order_id":"ord-123","amount":99.99}`)
	mustPublish(t, js, "event.payment.completed", eventData)

	// Wait for the match event on the history stream.
	msg, err := historySub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no match event received: %v", err)
	}

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	// Positive: correct event type and step ID.
	if evt.Type != protocol.EventStepWaitMatched {
		t.Fatalf("event type = %v, want %v",
			evt.Type, protocol.EventStepWaitMatched)
	}
	if evt.StepID != "wait-step" {
		t.Fatalf("step ID = %v, want wait-step", evt.StepID)
	}

	// Positive: payload contains the matched event data.
	if string(evt.Payload) != string(eventData) {
		t.Fatalf("payload = %s, want %s",
			string(evt.Payload), string(eventData))
	}
}

func TestCorrelatorIgnoresNonMatchingEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	c := NewCorrelator(nc, jsNew)
	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	waiter := EventWaiter{
		RunID:     "run-c2",
		StepID:    "wait-step",
		EventType: "payment.completed",
		Match: dag.ResolvedMatch{
			Left:  "order_id",
			Op:    dag.MatchOpEq,
			Right: "ord-123",
		},
	}
	if err := c.AddWaiter(context.Background(), waiter); err != nil {
		t.Fatalf("AddWaiter failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	historySub, err := js.SubscribeSync(
		"history.run-c2",
		nats.DeliverNew(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history failed: %v", err)
	}

	// Publish a NON-matching event (different order_id).
	eventData := []byte(`{"order_id":"ord-999","amount":50.00}`)
	mustPublish(t, js, "event.payment.completed", eventData)

	// Negative: no match event should appear within 1 second.
	msg, err := historySub.NextMsg(1 * time.Second)
	if err == nil {
		t.Fatalf(
			"expected no match, but got event: %s", msg.Data,
		)
	}

	// Positive: the waiter should still be in the index.
	c.mu.RLock()
	count := len(c.waiters["payment.completed"])
	c.mu.RUnlock()
	if count != 1 {
		t.Fatalf(
			"expected 1 waiter remaining, got %d", count,
		)
	}
}

func TestCorrelatorRemoveWaitersForRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	c := NewCorrelator(nc, jsNew)
	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer c.Stop()

	waiter := EventWaiter{
		RunID:     "run-c3",
		StepID:    "wait-step",
		EventType: "payment.completed",
		Match: dag.ResolvedMatch{
			Left:  "order_id",
			Op:    dag.MatchOpEq,
			Right: "ord-456",
		},
	}
	if err := c.AddWaiter(context.Background(), waiter); err != nil {
		t.Fatalf("AddWaiter failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Remove all waiters for the run.
	c.RemoveWaitersForRun(context.Background(), "run-c3")

	// Wait for KV delete to propagate to the in-memory index.
	time.Sleep(200 * time.Millisecond)

	historySub, err := js.SubscribeSync(
		"history.run-c3",
		nats.DeliverNew(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history failed: %v", err)
	}

	// Publish a matching event — should NOT trigger a match.
	eventData := []byte(`{"order_id":"ord-456","amount":75.00}`)
	mustPublish(t, js, "event.payment.completed", eventData)

	// Negative: no match event since waiter was removed.
	msg, err := historySub.NextMsg(1 * time.Second)
	if err == nil {
		t.Fatalf(
			"expected no match after removal, got: %s", msg.Data,
		)
	}

	// Positive: in-memory index should be empty for this type.
	c.mu.RLock()
	count := len(c.waiters["payment.completed"])
	c.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 waiters after removal, got %d", count)
	}
}

func TestCorrelatorLazyStart(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	c := NewCorrelator(nc, jsNew)
	defer c.Stop()

	// Register a waiter WITHOUT calling Start() first.
	waiter := EventWaiter{
		RunID:     "run-c4",
		StepID:    "wait-step",
		EventType: "payment.completed",
		Match: dag.ResolvedMatch{
			Left:  "order_id",
			Op:    dag.MatchOpEq,
			Right: "ord-789",
		},
	}
	if err := c.AddWaiter(context.Background(), waiter); err != nil {
		t.Fatalf("AddWaiter failed: %v", err)
	}

	// Give KV watch time to populate the in-memory index.
	time.Sleep(200 * time.Millisecond)

	// Subscribe to history for the match event.
	historySub, err := js.SubscribeSync(
		"history.run-c4",
		nats.DeliverNew(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history failed: %v", err)
	}

	// Publish a matching event.
	eventData := []byte(`{"order_id":"ord-789","amount":199.99}`)
	mustPublish(t, js, "event.payment.completed", eventData)

	// Positive: match event should appear on history stream.
	msg, err := historySub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no match event received: %v", err)
	}

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	// Positive: correct event type.
	if evt.Type != protocol.EventStepWaitMatched {
		t.Fatalf("event type = %v, want %v",
			evt.Type, protocol.EventStepWaitMatched)
	}
	// Negative: verify payload matches what we published.
	if string(evt.Payload) != string(eventData) {
		t.Fatalf("payload = %s, want %s",
			string(evt.Payload), string(eventData))
	}
}
