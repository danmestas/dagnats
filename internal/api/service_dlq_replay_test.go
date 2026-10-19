// api/service_dlq_replay_test.go
// Methodology: real embedded NATS. Seeds more DEAD_LETTERS entries than
// any scan window would cover and checks ReplayDeadLetter resolves a
// sequence directly instead of scanning a bounded prefix (#784).
package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

func TestReplayDeadLetterBeyondFirstHundred(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)
	js, _ := nc.JetStream()
	const entries = 120
	var lastSeq uint64
	for i := 0; i < entries; i++ {
		run := fmt.Sprintf("run-%d", i)
		ack, err := js.PublishMsg(&nats.Msg{
			Subject: "dead.task-bulk." + run + ".step",
			Data:    []byte(`{"n":1}`),
			Header: nats.Header{
				engine.HeaderDLQRunID:       {run},
				engine.HeaderDLQStepID:      {"step"},
				engine.HeaderDLQTask:        {"task-bulk"},
				engine.HeaderDLQTaskSubject: {"task.task-bulk." + run},
			},
		})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		lastSeq = ack.Sequence
	}
	sub, err := js.SubscribeSync("task.task-bulk.run-119")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	if err := svc.ReplayDeadLetter(context.Background(), lastSeq); err != nil {
		t.Fatalf("replay of seq %d past index 100: %v", lastSeq, err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("replayed message not received: %v", err)
	}
	missing := svc.ReplayDeadLetter(context.Background(), lastSeq+1000)
	if missing == nil || !contains(missing.Error(), "not found") {
		t.Fatalf("want not-found error, got %v", missing)
	}
}
