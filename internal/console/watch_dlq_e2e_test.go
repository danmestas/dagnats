// watch_dlq_e2e_test.go exercises the real apiServiceAdapter.WatchDLQ
// against an embedded NATS server with the production DEAD_LETTERS
// stream — the wire the fake-based unit tests cannot cover.
//
// Methodology:
//   - dagnatstest.NewHarness stands up embedded NATS + the production
//     streams, including DEAD_LETTERS (subjects: dead.>).
//   - The real NewAPIDataSource (no fake) backs WatchDLQ.
//   - A dead letter published on dead.<task>.<run> must reach the watch
//     channel, proving the subscription subject matches the stream.
//   - Regression for the live DLQ feed silently 503'ing with
//     "console: sse dlq watch err=no stream matches subject": WatchDLQ
//     subscribed to dead_letters.> (no such stream) instead of dead.>.
//   - Two assertions: WatchDLQ returns no error (the bug failed here),
//     and a published entry is delivered (negative space: the channel
//     must not stay silent).
package console

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
)

func TestWatchDLQ_receivesPublishedDeadLetter(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	ds := NewAPIDataSource(
		h.Svc, h.NC, nil,
		slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The watch must bind to the DEAD_LETTERS stream. With the wrong
	// subject this errors immediately ("no stream matches subject").
	ch, err := ds.WatchDLQ(ctx)
	if err != nil {
		t.Fatalf("WatchDLQ: %v — the watch subject must match the "+
			"DEAD_LETTERS stream's dead.> subjects", err)
	}

	// WatchDLQ uses DeliverNew, so publish AFTER subscribing to be sure
	// the live feed (not just the initial replay) carries the entry.
	f := dagnatstest.NewDLQFixture(h)
	pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pubCancel()
	if err := f.PublishDLQWithMsgID(
		pubCtx, "dead.e2e-watch-task.run-1", "watch-dlq-1",
		[]byte(`{"error":"boom"}`),
	); err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	select {
	case du, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed before delivering the entry")
		}
		if du.View.Sequence == 0 {
			t.Fatalf("delivered DLQUpdate has zero sequence: %+v", du)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("published dead letter never reached the watch channel")
	}
}
