// worker/lane_consume_test.go
// Tests for consumeLane, the one place a lane's Consume pull timings are
// set, and for the start-window log level of consumeErrHandler (#992).
// Methodology: a real embedded NATS server per test. A pull consumer on a
// cluster can meet "no responders" when its first pull reaches the
// worker's server before the consumer leader's MSG.NEXT interest does;
// an in-process cluster routes too fast to hit that race on demand, so
// the test recreates its shape deterministically on one server. It
// deletes the durable, starts consumeLane on the stale handle so the
// first pull is answered "no responders", then recreates the durable,
// as the late interest would arrive, and publishes one task.
// Positive: the task reaches the handler within laneRecoverBound, where
// nats.go's defaults wait out a 30s heartbeat interval first.
// Negative: the lane saw the "no responders" it recovered from, and the
// task is delivered exactly once.
// The log-level tests are pure: no NATS.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// laneRecoverBound is how soon a lane must consume after a missed first
// pull. nats.go's defaults take 30s; the fix takes about two seconds.
const laneRecoverBound = 5 * time.Second

func TestConsumeLane_RecoversFromMissedFirstPull(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := jetstream.ConsumerConfig{
		Durable:       "workers-lane-ready",
		FilterSubject: "task.lane-ready",
		AckPolicy:     jetstream.AckExplicitPolicy,
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if err := js.DeleteConsumer(ctx, "TASK_QUEUES", cfg.Durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}

	missed := make(chan struct{}, 1)
	var delivered atomic.Int32
	got := make(chan struct{}, 1)
	cc, err := consumeLane(cons, func(msg jetstream.Msg) {
		delivered.Add(1)
		if ackErr := msg.Ack(); ackErr != nil {
			t.Errorf("ack: %v", ackErr)
		}
		select {
		case got <- struct{}{}:
		default:
		}
	}, func(_ jetstream.ConsumeContext, err error) {
		if errors.Is(err, nats.ErrNoResponders) {
			select {
			case missed <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("consumeLane: %v", err)
	}
	t.Cleanup(cc.Stop)

	// Negative: the first pull must really have been missed, or this
	// test proves nothing about recovering from one.
	select {
	case <-missed:
	case <-time.After(laneRecoverBound):
		t.Fatal("first pull was not answered no responders")
	}

	if _, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg); err != nil {
		t.Fatalf("recreate consumer: %v", err)
	}
	started := time.Now()
	if _, err := js.Publish(ctx, "task.lane-ready", []byte("{}")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Positive: the lane re-pulls on its own, well inside the 30s that
	// nats.go's default heartbeat interval would leave it idle.
	select {
	case <-got:
	case <-time.After(laneRecoverBound):
		t.Fatalf("task not dispatched within %v of the consumer appearing",
			laneRecoverBound)
	}
	t.Logf("dispatched %v after the consumer appeared", time.Since(started))
	time.Sleep(200 * time.Millisecond)
	if n := delivered.Load(); n != 1 {
		t.Fatalf("delivered %d times, want exactly 1", n)
	}
}

func TestConsumeErrLevel_StartWindowMissIsDebug(t *testing.T) {
	early := laneStartQuietWindow / 2

	// Positive: a missed first pull and the heartbeat gap that follows it
	// are expected right after a lane starts.
	if got := consumeErrLevel(nats.ErrNoResponders, early); got != slog.LevelDebug {
		t.Fatalf("no responders at start: got %v, want Debug", got)
	}
	if got := consumeErrLevel(jetstream.ErrNoHeartbeat, early); got != slog.LevelDebug {
		t.Fatalf("no heartbeat at start: got %v, want Debug", got)
	}
	// Negative: a real error stays a warning even at start.
	if got := consumeErrLevel(jetstream.ErrConsumerDeleted, early); got != slog.LevelWarn {
		t.Fatalf("consumer deleted at start: got %v, want Warn", got)
	}
}

func TestConsumeErrLevel_AfterWindowWarns(t *testing.T) {
	late := laneStartQuietWindow + time.Second

	// Positive: past the start window, no responders means the lane is
	// really cut off from its consumer, so it warns.
	if got := consumeErrLevel(nats.ErrNoResponders, late); got != slog.LevelWarn {
		t.Fatalf("no responders after window: got %v, want Warn", got)
	}
	// Negative: a wrapped start-window error is still recognised.
	wrapped := errors.Join(errors.New("pull"), nats.ErrNoResponders)
	if got := consumeErrLevel(wrapped, 0); got != slog.LevelDebug {
		t.Fatalf("wrapped no responders at start: got %v, want Debug", got)
	}
}

func TestConsumeErrLevel_PanicsOnNil(t *testing.T) {
	defer func() {
		// Positive: a nil error is a nats.go contract breach.
		if recover() == nil {
			t.Fatal("expected panic on nil error")
		}
	}()
	consumeErrLevel(nil, 0)
	// Negative: unreachable if the panic fired.
	t.Fatal("consumeErrLevel did not panic")
}
