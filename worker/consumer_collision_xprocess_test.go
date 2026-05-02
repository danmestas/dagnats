// worker/consumer_collision_xprocess_test.go
// Methodology: real embedded NATS server per test. Pre-seed a durable on
// TASK_QUEUES with the same name a second worker would claim, then drive
// subscribePullConsumer through the public Worker API. Verify the helper
// panics when filter subjects differ (the routing-corruption case), stays
// silent when filter subjects match (idempotent re-registration), and stays
// silent on a clean stream. Bounded 5-10s timeouts on every wait.
package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCrossProcessCollision_DifferentFilter_Panics(t *testing.T) {
	// Pre-seed durable workers-foo with FilterSubject task.bar.> as if
	// Worker A (a different process) had claimed it for some other task
	// type whose sanitized name happens to be "foo". Then drive Worker B
	// with taskType "foo", which derives FilterSubject task.foo.>. Same
	// durable, different filters — without the precheck,
	// CreateOrUpdateConsumer would silently mutate the FilterSubject and
	// corrupt routing.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-foo",
		Name:          "workers-foo",
		FilterSubject: "task.bar.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("seed worker-A durable: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on cross-process collision, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		// Both filter subjects must be named so the operator can identify
		// which task types collided.
		if !strings.Contains(msg, "task.bar.>") {
			t.Errorf("panic must name pre-existing filter task.bar.>, got: %s", msg)
		}
		if !strings.Contains(msg, "task.foo.>") {
			t.Errorf("panic must name claiming filter task.foo.>, got: %s", msg)
		}
		if !strings.Contains(msg, "workers-foo") {
			t.Errorf("panic must name colliding durable workers-foo, got: %s", msg)
		}
	}()

	w := NewWorker(nc)
	w.subscribePullConsumer("foo", "",
		func(ctx TaskContext) error { return nil })
}

func TestCrossProcessCollision_SameFilter_NoPanic(t *testing.T) {
	// Negative-space test: same durable name AND same filter subject.
	// CreateOrUpdateConsumer is idempotent in this case; the helper must
	// not flag this as a collision.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-foo",
		Name:          "workers-foo",
		FilterSubject: "task.foo.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("seed durable: %v", err)
	}

	w := NewWorker(nc)
	cc := w.subscribePullConsumer("foo", "",
		func(ctx TaskContext) error { return nil })
	t.Cleanup(cc.Stop)

	// Positive assertion: durable still present, filter unchanged.
	cons, err := stream.Consumer(ctx, "workers-foo")
	if err != nil {
		t.Fatalf("workers-foo missing after subscribe: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.FilterSubject != "task.foo.>" {
		t.Errorf("FilterSubject = %q, want task.foo.>", info.Config.FilterSubject)
	}
	if info.Config.Durable != "workers-foo" {
		t.Errorf("Durable = %q, want workers-foo", info.Config.Durable)
	}
}

func TestCrossProcessCollision_EmptyStream_NoPanic(t *testing.T) {
	// Negative-space test: no pre-existing consumers. Helper must scan,
	// find nothing, and let subscribePullConsumer proceed.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	cc := w.subscribePullConsumer("foo", "",
		func(ctx TaskContext) error { return nil })
	t.Cleanup(cc.Stop)

	// Positive assertions: durable was created cleanly, filter is
	// the one we expected (the helper didn't trip on a phantom).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-foo")
	if err != nil {
		t.Fatalf("workers-foo not created: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.FilterSubject != "task.foo.>" {
		t.Errorf("FilterSubject = %q, want task.foo.>", info.Config.FilterSubject)
	}
	if info.Config.Durable != "workers-foo" {
		t.Errorf("Durable = %q, want workers-foo", info.Config.Durable)
	}
}
