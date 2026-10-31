// worker/sticky_consumer_test.go
// Methodology: pure checks of the sticky consumer's config, plus one
// real embedded server confirming a started Worker's sticky consumer is
// durable with an inactivity reaper. Surviving a node restart needs a
// cluster and lives in dagnatstest/sticky_restart_test.go.
package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStickyConsumerConfig_DurableReplicatedUnguessable(t *testing.T) {
	stream := &jetstream.StreamInfo{Config: jetstream.StreamConfig{Replicas: 3}}
	first := stickyConsumerConfig("toy.echo", "worker-ab", stream)
	second := stickyConsumerConfig("toy.echo", "worker-ab", stream)

	if !strings.HasPrefix(first.Durable, "sticky-") || strings.ContainsAny(first.Durable, ".*> ") {
		t.Fatalf("durable %q is not a legal sticky consumer name", first.Durable)
	}
	if first.Replicas != 3 {
		t.Fatalf("replicas = %d, want the stream's 3", first.Replicas)
	}
	if first.InactiveThreshold != stickyInactiveThreshold || stickyInactiveThreshold <= 0 {
		t.Fatalf("inactive threshold = %v, want %v", first.InactiveThreshold,
			stickyInactiveThreshold)
	}
	if first.FilterSubject != "sticky.toy.echo.worker-ab.>" {
		t.Fatalf("filter = %q", first.FilterSubject)
	}
	// Negative space: the name does not carry the worker ID, and two
	// consumers never share a name.
	if strings.Contains(first.Durable, "worker-ab") || first.Durable == second.Durable {
		t.Fatalf("durable names %q and %q are guessable", first.Durable, second.Durable)
	}
}

func TestStickyConsumer_StartedWorkerHasDurable(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	w := NewWorker(nc)
	w.Handle("probe.sticky", func(tc TaskContext) error { return nil })
	w.Start()
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "STICKY_TASKS")
	if err != nil {
		t.Fatalf("STICKY_TASKS: %v", err)
	}
	lister := stream.ListConsumers(ctx)
	var found []*jetstream.ConsumerInfo
	for info := range lister.Info() {
		found = append(found, info)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("STICKY_TASKS has %d consumers, want 1", len(found))
	}
	if found[0].Config.Durable == "" || found[0].Config.InactiveThreshold != stickyInactiveThreshold {
		t.Fatalf("sticky consumer not durable with a reaper: %+v", found[0].Config)
	}
}
