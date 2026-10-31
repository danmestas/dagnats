// natsutil/ordered_consumer_test.go
// Tests for OrderedConsumerConfig, the single place any ordered
// consumer's reset bound is applied repo-wide, and its Open* callers.
// Methodology: OrderedConsumerConfig is a pure unit test (config-struct
// math, no NATS). OpenConsumer/OpenStreamConsumer's actual bounded
// behavior against a deleted stream is exercised by the deleted-stream
// integration tests in cli/ and internal/observe/spanread — this file
// only pins the config assembly they all share.
// Positive: the bound is set to OrderedConsumerResetMax and the rest
// of the spec passes through untouched.
// Negative: a spec with both start-position fields set panics rather
// than silently picking one.
package natsutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrderedConsumerConfig_SetsBound(t *testing.T) {
	spec := OrderedConsumerSpec{
		FilterSubjects: []string{"logs.run1.step1.1.1"},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	}

	out := OrderedConsumerConfig(spec)

	// Positive: the bound is applied.
	if out.MaxResetAttempts != OrderedConsumerResetMax {
		t.Fatalf("MaxResetAttempts: got %d, want %d",
			out.MaxResetAttempts, OrderedConsumerResetMax)
	}
	// Negative: the rest of the caller's spec must survive untouched.
	if out.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Fatalf("DeliverPolicy: got %v, want DeliverAllPolicy", out.DeliverPolicy)
	}
	if len(out.FilterSubjects) != 1 || out.FilterSubjects[0] != "logs.run1.step1.1.1" {
		t.Fatalf("FilterSubjects: got %v", out.FilterSubjects)
	}
}

func TestOrderedConsumerConfig_StartTimePassesThrough(t *testing.T) {
	startTime := time.Now().Add(-time.Hour)
	out := OrderedConsumerConfig(OrderedConsumerSpec{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &startTime,
	})

	// Positive: bound still applied alongside the caller's start time.
	if out.MaxResetAttempts != OrderedConsumerResetMax {
		t.Fatalf("MaxResetAttempts: got %d", out.MaxResetAttempts)
	}
	// Negative: OptStartTime must not be dropped or replaced.
	if out.OptStartTime == nil || !out.OptStartTime.Equal(startTime) {
		t.Fatalf("OptStartTime: got %v, want %v", out.OptStartTime, startTime)
	}
}

func TestOrderedConsumerConfig_PanicsOnConflictingStartFields(t *testing.T) {
	defer func() {
		// Positive: a spec naming both start positions must panic —
		// jetstream would silently prefer one, hiding a caller bug.
		if recover() == nil {
			t.Fatal("expected panic on conflicting OptStartSeq/OptStartTime")
		}
	}()

	startTime := time.Now()
	OrderedConsumerConfig(OrderedConsumerSpec{
		OptStartSeq:  5,
		OptStartTime: &startTime,
	})

	// Negative: unreachable if the panic fired as expected.
	t.Fatal("OrderedConsumerConfig did not panic")
}

// Close tests (integration, embedded server): Close removes the
// server-side consumer, whether or not a fetch ever ran, and a second
// Close or a consumer the server already dropped is not an error.
func TestOrderedConsumer_CloseDeletesAndIsIdempotent(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := OrderedConsumerSpec{FilterSubjects: []string{"telemetry.logs.>"}}
	lazy, err := OpenConsumer(ctx, js, "TELEMETRY", spec)
	if err != nil {
		t.Fatalf("OpenConsumer: %v", err)
	}
	lazy.Close() // never fetched: the consumer created at open goes too
	if n := telemetryConsumerCount(t, js); n != 0 {
		t.Fatalf("after Close with no fetch: %d consumers, want 0", n)
	}
	for open := 0; open < 3; open++ {
		cons, err := OpenConsumer(ctx, js, "TELEMETRY", spec)
		if err != nil {
			t.Fatalf("OpenConsumer: %v", err)
		}
		if _, err := cons.Next(jetstream.FetchMaxWait(200 * time.Millisecond)); err == nil {
			t.Fatal("Next on an empty subject returned a message")
		}
		if n := telemetryConsumerCount(t, js); n != 1 {
			t.Fatalf("while open: %d consumers, want 1", n)
		}
		cons.Close()
		cons.Close() // idempotent
		if n := telemetryConsumerCount(t, js); n != 0 {
			t.Fatalf("after Close: %d consumers, want 0", n)
		}
	}
	gone, err := OpenConsumer(ctx, js, "TELEMETRY", spec)
	if err != nil {
		t.Fatalf("OpenConsumer: %v", err)
	}
	_, _ = gone.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
	if err := js.DeleteConsumer(ctx, "TELEMETRY", gone.CachedInfo().Name); err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	gone.Close() // already deleted server-side: tolerated
}

func telemetryConsumerCount(t *testing.T, js jetstream.JetStream) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TELEMETRY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	return info.State.Consumers
}

// A fetch on an ordered consumer replaces the server-side consumer, and
// nats.go deletes the replaced one from a goroutine nobody waits on, so
// under load it outlives the read (#827's flake) or, when the process
// exits first, the read itself. The wrapper must delete it before the
// fetch returns. The remover is recorded, so the check does not depend
// on which delete wins the race.
func TestOrderedConsumer_FetchDeletesTheConsumerItReplaced(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cons, err := OpenConsumer(ctx, js, "TELEMETRY",
		OrderedConsumerSpec{FilterSubjects: []string{"telemetry.logs.>"}})
	if err != nil {
		t.Fatalf("OpenConsumer: %v", err)
	}
	var removed []string
	remove := cons.remove
	cons.remove = func(ctx context.Context, name string) error {
		removed = append(removed, name)
		return remove(ctx, name)
	}
	drain := func(batch jetstream.MessageBatch, err error) error {
		if err != nil {
			return err
		}
		for range batch.Messages() {
		}
		return nil
	}
	wait := jetstream.FetchMaxWait(100 * time.Millisecond)
	fetches := []func() error{
		func() error { return drain(cons.Fetch(1, wait)) },
		func() error { return drain(cons.FetchNoWait(1)) },
		func() error { return drain(cons.FetchBytes(1024, wait)) },
		func() error {
			_, err := cons.Next(wait)
			if errors.Is(err, nats.ErrTimeout) {
				return nil
			}
			return err
		},
	}
	for i, fetch := range fetches {
		previous := cons.CachedInfo().Name
		if err := fetch(); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		if len(removed) != i+1 || removed[i] != previous {
			t.Fatalf("fetch %d: removed %v, want the replaced %s last", i, removed, previous)
		}
		if cons.CachedInfo().Name == previous {
			t.Fatalf("fetch %d: consumer %s was not replaced", i, previous)
		}
	}
	cons.Close()
	if n := telemetryConsumerCount(t, js); n != 0 {
		t.Fatalf("after Close: %d consumers, want 0", n)
	}
}

// When nats.go's delete of a replaced consumer races ours, nats-server
// answers one of them with success while the other is still removing
// it. deleteConsumer must not return until a lookup says it is gone.
// Pure unit test: remove and lookup are fakes.
func TestDeleteConsumer_WaitsUntilTheConsumerIsGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lookups := 0
	remove := func(context.Context, string) error { return nil }
	lookup := func(context.Context, string) error {
		lookups++
		if lookups < 3 {
			return nil // closed but still counted by the stream
		}
		return jetstream.ErrConsumerNotFound
	}
	if err := deleteConsumer(ctx, "c_1", remove, lookup); err != nil {
		t.Fatalf("deleteConsumer: %v", err)
	}
	if lookups != 3 {
		t.Fatalf("lookups = %d, want 3: returned before the consumer was gone", lookups)
	}
	boom := errors.New("boom")
	failing := func(context.Context, string) error { return boom }
	if err := deleteConsumer(ctx, "c_1", failing, lookup); !errors.Is(err, boom) {
		t.Fatalf("deleteConsumer with a failing delete = %v, want boom", err)
	}
}
