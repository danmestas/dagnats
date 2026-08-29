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
	"testing"
	"time"

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
