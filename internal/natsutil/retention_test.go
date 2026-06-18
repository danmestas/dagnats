// natsutil/retention_test.go
// Tests for JetStream stream retention bounds (issue #441): the history
// streams are the real storage grower. Each test starts an embedded NATS
// server, provisions the streams, and asserts the MaxAge durability bound
// (max_age is the retention lever) — plus the negative space that
// TASK_QUEUES and SLEEP_TIMERS carry NO age bound (aging them would drop
// live un-acked tasks / un-fired timers).
// Bounded 5-second timeout on all operations.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// streamConfigByName provisions the core + trigger-history streams and
// returns the live config for the named stream.
func streamConfigByName(
	t *testing.T, name string,
) jetstream.StreamConfig {
	t.Helper()
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := SetupStreams(js, 1); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}
	if err := SetupTriggerHistoryStream(js); err != nil {
		t.Fatalf("SetupTriggerHistoryStream: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("Stream(%s): %v", name, err)
	}
	return stream.CachedInfo().Config
}

func TestWorkflowHistoryBounds(t *testing.T) {
	cfg := streamConfigByName(t, "WORKFLOW_HISTORY")

	// Positive: 30-day age window is the retention bound.
	if cfg.MaxAge != historyMaxAge {
		t.Fatalf("MaxAge = %v, want %v", cfg.MaxAge, historyMaxAge)
	}
	if cfg.MaxAge != 30*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 30d", cfg.MaxAge)
	}
}

func TestEventsBounds(t *testing.T) {
	cfg := streamConfigByName(t, "EVENTS")

	if cfg.MaxAge != eventsMaxAge {
		t.Fatalf("MaxAge = %v, want %v", cfg.MaxAge, eventsMaxAge)
	}
	if cfg.MaxAge != 14*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 14d", cfg.MaxAge)
	}
}

func TestDeadLettersBounds(t *testing.T) {
	cfg := streamConfigByName(t, "DEAD_LETTERS")

	if cfg.MaxAge != deadLettersMaxAge {
		t.Fatalf("MaxAge = %v, want %v", cfg.MaxAge, deadLettersMaxAge)
	}
	if cfg.MaxAge != 30*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 30d", cfg.MaxAge)
	}
}

func TestTriggerHistoryBounds(t *testing.T) {
	cfg := streamConfigByName(t, "TRIGGER_HISTORY")

	// Positive: existing 30-day MaxAge + DiscardOld preserved.
	if cfg.MaxAge != 30*24*time.Hour {
		t.Fatalf("MaxAge = %v, want 30d (unchanged)", cfg.MaxAge)
	}
	if cfg.Discard != jetstream.DiscardOld {
		t.Fatalf("Discard = %v, want DiscardOld (unchanged)", cfg.Discard)
	}
}

// TestTaskQueuesNoAgeBound is the hard negative-space guard: a MaxAge on a
// work-queue stream silently deletes un-acked (live) tasks. The age bound is
// forbidden.
func TestTaskQueuesNoAgeBound(t *testing.T) {
	cfg := streamConfigByName(t, "TASK_QUEUES")

	if cfg.MaxAge != 0 {
		t.Fatalf("TASK_QUEUES MaxAge = %v, want 0 (un-acked work is live)",
			cfg.MaxAge)
	}
}

// TestSleepTimersNoAgeBound guards the other forbidden age bound: SLEEP_TIMERS
// holds pending future sleep messages; aging would drop un-fired timers.
func TestSleepTimersNoAgeBound(t *testing.T) {
	cfg := streamConfigByName(t, "SLEEP_TIMERS")

	if cfg.MaxAge != 0 {
		t.Fatalf("SLEEP_TIMERS MaxAge = %v, want 0 (pending timers are live)",
			cfg.MaxAge)
	}
}
