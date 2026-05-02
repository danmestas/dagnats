// worker/withackwait_test.go
// Methodology: real embedded NATS per test, register handlers via the public
// Worker.Handle API with WithAckWait HandlerOption, drive Start(), then read
// back each consumer's Info().Config.AckWait via stream.Consumer(name).Info()
// to verify the override (or lack thereof) made it onto the JetStream
// consumer config. Bounded 10s timeouts. Tests cover: positive override,
// no-option default fall-through (with negative-space check that the override
// map stays sparse), and panic guards on non-positive durations.
package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWithAckWait_AppliesPerTaskOverride(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("task-fast", func(ctx TaskContext) error { return nil },
		WithAckWait(2*time.Second))
	w.Handle("task-slow", func(ctx TaskContext) error { return nil },
		WithAckWait(10*time.Minute))
	w.Handle("task-default", func(ctx TaskContext) error { return nil })
	w.Start()
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	cases := []struct {
		durable string
		want    time.Duration
	}{
		{"workers-task-fast", 2 * time.Second},
		{"workers-task-slow", 10 * time.Minute},
		{"workers-task-default", defaultAckWait},
	}
	for _, c := range cases {
		cons, err := stream.Consumer(ctx, c.durable)
		if err != nil {
			t.Fatalf("Consumer(%q): %v", c.durable, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatalf("Info(%q): %v", c.durable, err)
		}
		if info.Config.AckWait != c.want {
			t.Errorf("AckWait for %q = %v, want %v",
				c.durable, info.Config.AckWait, c.want)
		}
	}
}

func TestWithAckWait_NoOptionLeavesOverrideMapSparse(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("task-only", func(ctx TaskContext) error { return nil })
	w.Start()
	t.Cleanup(w.Stop)

	// Negative space: the per-task override map must not be populated
	// for handlers registered without WithAckWait. Sparse-by-default
	// keeps coalesceAckWait's lookup honest.
	if _, found := w.handlerAckWait["task-only"]; found {
		t.Errorf(
			"handlerAckWait must not contain task-only without WithAckWait;"+
				" got %v",
			w.handlerAckWait,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-task-only")
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.AckWait != defaultAckWait {
		t.Errorf("AckWait = %v, want defaultAckWait %v",
			info.Config.AckWait, defaultAckWait)
	}
}

func TestWithAckWait_RejectsNonPositiveDuration(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
	}{
		{"zero", 0},
		{"negative", -1 * time.Second},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("expected panic for d=%v, got none", c.d)
				}
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "WithAckWait") {
					t.Fatalf(
						"expected panic mentioning WithAckWait, got %#v",
						r,
					)
				}
			}()
			_ = WithAckWait(c.d)
		})
	}
}
