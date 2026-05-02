// worker/consumer_subscribe_test.go
// Integration tests for subscribePullConsumer and the surrounding wiring.
// Methodology: real embedded NATS server per test, drive the helper through
// the Worker public API, read back ConsumerInfo from the stream to verify
// owned config, exercise restart/migration/scale-out paths end-to-end.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSubscribePullConsumer_AppliesExpectedConfig(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	cc := w.subscribePullConsumer("render", "",
		func(ctx TaskContext) error { return nil })
	defer cc.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("Consumer(workers-render): %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	if info.Config.Durable != "workers-render" {
		t.Errorf("Durable = %q, want %q", info.Config.Durable, "workers-render")
	}
	if info.Config.Name != "workers-render" {
		t.Errorf("Name = %q, want %q", info.Config.Name, "workers-render")
	}
	if info.Config.FilterSubject != "task.render.>" {
		t.Errorf("FilterSubject = %q, want %q",
			info.Config.FilterSubject, "task.render.>")
	}
	if info.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want AckExplicitPolicy", info.Config.AckPolicy)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want DeliverAllPolicy", info.Config.DeliverPolicy)
	}
	if info.Config.AckWait != defaultAckWait {
		t.Errorf("AckWait = %v, want %v", info.Config.AckWait, defaultAckWait)
	}
	if info.Config.MaxDeliver != -1 {
		t.Errorf("MaxDeliver = %d, want -1", info.Config.MaxDeliver)
	}
}

func TestSubscribePullConsumer_RejectsEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "taskType") {
			t.Fatalf("expected panic mentioning taskType, got %#v", r)
		}
	}()
	w.subscribePullConsumer("", "",
		func(ctx TaskContext) error { return nil })
}

// captureLogs swaps the default slog handler with a capturing one for the
// duration of fn. Returns every log line written, in order.
func captureLogs(t *testing.T, fn func()) []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string

	prior := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prior) })

	captured := slog.New(slog.NewTextHandler(
		&logCapture{mu: &mu, lines: &lines},
		nil,
	))
	slog.SetDefault(captured)
	fn()
	return lines
}

type logCapture struct {
	mu    *sync.Mutex
	lines *[]string
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.lines = append(*l.lines, string(p))
	return len(p), nil
}

func TestMigration_OrphanEphemeralRemoved(t *testing.T) {
	// Methodology: pre-seed an ephemeral consumer matching task.render.>,
	// drive subscribePullConsumer directly (bypassing Start which still
	// uses the legacy createConsumer path until Task 12), assert the orphan
	// is deleted, the migration INFO log fires with all five expected
	// fields, the durable is created, and a published message round-trips.
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
	orphan, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	orphanInfo, err := orphan.Info(ctx)
	if err != nil {
		t.Fatalf("orphan.Info: %v", err)
	}
	if orphanInfo.Config.Durable != "" {
		t.Fatalf("seeded consumer must be ephemeral, Durable=%q",
			orphanInfo.Config.Durable)
	}
	orphanName := orphanInfo.Name

	var processed atomic.Int32
	w := NewWorker(nc)
	// Don't call Handle/Start. Drive helper directly so we exercise the
	// new path; Start still routes through the legacy createConsumer until
	// Task 12 wires this in.

	logs := captureLogs(t, func() {
		cc := w.subscribePullConsumer("render", "",
			func(ctx TaskContext) error {
				processed.Add(1)
				return ctx.Complete([]byte(`"ok"`))
			})
		t.Cleanup(cc.Stop)
	})

	// Orphan deleted
	_, err = stream.Consumer(ctx, orphanName)
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("orphan %q still exists or unexpected error: %v",
			orphanName, err)
	}

	// Durable created
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("durable workers-render not created: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}

	// Migration INFO log emitted with all five fields.
	var migrationLog string
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			migrationLog = l
			break
		}
	}
	if migrationLog == "" {
		t.Fatalf("migration log not emitted; logs: %v", logs)
	}
	for _, want := range []string{
		"consumer_name=" + orphanName,
		"filter_subject=task.render.>",
		"stream=TASK_QUEUES",
		"durable_being_claimed=workers-render",
		`reason="ephemeral with matching filter; pre-fix dagnats orphan"`,
	} {
		if !strings.Contains(migrationLog, want) {
			t.Errorf("migration log missing %q; got: %s", want, migrationLog)
		}
	}

	// Round-trip a message through the durable.
	payload := protocol.TaskPayload{
		RunID: "run-mig", StepID: "s1",
		Input: json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.run-mig", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for processed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if processed.Load() != 1 {
		t.Errorf("processed = %d, want 1", processed.Load())
	}
}

func TestMigration_PreservesManagedConsumer(t *testing.T) {
	// Methodology: pre-seed a durable named workers-render with the same
	// filter we'd claim. subscribePullConsumer must not delete it; the durable
	// count on the stream stays 1 (CreateOrUpdate is idempotent) and no
	// migration log fires.
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
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-render",
		Name:          "workers-render",
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed managed durable: %v", err)
	}

	w := NewWorker(nc)
	logs := captureLogs(t, func() {
		cc := w.subscribePullConsumer("render", "",
			func(ctx TaskContext) error { return nil })
		t.Cleanup(cc.Stop)
	})

	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("Consumer(workers-render): %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Errorf("Durable lost: %q", info.Config.Durable)
	}
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			t.Fatalf("must not emit migration log for managed durable; got: %s", l)
		}
	}
}

func TestMigration_PreservesUnrelatedConsumer(t *testing.T) {
	// Methodology: pre-seed an unrelated durable (audit-tap on task.audit.>)
	// on the same stream, drive subscribePullConsumer for render, assert the
	// unrelated consumer is untouched and no migration log fires.
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
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "audit-tap",
		Name:          "audit-tap",
		FilterSubject: "task.audit.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed unrelated: %v", err)
	}

	w := NewWorker(nc)
	logs := captureLogs(t, func() {
		cc := w.subscribePullConsumer("render", "",
			func(ctx TaskContext) error { return nil })
		t.Cleanup(cc.Stop)
	})

	if _, err := stream.Consumer(ctx, "audit-tap"); err != nil {
		t.Fatalf("audit-tap was deleted or unreachable: %v", err)
	}
	for _, l := range logs {
		if strings.Contains(l, "audit-tap") &&
			strings.Contains(l, "removing orphan") {
			t.Fatalf("must not log migration for unrelated consumer; got: %s", l)
		}
	}
}

func TestMigration_ConcurrentStartup_OneOrphan(t *testing.T) {
	// Methodology: pre-seed one orphan ephemeral. Two workers race to
	// delete it via subscribePullConsumer; both must succeed without
	// panic, both bind to the same durable, the orphan must be deleted
	// exactly once, and the migration log fires exactly once (only the
	// winning worker logs; the loser swallows ErrConsumerNotFound).
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	orphan, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	orphanInfo, err := orphan.Info(ctx)
	if err != nil {
		t.Fatalf("orphan.Info: %v", err)
	}

	logs := captureLogs(t, func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			w := NewWorker(nc1)
			cc := w.subscribePullConsumer("render", "",
				func(ctx TaskContext) error { return nil })
			t.Cleanup(cc.Stop)
		}()
		go func() {
			defer wg.Done()
			w := NewWorker(nc2)
			cc := w.subscribePullConsumer("render", "",
				func(ctx TaskContext) error { return nil })
			t.Cleanup(cc.Stop)
		}()
		wg.Wait()
	})

	// Durable exists.
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}
	// Orphan gone.
	if _, err := stream.Consumer(ctx, orphanInfo.Name); !errors.Is(err,
		jetstream.ErrConsumerNotFound) {
		t.Fatalf("orphan still present or unexpected error: %v", err)
	}
	// Migration log fired exactly once across both workers.
	count := 0
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("migration log fired %d times, want 1", count)
	}
}
