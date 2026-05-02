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
	"fmt"
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
	// drive subscribePullConsumer directly to isolate the helper from the
	// Start() wiring, assert the orphan is deleted, the migration INFO log
	// fires with all five expected fields, the durable is created, and a
	// published message round-trips.
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
	// Don't call Handle/Start. Drive helper directly so we isolate
	// subscribePullConsumer from the Start() wiring under test elsewhere.

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

func TestMigration_PaginationManyConsumers(t *testing.T) {
	// Methodology: pre-seed 300 consumers on TASK_QUEUES — well past the
	// SDK's typical 256-entry first-page boundary. One of them is the
	// orphan ephemeral matching task.render.>, placed at index 250. Drive
	// subscribePullConsumer directly. Asserts the iterator form (not the
	// single-page list) finds and deletes the orphan regardless of position.
	if testing.Short() {
		t.Skip("skipping 300-consumer pagination test in -short mode")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Seed 300 unrelated durables with distinct filter subjects so they
	// don't match the cleanup rule. The orphan goes in at index 250.
	for i := 0; i < 300; i++ {
		var cfg jetstream.ConsumerConfig
		if i == 250 {
			cfg = jetstream.ConsumerConfig{
				FilterSubject: "task.render.>",
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			}
		} else {
			cfg = jetstream.ConsumerConfig{
				Durable:       fmt.Sprintf("filler-%03d", i),
				Name:          fmt.Sprintf("filler-%03d", i),
				FilterSubject: fmt.Sprintf("task.filler%03d.>", i),
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			}
		}
		if _, err := stream.CreateOrUpdateConsumer(ctx, cfg); err != nil {
			t.Fatalf("seed consumer %d: %v", i, err)
		}
	}

	w := NewWorker(nc)
	cc := w.subscribePullConsumer("render", "",
		func(ctx TaskContext) error { return nil })
	t.Cleanup(cc.Stop)

	// Durable workers-render must exist; orphan must be gone.
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}

	// Re-scan: there must be no remaining ephemeral with task.render.> filter.
	iter := stream.ListConsumers(ctx)
	for ci := range iter.Info() {
		if ci.Config.FilterSubject == "task.render.>" &&
			ci.Config.Durable == "" {
			t.Fatalf("orphan ephemeral still present: %s", ci.Name)
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
}

func TestMigration_NoOrphan(t *testing.T) {
	// Methodology: fresh stream, no pre-seeded orphan. subscribePullConsumer
	// creates the durable cleanly and emits no migration log. Round-trips
	// a message through the durable to confirm end-to-end happy path.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var processed atomic.Int32
	w := NewWorker(nc)
	logs := captureLogs(t, func() {
		cc := w.subscribePullConsumer("render", "",
			func(ctx TaskContext) error {
				processed.Add(1)
				return ctx.Complete([]byte(`"ok"`))
			})
		t.Cleanup(cc.Stop)
	})

	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			t.Fatalf("unexpected migration log: %s", l)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}

	payload := protocol.TaskPayload{
		RunID: "run-baseline", StepID: "s1",
		Input: json.RawMessage(`"hi"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.run-baseline", data); err != nil {
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
}

func TestTwoWorkers_SameTaskType_NoPanic(t *testing.T) {
	// Methodology: two Workers handling render, both Start() against the
	// same stream. Original repro from #136: WorkQueuePolicy refuses two
	// consumers with the same FilterSubject; pre-fix this panics with NATS
	// error 10100. Post-fix both share the durable workers-render.
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error { return nil })
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error { return nil })

	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start() // must not panic
	t.Cleanup(w2.Stop)

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
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("workers-render not present: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}
}

func TestTwoWorkers_LoadBalance(t *testing.T) {
	// Methodology: two workers, 10 messages, NATS-managed load balance via
	// the shared durable. Each worker tracks how many messages it processed;
	// the sum must be 10 and each must process at least one (otherwise
	// "load-balance" is a misnomer).
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	var w1Count, w2Count atomic.Int32
	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error {
		w1Count.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error {
		w2Count.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start()
	t.Cleanup(w2.Stop)

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		payload := protocol.TaskPayload{
			RunID:  fmt.Sprintf("run-%d", i),
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		if _, err := js.Publish(ctx,
			fmt.Sprintf("task.render.run-%d", i), data); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	deadline := time.After(15 * time.Second)
	for w1Count.Load()+w2Count.Load() < 10 {
		select {
		case <-deadline:
			t.Fatalf("only %d/10 processed after 15s (w1=%d w2=%d)",
				w1Count.Load()+w2Count.Load(), w1Count.Load(), w2Count.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if w1Count.Load() == 0 {
		t.Errorf("w1 processed 0 — no load balance happened (w2=%d)", w2Count.Load())
	}
	if w2Count.Load() == 0 {
		t.Errorf("w2 processed 0 — no load balance happened (w1=%d)", w1Count.Load())
	}
	if total := w1Count.Load() + w2Count.Load(); total != 10 {
		t.Errorf("total processed = %d, want 10", total)
	}
}

func TestTwoWorkers_KillOne_OtherDrains(t *testing.T) {
	// Methodology: two workers, kill one mid-processing, remaining worker
	// drains the queue. Bounded timeout = 30s + AckWait so a redelivery
	// after the killed worker's ackWait expiry can succeed.
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	var processed atomic.Int32
	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start()
	t.Cleanup(w2.Stop)

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(),
		defaultAckWait+30*time.Second)
	defer cancel()

	// Publish 5 messages, then kill w1.
	for i := 0; i < 5; i++ {
		payload := protocol.TaskPayload{
			RunID:  fmt.Sprintf("kill-%d", i),
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		if _, err := js.Publish(ctx,
			fmt.Sprintf("task.render.kill-%d", i), data); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	w1.Stop()

	deadline := time.After(defaultAckWait + 30*time.Second)
	for processed.Load() < 5 {
		select {
		case <-deadline:
			t.Fatalf("only %d/5 processed before timeout", processed.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestWorkerStart_DurableIdempotent(t *testing.T) {
	// Methodology: Start, Stop, Start again on the same Worker. Both Start
	// calls succeed; the durable persists across Stop/Start; a message
	// published between phases delivers after the second Start.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var processed atomic.Int32
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("after first Start, workers-render missing: %v", err)
	}
	w.Stop()
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("after Stop, durable should persist: %v", err)
	}

	// Publish while stopped.
	payload := protocol.TaskPayload{
		RunID:  "between",
		StepID: "s",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.between", data); err != nil {
		t.Fatalf("Publish between phases: %v", err)
	}

	// Restart: same Worker instance Start() again.
	w2 := NewWorker(nc)
	w2.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2.Start()
	t.Cleanup(w2.Stop)

	deadline := time.After(5 * time.Second)
	for processed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("queued message not processed after restart")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestWorkerStart_NewProcessReclaimsDurable(t *testing.T) {
	// Methodology: first Worker starts, registers durable, processes a
	// message, exits without unbinding. Second Worker (separate instance,
	// same handlers) starts against the same stream — no panic, durable
	// resumes, in-flight message redelivers within AckWait if first worker
	// died holding it. We use a handler that errors once to force NAK and
	// redelivery; restart between attempts means the second worker handles
	// the redelivered message.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w1 := NewWorker(nc)
	var w1Calls atomic.Int32
	w1.Handle("render", func(ctx TaskContext) error {
		w1Calls.Add(1)
		return fmt.Errorf("force redelivery")
	})
	w1.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload := protocol.TaskPayload{
		RunID:  "reclaim",
		StepID: "s",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.reclaim", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for w1Calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("w1 didn't process initial message")
		case <-time.After(50 * time.Millisecond):
		}
	}
	w1.Stop()

	var w2Calls atomic.Int32
	w2 := NewWorker(nc)
	w2.Handle("render", func(ctx TaskContext) error {
		w2Calls.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2.Start()
	t.Cleanup(w2.Stop)

	deadline = time.After(30 * time.Second)
	for w2Calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("w2 didn't pick up redelivered message")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestRealisticTaskNames_AllSanitizationPaths(t *testing.T) {
	// Methodology: register two task types covering the end-to-end-viable
	// sanitization branches — identity (nasr-ingest) and dot-collapse
	// (render.gpu). Start the worker, publish one message per type, assert
	// each is processed by the correct handler. Verify the durable names
	// match expected by reading back the stream's consumers.
	//
	// The safe-escape branch (vendor::ingest → vendor__ingest) is NOT
	// exercised end-to-end: any character that triggers safe-escape also
	// makes the resulting filter subject invalid in NATS. That branch is
	// covered by the unit test in TestSanitizeConsumerName.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var counts sync.Map
	w := NewWorker(nc)
	for _, tt := range []string{"nasr-ingest", "render.gpu"} {
		tt := tt
		w.Handle(tt, func(ctx TaskContext) error {
			v, _ := counts.LoadOrStore(tt, new(atomic.Int32))
			v.(*atomic.Int32).Add(1)
			return ctx.Complete([]byte(`"ok"`))
		})
	}
	w.Start()
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for tt, want := range map[string]string{
		"nasr-ingest": "workers-nasr-ingest",
		"render.gpu":  "workers-render-gpu",
	} {
		cons, err := stream.Consumer(ctx, want)
		if err != nil {
			t.Errorf("expected durable %q for task %q: %v", want, tt, err)
			continue
		}
		info, err := cons.Info(ctx)
		if err != nil {
			t.Errorf("Info(%q): %v", want, err)
			continue
		}
		if info.Config.Durable != want {
			t.Errorf("Durable for %q = %q, want %q",
				tt, info.Config.Durable, want)
		}
	}

	for _, tt := range []string{"nasr-ingest", "render.gpu"} {
		payload := protocol.TaskPayload{
			RunID:  "san-" + tt,
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		// Subjects allow dots (separator), so render.gpu publishes on
		// "task.render.gpu.san" — this is the publisher's contract, the
		// filter "task.render.gpu.>" matches.
		subj := "task." + tt + ".san"
		if _, err := js.Publish(ctx, subj, data); err != nil {
			t.Fatalf("Publish %s: %v", tt, err)
		}
	}

	deadline := time.After(10 * time.Second)
	for {
		all := true
		for _, tt := range []string{"nasr-ingest", "render.gpu"} {
			v, ok := counts.Load(tt)
			if !ok || v.(*atomic.Int32).Load() == 0 {
				all = false
				break
			}
		}
		if all {
			break
		}
		select {
		case <-deadline:
			t.Fatal("not all task types processed within 10s")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestMigration_ListFailure_Panics(t *testing.T) {
	// Methodology: start Worker against a NATS server, shut the server
	// down before Start runs, expect Start to panic with a message naming
	// the cleanup operation. If injecting list-failure proves > 1 hour of
	// effort, defer per ADR-006 §6.3 — file follow-up issue and add a
	// TODO referencing it.
	ns, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	ns.Shutdown()
	ns.WaitForShutdown()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on shut-down NATS, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "Stream") &&
			!strings.Contains(msg, "cleanupOrphanEphemerals") &&
			!strings.Contains(msg, "iterator") {
			t.Fatalf("expected cleanup-related panic, got: %s", msg)
		}
	}()
	w.Start()
}

func TestMigration_DeleteFailure_Panics(t *testing.T) {
	// Methodology: same family — if delete failures (non-NotFound) are
	// painful to inject under embedded NATS, this test stays a placeholder
	// and is filed as a follow-up issue. The minimal viable shape: pre-seed
	// an orphan, force a delete error, expect panic. Without an SDK seam
	// for "make DeleteConsumer fail with non-NotFound," shutting the
	// server mid-cleanup is the cheapest reproduction.
	ns, nc := natsutil.StartTestServer(t)
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
	if _, err := stream.CreateOrUpdateConsumer(ctx,
		jetstream.ConsumerConfig{
			FilterSubject: "task.render.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })

	// Shut down NATS just before Start to fail the delete (or list).
	ns.Shutdown()
	ns.WaitForShutdown()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	w.Start()
}
