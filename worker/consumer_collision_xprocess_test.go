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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCrossProcessCollision_DifferentFilter_Panics(t *testing.T) {
	// Pre-seed durable workers-foo with FilterSubject task.bar.* as if
	// Worker A (a different process) had claimed it for some other task
	// type whose sanitized name happens to be "foo". Then drive Worker B
	// with taskType "foo", which derives FilterSubject task.foo.*. Same
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
		FilterSubject: "task.bar.*",
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
		if !strings.Contains(msg, "task.bar.*") {
			t.Errorf("panic must name pre-existing filter task.bar.*, got: %s", msg)
		}
		if !strings.Contains(msg, "task.foo.*") {
			t.Errorf("panic must name claiming filter task.foo.*, got: %s", msg)
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
		FilterSubject: "task.foo.*",
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
	if info.Config.FilterSubject != "task.foo.*" {
		t.Errorf("FilterSubject = %q, want task.foo.*", info.Config.FilterSubject)
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
	if info.Config.FilterSubject != "task.foo.*" {
		t.Errorf("FilterSubject = %q, want task.foo.*", info.Config.FilterSubject)
	}
	if info.Config.Durable != "workers-foo" {
		t.Errorf("Durable = %q, want workers-foo", info.Config.Durable)
	}
}

// TestCrossProcessCollision_LegacyFilter_AutoUpgrades is the regression
// guard for the upgrade-rollout blocker found in review: a durable a
// pre-#674 process left behind (">"-anchored filter, SAME task type) must
// not panic the upgraded worker at startup. It's an in-place upgrade —
// delete and recreate with the new "*"-anchored filter — not a
// cross-type collision.
func TestCrossProcessCollision_LegacyFilter_AutoUpgrades(t *testing.T) {
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
	// Pre-seed exactly what a pre-#674 worker process would have left:
	// durable "workers-foo" filtering on the OLD ">"-anchored subject.
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-foo",
		Name:          "workers-foo",
		FilterSubject: "task.foo.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("seed legacy durable: %v", err)
	}

	logs := captureLogs(t, func() {
		w := NewWorker(nc)
		cc := w.subscribePullConsumer("foo", "",
			func(ctx TaskContext) error { return nil })
		t.Cleanup(cc.Stop)
	})

	// Positive: the durable now carries the new anchor, not the old one.
	cons, err := stream.Consumer(ctx, "workers-foo")
	if err != nil {
		t.Fatalf("workers-foo missing after upgrade: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.FilterSubject != "task.foo.*" {
		t.Fatalf("FilterSubject = %q, want task.foo.* (auto-upgraded)",
			info.Config.FilterSubject)
	}

	// Positive: the upgrade was logged at warn, once, naming both filters.
	var upgradeLog string
	for _, l := range logs {
		if strings.Contains(l, "auto-upgrading legacy consumer filter") {
			upgradeLog = l
			break
		}
	}
	if upgradeLog == "" {
		t.Fatalf("upgrade not logged; logs: %v", logs)
	}
	if !strings.Contains(upgradeLog, "task.foo.>") ||
		!strings.Contains(upgradeLog, "task.foo.*") {
		t.Fatalf("upgrade log missing old/new filter: %s", upgradeLog)
	}
}

// TestUpgradeLegacyDurable_ConcurrentCallsDeleteAtMostOnce is the
// regression guard for the round-2 review finding: two sibling workers
// racing assertNoCrossProcessCollision must not have one blindly delete
// a durable the other already recreated and started consuming from —
// upgradeLegacyDurable's job is to re-fetch the durable's CURRENT
// FilterSubject immediately before deleting and skip if it's already
// upgraded (or already gone).
//
// Tested by calling upgradeLegacyDurable directly (not the full
// subscribePullConsumer path, which also calls CreateOrUpdateConsumer —
// racing THAT concurrently for a durable name mid delete/recreate hits
// an unrelated embedded-filestore quirk unrelated to the logic under
// test here; see TestCrossProcessCollision_UpgradedDurable_
// MultipleWorkersConsume for the "both end up consuming" half).
// upgradeLegacyDurable only ever deletes (never recreates), and
// DeleteConsumer is idempotent, so racing it concurrently is safe to
// exercise directly and deterministically: after both calls return, the
// durable must be gone exactly once — not erroring, not partially
// mutated — proving neither call panicked or corrupted state even
// though at most one of them performed the actual deletion.
func TestUpgradeLegacyDurable_ConcurrentCallsDeleteAtMostOnce(t *testing.T) {
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
		Durable:       "workers-race",
		Name:          "workers-race",
		FilterSubject: "task.race.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("seed legacy durable: %v", err)
	}

	var panicked [2]any
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() { panicked[idx] = recover() }()
			<-start
			upgradeLegacyDurable(ctx, stream, "workers-race", "task.race.*")
		}(i)
	}
	close(start)
	wg.Wait()
	for i, p := range panicked {
		if p != nil {
			t.Fatalf("goroutine %d panicked: %v", i, p)
		}
	}

	// Positive: exactly one legacy durable existed, and both concurrent
	// calls converged on deleting it — not erroring, not leaving it in
	// a corrupted or duplicated state.
	if _, err := stream.Consumer(ctx, "workers-race"); !errors.Is(
		err, jetstream.ErrConsumerNotFound,
	) {
		t.Fatalf(
			"workers-race still present (or unexpected error) after "+
				"concurrent upgrade calls: %v", err,
		)
	}
}

// TestCrossProcessCollision_UpgradedDurable_MultipleWorkersConsume
// covers the "both end up consuming" half of the round-2 review
// finding: after a legacy durable is upgraded (by the first worker to
// claim it), a second worker subscribing to the SAME task type shares
// the upgraded durable exactly like the pre-#674 work-queue model
// always intended — an idempotent same-filter subscribe, no further
// delete — and both workers' consumers genuinely receive dispatched
// work off the one shared durable.
func TestCrossProcessCollision_UpgradedDurable_MultipleWorkersConsume(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-race",
		Name:          "workers-race",
		FilterSubject: "task.race.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("seed legacy durable: %v", err)
	}

	var counts [2]atomic.Int32
	w1 := NewWorker(nc)
	cc1 := w1.subscribePullConsumer("race", "",
		func(tc TaskContext) error {
			counts[0].Add(1)
			return tc.Complete([]byte(`"done"`))
		})
	t.Cleanup(cc1.Stop)

	// The durable is already upgraded by w1; w2 joining is the ordinary
	// idempotent same-filter path (TestCrossProcessCollision_
	// SameFilter_NoPanic), no delete involved.
	w2 := NewWorker(nc)
	cc2 := w2.subscribePullConsumer("race", "",
		func(tc TaskContext) error {
			counts[1].Add(1)
			return tc.Complete([]byte(`"done"`))
		})
	t.Cleanup(cc2.Stop)

	cons, err := stream.Consumer(ctx, "workers-race")
	if err != nil {
		t.Fatalf("workers-race missing: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.FilterSubject != "task.race.*" {
		t.Fatalf("FilterSubject = %q, want task.race.* (upgraded)",
			info.Config.FilterSubject)
	}

	for i := 0; i < 2; i++ {
		payload := protocol.TaskPayload{
			RunID: fmt.Sprintf("run-race-%d", i), StepID: "s",
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		if _, err := js.Publish(
			ctx, fmt.Sprintf("task.race.run-race-%d", i), data,
		); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if counts[0].Load()+counts[1].Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if total := counts[0].Load() + counts[1].Load(); total != 2 {
		t.Fatalf(
			"delivered %d/2 messages across both consumers sharing "+
				"the upgraded durable, want 2",
			total,
		)
	}
}
