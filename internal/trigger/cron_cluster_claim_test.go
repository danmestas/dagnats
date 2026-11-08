package trigger

// Methodology: integration tests with one embedded NATS server and two
// Schedulers over the same trigger_state bucket, standing in for two
// cluster nodes. Ticks pass an explicit `now`, so "19 seconds apart in
// the same minute" is exact. Fires are counted on a core subscription to
// history.>, which sees every publish BEFORE the stream's 5 s duplicate
// window can hide one; counting stream contents would mask the bug. Each
// test asserts both sides: one fire for the contested minute (negative
// space: not two) and a fire for the next minute (positive: not zero).
//
// Production trace: notify.health-verify-daily (cron 17 6 * * *) started
// two runs, 06:17:02Z and 06:17:21Z, from two nodes.

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

func clusterClaimDef() TriggerDef {
	return TriggerDef{
		ID:         "notify.health-verify-daily",
		WorkflowID: "health-verify",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "17 6 * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
}

// twoNodeSchedulers returns two schedulers sharing one server plus a
// core subscription counting every started publish.
func twoNodeSchedulers(
	t *testing.T,
) (*Scheduler, *Scheduler, *nats.Subscription) {
	t.Helper()
	_, nc := natstest.StartServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	sub, err := nc.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	nodes := make([]*Scheduler, 2)
	for i := range nodes {
		node, err := NewScheduler(nc)
		if err != nil {
			t.Fatalf("NewScheduler: %v", err)
		}
		if err := node.AddTrigger(clusterClaimDef()); err != nil {
			t.Fatalf("AddTrigger: %v", err)
		}
		nodes[i] = node
	}
	return nodes[0], nodes[1], sub
}

func countFires(sub *nats.Subscription, wait time.Duration) int {
	count := 0
	for count < 10 {
		if _, err := sub.NextMsg(wait); err != nil {
			break
		}
		count++
	}
	return count
}

func TestCronClusterClaimTwoNodesSameMinuteFireOnce(t *testing.T) {
	a, b, sub := twoNodeSchedulers(t)
	first := time.Date(2026, 11, 7, 6, 17, 2, 0, time.UTC)
	second := time.Date(2026, 11, 7, 6, 17, 21, 0, time.UTC)

	if err := a.Tick(first); err != nil {
		t.Fatalf("Tick a: %v", err)
	}
	if err := b.Tick(second); err != nil {
		t.Fatalf("Tick b: %v", err)
	}
	if got := countFires(sub, 500*time.Millisecond); got != 1 {
		t.Fatalf("fires for one minute across two nodes = %d, want 1", got)
	}

	// The loser must not have burned the next day locally, and the next
	// scheduled time must fire exactly once again.
	next := time.Date(2026, 11, 8, 6, 17, 5, 0, time.UTC)
	if err := b.Tick(next); err != nil {
		t.Fatalf("Tick b next: %v", err)
	}
	if err := a.Tick(next.Add(19 * time.Second)); err != nil {
		t.Fatalf("Tick a next: %v", err)
	}
	if got := countFires(sub, 500*time.Millisecond); got != 1 {
		t.Fatalf("fires for next day = %d, want 1", got)
	}
}

func TestCronClusterClaimBackfillAndLiveTickFireOnce(t *testing.T) {
	a, b, sub := twoNodeSchedulers(t)
	def := clusterClaimDef()
	minute := time.Date(2026, 11, 7, 6, 17, 0, 0, time.UTC)
	ctx := context.Background()

	// Node a's backfill claims each match before firing it.
	won, _, err := a.claimCluster(ctx, def.ID, minute)
	if err != nil || !won {
		t.Fatalf("backfill claim = %v, %v; want true, nil", won, err)
	}
	if err := a.fireWorkflow(ctx, def, minute); err != nil {
		t.Fatalf("fire: %v", err)
	}
	if err := b.Tick(minute.Add(21 * time.Second)); err != nil {
		t.Fatalf("Tick b: %v", err)
	}
	if got := countFires(sub, 500*time.Millisecond); got != 1 {
		t.Fatalf("fires = %d, want 1", got)
	}

	// A stale backfill minute must not win once a newer one is claimed.
	won, _, err = b.claimCluster(ctx, def.ID, minute.Add(-time.Hour))
	if err != nil || won {
		t.Fatalf("stale claim = %v, %v; want false, nil", won, err)
	}
}

func TestCronClusterClaimErrorSkipsThenRetriesSameMinute(t *testing.T) {
	a, _, sub := twoNodeSchedulers(t)
	at := time.Date(2026, 11, 7, 6, 17, 2, 0, time.UTC)
	good := a.stateKV

	a.stateKV = failingKV{KeyValue: good}
	if err := a.Tick(at); err == nil {
		t.Fatalf("Tick on KV error must report the failed claim")
	}
	if got := countFires(sub, 300*time.Millisecond); got != 0 {
		t.Fatalf("fired %d times on claim error, want 0", got)
	}

	a.stateKV = good
	if err := a.Tick(at.Add(30 * time.Second)); err != nil {
		t.Fatalf("Tick retry: %v", err)
	}
	if got := countFires(sub, 500*time.Millisecond); got != 1 {
		t.Fatalf("retry in same minute fired %d, want 1", got)
	}
}

// flakyFire fails the first `failures` calls, then delegates to Fire.
func flakyFire(failures int) (fireFunc, *int) {
	calls := 0
	return func(
		ctx context.Context, tp *natsutil.TracingPublisher,
		def TriggerDef, source string, now time.Time,
	) (string, error) {
		calls++
		if calls <= failures {
			return "", errInjectedKV
		}
		return Fire(ctx, tp, def, source, now)
	}, &calls
}

func TestCronClusterClaimFireRetriesThenFiresOnce(t *testing.T) {
	a, b, sub := twoNodeSchedulers(t)
	at := time.Date(2026, 11, 7, 6, 17, 2, 0, time.UTC)
	var calls *int
	a.fire, calls = flakyFire(2)

	if err := a.Tick(at); err != nil {
		t.Fatalf("Tick a: %v", err)
	}
	if err := b.Tick(at.Add(21 * time.Second)); err != nil {
		t.Fatalf("Tick b: %v", err)
	}
	if *calls != 3 {
		t.Fatalf("fire attempts = %d, want 3", *calls)
	}
	if got := countFires(sub, 500*time.Millisecond); got != 1 {
		t.Fatalf("fires = %d, want 1", got)
	}
}

func TestCronClusterClaimLostFireKeepsClaim(t *testing.T) {
	a, b, sub := twoNodeSchedulers(t)
	at := time.Date(2026, 11, 7, 6, 17, 2, 0, time.UTC)
	var calls *int
	a.fire, calls = flakyFire(1000)

	if err := a.Tick(at); err == nil {
		t.Fatalf("Tick must report the lost fire")
	}
	if *calls != fireAttemptsMax {
		t.Fatalf("fire attempts = %d, want %d", *calls, fireAttemptsMax)
	}
	if err := b.Tick(at.Add(21 * time.Second)); err != nil {
		t.Fatalf("Tick b: %v", err)
	}
	if got := countFires(sub, 300*time.Millisecond); got != 0 {
		t.Fatalf("other node fired %d after a lost fire, want 0", got)
	}
}
