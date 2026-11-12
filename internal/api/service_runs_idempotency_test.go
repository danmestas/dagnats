// api/service_runs_idempotency_test.go
// Tests for atomic runs.start idempotency (#946). Methodology: fresh
// embedded NATS + orchestrator per test, bounded <=10s waits. Each test
// asserts the positive case (same run ID returned, run exists) and the
// negative space (the run count did not grow, a KV failure started
// nothing). The claim-then-publish-failed window is simulated by
// pre-claiming the KV slot with a run ID that was never published; the KV
// failure is a real server-side one (the idempotency_keys bucket deleted).
package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const idemWorkflow = "idem-atomic"

func idemHarness(t *testing.T) (*Service, *nats.Conn) {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	svc := NewService(nc)
	wb := dag.NewWorkflow(idemWorkflow)
	wb.Task("process", "process-task")
	wb.WithIdempotencyKey("request_id")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, nc
}

func idemRunCount(t *testing.T, svc *Service) int {
	t.Helper()
	env, err := svc.ListRuns(context.Background(), RunsFilter{}, 100)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	return len(env.Runs)
}

func waitRunExists(t *testing.T, svc *Service, runID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := svc.store.Load(context.Background(), runID); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s never appeared", runID)
}

// historyMsgCount totals the messages of the stream holding history.>.
func historyMsgCount(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	ctx := context.Background()
	name, err := js.StreamNameBySubject(ctx, "history.x")
	if err != nil {
		t.Fatalf("stream for history: %v", err)
	}
	stream, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	return info.State.Msgs
}

func TestStartRunIdempotency_ConcurrentSameKey(t *testing.T) {
	svc, _ := idemHarness(t)
	const callers = 12
	input := []byte(`{"request_id":"race-1"}`)
	ids := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = svc.StartRun(
				context.Background(), idemWorkflow, input)
		}(i)
	}
	wg.Wait()
	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("caller %d got %q, want %q", i, ids[i], ids[0])
		}
	}
	waitRunExists(t, svc, ids[0])
	time.Sleep(300 * time.Millisecond) // let any stray second run land
	if got := idemRunCount(t, svc); got != 1 {
		t.Fatalf("run count = %d, want 1", got)
	}
}

func TestStartRunIdempotency_RetryAfterCompletedStart(t *testing.T) {
	svc, nc := idemHarness(t)
	input := []byte(`{"request_id":"retry-1"}`)
	first, err := svc.StartRun(context.Background(), idemWorkflow, input)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	waitRunExists(t, svc, first)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	before := historyMsgCount(t, js)
	second, err := svc.StartRun(context.Background(), idemWorkflow, input)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second != first {
		t.Fatalf("retry got %q, want %q", second, first)
	}
	if after := historyMsgCount(t, js); after != before {
		t.Fatalf("retry published: history %d -> %d", before, after)
	}
}

func TestStartRunIdempotency_ClaimWithoutRunPublishes(t *testing.T) {
	svc, _ := idemHarness(t)
	stored := runid.New()
	slot := idempotencyHash(idemWorkflow, "orphan-1")
	if _, err := svc.idempotencyKV.Create(
		context.Background(), slot, []byte(stored),
	); err != nil {
		t.Fatalf("pre-claim: %v", err)
	}
	got, err := svc.StartRun(context.Background(), idemWorkflow,
		[]byte(`{"request_id":"orphan-1"}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if got != stored {
		t.Fatalf("got %q, want stored %q", got, stored)
	}
	waitRunExists(t, svc, stored)
	if n := idemRunCount(t, svc); n != 1 {
		t.Fatalf("run count = %d, want 1", n)
	}
}

func TestStartRunIdempotency_KVFailureFailsClosed(t *testing.T) {
	svc, nc := idemHarness(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := js.DeleteKeyValue(
		context.Background(), "idempotency_keys",
	); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}
	id, err := svc.StartRun(context.Background(), idemWorkflow,
		[]byte(`{"request_id":"kv-down"}`))
	if err == nil {
		t.Fatalf("StartRun succeeded with run %q; want error", id)
	}
	if id != "" {
		t.Fatalf("got run ID %q alongside error", id)
	}
	time.Sleep(300 * time.Millisecond)
	if n := idemRunCount(t, svc); n != 0 {
		t.Fatalf("run count = %d, want 0", n)
	}
}
