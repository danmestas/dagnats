// e2e/durability/restart_limit_test.go
// The crash loop (ADR-028, #809): a worker killed right after its
// claim on every delivery of one iteration. After RestartLimitDefault
// claims with nothing between them the SDK fails the step with reason
// restart_limit; the step's retry policy retries it as a new attempt,
// which resumes from the journal and completes with the crash-free
// answer. Deleting the guard (mutation check) lets the loop run on
// until the kill budget is spent and never fails the step, which the
// history assertion catches.
//
// Methodology: the shared fixture; one run whose hook kills the first
// restartLimit handlings of the unsafe iteration after their claim.
// Every wait is bounded.
package durability

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/chaos"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestCrashLoop_RestartLimitFailsThenRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("crash loop test skipped in -short mode")
	}
	f := newFixture(t, fixtureOptions{engines: 2})
	r := &router{}
	r.install(t)
	a := f.newAgent(t)
	baseline := f.waitCompleted(t, a.startRun(t))
	hook := &crashLoopHook{kills: worker.RestartLimitDefault}
	runID := a.startRunRouted(t, r, hook.fire)
	got := f.waitCompleted(t, runID)
	if killed := hook.killed(); killed != worker.RestartLimitDefault {
		t.Errorf("killed %d deliveries, want %d", killed, worker.RestartLimitDefault)
	}
	if !sameAnswer(expectedAnswer(baseline, map[chaos.Point]int{}), got) {
		t.Errorf("answer after the crash loop = %+v, want %+v", got, baseline)
	}
	failures := stepFailures(t, f.nc, runID)
	if len(failures) != 1 || !strings.Contains(failures[0], worker.ReasonRestartLimit) {
		t.Errorf("step.failed errors = %q, want one %s failure", failures, worker.ReasonRestartLimit)
	}
	f.audit(t, runID)
}

// crashLoopHook kills the first kills handlings of the unsafe
// iteration after their claim (the router already routes by run).
type crashLoopHook struct {
	kills int
	mu    sync.Mutex
	count int
}

// fire is the chaos.Hook.
func (h *crashLoopHook) fire(_ context.Context, p chaos.Point, site chaos.Site) error {
	if site.RunID == "" {
		panic("crashLoopHook: site must name a run")
	}
	if h.kills < 1 {
		panic("crashLoopHook: kills must be positive")
	}
	if p != chaos.WorkerAfterClaim || site.Iteration != unsafeIteration {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count >= h.kills {
		return nil
	}
	h.count++
	return chaos.ErrKilled
}

// killed is how many handlings the hook killed.
func (h *crashLoopHook) killed() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// stepFailures reads the run's history and returns every step.failed
// error, in order.
func stepFailures(t *testing.T, nc *nats.Conn, runID string) []string {
	t.Helper()
	if runID == "" {
		panic("stepFailures: runID must not be empty")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll(), nats.OrderedConsumer())
	if err != nil {
		t.Fatalf("subscribe history: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Logf("unsubscribe: %v", err)
		}
	}()
	var failures []string
	for i := 0; i < historyReadMax; i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			break
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		if err != nil {
			t.Fatalf("decode history event: %v", err)
		}
		if evt.Type == protocol.EventStepFailed {
			failures = append(failures, string(evt.Payload))
		}
	}
	return failures
}
