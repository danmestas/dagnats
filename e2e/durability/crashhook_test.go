// e2e/durability/crashhook_test.go
// The matrix's crash hook fires on the first handling only, and must
// not hold any later handling back. A stall is the zombie resuming
// after its successor took over; were the successor's own visit to the
// point made to wait for the stall to end, both would leave it
// together and race for the claim, and a zombie that wins and reaches
// the tool is fenced mid-call: the unsafe call is then answered as
// interrupted, which is correct but not the cell's expected answer.
//
// Methodology: pure, no NATS. Fire the hook as a stall from one
// goroutine and, once it is stalling, fire it again for a later
// delivery; the second call must return well within stallFor and the
// hook must count one firing. Every wait is bounded.
package durability

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/chaos"
)

func TestCrashHookDoesNotHoldSuccessor(t *testing.T) {
	hook := &crashHook{point: chaos.WorkerBeforeClaim, iteration: unsafeIteration}
	site := chaos.Site{RunID: "run", StepID: stepID, Iteration: unsafeIteration, Delivered: 1}
	stalled := make(chan error, 1)
	go func() { stalled <- hook.fire(context.Background(), chaos.WorkerBeforeClaim, site) }()
	deadline := time.Now().Add(stallFor)
	for hook.count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if hook.count.Load() != 1 {
		t.Fatalf("first fire did not start stalling within %s", stallFor)
	}
	site.Delivered = 2
	start := time.Now()
	if err := hook.fire(context.Background(), chaos.WorkerBeforeClaim, site); err != nil {
		t.Fatalf("successor fire = %v, want nil", err)
	}
	if held := time.Since(start); held > stallFor/3 {
		t.Errorf("successor held %s by the zombie's stall, want no wait", held)
	}
	if n := hook.count.Load(); n != 1 {
		t.Errorf("hook fired %d times, want once", n)
	}
	select {
	case err := <-stalled:
		if err != nil {
			t.Errorf("stall fire = %v, want nil", err)
		}
	case <-time.After(2 * stallFor):
		t.Fatalf("stall did not end within %s", 2*stallFor)
	}
}
