// engine/orchestrator_stop_drain_test.go
// Regression test for the intermittent t.TempDir() cleanup race seen on
// TestOrchestratorPersistsTraceParentOnStart under parallel/-race load.
// jetstream.ConsumeContext.Stop() only *signals* the pull-consumer
// goroutine to exit — the goroutine itself notices and reports done
// asynchronously via Closed(). Orchestrator.Stop() must block until that
// signal lands, otherwise a consumer goroutine can still be mid-fetch
// against JetStream (whose storage lives under t.TempDir()) after Stop()
// returns, racing the embedded server shutdown and temp-dir removal that
// follow in test teardown.
// Methodology: start the orchestrator, capture its ConsumeContext before
// Stop() nils the field, call Stop(), then assert Closed() is already
// closed — i.e. the consumer goroutine had fully quiesced by the time
// Stop() returned control to the caller.
package engine

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestOrchestratorStopDrainsHistoryConsumer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	cc := orch.cc
	if cc == nil {
		t.Fatal("orch.cc must not be nil after Start")
	}

	orch.Stop()

	// Positive: the consumer's pull goroutine fully quiesced before
	// Stop() returned — Closed() is already closed, no select needed.
	select {
	case <-cc.Closed():
	default:
		t.Fatal("Stop() returned before the history consumer's pull " +
			"goroutine reported Closed() — a late fetch can still race " +
			"JetStream storage teardown")
	}
	// Negative: Stop() cleared the field so a second Stop() is a no-op.
	if orch.cc != nil {
		t.Fatal("orch.cc must be nil after Stop")
	}
}
