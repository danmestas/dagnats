// run_visible_at_start_test.go
// Tests that a run is readable and cancellable from the moment
// runs.start replies with its id (#1006), before the orchestrator has
// handled workflow.started and written the snapshot.
// Methodology: real embedded NATS and the scoped NATS API, with the
// orchestrator held back (constructed but not started) so the window
// between the start reply and the snapshot stays open as long as the
// test needs. The orchestrator is then started to show a cancel sent in
// that window sticks and dispatches nothing (TASK_QUEUES stays empty).
// Negative space: unknown ids and another program's runs still read as
// not found, byte for byte.
package api

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startHeldAPI serves the NATS API with no orchestrator running and
// returns the connection, the service, and an orchestrator to start
// when the test is ready for the start to be handled.
func startHeldAPI(t *testing.T) (*nats.Conn, *Service, *engine.Orchestrator) {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	for _, program := range []string{"notify", "artifact"} {
		wb := dag.NewWorkflow(program + ".send")
		wb.Task("a", program+".deliver")
		registerScoped(t, nc, program, wb)
	}
	return nc, svc, engine.NewOrchestrator(nc)
}

func TestRunReadableBeforeStartHandled(t *testing.T) {
	nc, svc, _ := startHeldAPI(t)
	notifyRun := replyField(t, startScoped(t, nc, "notify", "notify.send"), "run_id")
	artifactRun := replyField(t, startScoped(t, nc, "artifact", "artifact.send"), "run_id")
	if notifyRun == "" || artifactRun == "" {
		t.Fatal("scoped start returned no run id")
	}

	status, found := scopedRunStatus(t, nc, "notify", notifyRun)
	if !found || status != dag.RunStatusPending {
		t.Fatalf("own run before its start was handled: found=%v status=%s, want pending",
			found, status)
	}
	run, err := svc.GetRun(context.Background(), notifyRun)
	if err != nil || run.Status != dag.RunStatusPending || run.WorkflowID != "notify.send" {
		t.Fatalf("svc.GetRun = %s %q err=%v, want pending notify.send",
			run.Status, run.WorkflowID, err)
	}

	unknown := scopedRequest(t, nc, "api.p.notify.runs.get", []byte("nosuchrun"))
	foreign := scopedRequest(t, nc, "api.p.notify.runs.get", []byte(artifactRun))
	if !bytes.Equal(unknown, foreign) || replyField(t, foreign, "error") == "" {
		t.Fatalf("foreign run %s differs from unknown run %s", foreign, unknown)
	}
	_, err = svc.GetRun(context.Background(), "nosuchrun")
	if !errors.Is(err, engine.ErrRunNotFound) {
		t.Fatalf("svc.GetRun(unknown) err=%v, want ErrRunNotFound", err)
	}
}

func TestRunCancelledBeforeStartHandledNeverDispatches(t *testing.T) {
	nc, _, orch := startHeldAPI(t)
	notifyRun := replyField(t, startScoped(t, nc, "notify", "notify.send"), "run_id")
	artifactRun := replyField(t, startScoped(t, nc, "artifact", "artifact.send"), "run_id")

	foreign := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte(artifactRun))
	unknown := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte("nosuchrun"))
	if !bytes.Equal(foreign, unknown) || replyField(t, foreign, "error") == "" {
		t.Fatalf("foreign cancel %s differs from unknown cancel %s", foreign, unknown)
	}
	cancelled := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte(notifyRun))
	if replyField(t, cancelled, "status") != "cancelled" {
		t.Fatalf("cancel before the start was handled = %s", cancelled)
	}

	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	waitScopedStatus(t, nc, "notify", notifyRun, dag.RunStatusCancelled)
	waitScopedStatus(t, nc, "artifact", artifactRun, dag.RunStatusRunning)
	waitHistoryDrained(t, nc)
	if got := taskMessages(t, nc, "notify.deliver"); got != 0 {
		t.Fatalf("the cancelled run dispatched %d task(s)", got)
	}
	if got := taskMessages(t, nc, "artifact.deliver"); got != 1 {
		t.Fatalf("the live run dispatched %d task(s), want 1", got)
	}
	if status, _ := scopedRunStatus(t, nc, "notify", notifyRun); status != dag.RunStatusCancelled {
		t.Fatalf("cancelled run became %s once every event was handled", status)
	}
	// Both runs now have snapshots, so a wildcard id that reached a
	// last_by_subj read would match the newest, which one of the two
	// programs owns: ask as both.
	for _, program := range []string{"notify", "artifact"} {
		for _, id := range []string{"*", ">"} {
			reply := scopedRequest(t, nc, "api.p."+program+".runs.get", []byte(id))
			if replyField(t, reply, "error") == "" {
				t.Fatalf("%s: wildcard run id %q matched a run: %s", program, id, reply)
			}
		}
	}
}

// waitHistoryDrained waits until the orchestrator has acked every
// history event, so "nothing dispatched" is not read too early.
func waitHistoryDrained(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cons, err := js.Consumer(context.Background(), "WORKFLOW_HISTORY", "orchestrator")
		if err == nil {
			info, err := cons.Info(context.Background())
			if err == nil && info.NumPending == 0 && info.NumAckPending == 0 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the orchestrator never drained the history stream")
}

// taskMessages counts TASK_QUEUES messages on task's subjects.
func taskMessages(t *testing.T, nc *nats.Conn, task string) uint64 {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(context.Background(), "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream TASK_QUEUES: %v", err)
	}
	info, err := stream.Info(context.Background(), jetstream.WithSubjectFilter("task."+task+".>"))
	if err != nil {
		t.Fatalf("TASK_QUEUES info: %v", err)
	}
	var total uint64
	for _, count := range info.State.Subjects {
		total += count
	}
	return total
}
