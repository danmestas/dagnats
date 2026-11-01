// natsapi_relay_stores_test.go
// Tests for the store relays a program reaches its step's checkpoint,
// signals and agent inputs through (#885): each serves only the
// program's own step (and, for checkpoints and inputs, its current
// dispatch nonce), or for signals only runs the program takes part in,
// and every refusal names the check that fired.
// Methodology: real embedded NATS, orchestrator and NATSAPI; a run is
// started with no worker so its step stays dispatched, and calls go
// through natsutil.TracingPublisher.RelayCall as the worker's do.
package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func storeHeader(op, runID, stepID, nonce string) nats.Header {
	header := nats.Header{}
	header.Set(natsutil.RelayOpHeader, op)
	header.Set(natsutil.RelayRunHeader, runID)
	header.Set(natsutil.RelayStepHeader, stepID)
	header.Set(journal.HeaderNonce, nonce)
	return header
}

func TestRelayCheckpointOnlyOwnStep(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type publisher = *natsutil.TracingPublisher
	call := func(tp publisher, header nats.Header, data []byte) (*nats.Msg, error) {
		return tp.RelayCall(ctx, natsutil.RelayCheckpointOp, header, data)
	}
	reply, err := call(notify, storeHeader("get", runID, "work", nonce), nil)
	if err != nil || reply.Header.Get(natsutil.RelayEmptyHeader) != "true" {
		t.Fatalf("empty checkpoint get = %v, %v", reply, err)
	}
	put := storeHeader("put", runID, "work", nonce)
	if _, err := call(notify, put, []byte("state")); err != nil {
		t.Fatalf("own put: %v", err)
	}
	reply, err = call(notify, storeHeader("get", runID, "work", nonce), nil)
	if err != nil || string(reply.Data) != "state" {
		t.Fatalf("own get = %v, %v", reply, err)
	}
	refusals := map[string]struct {
		code   string
		tp     *natsutil.TracingPublisher
		header nats.Header
	}{
		"another program": {natsutil.RefusalNotYours, relayClient(t, nc, "artifact"),
			storeHeader("put", runID, "work", nonce)},
		"wrong nonce": {natsutil.RefusalNotYours, notify, storeHeader("put", runID, "work", "x")},
		"unknown step": {natsutil.RefusalNotYours, notify,
			storeHeader("put", runID, "nope", nonce)},
		"no nonce": {natsutil.RefusalIdentity, notify, storeHeader("put", runID, "work", "")},
		"bad op": {natsutil.RefusalBadRequest, notify,
			storeHeader("purge", runID, "work", nonce)},
	}
	for name, tc := range refusals {
		if _, err := call(tc.tp, tc.header, []byte("evil")); refusalCode(err) != tc.code {
			t.Errorf("%s: err %v, want %q", name, err, tc.code)
		}
	}
	reply, err = call(notify, storeHeader("get", runID, "work", nonce), nil)
	if err != nil || string(reply.Data) != "state" {
		t.Fatalf("a refused put changed the checkpoint: %v, %v", reply, err)
	}
	if _, err := call(notify, storeHeader("delete", runID, "work", nonce), nil); err != nil {
		t.Fatalf("own delete: %v", err)
	}
}

func signalHeader(op, runID, name string) nats.Header {
	header := nats.Header{}
	header.Set(natsutil.RelayOpHeader, op)
	header.Set(natsutil.RelayRunHeader, runID)
	header.Set(natsutil.RelayKeyHeader, name)
	return header
}

// signalRead is a signal get carrying the caller's own dispatch.
func signalRead(runID, stepID, nonce, name string) nats.Header {
	header := storeHeader("get", runID, stepID, nonce)
	header.Set(natsutil.RelayKeyHeader, name)
	return header
}

func TestRelaySignalOnlyOwnNamesAndDispatch(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signal := natsutil.RelaySignalOp
	if _, err := notify.RelayCall(ctx, signal,
		signalHeader("put", runID, "notify.go"), []byte("now")); err != nil {
		t.Fatalf("own run, own name: %v", err)
	}
	reply, err := notify.RelayCall(ctx, signal, signalRead(runID, "work", nonce, "notify.go"), nil)
	if err != nil || string(reply.Data) != "now" {
		t.Fatalf("own read = %v, %v", reply, err)
	}
	refusals := map[string]struct {
		code   string
		tp     *natsutil.TracingPublisher
		header nats.Header
	}{
		"name outside the prefix": {natsutil.RefusalNotYours, notify,
			signalHeader("put", runID, "approved")},
		"read outside the prefix": {natsutil.RefusalNotYours, notify,
			signalRead(runID, "work", nonce, "approved")},
		"read without a dispatch": {natsutil.RefusalIdentity, notify,
			signalHeader("get", runID, "notify.go")},
		"read with a stale nonce": {natsutil.RefusalNotYours, notify,
			signalRead(runID, "work", "old", "notify.go")},
		"another program's run": {natsutil.RefusalNotYours, relayClient(t, nc, "artifact"),
			signalHeader("put", runID, "artifact.go")},
		"an unknown run": {natsutil.RefusalNotYours, notify,
			signalHeader("put", "nosuchrun", "notify.go")},
	}
	for name, tc := range refusals {
		_, err := tc.tp.RelayCall(ctx, signal, tc.header, []byte("x"))
		if refusalCode(err) != tc.code {
			t.Errorf("%s: err %v, want %q", name, err, tc.code)
		}
	}
}

// TestRelaySignalComposedRun: notify and artifact each own a step of one
// run, and artifact's step waits for "artifact.approved", which a human
// sends through the API. notify takes part in the run, but can neither
// read that signal nor forge it.
func TestRelaySignalComposedRun(t *testing.T) {
	nc := startScopedAPI(t)
	wb := dag.NewWorkflow("mixed")
	wb.Task("notifystep", "notify.work")
	wb.Task("artifactstep", "artifact.wait")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if replyField(t, scopedRequest(t, nc, "api.workflows.register", mustMarshal(t, def)),
		"status") != "registered" {
		t.Fatal("operator workflow not registered")
	}
	start := mustMarshal(t, startRunRequest{Workflow: "mixed"})
	runID := replyField(t, scopedRequest(t, nc, "api.runs.start", start), "run_id")
	deadline := time.Now().Add(5 * time.Second)
	var notifyNonce string
	for time.Now().Before(deadline) && notifyNonce == "" {
		var run dag.WorkflowRun
		data := scopedRequest(t, nc, "api.runs.get", []byte(runID))
		if err := json.Unmarshal(data, &run); err == nil {
			notifyNonce = run.Steps["notifystep"].DispatchNonce
		}
		time.Sleep(20 * time.Millisecond)
	}
	if notifyNonce == "" {
		t.Fatal("notify's step was never dispatched")
	}
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signal := natsutil.RelaySignalOp
	forge := signalHeader("put", runID, "artifact.approved")
	_, err = notify.RelayCall(ctx, signal, forge, []byte("y"))
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("notify forged artifact's signal: %v", err)
	}
	read := signalRead(runID, "notifystep", notifyNonce, "artifact.approved")
	_, err = notify.RelayCall(ctx, signal, read, nil)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("notify read artifact's signal: %v", err)
	}
	if _, err := notify.RelayCall(ctx, signal,
		signalHeader("put", runID, "notify.ready"), []byte("y")); err != nil {
		t.Fatalf("notify's own signal in the shared run: %v", err)
	}
}

func TestRelayInputsOnlyOwnStep(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := notify.RelayCall(ctx, natsutil.RelayInputsOp,
		storeHeader("list", runID, "work", nonce), nil)
	if err != nil {
		t.Fatalf("own list: %v", err)
	}
	var listed inputsReply
	if err := json.Unmarshal(reply.Data, &listed); err != nil || len(listed.Entries) != 0 {
		t.Fatalf("own list = %s, %v", reply.Data, err)
	}
	_, err = relayClient(t, nc, "artifact").RelayCall(ctx, natsutil.RelayInputsOp,
		storeHeader("list", runID, "work", nonce), nil)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("another program's list: %v, want not_yours", err)
	}
	// A transition must be for the caller's own step: an entry naming
	// another step is refused before the store is touched.
	foreign, err := json.Marshal(inputsRequest{
		Entry:  inputs.Submission{RunID: runID, StepID: "other", RequestID: "r1"},
		Status: inputs.StatusDone,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = notify.RelayCall(ctx, natsutil.RelayInputsOp,
		storeHeader("transition", runID, "work", nonce), foreign)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("transition of another step's entry: %v, want not_yours", err)
	}
}

// TestRelayedWorkerSignalsThroughTheEngine runs a real worker in relayed
// mode (as program.Attach builds it) whose handler signals its own run
// and waits for that signal: both go through api.p.<name>.signal.
func TestRelayedWorkerSignalsThroughTheEngine(t *testing.T) {
	nc := startScopedAPI(t)
	w := worker.NewWorker(nc, worker.WithEngineRelay("api.p.notify."))
	got := make(chan string, 1)
	w.Handle("notify.work", func(tc worker.TaskContext) error {
		if err := tc.SendSignal(tc.RunID(), "notify.ready", []byte("yes")); err != nil {
			return err
		}
		value, err := tc.WaitForSignal("notify.ready", 5*time.Second)
		if err != nil {
			return err
		}
		got <- string(value)
		return tc.Complete(nil)
	})
	if err := w.Start(); err != nil {
		t.Fatalf("worker start: %v", err)
	}
	t.Cleanup(w.Stop)
	dispatchedStep(t, nc)
	select {
	case value := <-got:
		if value != "yes" {
			t.Fatalf("signal value = %q, want yes", value)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the relayed worker never received its signal")
	}
}

// TestRelayedStepWaitsForTheControlPlane: a relayed worker running while
// no dagnats answers the relay cannot publish step.started, so it NAKs
// the task for redelivery instead of losing it; once the control plane
// is up, the same run completes.
func TestRelayedStepWaitsForTheControlPlane(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	svc := NewService(nc)
	wb := dag.NewWorkflow("notify.late")
	wb.Task("work", "notify.work")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("register: %v", err)
	}
	w := worker.NewWorker(nc, worker.WithEngineRelay("api.p.notify."))
	w.Handle("notify.work", func(tc worker.TaskContext) error { return tc.Complete(nil) })
	if err := w.Start(); err != nil {
		t.Fatalf("worker start: %v", err)
	}
	t.Cleanup(w.Stop)
	runID, err := svc.StartRun(ctx, "notify.late", nil)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	time.Sleep(2500 * time.Millisecond) // a few NAK-and-redeliver cycles
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.IsTerminal() || stepTerminal(run.Steps["work"].Status) {
		t.Fatalf("with no control plane the run went %s / %s", run.Status, run.Steps["work"].Status)
	}
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if run, err := svc.GetRun(ctx, runID); err == nil && run.Status == dag.RunStatusCompleted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the task was lost: the run never completed once the control plane came up")
}

// TestRelayInputsTransitionKeepsTheStoredSubmission: a transition takes
// only the status, iteration and the revision the program read; the
// submission it changes is the stored one, never a body the program
// sends. Revision 0 and a malformed request id are refused at the
// boundary.
func TestRelayInputsTransitionKeepsTheStoredSubmission(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := inputs.Open(ctx, js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	submitted, _, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: "work", RequestID: "r1", Mode: inputs.ModeSteer,
		Content: json.RawMessage(`"real"`),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	stored, err := store.Get(ctx, runID, "work", "r1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	notify := relayClient(t, nc, "notify")
	transition := func(body inputsRequest) error {
		_, err := notify.RelayCall(ctx, natsutil.RelayInputsOp,
			storeHeader("transition", runID, "work", nonce), mustMarshal(t, body))
		return err
	}
	forged := submitted
	forged.Content = json.RawMessage(`"forged"`)
	forged.Mode = inputs.ModeFollowUp
	boundary := map[string]inputsRequest{
		"revision 0": {Entry: forged, Revision: 0, Status: inputs.StatusDone},
		"bad id": {Entry: inputs.Submission{RunID: runID, StepID: "work", RequestID: "a.b"},
			Revision: 1, Status: inputs.StatusDone},
		"bogus status": {Entry: forged, Revision: stored.Revision, Status: "bogus"},
	}
	for name, body := range boundary {
		if err := transition(body); refusalCode(err) != natsutil.RefusalBadRequest {
			t.Errorf("%s: err %v, want bad_request", name, err)
		}
	}
	if err := transition(inputsRequest{
		Entry: forged, Revision: stored.Revision, Status: inputs.StatusDone,
	}); err != nil {
		t.Fatalf("transition: %v", err)
	}
	after, err := store.Get(ctx, runID, "work", "r1")
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if after.Status != inputs.StatusDone || string(after.Content) != `"real"` ||
		after.Mode != inputs.ModeSteer {
		t.Fatalf("after transition = %+v, want the stored submission marked done", after.Submission)
	}
	stale := transition(inputsRequest{Entry: forged, Revision: stored.Revision,
		Status: inputs.StatusUnanswered})
	if refusalCode(stale) != natsutil.RefusalInputsConflict {
		t.Fatalf("a stale revision: %v, want inputs_conflict", stale)
	}
}
