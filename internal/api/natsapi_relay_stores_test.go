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

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
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

func TestRelaySignalOnlyRunsTheProgramTakesPartIn(t *testing.T) {
	nc := startScopedAPI(t)
	runID, _ := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := notify.RelayCall(ctx, natsutil.RelaySignalOp,
		signalHeader("put", runID, "go"), []byte("now")); err != nil {
		t.Fatalf("own run signal: %v", err)
	}
	signal := natsutil.RelaySignalOp
	reply, err := notify.RelayCall(ctx, signal, signalHeader("get", runID, "go"), nil)
	if err != nil || string(reply.Data) != "now" {
		t.Fatalf("signal get = %v, %v", reply, err)
	}
	artifact := relayClient(t, nc, "artifact")
	for _, op := range []string{"put", "get"} {
		_, err := artifact.RelayCall(ctx, signal, signalHeader(op, runID, "go"), nil)
		if refusalCode(err) != natsutil.RefusalNotYours {
			t.Errorf("another program's signal %s: %v, want not_yours", op, err)
		}
	}
	_, err = notify.RelayCall(ctx, signal, signalHeader("put", "nosuchrun", "go"), nil)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("signal to an unknown run: %v, want not_yours", err)
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
		if err := tc.SendSignal(tc.RunID(), "ready", []byte("yes")); err != nil {
			return err
		}
		value, err := tc.WaitForSignal("ready", 5*time.Second)
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
