// worker/watch_triggers_test.go
// Methodology: real embedded NATS + real engine-side TriggerService.
// Each test exercises one slice of the WatchTriggers contract:
// catch-up, bounded catch-up, live activate, live deactivate, and
// Stop-time subscription drain. Q4 end-to-end (sticky owner receives
// the dispatch when the engine bridges through to the worker) lives
// at the bottom.
//
// All waits are bounded — no naked goroutines, no untimed
// channels. Each callback writes to an atomic.Int32 or unbuffered
// channel so the test's positive/negative assertions are
// deterministic.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

// putTrigger writes a TriggerDef directly into the triggers KV
// bucket. Mirrors the engine-side test helper of the same name.
func putTrigger(t *testing.T, w *Worker, def trigger.TriggerDef) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	kv, err := w.js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("KeyValue(triggers): %v", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := kv.Put(ctx, def.ID, data); err != nil {
		t.Fatalf("triggers Put %q: %v", def.ID, err)
	}
}

func TestWatchTriggers_CatchUpOnStart(t *testing.T) {
	w := startWorkerForTriggers(t)
	const kind = "fs.watch"

	// Pre-populate the triggers KV with two matching entries and one
	// non-matching entry. WatchTriggers must fire onActivate exactly
	// for the matching pair.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "t-1", WorkflowID: "wf-1", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/a"}`),
		},
	})
	putTrigger(t, w, trigger.TriggerDef{
		ID: "t-2", WorkflowID: "wf-2", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/b"}`),
		},
	})
	putTrigger(t, w, trigger.TriggerDef{
		ID: "t-other", WorkflowID: "wf-other", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind:   "other.kind",
			Config: json.RawMessage(`{}`),
		},
	})
	// Also a disabled matching entry — must NOT fire.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "t-disabled", WorkflowID: "wf-x", Enabled: false,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/d"}`),
		},
	})

	var activates atomic.Int32
	seen := make(chan string, 8)
	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	err := w.WatchTriggers(ctx, kind,
		func(_ context.Context, def dagnatsext.TriggerDef) error {
			activates.Add(1)
			seen <- def.ID
			return nil
		},
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	)
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Positive: exactly two activations fire (t-1, t-2).
	deadline := time.After(2 * time.Second)
	gotIDs := map[string]bool{}
	for len(gotIDs) < 2 {
		select {
		case <-deadline:
			t.Fatalf("only saw %d activations: %v",
				activates.Load(), gotIDs)
		case id := <-seen:
			gotIDs[id] = true
		}
	}
	if !gotIDs["t-1"] || !gotIDs["t-2"] {
		t.Fatalf("missing matching IDs: %v", gotIDs)
	}
	// Negative: the unrelated kind did NOT fire, and disabled did NOT.
	if gotIDs["t-other"] || gotIDs["t-disabled"] {
		t.Fatalf("non-matching entry fired: %v", gotIDs)
	}
}

func TestWatchTriggers_BoundedCatchUp(t *testing.T) {
	// Honour the documented bound. Inserting 10001 matching entries
	// must produce exactly maxCatchupKeys (10000) onActivate fires.
	// CI-conscious: KV is in-memory; ~10k Puts on the embedded server
	// fits well under the test's bounded deadline.
	if testing.Short() {
		t.Skip("BoundedCatchUp inserts 10001 KV entries; skipping in -short")
	}
	w := startWorkerForTriggers(t)
	const kind = "fs.watch"

	insertCtx, cancelInsert := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancelInsert()
	kv, err := w.js.KeyValue(insertCtx, "triggers")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	total := maxCatchupKeys + 1
	for i := 0; i < total; i++ {
		def := trigger.TriggerDef{
			ID:         fmt.Sprintf("t-%05d", i),
			WorkflowID: "wf-bound",
			Enabled:    true,
			External: &trigger.ExternalTriggerConfig{
				Kind: kind, Config: json.RawMessage(`{}`),
			},
		}
		data, err := json.Marshal(def)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := kv.Put(insertCtx, def.ID, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var activates atomic.Int32
	watchCtx, cancelWatch := context.WithTimeout(
		context.Background(), 60*time.Second,
	)
	defer cancelWatch()
	err = w.WatchTriggers(watchCtx, kind,
		func(_ context.Context, _ dagnatsext.TriggerDef) error {
			activates.Add(1)
			return nil
		},
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	)
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Positive: count is exactly maxCatchupKeys (cap respected).
	if got := activates.Load(); got != int32(maxCatchupKeys) {
		t.Fatalf("activate count = %d, want %d", got, maxCatchupKeys)
	}
}

func TestWatchTriggers_LiveActivate(t *testing.T) {
	w := startWorkerForTriggers(t)
	const kind = "fs.live"

	// Register the kind via the engine ack so an externalRegistrar
	// exists; this is the path that actually fires
	// `_TRIGGER.<kind>.activate` requests when the triggers KV gets
	// a new matching entry.
	ackCtx, cancelAck := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancelAck()
	if err := w.RegisterTriggerType(ackCtx, dagnatsext.TriggerTypeDef{
		Name:         kind,
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	var liveHits atomic.Int32
	done := make(chan dagnatsext.TriggerDef, 4)
	watchCtx, cancelWatch := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancelWatch()
	err := w.WatchTriggers(watchCtx, kind,
		func(_ context.Context, def dagnatsext.TriggerDef) error {
			liveHits.Add(1)
			done <- def
			return nil
		},
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	)
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Insert a NEW trigger after subscribe — the engine's KV watcher
	// must pick it up and fire `_TRIGGER.<kind>.activate`, which the
	// worker SDK callback observes.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "live-1", WorkflowID: "wf-live", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/live"}`),
		},
	})

	// Positive: callback fires within the live deadline. We allow 2s
	// to absorb KV-watch latency + ack roundtrip + bridge request.
	select {
	case def := <-done:
		if def.ID != "live-1" {
			t.Fatalf("def.ID = %q, want live-1", def.ID)
		}
		if def.External.Kind != kind {
			t.Fatalf("def.External wrong kind: %#v",
				def.External)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("live activate not observed within 2s "+
			"(hits=%d)", liveHits.Load())
	}
}

func TestWatchTriggers_LiveDeactivate(t *testing.T) {
	w := startWorkerForTriggers(t)
	const kind = "fs.deact"

	ackCtx, cancelAck := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancelAck()
	if err := w.RegisterTriggerType(ackCtx, dagnatsext.TriggerTypeDef{
		Name:         kind,
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	activated := make(chan struct{}, 1)
	deactivated := make(chan dagnatsext.TriggerDef, 1)
	watchCtx, cancelWatch := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancelWatch()
	err := w.WatchTriggers(watchCtx, kind,
		func(_ context.Context, _ dagnatsext.TriggerDef) error {
			select {
			case activated <- struct{}{}:
			default:
			}
			return nil
		},
		func(_ context.Context, def dagnatsext.TriggerDef) error {
			deactivated <- def
			return nil
		},
	)
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Add then delete. Engine's KV watcher fires deactivate on KV
	// delete via the externalRegistrar.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "deact-1", WorkflowID: "wf-deact", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{}`),
		},
	})
	select {
	case <-activated:
	case <-time.After(2 * time.Second):
		t.Fatalf("activate not observed before deactivate")
	}

	delCtx, cancelDel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancelDel()
	kv, err := w.js.KeyValue(delCtx, "triggers")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if err := kv.Delete(delCtx, "deact-1"); err != nil {
		t.Fatalf("kv.Delete: %v", err)
	}

	// Positive: deactivate callback fires within 2s.
	select {
	case def := <-deactivated:
		if def.ID != "deact-1" {
			t.Fatalf("deactivate def.ID = %q, want deact-1", def.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("deactivate not observed within 2s")
	}
}

func TestWorkerStop_UnsubscribesTriggerSubs(t *testing.T) {
	// Goroutine-leak guard. After Stop(), the worker must have no
	// trigger subscriptions still bound to the connection.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	if err := w.WatchTriggers(ctx, "fs.stop",
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	); err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	w.triggerSubsMu.Lock()
	before := len(w.triggerSubs)
	w.triggerSubsMu.Unlock()
	if before != 2 {
		t.Fatalf("expected 2 subs after WatchTriggers, got %d", before)
	}

	w.Stop()

	w.triggerSubsMu.Lock()
	after := len(w.triggerSubs)
	w.triggerSubsMu.Unlock()
	// Positive: Stop drained the slice.
	if after != 0 {
		t.Fatalf("triggerSubs len after Stop = %d, want 0", after)
	}
	// Negative: a subsequent publish on either subject must not be
	// delivered to a worker callback (we have no callback path
	// anyway, but verifying subscription state directly is the
	// strongest signal).
	subj := "_TRIGGER.fs.stop.activate"
	if err := nc.Publish(subj, []byte("{}")); err != nil {
		t.Fatalf("nc.Publish post-Stop: %v", err)
	}
	// Allow a moment for any stray subscription to deliver.
	time.Sleep(50 * time.Millisecond)
	// No assertion needed beyond the above — the goroutine leak
	// detector (run with -race) is the real guard here.
}

// TestWatchTriggers_Q4_EndToEndStickyOwner is the Phase 2.4 Q4 e2e:
// worker calls RegisterTriggerType, engine acks, a TriggerDef
// landing in the `triggers` KV gets bridged through to the worker's
// onActivate callback. Mirrors the engine-side
// TestExternalRegistrar_Q4_StickyOwnerReceivesTask but exercised
// fully through the worker SDK.
func TestWatchTriggers_Q4_EndToEndStickyOwner(t *testing.T) {
	w := startWorkerForTriggers(t)
	const kind = "fs.q4"

	regCtx, cancelReg := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancelReg()
	if err := w.RegisterTriggerType(regCtx, dagnatsext.TriggerTypeDef{
		Name:         kind,
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	delivered := make(chan dagnatsext.TriggerDef, 1)
	watchCtx, cancelWatch := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancelWatch()
	if err := w.WatchTriggers(watchCtx, kind,
		func(_ context.Context, def dagnatsext.TriggerDef) error {
			delivered <- def
			return nil
		},
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	); err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Operator-style insert into the triggers KV — the engine's
	// externalRegistrar bridges to the owner worker subject and
	// WatchTriggers callback fires.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "q4-1", WorkflowID: "wf-q4", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/q4"}`),
		},
	})

	select {
	case def := <-delivered:
		// Positive: arrived; payload reconstructed correctly.
		if def.ID != "q4-1" {
			t.Fatalf("def.ID = %q, want q4-1", def.ID)
		}
		if def.WorkflowID != "wf-q4" {
			t.Fatalf("def.WorkflowID = %q, want wf-q4",
				def.WorkflowID)
		}
		if string(def.External.Config) != `{"path":"/q4"}` {
			t.Fatalf("def.External.Config mismatch: %#v",
				def.External)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Q4 e2e did not deliver activate within 2s")
	}
	// Negative: the catch-up scan + live event must not double-fire.
	// We've already drained 1; nothing else should arrive within a
	// short follow-on window.
	select {
	case extra := <-delivered:
		t.Fatalf("unexpected second delivery: %#v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestWatchTriggers_PublicSeamEndToEnd proves the public extension seam
// (dagnatsext) end to end: RegisterTriggerType accepts a dagnatsext.TriggerTypeDef
// and WatchTriggers delivers a dagnatsext.TriggerDef carrying the correct
// ID and External.Kind to the handler without importing internal/trigger.
//
// Positive: handler receives the expected ID and External.Kind.
// Negative: a non-matching kind does not fire.
func TestWatchTriggers_PublicSeamEndToEnd(t *testing.T) {
	w := startWorkerForTriggers(t)
	const kind = "seam.test"
	const otherKind = "seam.other"

	regCtx, cancelReg := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancelReg()
	// Register using the public dagnatsext type — no internal/trigger import
	// required by callers.
	if err := w.RegisterTriggerType(regCtx, dagnatsext.TriggerTypeDef{
		Name:         kind,
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
		Version:      "1.0.0",
	}); err != nil {
		t.Fatalf("RegisterTriggerType: %v", err)
	}

	received := make(chan dagnatsext.TriggerDef, 2)
	watchCtx, cancelWatch := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancelWatch()
	if err := w.WatchTriggers(watchCtx, kind,
		func(_ context.Context, def dagnatsext.TriggerDef) error {
			received <- def
			return nil
		},
		func(_ context.Context, _ dagnatsext.TriggerDef) error { return nil },
	); err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}

	// Insert matching trigger. putTrigger writes the rich internal type to KV
	// (the engine side); the worker SDK delivers the slim public view.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "seam-1", WorkflowID: "wf-seam", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: kind, Config: json.RawMessage(`{"path":"/seam"}`),
		},
	})
	// Also insert a trigger of a different kind — must not fire.
	putTrigger(t, w, trigger.TriggerDef{
		ID: "seam-other", WorkflowID: "wf-other", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind: otherKind, Config: json.RawMessage(`{}`),
		},
	})

	// Positive: handler receives a dagnatsext.TriggerDef with correct ID + Kind.
	select {
	case def := <-received:
		if def.ID != "seam-1" {
			t.Fatalf("def.ID = %q, want seam-1", def.ID)
		}
		if def.External.Kind != kind {
			t.Fatalf("def.External.Kind = %q, want %q",
				def.External.Kind, kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("public seam: activate not received within 2s")
	}
	// Negative: the non-matching kind must not fire the handler.
	select {
	case extra := <-received:
		t.Fatalf("unexpected delivery for non-matching kind: %#v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}
