// examples/external-trigger-type/filewatcher/filewatcher_test.go
// Methodology: real embedded NATS + real engine-side TriggerService.
// Each test spins up its own Service against an isolated NATS server
// so trigger_types KV state cannot leak between subtests. Waits are
// bounded by context timeouts and channel selects with explicit
// time.After deadlines — no naked sleeps.
//
// Two slices covered:
//   - TestFilewatcher_FiresOnFileCreate: end-to-end activate path. A
//     trigger lands in the triggers KV, the worker's onActivate spins
//     up an fsnotify watcher, a temp-dir file create produces a
//     workflow.started event on the history.* subject.
//   - TestFilewatcher_RestartIsIdempotent: second start with the same
//     Version="1" + same OwnerWorkerID + same ConfigSchema returns
//     nil at the ack-micro layer (issue #350 audit constraint).
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// newJSForTest is a thin wrapper around jetstream.New that returns the
// error verbatim so test helpers stay focused. Centralised so the
// import lives in exactly one place.
func newJSForTest(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}

// startTestEngine spins up an embedded NATS server, provisions the
// streams + KV buckets the filewatcher example depends on, and starts
// a real TriggerService so the ack-micro path is exercised end-to-end.
// Returns the connection. The test t.Cleanup chain handles shutdown.
func startTestEngine(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc, err := trigger.NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("TriggerService.Start: %v", err)
	}
	t.Cleanup(svc.Stop)
	return nc
}

// putTriggerActual writes a TriggerDef directly into the triggers KV
// bucket. Simulates the engine-side trigger creation that drives the
// worker's activate path on the WatchTriggers catch-up scan.
func putTriggerActual(t *testing.T, nc *nats.Conn, def trigger.TriggerDef) {
	t.Helper()
	js, err := newJSForTest(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	kv, err := js.KeyValue(ctx, "triggers")
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

// TestFilewatcher_FiresOnFileCreate exercises the end-to-end fire
// path. After start() the worker registers the kind and the catch-up
// scan activates the pre-populated trigger; touching the watched
// directory produces a workflow.started on history.<run-id>.
func TestFilewatcher_FiresOnFileCreate(t *testing.T) {
	nc := startTestEngine(t)
	dir := t.TempDir()

	// Subscribe to history.> on the embedded server BEFORE start so
	// the test cannot miss a fast-arriving workflow.started.
	startedCh := make(chan protocol.Event, 4)
	sub, err := nc.Subscribe("history.>", func(msg *nats.Msg) {
		var evt protocol.Event
		if uerr := json.Unmarshal(msg.Data, &evt); uerr != nil {
			t.Logf("history decode: %v", uerr)
			return
		}
		if evt.Type != protocol.EventWorkflowStarted {
			return
		}
		select {
		case startedCh <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe history.>: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Pre-populate the triggers KV with one enabled filewatcher entry
	// pointed at the temp dir. The catch-up scan inside WatchTriggers
	// will fire onActivate once the worker boots.
	cfg, err := json.Marshal(filewatcherConfig{
		Path:   dir,
		Events: []string{"create", "write"},
	})
	if err != nil {
		t.Fatalf("marshal cfg: %v", err)
	}
	putTriggerActual(t, nc, trigger.TriggerDef{
		ID: "fw-create-1", WorkflowID: "wf-fw", Enabled: true,
		External: &trigger.ExternalTriggerConfig{
			Kind:   filewatcherKind,
			Config: cfg,
		},
	})

	svc := newService(nc)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	if err := svc.start(ctx); err != nil {
		t.Fatalf("svc.start: %v", err)
	}
	t.Cleanup(svc.stop)

	// Give fsnotify a moment to install the kernel-level watch before
	// the file create races it. 100ms is well above the fsnotify
	// upstream guidance of "let the watcher initialise" — without it
	// the create can land before the watcher hooks the inode.
	time.Sleep(100 * time.Millisecond)

	target := filepath.Join(dir, "hello.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Positive: workflow.started arrives within 3s with the expected
	// trigger envelope payload.
	select {
	case evt := <-startedCh:
		if evt.RunID == "" {
			t.Fatalf("workflow.started has empty RunID")
		}
		var env trigger.TriggerEnvelope
		if err := json.Unmarshal(evt.Payload, &env); err != nil {
			t.Fatalf("envelope decode: %v", err)
		}
		if env.WorkflowID != "wf-fw" {
			t.Fatalf("envelope.WorkflowID = %q, want wf-fw",
				env.WorkflowID)
		}
		if env.Trigger != filewatcherKind {
			t.Fatalf("envelope.Trigger = %q, want %q",
				env.Trigger, filewatcherKind)
		}
		var fp firePayload
		if err := json.Unmarshal(env.Data, &fp); err != nil {
			t.Fatalf("fire payload decode: %v", err)
		}
		// Negative: payload reports the actual filename and a
		// non-empty event name. Empty would indicate a bug in opName.
		if fp.Event == "" {
			t.Fatalf("fire payload event is empty")
		}
		if filepath.Base(fp.Path) != "hello.txt" {
			t.Fatalf("fire payload path = %q, want basename hello.txt",
				fp.Path)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("workflow.started not observed within 3s")
	}
}

// TestFilewatcher_RestartIsIdempotent encodes the #350 audit-locked
// constraint: re-running the example with the same Version="1" and
// the same stable OwnerWorkerID is a no-op at the engine-ack layer.
// A bumped version or randomized worker ID would error here. The test
// stops and re-starts within the same NATS server so the engine's
// installExternalRegistrar idempotency path is exercised directly.
func TestFilewatcher_RestartIsIdempotent(t *testing.T) {
	nc := startTestEngine(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	svc1 := newService(nc)
	if err := svc1.start(ctx); err != nil {
		t.Fatalf("first start: %v", err)
	}
	// Stop before the second start so the worker SDK subscriptions
	// are torn down, mirroring a real binary restart.
	svc1.stop()

	// Positive: a second start with identical Version + OwnerWorkerID
	// + ConfigSchema returns nil. This is the audit-locked path that
	// keeps rebuilds from tripping the Phase 2.7 version-mismatch
	// hard error (#351).
	svc2 := newService(nc)
	t.Cleanup(svc2.stop)
	if err := svc2.start(ctx); err != nil {
		t.Fatalf("idempotent re-start: %v", err)
	}

	// Negative: the trigger_types KV still records the stable
	// OwnerWorkerID. A randomized boot ID would have replaced it.
	js, err := newJSForTest(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := js.KeyValue(ctx, "trigger_types")
	if err != nil {
		t.Fatalf("KeyValue(trigger_types): %v", err)
	}
	entry, err := kv.Get(ctx, filewatcherKind)
	if err != nil {
		t.Fatalf("kv.Get(%q): %v", filewatcherKind, err)
	}
	var got trigger.TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.OwnerWorkerID != filewatcherWorkerID {
		t.Fatalf("OwnerWorkerID = %q, want stable %q",
			got.OwnerWorkerID, filewatcherWorkerID)
	}
	if got.Version != filewatcherVersion {
		t.Fatalf("Version = %q, want stable %q",
			got.Version, filewatcherVersion)
	}
}
