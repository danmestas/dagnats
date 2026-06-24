// trigger/registrar_external_test.go
// Methodology: real embedded NATS server, real TriggerService. Tests
// cover the `_REGISTRY.trigger_types.ack` micro endpoint and the
// ExternalRegistrar's idempotency + boot-scan + activate-bridge
// contracts (#327, parent #273 Phase 2.3).
//
// Bounded waits via t.Context-bound timeouts so a hung NATS server
// surfaces as a test failure within seconds, not a CI hang.
package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startExternalSvc spins up an embedded NATS server, provisions the
// triggers + trigger_types buckets and returns a started
// TriggerService. Shared by every test in this file so the boilerplate
// stays in one place.
func startExternalSvc(t *testing.T) (*nats.Conn, *TriggerService) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc, err := NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(svc.Stop)
	return nc, svc
}

// putTriggerType writes a TriggerTypeDef directly into the
// trigger_types KV bucket (simulates the worker side of
// RegisterTriggerType: the worker Puts first, then asks the engine to
// ack).
func putTriggerType(t *testing.T, nc *nats.Conn, tdef TriggerTypeDef) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	kv, err := js.KeyValue(ctx, "trigger_types")
	if err != nil {
		t.Fatalf("KeyValue(trigger_types): %v", err)
	}
	data, err := json.Marshal(tdef)
	if err != nil {
		t.Fatalf("marshal tdef: %v", err)
	}
	if _, err := kv.Put(ctx, tdef.Name, data); err != nil {
		t.Fatalf("kv.Put: %v", err)
	}
}

// ackResponseFromMsg parses an ack reply. Empty body = success.
func ackResponseFromMsg(t *testing.T, msg *nats.Msg) ackResponse {
	t.Helper()
	if msg == nil {
		t.Fatal("ack reply is nil")
	}
	if len(msg.Data) == 0 {
		return ackResponse{}
	}
	var resp ackResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("decode ack response: %v; raw=%s", err, string(msg.Data))
	}
	return resp
}

// requestAck wraps the request/reply call to the ack subject with a
// bounded timeout. Test helpers do not panic — they Fatal so the test
// name is the failure context.
func requestAck(
	t *testing.T, nc *nats.Conn, req RegisterTriggerTypeRequest,
) ackResponse {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 1*time.Second,
	)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, ackSubject, body)
	if err != nil {
		t.Fatalf("request %s: %v", ackSubject, err)
	}
	return ackResponseFromMsg(t, msg)
}

func TestRegisterTriggerType_Ack(t *testing.T) {
	nc, svc := startExternalSvc(t)

	tdef := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		Description:   "Filesystem watcher",
		ConfigSchema: json.RawMessage(
			`{"type":"object","required":["path"]}`,
		),
		Version:      "1.0.0",
		RegisteredAt: time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)

	// Positive: ack returns success within 1s.
	resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	})
	if resp.Error != "" {
		t.Fatalf("ack returned error: %q", resp.Error)
	}

	// Positive: registrar was installed under "external::<kind>".
	svc.mu.RLock()
	reg, ok := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()
	if !ok {
		t.Fatal("registrar not installed after ack")
	}
	ext, isExt := reg.(*externalRegistrar)
	if !isExt {
		t.Fatalf("registrar is %T, want *externalRegistrar", reg)
	}
	// Negative: ownerWorkerID is captured for observability.
	if ext.ownerWorkerID != tdef.OwnerWorkerID {
		t.Fatalf("ownerWorkerID = %q, want %q",
			ext.ownerWorkerID, tdef.OwnerWorkerID)
	}
}

func TestRegisterTriggerType_Idempotent(t *testing.T) {
	nc, svc := startExternalSvc(t)

	tdef := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema: json.RawMessage(
			`{"type":"object"}`,
		),
		Version:      "1.0.0",
		RegisteredAt: time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)

	req := RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	}
	// First call: success.
	if resp := requestAck(t, nc, req); resp.Error != "" {
		t.Fatalf("first ack errored: %q", resp.Error)
	}
	svc.mu.RLock()
	first := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()

	// Second call with identical Name + schema: success, no
	// replacement of the registrar (pointer identity preserved).
	if resp := requestAck(t, nc, req); resp.Error != "" {
		t.Fatalf("second ack errored: %q", resp.Error)
	}
	svc.mu.RLock()
	second := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()

	// Positive: same instance — idempotent re-register.
	if first != second {
		t.Fatal("second ack replaced the registrar; idempotency broken")
	}
}

func TestRegisterTriggerType_ConflictingSchema(t *testing.T) {
	nc, svc := startExternalSvc(t)

	tdef := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema: json.RawMessage(
			`{"type":"object","required":["path"]}`,
		),
		Version:      "1.0.0",
		RegisteredAt: time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)
	if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	}); resp.Error != "" {
		t.Fatalf("initial ack errored: %q", resp.Error)
	}

	// Mutate the KV record's ConfigSchema, leaving Name+Owner intact,
	// then re-ack. The engine must reject because the registrar
	// already bound a different schema.
	tdef.ConfigSchema = json.RawMessage(
		`{"type":"object","required":["different"]}`,
	)
	putTriggerType(t, nc, tdef)

	resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	})
	// Positive: ack returns an error.
	if resp.Error == "" {
		t.Fatal("expected error on schema mismatch, got success")
	}
	// Negative: original registrar is still installed (engine state
	// must not be corrupted by a failed re-register).
	svc.mu.RLock()
	_, stillOK := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()
	if !stillOK {
		t.Fatal("registrar removed by failed re-register")
	}
}

// TestExternalRegistrar_FiresExistingEntriesOnStart pre-populates the
// triggers bucket with an External entry, then calls ack. The
// registrar must fire Activate (→ `_TRIGGER.<kind>.activate` request)
// for that entry without further operator action.
func TestExternalRegistrar_FiresExistingEntriesOnStart(t *testing.T) {
	nc, _ := startExternalSvc(t)

	// Stand up a fake owner worker that subscribes to the activate
	// subject and counts requests.
	const kind = "fs.watch"
	var activateCount atomic.Int32
	sub, err := nc.Subscribe(
		"_TRIGGER."+kind+".activate",
		func(msg *nats.Msg) {
			activateCount.Add(1)
			_ = msg.Respond(nil)
		},
	)
	if err != nil {
		t.Fatalf("subscribe activate: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Seed the triggers bucket with an External entry BEFORE ack so
	// fireExistingEntries has something to find.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	triggerKV, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("KeyValue(triggers): %v", err)
	}
	def := TriggerDef{
		ID:         "ext-1",
		WorkflowID: "wf",
		Enabled:    true,
		External: &ExternalTriggerConfig{
			Kind:   kind,
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	defBytes, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := triggerKV.Put(ctx, def.ID, defBytes); err != nil {
		t.Fatalf("triggers Put: %v", err)
	}

	// Now register the type so the engine allocates the registrar
	// and fires existing entries.
	tdef := TriggerTypeDef{
		Name:          kind,
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema:  json.RawMessage(`{"type":"object"}`),
		Version:       "1.0.0",
		RegisteredAt:  time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)
	if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	}); resp.Error != "" {
		t.Fatalf("ack errored: %q", resp.Error)
	}

	// Positive: the owner worker saw the activate within 1s.
	deadline := time.After(1 * time.Second)
	for activateCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("fake worker did not receive activate within 1s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Negative: no spurious extra activations after the first.
	time.Sleep(100 * time.Millisecond)
	if got := activateCount.Load(); got != 1 {
		t.Fatalf("activate count = %d, want 1", got)
	}
}

// TestExternalRegistrar_Q4_StickyOwnerReceivesTask is the Q4
// verification — narrow scope appropriate for Phase 2.3: assert that
// (a) the ExternalRegistrar bridges to the owner worker's subject and
// (b) when the External trigger's TriggerDef is in the `triggers` KV
// with an existing entry, the OWNER worker is the one whose
// `_TRIGGER.<kind>.activate` subject sees the request. This is the
// engine-side half of the sticky-routing chain. The full
// trigger-fire-to-task-execution e2e lives behind Phase 2.4 (worker
// SDK) and is out of scope here.
func TestExternalRegistrar_Q4_StickyOwnerReceivesTask(t *testing.T) {
	nc, _ := startExternalSvc(t)

	// Two competing subscribers: only the owner-worker subject should
	// be addressed by activate. We stand up a "wrong worker" sub on a
	// different kind to prove the bridge is kind-scoped.
	const kind = "fs.watch"
	var ownerHits atomic.Int32
	var otherHits atomic.Int32
	ownerSub, err := nc.Subscribe(
		"_TRIGGER."+kind+".activate",
		func(msg *nats.Msg) {
			ownerHits.Add(1)
			_ = msg.Respond(nil)
		},
	)
	if err != nil {
		t.Fatalf("owner subscribe: %v", err)
	}
	defer func() { _ = ownerSub.Unsubscribe() }()
	otherSub, err := nc.Subscribe(
		"_TRIGGER.other.kind.activate",
		func(msg *nats.Msg) {
			otherHits.Add(1)
			_ = msg.Respond(nil)
		},
	)
	if err != nil {
		t.Fatalf("other subscribe: %v", err)
	}
	defer func() { _ = otherSub.Unsubscribe() }()

	// Register both kinds.
	for _, name := range []string{kind, "other.kind"} {
		putTriggerType(t, nc, TriggerTypeDef{
			Name:          name,
			OwnerWorkerID: "worker-" + name,
			ConfigSchema:  json.RawMessage(`{"type":"object"}`),
			Version:       "1.0.0",
			RegisteredAt:  time.Now().UTC(),
		})
		if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
			Name:          name,
			OwnerWorkerID: "worker-" + name,
		}); resp.Error != "" {
			t.Fatalf("ack %q errored: %q", name, resp.Error)
		}
	}

	// Insert a TriggerDef for the fs.watch kind via the triggers KV.
	// The KV watcher must pick it up and addTrigger → Activate fires
	// the bridge to the owner worker subject.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	triggerKV, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("KeyValue(triggers): %v", err)
	}
	def := TriggerDef{
		ID:         "ext-sticky",
		WorkflowID: "wf-sticky",
		Enabled:    true,
		External: &ExternalTriggerConfig{
			Kind:   kind,
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	defBytes, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := triggerKV.Put(ctx, def.ID, defBytes); err != nil {
		t.Fatalf("triggers Put: %v", err)
	}

	// Positive: owner subject saw the activate within 2s. The watcher
	// delivers asynchronously so we poll.
	deadline := time.After(2 * time.Second)
	for ownerHits.Load() < 1 {
		select {
		case <-deadline:
			t.Fatalf(
				"owner did not receive activate within 2s "+
					"(owner=%d, other=%d)",
				ownerHits.Load(), otherHits.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Negative: the unrelated kind's subscriber was NOT addressed.
	if got := otherHits.Load(); got != 0 {
		t.Fatalf("other kind received %d activations, want 0", got)
	}
}

// putTriggerDef writes a TriggerDef directly into the triggers KV.
// Test helper for the #351 version-bump tests.
func putTriggerDef(t *testing.T, nc *nats.Conn, def TriggerDef) {
	t.Helper()
	js, err := jetstream.New(nc)
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
		t.Fatalf("triggers Put: %v", err)
	}
}

// TestRegisterTriggerType_IdempotentWithVersion asserts the short-circuit
// path (#351): identical Version on re-register skips the live-trigger
// scan entirely. Verified by installing the scan counter and asserting
// it is never invoked on the second ack.
func TestRegisterTriggerType_IdempotentWithVersion(t *testing.T) {
	nc, svc := startExternalSvc(t)

	tdef := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema:  json.RawMessage(`{"type":"object"}`),
		Version:       "1.0.0",
		RegisteredAt:  time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)

	req := RegisterTriggerTypeRequest{
		Name:          tdef.Name,
		OwnerWorkerID: tdef.OwnerWorkerID,
	}
	if resp := requestAck(t, nc, req); resp.Error != "" {
		t.Fatalf("first ack errored: %q", resp.Error)
	}

	// Install the scan counter AFTER the first ack so the boot-time
	// fire-existing scan does not pollute the count.
	var scanCalls atomic.Int32
	oldCounter := liveTriggersScanCounter
	liveTriggersScanCounter = func() { scanCalls.Add(1) }
	t.Cleanup(func() { liveTriggersScanCounter = oldCounter })

	// Second ack with identical version: must short-circuit before any
	// KV scan.
	if resp := requestAck(t, nc, req); resp.Error != "" {
		t.Fatalf("second ack errored: %q", resp.Error)
	}
	if got := scanCalls.Load(); got != 0 {
		t.Fatalf("scan counter = %d on version-match short-circuit, "+
			"want 0", got)
	}

	// Positive: registrar still installed; pointer identity preserved.
	svc.mu.RLock()
	reg := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()
	if reg == nil {
		t.Fatal("registrar disappeared after idempotent ack")
	}
	ext, ok := reg.(*externalRegistrar)
	if !ok {
		t.Fatalf("registrar is %T, want *externalRegistrar", reg)
	}
	if ext.version != "1.0.0" {
		t.Fatalf("registrar.version = %q, want %q",
			ext.version, "1.0.0")
	}
}

// TestRegisterTriggerType_VersionBumpAllowedWhenNoLiveTriggers covers
// the #351 happy-path overwrite: version differs, no live triggers
// exist, registrar is replaced with the new version cleanly.
func TestRegisterTriggerType_VersionBumpAllowedWhenNoLiveTriggers(
	t *testing.T,
) {
	nc, svc := startExternalSvc(t)

	tdef := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema:  json.RawMessage(`{"type":"object"}`),
		Version:       "1.0.0",
		RegisteredAt:  time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)
	if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name: tdef.Name, OwnerWorkerID: tdef.OwnerWorkerID,
	}); resp.Error != "" {
		t.Fatalf("v1 ack errored: %q", resp.Error)
	}

	// Bump version. Same schema bytes — only Version changes. With
	// zero live triggers of this kind in the bucket, the second ack
	// must succeed and the registrar must reflect the new version.
	tdef.Version = "2.0.0"
	putTriggerType(t, nc, tdef)
	if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name: tdef.Name, OwnerWorkerID: tdef.OwnerWorkerID,
	}); resp.Error != "" {
		t.Fatalf("v2 ack rejected with no live triggers: %q",
			resp.Error)
	}
	svc.mu.RLock()
	reg := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()
	ext, ok := reg.(*externalRegistrar)
	if !ok {
		t.Fatalf("registrar is %T, want *externalRegistrar", reg)
	}
	if ext.version != "2.0.0" {
		t.Fatalf("registrar.version = %q, want %q",
			ext.version, "2.0.0")
	}
}

// TestRegisterTriggerType_VersionBumpRejectedWithLiveTriggers covers
// the #351 hard-error path: version differs and ≥1 live trigger of the
// kind exists. Must reject with a clear error and leave the existing
// registrar untouched.
func TestRegisterTriggerType_VersionBumpRejectedWithLiveTriggers(
	t *testing.T,
) {
	nc, svc := startExternalSvc(t)

	const kind = "fs.watch"
	// Stand up a fake owner worker so fireExistingEntries' Activate
	// request has someone to talk to (otherwise it logs and skips,
	// which is also fine — but the activate-reply makes the test
	// less timing-sensitive).
	sub, err := nc.Subscribe(
		"_TRIGGER."+kind+".activate",
		func(msg *nats.Msg) { _ = msg.Respond(nil) },
	)
	if err != nil {
		t.Fatalf("subscribe activate: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	tdef := TriggerTypeDef{
		Name:          kind,
		OwnerWorkerID: "worker-fs-1",
		ConfigSchema:  json.RawMessage(`{"type":"object"}`),
		Version:       "1.0.0",
		RegisteredAt:  time.Now().UTC(),
	}
	putTriggerType(t, nc, tdef)
	if resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name: tdef.Name, OwnerWorkerID: tdef.OwnerWorkerID,
	}); resp.Error != "" {
		t.Fatalf("v1 ack errored: %q", resp.Error)
	}

	// Insert a live trigger of this kind.
	putTriggerDef(t, nc, TriggerDef{
		ID:         "live-1",
		WorkflowID: "wf",
		Enabled:    true,
		External: &ExternalTriggerConfig{
			Kind:   kind,
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	})

	// Bump version. With ≥1 enabled live trigger, the ack must reject.
	tdef.Version = "2.0.0"
	putTriggerType(t, nc, tdef)
	resp := requestAck(t, nc, RegisterTriggerTypeRequest{
		Name: tdef.Name, OwnerWorkerID: tdef.OwnerWorkerID,
	})
	if resp.Error == "" {
		t.Fatal("expected error on version bump with live triggers")
	}
	// Negative: the existing registrar must still be installed at the
	// old version. Failed re-register cannot corrupt state.
	svc.mu.RLock()
	reg := svc.registrars[externalKindPrefix+tdef.Name]
	svc.mu.RUnlock()
	ext, ok := reg.(*externalRegistrar)
	if !ok {
		t.Fatalf("registrar is %T, want *externalRegistrar", reg)
	}
	if ext.version != "1.0.0" {
		t.Fatalf("registrar.version = %q after failed bump, want %q",
			ext.version, "1.0.0")
	}
}

// TestHasLiveTriggersOfKind_EarlyReturn proves the scan stops at the
// first matching entry rather than draining the whole bucket. We seed
// 50 matching live triggers and assert the scan counter records exactly
// 1 inspection.
//
// The KV's key-iteration order is unspecified, so the test only checks
// the upper bound: scan stops at the first match, not "after key N".
func TestHasLiveTriggersOfKind_EarlyReturn(t *testing.T) {
	nc, svc := startExternalSvc(t)

	const kind = "fs.watch"
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("live-%03d", i)
		putTriggerDef(t, nc, TriggerDef{
			ID:         id,
			WorkflowID: "wf",
			Enabled:    true,
			External: &ExternalTriggerConfig{
				Kind:   kind,
				Config: json.RawMessage(`{}`),
			},
		})
	}

	var scanCalls atomic.Int32
	oldCounter := liveTriggersScanCounter
	liveTriggersScanCounter = func() { scanCalls.Add(1) }
	t.Cleanup(func() { liveTriggersScanCounter = oldCounter })

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	has, err := svc.hasLiveTriggersOfKind(ctx, kind, liveTriggersScanMax)
	if err != nil {
		t.Fatalf("hasLiveTriggersOfKind: %v", err)
	}
	if !has {
		t.Fatal("hasLiveTriggersOfKind = false, want true")
	}
	// Positive: scan stopped at the first match, so the counter is 1.
	if got := scanCalls.Load(); got != 1 {
		t.Fatalf("scan counter = %d, want 1 (early-return broken)", got)
	}
}

// TestHasLiveTriggersOfKind_BoundedScan asserts the explicit scanMax
// bound caps the inspection cost. We populate >scanMax non-matching
// entries (different kind) and a small scanMax, then prove the scan
// returns false within the bound. Running with the production
// liveTriggersScanMax (10000) would make the test slow; we use a
// smaller bound here — the bound is a parameter on
// hasLiveTriggersOfKind precisely to make this testable.
func TestHasLiveTriggersOfKind_BoundedScan(t *testing.T) {
	nc, svc := startExternalSvc(t)

	const kind = "fs.watch"
	// Seed 50 entries of an UNRELATED kind. Scan must not return true.
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("other-%03d", i)
		putTriggerDef(t, nc, TriggerDef{
			ID:         id,
			WorkflowID: "wf",
			Enabled:    true,
			External: &ExternalTriggerConfig{
				Kind:   "other.kind",
				Config: json.RawMessage(`{}`),
			},
		})
	}

	var scanCalls atomic.Int32
	oldCounter := liveTriggersScanCounter
	liveTriggersScanCounter = func() { scanCalls.Add(1) }
	t.Cleanup(func() { liveTriggersScanCounter = oldCounter })

	const scanMax = 10
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	has, err := svc.hasLiveTriggersOfKind(ctx, kind, scanMax)
	if err != nil {
		t.Fatalf("hasLiveTriggersOfKind: %v", err)
	}
	// Positive: no matching kind → no live triggers reported.
	if has {
		t.Fatal("hasLiveTriggersOfKind = true with no matching kind")
	}
	// Negative: the bound capped the scan. The counter must be at most
	// scanMax (it's a strict upper bound). We allow equal to scanMax
	// because the bound-check fires *after* the increment of the
	// scanMax-th entry on the next iteration — see hasLiveTriggersOfKind.
	if got := scanCalls.Load(); got > scanMax {
		t.Fatalf("scan counter = %d, want ≤ %d (scanMax bound broken)",
			got, scanMax)
	}
}
