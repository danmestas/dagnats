// api/controlplane_spawn_key_test.go
// Tests for idempotent child-run spawn (#803): an optional spawn key makes
// a replayed api.runs.spawn return the existing child instead of minting a
// second one. Methodology: fresh embedded NATS + orchestrator + api per
// test; bounded <=10s waits; each test asserts the positive case (same
// child returned, child exists) and the negative space (the run count did
// not grow, or an unkeyed/different-key spawn still adds a child). The
// Create-succeeded-publish-failed race is simulated by pre-claiming the
// KV slot with a child ID that was never published.
package api

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

const spawnKeyParent = "kp"

// spawnKeyHarness seeds a parent run and a registered child def.
func spawnKeyHarness(t *testing.T) (*cpHarness, string) {
	t.Helper()
	h := newCPHarness(t, true)
	seedRootRun(t, h.svc, spawnKeyParent)
	if _, _, err := h.svc.RegisterRuntimeWorkflow(
		context.Background(), childDef(), spawnKeyParent, false,
	); err != nil {
		t.Fatalf("register child def: %v", err)
	}
	return h, scopeName(spawnKeyParent, "do-step")
}

// activeCount is parent + live children under the seeded root.
func activeCount(t *testing.T, h *cpHarness) int {
	t.Helper()
	n, err := h.svc.countActiveRunsForRoot(
		context.Background(), spawnKeyParent)
	if err != nil {
		t.Fatalf("count active: %v", err)
	}
	return n
}

func TestSpawnKey_SameKeyReturnsSameChild(t *testing.T) {
	h, scoped := spawnKeyHarness(t)
	ctx := context.Background()
	first, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "k1")
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	waitRunStatus(t, h.svc, first, dag.RunStatusRunning)
	second, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "k1")
	if err != nil {
		t.Fatalf("second spawn: %v", err)
	}
	if first == "" || second != first {
		t.Fatalf("same key: got %q then %q, want identical", first, second)
	}
	if got := activeCount(t, h); got != 2 {
		t.Fatalf("active runs = %d, want 2 (parent + one child)", got)
	}
}

func TestSpawnKey_DifferentKeysSpawnTwoChildren(t *testing.T) {
	h, scoped := spawnKeyHarness(t)
	ctx := context.Background()
	a, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "a")
	if err != nil {
		t.Fatalf("spawn a: %v", err)
	}
	b, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "b")
	if err != nil {
		t.Fatalf("spawn b: %v", err)
	}
	if a == b {
		t.Fatalf("different keys returned the same child %q", a)
	}
	waitRunStatus(t, h.svc, a, dag.RunStatusRunning)
	waitRunStatus(t, h.svc, b, dag.RunStatusRunning)
	if got := activeCount(t, h); got != 3 {
		t.Fatalf("active runs = %d, want 3", got)
	}
}

func TestSpawnKey_NoKeyKeepsSpawningDistinctChildren(t *testing.T) {
	h, scoped := spawnKeyHarness(t)
	ctx := context.Background()
	a, _, err := h.svc.SpawnChildRun(
		ctx, scoped, spawnKeyParent, "step", nil)
	if err != nil {
		t.Fatalf("spawn 1: %v", err)
	}
	b, _, err := h.svc.SpawnChildRun(
		ctx, scoped, spawnKeyParent, "step2", nil)
	if err != nil {
		t.Fatalf("spawn 2: %v", err)
	}
	if a == b || a == "" || b == "" {
		t.Fatalf("unkeyed spawns must differ: %q %q", a, b)
	}
	waitRunStatus(t, h.svc, a, dag.RunStatusRunning)
	waitRunStatus(t, h.svc, b, dag.RunStatusRunning)
}

// TestSpawnKey_CreateSucceededPublishFailedRetry: the claim exists but the
// spawn event was never published (crash between Create and publish). The
// retry must return the claimed ID AND publish so exactly one child exists.
func TestSpawnKey_CreateSucceededPublishFailedRetry(t *testing.T) {
	h, scoped := spawnKeyHarness(t)
	ctx := context.Background()
	const orphan = "orphan-child-run-id"
	slot := spawnKeySlot(spawnKeyParent, "step", "k1")
	if _, err := h.svc.idempotencyKV.Create(
		ctx, slot, []byte(orphan)); err != nil {
		t.Fatalf("pre-claim: %v", err)
	}
	got, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "k1")
	if err != nil {
		t.Fatalf("retry spawn: %v", err)
	}
	if got != orphan {
		t.Fatalf("retry returned %q, want claimed %q", got, orphan)
	}
	waitRunStatus(t, h.svc, orphan, dag.RunStatusRunning)
	again, _, err := h.svc.SpawnChildRunKeyed(
		ctx, scoped, spawnKeyParent, "step", nil, "k1")
	if err != nil || again != orphan {
		t.Fatalf("second retry = %q, %v; want %q", again, err, orphan)
	}
	if n := activeCount(t, h); n != 2 {
		t.Fatalf("active runs = %d, want exactly parent + 1 child", n)
	}
}

func TestSpawnKey_RejectsOversizedKey(t *testing.T) {
	h, scoped := spawnKeyHarness(t)
	big := strings.Repeat("x", spawnKeyMaxBytes+1)
	id, kind, err := h.svc.SpawnChildRunKeyed(
		context.Background(), scoped, spawnKeyParent, "step", nil, big)
	if err == nil || id != "" {
		t.Fatalf("oversized key accepted: id=%q err=%v", id, err)
	}
	if kind != cpKindInvalidDef {
		t.Fatalf("kind = %q, want %q", kind, cpKindInvalidDef)
	}
	if n := activeCount(t, h); n != 1 {
		t.Fatalf("active runs = %d, want 1 (no child spawned)", n)
	}
}

func TestSpawnKey_SlotIsBoundedSubjectSafeAndScoped(t *testing.T) {
	a := spawnKeySlot("r", "s", strings.Repeat("é.>*", 50))
	b := spawnKeySlot("r", "s2", strings.Repeat("é.>*", 50))
	if a == b {
		t.Fatal("slot must depend on parent step")
	}
	if len(a) > 80 {
		t.Fatalf("slot %q not bounded", a)
	}
	if !strings.HasPrefix(a, "spawn.") {
		t.Fatalf("slot %q must carry the spawn. namespace", a)
	}
	rest := strings.TrimPrefix(a, "spawn.")
	if strings.ContainsAny(rest, ".>*") {
		t.Fatalf("slot suffix %q has subject metacharacters", rest)
	}
}

// TestSpawnKey_WireReplayThroughWorkerHandle drives the full path: a gated
// handler calls StartRun twice with WithSpawnKey (the replayed tool call)
// and once without; keyed calls agree, the unkeyed call differs.
func TestSpawnKey_WireReplayThroughWorkerHandle(t *testing.T) {
	h := newCPHarness(t, true)
	var mu sync.Mutex
	var keyed1, keyed2, plain string
	h.w.Handle("plan-task", func(ctx worker.TaskContext) error {
		cp := ctx.ControlPlane()
		if cp == nil {
			return ctx.Fail(errors.New("nil control plane"))
		}
		name, err := cp.RegisterWorkflow(
			ctx.Context(), childDef(), worker.RegisterOpts{})
		if err != nil {
			return ctx.Fail(err)
		}
		a, errA := cp.StartRun(ctx.Context(), name, nil,
			worker.WithSpawnKey("call-1"))
		b, errB := cp.StartRun(ctx.Context(), name, nil,
			worker.WithSpawnKey("call-1"))
		c, errC := cp.StartRun(ctx.Context(), name, nil)
		if err := errors.Join(errA, errB, errC); err != nil {
			return ctx.Fail(err)
		}
		mu.Lock()
		keyed1, keyed2, plain = a, b, c
		mu.Unlock()
		return ctx.Complete(nil)
	})
	h.w.Handle("child-work", func(ctx worker.TaskContext) error {
		return ctx.Complete(nil)
	})
	h.w.Start()
	t.Cleanup(h.w.Stop)
	if err := h.svc.RegisterWorkflow(
		context.Background(), plannerDef()); err != nil {
		t.Fatalf("register planner: %v", err)
	}
	parent, err := h.svc.StartRun(context.Background(), "planner", nil)
	if err != nil {
		t.Fatalf("start planner: %v", err)
	}
	waitRunStatus(t, h.svc, parent, dag.RunStatusCompleted)
	mu.Lock()
	defer mu.Unlock()
	if keyed1 == "" || keyed1 != keyed2 {
		t.Fatalf("keyed replay: %q vs %q, want identical", keyed1, keyed2)
	}
	if plain == "" || plain == keyed1 {
		t.Fatalf("unkeyed call = %q, want a distinct child (keyed %q)",
			plain, keyed1)
	}
}
