// internal/api/enrich_fire_status_test.go
// Tests for enrichFireStatus: verifies a terminal run's duration is
// frozen at CompletedAt-CreatedAt (issue #440 zombie-duration fix),
// not the ever-growing time.Since(CreatedAt) wall-clock age.
// Uses real embedded NATS server; no orchestrator required.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestEnrichFireStatusFrozenDuration(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("frozen-dur-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()

	ctx := context.Background()
	run := dag.NewWorkflowRun(def, "frozen-dur-run")
	created := time.Now().UTC().Add(-20 * time.Hour)
	completed := created.Add(25 * time.Second)
	run.CreatedAt = created
	run.CompletedAt = &completed
	run.Status = dag.RunStatusCompleted
	if err := svc.store.Save(ctx, run); err != nil {
		t.Fatalf("store.Save: %v", err)
	}

	status, dur := svc.enrichFireStatus(ctx, "frozen-dur-run")

	if status != dag.RunStatusCompleted.String() {
		t.Fatalf("status = %q, want %q", status,
			dag.RunStatusCompleted.String())
	}
	// Positive: duration frozen at CompletedAt-CreatedAt.
	if dur != 25*time.Second {
		t.Fatalf("dur = %v, want %v", dur, 25*time.Second)
	}
	// Negative space: must NOT be the ~20h zombie value the bug
	// produced via time.Since(CreatedAt).
	if dur >= 1*time.Hour {
		t.Fatalf("dur = %v is the zombie wall-clock age, want < 1h",
			dur)
	}
}

func TestEnrichFireStatusNilCompletedAtFallback(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("fallback-dur-wf")
	wb.Task("s", "echo")
	def, _ := wb.Build()

	ctx := context.Background()
	// Pre-#443 snapshot: terminal but no CompletedAt stamped.
	run := dag.NewWorkflowRun(def, "fallback-dur-run")
	run.CreatedAt = time.Now().UTC().Add(-30 * time.Second)
	run.Status = dag.RunStatusCompleted
	if err := svc.store.Save(ctx, run); err != nil {
		t.Fatalf("store.Save: %v", err)
	}

	_, dur := svc.enrichFireStatus(ctx, "fallback-dur-run")
	// Best-effort fallback: time.Since(CreatedAt), so at least 30s.
	if dur < 30*time.Second {
		t.Fatalf("dur = %v, want >= 30s fallback", dur)
	}
}
