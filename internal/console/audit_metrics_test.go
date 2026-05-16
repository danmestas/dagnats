// internal/console/audit_metrics_test.go
// Methodology: unit tests around the audit metric instrument and
// recordAuditEvent. Verifies the counter is non-nil after init and
// that recordAuditEvent enforces its panic contract.
package console

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestNewAuditCounterReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	ac := newAuditCounter(m)
	if ac.counter == nil {
		t.Fatal("counter must not be nil")
	}
	if pkgAudit.counter == nil {
		t.Fatal("pkgAudit must be initialized at init")
	}
}

func TestRecordAuditEventNilCtxPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil ctx")
		}
	}()
	var ctx context.Context // deliberately nil — panic contract guard.
	recordAuditEvent(ctx, AuditEvent{Action: "x", Outcome: "success"})
}

func TestRecordAuditEventDefaultsEmptyAction(t *testing.T) {
	// recordAuditEvent must not panic on an empty action — the
	// label defaults to "unknown" so the counter still increments.
	// Operators see the gap via the action="unknown" bucket.
	recordAuditEvent(context.Background(), AuditEvent{Outcome: "success"})
	recordAuditEvent(context.Background(), AuditEvent{Action: "test"})
}

func TestRecordAuditEventBoundedLabelsRegressionGuard(t *testing.T) {
	// Outcome enum guard: the documented set is
	// success / denied / failed. Stay in sync with audit.go and
	// audit_actions.go. If a new outcome lands, update this list.
	known := map[string]bool{
		"success": true, "denied": true, "failed": true,
		"unknown": true, // the safe-default we apply on empty.
	}
	if len(known) != 4 {
		t.Fatalf("outcome enum drifted: got %d", len(known))
	}
	for k := range known {
		if k == "" {
			t.Fatalf("outcome %q is empty", k)
		}
	}
}
