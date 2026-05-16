// internal/console/audit_metrics.go
// Metric instrument for operator-action audit events. The counter
// is package-global because the audit emitter lives on the
// apiServiceAdapter and the dashboard binding is the only path; no
// other package needs to write it. Labels: action (the verb tag the
// emitter passes in) + outcome ("success" | "denied" | "failed").
// Both come from a bounded set the action layer enumerates — no
// cardinality risk.
package console

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// auditCounter holds the OTel instrument. Created at init() so every
// emit site has a non-nil counter without constructor plumbing.
type auditCounter struct {
	counter metric.Int64Counter
}

// newAuditCounter builds the counter from the given meter. Panics if
// meter is nil — programmer error caught at startup.
func newAuditCounter(m metric.Meter) auditCounter {
	if m == nil {
		panic("newAuditCounter: meter must not be nil")
	}
	c, _ := m.Int64Counter("console_audit_events_total")
	return auditCounter{counter: c}
}

// pkgAudit is the package-global counter the audit emit path uses.
var pkgAudit auditCounter

func init() {
	pkgAudit = newAuditCounter(otel.Meter("dagnats/console"))
}

// recordAuditEvent increments the audit counter with bounded labels.
// Called from the apiServiceAdapter EmitAuditEvent path after the KV
// put. Labels come from the AuditEvent's Action + Outcome — both
// already constrained to a closed enum at the action layer (see
// audit_actions.go).
func recordAuditEvent(ctx context.Context, evt AuditEvent) {
	if ctx == nil {
		panic("recordAuditEvent: ctx must not be nil")
	}
	if pkgAudit.counter == nil {
		return
	}
	action := evt.Action
	if action == "" {
		action = "unknown"
	}
	outcome := evt.Outcome
	if outcome == "" {
		outcome = "unknown"
	}
	pkgAudit.counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("action", action),
		attribute.String("outcome", outcome),
	))
}
