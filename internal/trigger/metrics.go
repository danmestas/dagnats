// internal/trigger/metrics.go
// Metric instruments for trigger firings. Owned by the trigger
// package so every handler can record without plumbing an OTel meter
// through its constructor (each handler type creates / accepts one
// at boot). One counter today: trigger_firings_total{type, outcome}.
// The trigger_id label is intentionally absent — id is bounded but
// would explode the per-series fanout in the metrics aggregator; the
// dashboard groups by (type, outcome) and lists per-id drilldowns via
// the existing trigger-fire stream.
package trigger

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Outcome is the fire-result enum. Bounded to three values so the
// label cardinality stays trivial. Recorded on every fire path; the
// trigger_id is captured in the audit / trigger-fire stream rather
// than as a metric label.
type Outcome string

const (
	// OutcomeFired means the trigger published a workflow event
	// successfully. The most common path.
	OutcomeFired Outcome = "fired"
	// OutcomeSkipped means the trigger matched but a guard caused
	// no fire (debounce window, disabled, dedup).
	OutcomeSkipped Outcome = "skipped"
	// OutcomeError means the trigger handler hit an unrecoverable
	// error while attempting to fire.
	OutcomeError Outcome = "error"
)

// TriggerType is the bounded enum the type-label takes. Mirrors the
// TriggerDef.Type field but is declared here so the metrics package
// can be imported without pulling the rest of trigger.
type TriggerType string

const (
	TypeCron    TriggerType = "cron"
	TypeWebhook TriggerType = "webhook"
	TypeSubject TriggerType = "subject"
	TypeHTTP    TriggerType = "http"
)

// firingsCounter is the package-global counter used by every fire
// site. Created lazily on the first NewTriggerMetrics call. The
// indirection lets test seams swap the counter; production paths
// call the package-global recordFiring directly.
type firingsCounter struct {
	counter metric.Int64Counter
}

// newFiringsCounter builds the counter from the given meter.
// Panics if meter is nil.
func newFiringsCounter(m metric.Meter) firingsCounter {
	if m == nil {
		panic("newFiringsCounter: meter must not be nil")
	}
	c, _ := m.Int64Counter("trigger_firings_total")
	return firingsCounter{counter: c}
}

// pkgFirings is the package-global instrument. Initialized from
// init() via the dagnats/trigger meter so every fire site has a
// non-nil counter without needing to thread it through constructors.
var pkgFirings firingsCounter

func init() {
	pkgFirings = newFiringsCounter(otel.Meter("dagnats/trigger"))
}

// RecordFiring increments the counter with bounded labels. Called
// from every trigger-fire path. ctx may be the request context or
// background — the counter never blocks.
func RecordFiring(
	ctx context.Context, t TriggerType, outcome Outcome,
) {
	if ctx == nil {
		panic("RecordFiring: ctx must not be nil")
	}
	if t == "" {
		panic("RecordFiring: type must not be empty")
	}
	if outcome == "" {
		panic("RecordFiring: outcome must not be empty")
	}
	if pkgFirings.counter == nil {
		return
	}
	pkgFirings.counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("type", string(t)),
		attribute.String("outcome", string(outcome)),
	))
}
