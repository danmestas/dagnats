// internal/trigger/metrics.go
// Metric instruments for trigger firings and scheduler state. Owned
// by the trigger package so every handler can record without
// plumbing an OTel meter through its constructor (each handler type
// creates / accepts one at boot). One counter today:
// trigger_firings_total{type, outcome}. The trigger_id label is
// intentionally absent from that counter — id is bounded but the
// series count there scales with fire volume (potentially
// unbounded over the metric's lifetime), which would explode the
// per-series fanout in the metrics aggregator; the dashboard groups
// by (type, outcome) and lists per-id drilldowns via the existing
// trigger-fire stream.
//
// The two scheduler gauges below (trigger_last_fired_seconds,
// trigger_next_fire_seconds) DO carry a per-trigger `trigger` label,
// and that is not the same cardinality risk: a gauge has exactly one
// active series per *registered* trigger (tens, bounded by how many
// triggers an operator configures), not one per fire. The series
// count is stable regardless of fire volume, so the counter's
// cardinality-avoidance rule does not apply here.
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

// schedulerGauges holds the two per-trigger observable gauges the
// scheduler's OTel callback reports on every collection.
type schedulerGauges struct {
	// lastFired is unix seconds of the most recent cron fire whose
	// Fire call returned nil (OutcomeFired), sourced from
	// Scheduler.firedAt. Scope: cron scheduler only — manual/API
	// fires (which call Fire directly via api.Service, bypassing
	// Scheduler.Tick) never move this gauge. In-process only: empty
	// at boot, populated only by successful fires in the current
	// process, with no seed from trigger_state KV on restart (no
	// production writer of <id>.last_run_at exists today — seeding
	// from it would be dead code; see trigger_next_fire_seconds for
	// the restart-safe alerting signal instead). Omitted entirely
	// for a trigger that has never fired in this process, rather
	// than emitted as 0/epoch.
	lastFired metric.Int64ObservableGauge
	// nextFire is unix seconds of the next matching minute for an
	// enabled cron trigger, computed fresh on every collection from
	// the trigger's cron expression/timezone. Present from the
	// moment a trigger is registered — no fire history required —
	// which makes it the restart-safe signal for alerting
	// (now > next_fire + grace), independent of firedAt/lastFired.
	nextFire metric.Int64ObservableGauge
}

// newSchedulerGauges builds both gauges from the given meter. Panics
// if meter is nil — matches newFiringsCounter's convention.
func newSchedulerGauges(m metric.Meter) schedulerGauges {
	if m == nil {
		panic("newSchedulerGauges: meter must not be nil")
	}
	lastFired, _ := m.Int64ObservableGauge(
		"trigger_last_fired_seconds",
		metric.WithDescription(
			"Unix seconds of the most recent successful cron fire "+
				"per trigger. Cron scheduler only; absent until the "+
				"trigger's first successful fire in this process "+
				"(no restart seed).",
		),
	)
	nextFire, _ := m.Int64ObservableGauge(
		"trigger_next_fire_seconds",
		metric.WithDescription(
			"Unix seconds of the next matching minute for an "+
				"enabled cron trigger. Present from registration; "+
				"restart-safe (no fire history needed).",
		),
	)
	return schedulerGauges{lastFired: lastFired, nextFire: nextFire}
}

// RegisterSchedulerMetrics wires the scheduler's last_fired/next_fire
// gauges to a single callback on the given meter. Panics on nil
// meter or nil scheduler (programmer error). Callers own the
// returned Registration and may Unregister it on shutdown; the
// production caller (NewScheduler) does not, since the scheduler has
// no explicit shutdown path today.
func RegisterSchedulerMetrics(
	m metric.Meter, s *Scheduler,
) (metric.Registration, error) {
	if m == nil {
		panic("RegisterSchedulerMetrics: meter must not be nil")
	}
	if s == nil {
		panic("RegisterSchedulerMetrics: scheduler must not be nil")
	}

	g := newSchedulerGauges(m)
	return m.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			return s.observeMetrics(ctx, o, g)
		},
		g.lastFired, g.nextFire,
	)
}
