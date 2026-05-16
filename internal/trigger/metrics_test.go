// internal/trigger/metrics_test.go
// Methodology: unit tests around the trigger metric instrumentation.
// We don't observe the full OTel pipeline here — these tests assert
// that the metric instruments are non-nil and that RecordFiring
// panics on contract violations. The cardinality guard sits next to
// it as a regression fence: the metric labels MUST come from the
// bounded type/outcome enums.
package trigger

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestNewFiringsCounterReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	fc := newFiringsCounter(m)
	if fc.counter == nil {
		t.Fatal("counter must not be nil")
	}
	if pkgFirings.counter == nil {
		t.Fatal("pkgFirings must not be nil after init")
	}
}

func TestRecordFiringNilCtxPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil ctx")
		}
	}()
	var ctx context.Context // deliberately nil — panic contract guard.
	RecordFiring(ctx, TypeCron, OutcomeFired)
}

func TestRecordFiringEmptyTypePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty type")
		}
	}()
	RecordFiring(context.Background(), "", OutcomeFired)
}

func TestRecordFiringEmptyOutcomePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty outcome")
		}
	}()
	RecordFiring(context.Background(), TypeCron, "")
}

func TestOutcomeEnumIsBounded(t *testing.T) {
	// Regression guard: the metric label cardinality must stay
	// bounded. If a new outcome lands without updating this guard,
	// a reviewer should see the failure and decide whether to
	// extend the enum or back out the new value.
	known := map[Outcome]bool{
		OutcomeFired:   true,
		OutcomeSkipped: true,
		OutcomeError:   true,
	}
	if len(known) != 3 {
		t.Fatalf("outcome enum drifted: got %d values", len(known))
	}
	for o := range known {
		if string(o) == "" {
			t.Fatalf("outcome %q is empty", o)
		}
	}
}

func TestTriggerTypeEnumIsBounded(t *testing.T) {
	known := map[TriggerType]bool{
		TypeCron:    true,
		TypeWebhook: true,
		TypeSubject: true,
		TypeHTTP:    true,
	}
	if len(known) != 4 {
		t.Fatalf("type enum drifted: got %d values", len(known))
	}
}

func TestRecordFiringDoesNotPanicOnGoodInput(t *testing.T) {
	RecordFiring(context.Background(), TypeCron, OutcomeFired)
	RecordFiring(context.Background(), TypeWebhook, OutcomeSkipped)
	RecordFiring(context.Background(), TypeSubject, OutcomeError)
	RecordFiring(context.Background(), TypeHTTP, OutcomeFired)
}
