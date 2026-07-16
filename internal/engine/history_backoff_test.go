// internal/engine/history_backoff_test.go
// Pure unit tests (no NATS) for the WORKFLOW_HISTORY redelivery schedule
// helpers (#508). Methodology: red-green TDD. Each test asserts both a
// positive property (the expected value/behavior) and a negative property
// (a boundary or invariant that must NOT be violated).
package engine

import (
	"testing"
	"time"
)

// TestHistoryRedeliverDelay_MatchesSchedule verifies historyRedeliverDelay
// indexes the schedule 1-based for every in-range delivery, and clamps to
// the last entry beyond the schedule length instead of panicking.
func TestHistoryRedeliverDelay_MatchesSchedule(t *testing.T) {
	schedule := historyRedeliverSchedule

	// Positive: every numDelivered in [1, len(schedule)] returns the
	// exact schedule[n-1] entry.
	for n := 1; n <= len(schedule); n++ {
		got := historyRedeliverDelay(schedule, uint64(n))
		want := schedule[n-1]
		if got != want {
			t.Fatalf(
				"historyRedeliverDelay(schedule, %d) = %v, want %v",
				n, got, want,
			)
		}
	}

	// Negative (boundary): numDelivered beyond the schedule length must
	// clamp to the last entry, not index out of range.
	beyond := historyRedeliverDelay(schedule, uint64(len(schedule)+5))
	last := schedule[len(schedule)-1]
	if beyond != last {
		t.Fatalf(
			"historyRedeliverDelay beyond schedule = %v, want clamp to %v",
			beyond, last,
		)
	}
}

// TestHistoryRedeliverDelay_ZeroDeliveredPanics verifies numDelivered==0
// is a programmer error (NATS never delivers with 0) and panics with a
// non-empty message rather than silently returning a bogus delay.
func TestHistoryRedeliverDelay_ZeroDeliveredPanics(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: a panic occurred.
		if r == nil {
			t.Fatal("expected panic on numDelivered=0, got none")
		}
		// Negative: the panic message must not be empty/uninformative.
		msg, ok := r.(string)
		if !ok || msg == "" {
			t.Fatalf("expected non-empty string panic message, got %v", r)
		}
	}()
	historyRedeliverDelay(historyRedeliverSchedule, 0)
}

// TestShouldDeadLetterHistory_Boundary verifies the exact boundary: a
// delivery below the cap never dead-letters, and the cap itself always
// does — off-by-one here would either dead-letter too early (dropping a
// message that still had retries left) or too late (redelivering forever,
// which NATS drops silently once MaxDeliver is exceeded).
func TestShouldDeadLetterHistory_Boundary(t *testing.T) {
	const maxDeliver = 8

	// Positive: exactly at the cap dead-letters.
	if !shouldDeadLetterHistory(maxDeliver, uint64(maxDeliver)) {
		t.Fatal("shouldDeadLetterHistory(8, 8) = false, want true")
	}

	// Negative: one delivery below the cap must NOT dead-letter yet.
	if shouldDeadLetterHistory(maxDeliver, uint64(maxDeliver-1)) {
		t.Fatal("shouldDeadLetterHistory(8, 7) = true, want false")
	}

	// Additional negative: well below the cap must not dead-letter.
	if shouldDeadLetterHistory(maxDeliver, 1) {
		t.Fatal("shouldDeadLetterHistory(8, 1) = true, want false")
	}

	// Additional positive: beyond the cap still dead-letters (defense
	// in depth — should not be reachable given MaxDeliver enforcement).
	if !shouldDeadLetterHistory(maxDeliver, uint64(maxDeliver)+1) {
		t.Fatal("shouldDeadLetterHistory(8, 9) = false, want true")
	}
}

// TestHistoryRedeliverSchedule_LengthDrivesMaxDeliver documents and
// pins the design invariant (#508 design decision C): len(schedule) IS
// the MaxDeliver cap. If someone edits the schedule length without
// intending to change MaxDeliver, this test forces them to touch it
// deliberately.
func TestHistoryRedeliverSchedule_LengthDrivesMaxDeliver(t *testing.T) {
	// Positive: the production schedule has the documented length.
	if len(historyRedeliverSchedule) != 8 {
		t.Fatalf(
			"len(historyRedeliverSchedule) = %d, want 8",
			len(historyRedeliverSchedule),
		)
	}

	// Negative: every entry must be positive — a zero or negative delay
	// would make NakWithDelay redeliver immediately (busy loop).
	for i, d := range historyRedeliverSchedule {
		if d <= 0 {
			t.Fatalf("historyRedeliverSchedule[%d] = %v, want > 0", i, d)
		}
	}
}

// TestWithHistoryRedeliverBackoff_OverridesSchedule verifies the
// functional option installs a caller-supplied schedule and that a
// nil/empty schedule is a no-op (keeps the default).
func TestWithHistoryRedeliverBackoff_OverridesSchedule(t *testing.T) {
	custom := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}

	o := &Orchestrator{historyRedeliverSchedule: historyRedeliverSchedule}
	WithHistoryRedeliverBackoff(custom)(o)

	// Positive: the custom schedule is installed.
	if len(o.historyRedeliverSchedule) != 2 {
		t.Fatalf(
			"len(o.historyRedeliverSchedule) = %d, want 2",
			len(o.historyRedeliverSchedule),
		)
	}
	if o.historyRedeliverSchedule[0] != 10*time.Millisecond {
		t.Fatalf(
			"o.historyRedeliverSchedule[0] = %v, want 10ms",
			o.historyRedeliverSchedule[0],
		)
	}

	// Negative: passing nil must not clear/replace the existing schedule.
	WithHistoryRedeliverBackoff(nil)(o)
	if len(o.historyRedeliverSchedule) != 2 {
		t.Fatalf(
			"nil override changed schedule length to %d, want unchanged 2",
			len(o.historyRedeliverSchedule),
		)
	}
}
