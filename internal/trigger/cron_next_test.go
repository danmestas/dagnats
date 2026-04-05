// trigger/cron_next_test.go
// Tests for CronExpr.NextN: computing upcoming fire times from a reference.
// Methodology: unit tests with known cron expressions and fixed reference times.
package trigger

import (
	"testing"
	"time"
)

func TestNextN_EveryMinute(t *testing.T) {
	expr, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ref := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	times := expr.NextN(ref, 3)
	// Positive: returns exactly 3 times
	if len(times) != 3 {
		t.Fatalf("got %d times, want 3", len(times))
	}
	// Positive: first fire is at 12:01
	want := time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC)
	if !times[0].Equal(want) {
		t.Fatalf("first = %v, want %v", times[0], want)
	}
	// Negative: times are strictly ascending
	for i := 1; i < len(times); i++ {
		if !times[i].After(times[i-1]) {
			t.Fatalf("times[%d] not after times[%d]", i, i-1)
		}
	}
}

func TestNextN_HourlyAtZero(t *testing.T) {
	expr, err := ParseCron("0 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ref := time.Date(2026, 4, 1, 12, 30, 0, 0, time.UTC)
	times := expr.NextN(ref, 2)
	// Positive: first fire is at 13:00
	want := time.Date(2026, 4, 1, 13, 0, 0, 0, time.UTC)
	if !times[0].Equal(want) {
		t.Fatalf("first = %v, want %v", times[0], want)
	}
	// Positive: second fire is at 14:00
	want2 := time.Date(2026, 4, 1, 14, 0, 0, 0, time.UTC)
	if !times[1].Equal(want2) {
		t.Fatalf("second = %v, want %v", times[1], want2)
	}
}

func TestNextN_ZeroCount(t *testing.T) {
	expr, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ref := time.Now()
	times := expr.NextN(ref, 0)
	// Positive: returns empty slice
	if len(times) != 0 {
		t.Fatalf("got %d times, want 0", len(times))
	}
	// Negative: no nil
	if times == nil {
		t.Fatal("expected non-nil empty slice")
	}
}
