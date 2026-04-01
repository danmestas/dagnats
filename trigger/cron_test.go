package trigger

// Methodology: unit tests for cron expression parsing and matching.
// No NATS dependency — pure time logic.

import (
	"testing"
	"time"
)

func TestParseCronEveryMinute(t *testing.T) {
	expr, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: matches any time
	now := time.Date(2026, 3, 31, 14, 30, 0, 0, time.UTC)
	if !expr.Matches(now) {
		t.Fatalf("* * * * * should match any time")
	}

	// Positive: 60 minute values
	if len(expr.Minutes) != 60 {
		t.Fatalf("minutes = %d, want 60", len(expr.Minutes))
	}
}

func TestParseCronWeekdayMorning(t *testing.T) {
	expr, err := ParseCron("0 9 * * 1-5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: Monday 9am matches
	mon9 := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC) // Monday
	if !expr.Matches(mon9) {
		t.Fatalf("should match Monday 9am")
	}

	// Negative: Sunday 9am does not match
	sun9 := time.Date(2026, 3, 29, 9, 0, 0, 0, time.UTC) // Sunday
	if expr.Matches(sun9) {
		t.Fatalf("should not match Sunday")
	}

	// Negative: Monday 10am does not match
	mon10 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if expr.Matches(mon10) {
		t.Fatalf("should not match 10am")
	}
}

func TestParseCronEvery5Minutes(t *testing.T) {
	expr, err := ParseCron("*/5 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: minute 0 matches
	if !expr.Matches(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("minute 0 should match */5")
	}

	// Positive: minute 15 matches
	if !expr.Matches(time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)) {
		t.Fatalf("minute 15 should match */5")
	}

	// Negative: minute 3 does not match
	if expr.Matches(time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)) {
		t.Fatalf("minute 3 should not match */5")
	}
}

func TestParseCronCommaList(t *testing.T) {
	expr, err := ParseCron("0,30 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: minute 0 and 30 match
	if len(expr.Minutes) != 2 {
		t.Fatalf("minutes = %d, want 2", len(expr.Minutes))
	}

	// Negative: minute 15 does not
	if expr.Matches(time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)) {
		t.Fatalf("minute 15 should not match 0,30")
	}
}

func TestParseCronRejectsInvalid(t *testing.T) {
	bad := []string{
		"",
		"* * *",
		"* * * * * *",
		"60 * * * *",
		"* 25 * * *",
		"abc * * * *",
	}
	for _, expr := range bad {
		_, err := ParseCron(expr)
		if err == nil {
			t.Errorf("expected error for %q", expr)
		}
	}
}
