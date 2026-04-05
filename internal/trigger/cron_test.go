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

func TestMatchesExactMinute(t *testing.T) {
	expr, err := ParseCron("30 14 * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: exact match at 14:30
	at := time.Date(2026, 6, 15, 14, 30, 0, 0, time.UTC)
	if !expr.Matches(at) {
		t.Fatalf("should match 14:30")
	}

	// Negative: one minute off
	off := time.Date(2026, 6, 15, 14, 31, 0, 0, time.UTC)
	if expr.Matches(off) {
		t.Fatalf("should not match 14:31")
	}
}

func TestMatchesBoundaryMinuteZeroAndFiftyNine(t *testing.T) {
	expr, err := ParseCron("0,59 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: minute 0 matches
	m0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !expr.Matches(m0) {
		t.Fatalf("minute 0 should match")
	}

	// Positive: minute 59 matches
	m59 := time.Date(2026, 1, 1, 12, 59, 0, 0, time.UTC)
	if !expr.Matches(m59) {
		t.Fatalf("minute 59 should match")
	}

	// Negative: minute 30 does not match
	m30 := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	if expr.Matches(m30) {
		t.Fatalf("minute 30 should not match 0,59")
	}
}

func TestMatchesBoundaryHourZeroAndTwentyThree(t *testing.T) {
	expr, err := ParseCron("0 0,23 * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: hour 0 matches
	h0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !expr.Matches(h0) {
		t.Fatalf("hour 0 should match")
	}

	// Positive: hour 23 matches
	h23 := time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC)
	if !expr.Matches(h23) {
		t.Fatalf("hour 23 should match")
	}

	// Negative: hour 12 does not match
	h12 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if expr.Matches(h12) {
		t.Fatalf("hour 12 should not match 0,23")
	}
}

func TestMatchesDayOfWeekBoundary(t *testing.T) {
	// Sunday=0
	expr, err := ParseCron("0 0 * * 0")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: Sunday matches (2026-03-29 is Sunday)
	sun := time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC)
	if !expr.Matches(sun) {
		t.Fatalf("Sunday should match day-of-week 0")
	}

	// Negative: Monday does not match
	mon := time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC)
	if expr.Matches(mon) {
		t.Fatalf("Monday should not match day-of-week 0")
	}
}

func TestMatchesDayOfWeekSaturday(t *testing.T) {
	// Saturday=6
	expr, err := ParseCron("0 0 * * 6")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: Saturday matches (2026-03-28 is Saturday)
	sat := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	if !expr.Matches(sat) {
		t.Fatalf("Saturday should match day-of-week 6")
	}

	// Negative: Friday does not match
	fri := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	if expr.Matches(fri) {
		t.Fatalf("Friday should not match day-of-week 6")
	}
}

func TestParseFieldRejectsInvalidStep(t *testing.T) {
	// Invalid step value (not a number)
	_, err := ParseCron("*/abc * * * *")
	if err == nil {
		t.Fatalf("expected error for */abc")
	}

	// Negative step
	_, err = ParseCron("*/0 * * * *")
	if err == nil {
		t.Fatalf("expected error for */0")
	}
}

func TestParseFieldRejectsInvalidRange(t *testing.T) {
	// Range start not a number
	_, err := ParseCron("a-5 * * * *")
	if err == nil {
		t.Fatalf("expected error for a-5")
	}

	// Range end not a number
	_, err = ParseCron("0-b * * * *")
	if err == nil {
		t.Fatalf("expected error for 0-b")
	}

	// Range out of bounds (lo > hi)
	_, err = ParseCron("30-10 * * * *")
	if err == nil {
		t.Fatalf("expected error for 30-10")
	}
}

func TestParseCronRejectsInvalidFields(t *testing.T) {
	// Invalid hour field
	_, err := ParseCron("0 abc * * *")
	if err == nil {
		t.Fatalf("expected error for invalid hour field")
	}

	// Invalid day-of-month field
	_, err = ParseCron("0 0 32 * *")
	if err == nil {
		t.Fatalf("expected error for day-of-month 32")
	}

	// Invalid month field
	_, err = ParseCron("0 0 * 13 *")
	if err == nil {
		t.Fatalf("expected error for month 13")
	}

	// Invalid day-of-week field
	_, err = ParseCron("0 0 * * 7")
	if err == nil {
		t.Fatalf("expected error for day-of-week 7")
	}
}

func TestMatchesWildcardAllFields(t *testing.T) {
	expr, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: midnight Jan 1
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !expr.Matches(t1) {
		t.Fatalf("wildcard should match midnight Jan 1")
	}

	// Positive: last minute of year
	t2 := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC)
	if !expr.Matches(t2) {
		t.Fatalf("wildcard should match 23:59 Dec 31")
	}
}
