// cronexpr/cronexpr_posix_day_test.go
// Tests for the POSIX day predicate: day-of-month and day-of-week are
// ORed when both are restricted, and ANDed (degenerately) otherwise.
// Methodology: unit tests over the four restriction combinations, with
// fixed UTC dates whose weekdays are pinned in the comments. No NATS.
package cronexpr

import (
	"testing"
	"time"
)

// Fixed fixtures. 2026-06-01 is a Monday, so it is the only date here
// where the old intersection semantics and the POSIX union semantics
// agree for "0 9 1 * 1" -- every other case is the regression.
var (
	monTheFirst   = time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)  // Mon, day 1
	wedTheFirst   = time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)  // Wed, day 1
	monTheSixth   = time.Date(2026, 7, 6, 9, 0, 0, 0, time.UTC)  // Mon, day 6
	thuTheSecond  = time.Date(2026, 7, 2, 9, 0, 0, 0, time.UTC)  // Thu, day 2
	monWrongHour  = time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC) // Mon, 10am
	tueTheNinth   = time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC)  // Tue, day 9
	augTheFirst   = time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)  // Sat, day 1
	julyThirdWeek = time.Date(2026, 7, 13, 9, 0, 0, 0, time.UTC) // Mon, day 13
)

// TestDayPredicateBothRestrictedUnions is the issue #552 regression:
// "0 9 1 * 1" must fire on every Monday AND on the 1st of every month,
// not only on Mondays that fall on the 1st.
func TestDayPredicateBothRestrictedUnions(t *testing.T) {
	expr, err := ParseCron("0 9 1 * 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: day-of-month matches, day-of-week does not.
	if !expr.Matches(wedTheFirst) {
		t.Errorf("Wed 2026-07-01 09:00 should match (it is the 1st)")
	}
	// Positive: day-of-week matches, day-of-month does not.
	if !expr.Matches(monTheSixth) {
		t.Errorf("Mon 2026-07-06 09:00 should match (it is a Monday)")
	}
	// Positive: both match.
	if !expr.Matches(monTheFirst) {
		t.Errorf("Mon 2026-06-01 09:00 should match (Monday and the 1st)")
	}
	// Negative: neither day field matches.
	if expr.Matches(thuTheSecond) {
		t.Errorf("Thu 2026-07-02 09:00 should not match (neither field)")
	}
	// Negative: the day predicate does not relax hour/minute -- those
	// stay conjunctive even when the day matches via the union.
	if expr.Matches(monWrongHour) {
		t.Errorf("Mon 2026-07-06 10:00 should not match (wrong hour)")
	}
}

// TestDayPredicateOnlyDayOfMonthRestricted proves the non-diverging case
// is unchanged: with day-of-week "*", only day-of-month constrains.
func TestDayPredicateOnlyDayOfMonthRestricted(t *testing.T) {
	expr, err := ParseCron("0 9 1 * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: the 1st matches on a Wednesday.
	if !expr.Matches(wedTheFirst) {
		t.Errorf("Wed 2026-07-01 should match day-of-month 1")
	}
	// Positive: the 1st matches on a Saturday too -- weekday is irrelevant.
	if !expr.Matches(augTheFirst) {
		t.Errorf("Sat 2026-08-01 should match day-of-month 1")
	}
	// Negative: a Monday that is not the 1st must NOT match. An
	// unrestricted day-of-week must not widen the day predicate.
	if expr.Matches(monTheSixth) {
		t.Errorf("Mon 2026-07-06 should not match day-of-month 1")
	}
	// Negative: any other day-of-month.
	if expr.Matches(thuTheSecond) {
		t.Errorf("Thu 2026-07-02 should not match day-of-month 1")
	}
}

// TestDayPredicateOnlyDayOfWeekRestricted proves the non-diverging case
// is unchanged: with day-of-month "*", only day-of-week constrains.
func TestDayPredicateOnlyDayOfWeekRestricted(t *testing.T) {
	expr, err := ParseCron("0 9 * * 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: a Monday mid-month matches.
	if !expr.Matches(monTheSixth) {
		t.Errorf("Mon 2026-07-06 should match day-of-week 1")
	}
	// Positive: a Monday that is also the 1st matches.
	if !expr.Matches(monTheFirst) {
		t.Errorf("Mon 2026-06-01 should match day-of-week 1")
	}
	// Negative: the 1st on a non-Monday must NOT match. An unrestricted
	// day-of-month must not widen the day predicate.
	if expr.Matches(wedTheFirst) {
		t.Errorf("Wed 2026-07-01 should not match day-of-week 1")
	}
	// Negative: any other weekday.
	if expr.Matches(tueTheNinth) {
		t.Errorf("Tue 2026-06-09 should not match day-of-week 1")
	}
}

// TestDayPredicateNeitherRestricted proves the day predicate is vacuous
// when both day fields are "*".
func TestDayPredicateNeitherRestricted(t *testing.T) {
	expr, err := ParseCron("0 9 * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: every day at 09:00 matches, whatever its weekday.
	for _, at := range []time.Time{
		monTheFirst, wedTheFirst, monTheSixth, thuTheSecond, tueTheNinth,
	} {
		if !expr.Matches(at) {
			t.Errorf("%v should match with both day fields unrestricted",
				at)
		}
	}
	// Negative: hour still constrains.
	if expr.Matches(monWrongHour) {
		t.Errorf("Mon 2026-07-06 10:00 should not match hour 9")
	}
}

// TestDayPredicateRangeCountsAsRestricted confirms restriction is about
// the field being "*", not about how many values it expands to: "1-31"
// covers every day-of-month yet must still count as restricted, so
// "0 9 1-31 * 1" unions rather than intersects.
func TestDayPredicateRangeCountsAsRestricted(t *testing.T) {
	expr, err := ParseCron("0 9 1-31 * 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: a Thursday matches, because day-of-month 1-31 covers it
	// and the union only needs one side.
	if !expr.Matches(thuTheSecond) {
		t.Errorf("Thu 2026-07-02 should match via day-of-month 1-31")
	}
	// Positive: a Monday matches via day-of-week.
	if !expr.Matches(monTheSixth) {
		t.Errorf("Mon 2026-07-06 should match via day-of-week 1")
	}
}

// TestNextNUsesPosixDayPredicate confirms NextN inherits the new
// semantics -- it drives scheduling and backfill, so a divergence here
// would leave triggers firing on the old intersection.
func TestNextNUsesPosixDayPredicate(t *testing.T) {
	expr, err := ParseCron("0 9 1 * 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	ref := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	times := expr.NextN(ref, 3)

	// Positive: three fires within two weeks, not one per year.
	if len(times) != 3 {
		t.Fatalf("got %d fire times, want 3", len(times))
	}
	want := []time.Time{wedTheFirst, monTheSixth, julyThirdWeek}
	for i := range want {
		if !times[i].Equal(want[i]) {
			t.Errorf("times[%d] = %v, want %v", i, times[i], want[i])
		}
	}
	// Negative: no fire lands on a day matching neither field.
	for _, at := range times {
		if at.Day() != 1 && at.Weekday() != time.Monday {
			t.Errorf("%v matches neither day-of-month 1 nor Monday", at)
		}
	}
}
