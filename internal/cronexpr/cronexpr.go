// Package cronexpr parses 5-field cron expressions and answers two
// questions about them: does an instant match, and what are the next N
// matching instants. It previously lived inside the trigger package,
// which made it unreachable from dag -- trigger imports dag, so a dag
// import of the cron type would close an import cycle. Sleep steps need
// the same parser trigger schedules use, and a second parser would be
// two definitions of "what a dagnats cron expression means" that drift.
//
// Ousterhout note: a leaf package with no in-module imports, so any
// layer may depend on it. The interface is one constructor and two
// methods; the field grammar (*, */N, N-M, comma lists) and the
// minute-by-minute scan are hidden behind it.
package cronexpr

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronExpr is a parsed 5-field cron expression.
type CronExpr struct {
	Minutes     []int // 0-59
	Hours       []int // 0-23
	DaysOfMonth []int // 1-31
	Months      []int // 1-12
	DaysOfWeek  []int // 0-6 (0=Sunday)

	// Parsing expands "*" to the full range, which erases the difference
	// between "restricted to every value" and "unrestricted" -- and the
	// POSIX day rule turns on exactly that difference. ParseCron records
	// it here. Unexported because it is bookkeeping for dayMatches, not
	// something a caller sets or reasons about; a zero value therefore
	// reads as "both day fields are *", the vacuous predicate.
	dayOfMonthRestricted bool
	dayOfWeekRestricted  bool
}

// ParseCron parses a 5-field cron expression into a CronExpr.
// Supports *, */N, N-M, and comma-separated values.
func ParseCron(expr string) (*CronExpr, error) {
	// Bounded input prevents unbounded parsing work
	if len(expr) > 256 {
		panic("ParseCron: expr exceeds maximum length of 256")
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		// Surface the field-count requirement explicitly. Operators
		// arriving from Quartz / robfig-cron-v3 / k8s default to a
		// 6-field form (leading seconds); the engine's minute-precision
		// dedup makes sub-minute scheduling unsupportable here, so 5
		// fields is the only valid form (issue #172).
		return nil, fmt.Errorf(
			"dagnats cron expressions use 5 fields "+
				"(minute hour day-of-month month day-of-week); "+
				"got %d. Drop any leading seconds field if present",
			len(fields))
	}

	minutes, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute field: %w", err)
	}
	hours, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour field: %w", err)
	}
	dom, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day-of-month field: %w", err)
	}
	months, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month field: %w", err)
	}
	dow, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("day-of-week field: %w", err)
	}

	result := &CronExpr{
		Minutes:     minutes,
		Hours:       hours,
		DaysOfMonth: dom,
		Months:      months,
		DaysOfWeek:  dow,
		// Only a bare "*" is unrestricted. "1-31" and "*/1" cover every
		// value but still express an author's intent to constrain the
		// day, so they restrict -- matching how operators read them.
		dayOfMonthRestricted: fields[2] != "*",
		dayOfWeekRestricted:  fields[4] != "*",
	}

	// Post-condition: all slices must be populated after successful parse
	if len(result.Minutes) == 0 || len(result.Hours) == 0 {
		panic("ParseCron: parsed expression has empty time slices")
	}

	return result, nil
}

// Matches returns true if the given time matches this cron expression.
// Panics if time is zero (programmer error: uninitialized time).
func (c *CronExpr) Matches(t time.Time) bool {
	if t.IsZero() {
		panic("Matches: time must not be zero")
	}
	if c.Minutes == nil {
		panic("Matches: Minutes slice must not be nil")
	}

	return contains(c.Minutes, t.Minute()) &&
		contains(c.Hours, t.Hour()) &&
		contains(c.Months, int(t.Month())) &&
		c.dayMatches(t)
}

// dayMatches answers whether t's day satisfies the expression, applying
// the POSIX day rule: when both day fields are restricted the day matches
// if EITHER matches. Vixie cron and everything descended from it behave
// this way, so an operator writing "0 9 1 * 1" means "the 1st, or any
// Monday" -- roughly 60 firings a year. Intersecting the two fields
// instead gave ~1, silently (issue #552).
func (c *CronExpr) dayMatches(t time.Time) bool {
	if t.IsZero() {
		panic("dayMatches: time must not be zero")
	}
	if c.DaysOfMonth == nil || c.DaysOfWeek == nil {
		panic("dayMatches: day slices must not be nil")
	}

	dayOfMonthMatches := contains(c.DaysOfMonth, t.Day())
	dayOfWeekMatches := contains(c.DaysOfWeek, int(t.Weekday()))

	if c.dayOfMonthRestricted && c.dayOfWeekRestricted {
		return dayOfMonthMatches || dayOfWeekMatches
	}
	// An unrestricted field spans its whole range, so its side of this
	// conjunction is always true. That collapses to "the restricted field
	// alone" when one is restricted, and to true when neither is -- the
	// remaining three POSIX cases, without branching on them.
	return dayOfMonthMatches && dayOfWeekMatches
}

func contains(vals []int, target int) bool {
	if vals == nil {
		panic("contains: vals must not be nil")
	}

	for _, v := range vals {
		if v == target {
			return true
		}
	}
	return false
}

// parseField parses one cron field (*, */N, N-M, N, comma-separated).
// The comma list is the only recursive-looking construct in the grammar and
// it is not actually recursive: a part is always comma-free, so splitting
// once and looping over the parts covers every expression a self-call could
// (issue #554, and CLAUDE.md forbids recursion outright).
func parseField(field string, min, max int) ([]int, error) {
	if field == "" {
		panic("parseField: field must not be empty")
	}
	if min > max {
		panic("parseField: min must not exceed max")
	}

	parts := strings.Split(field, ",")
	result := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			panic("parseField: field must not be empty")
		}
		values, err := parseFieldValue(part, min, max)
		if err != nil {
			return nil, err
		}
		result = append(result, values...)
	}
	return result, nil
}

// parseFieldValue parses one comma-free value form: *, */N, N-M, or N.
func parseFieldValue(value string, min, max int) ([]int, error) {
	if value == "" {
		panic("parseFieldValue: value must not be empty")
	}
	if min > max {
		panic("parseFieldValue: min must not exceed max")
	}

	if value == "*" {
		return rangeInts(min, max), nil
	}

	// Handle */N (step)
	if strings.HasPrefix(value, "*/") {
		stepStr := value[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step: %q", value)
		}
		var result []int
		for i := min; i <= max; i += step {
			result = append(result, i)
		}
		return result, nil
	}

	// Handle N-M (range)
	if strings.Contains(value, "-") {
		parts := strings.SplitN(value, "-", 2)
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %q", value)
		}
		hi, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid range end: %q", value)
		}
		if lo < min || hi > max || lo > hi {
			return nil, fmt.Errorf("range out of bounds: %q", value)
		}
		return rangeInts(lo, hi), nil
	}

	// Single value
	val, err := strconv.Atoi(value)
	if err != nil {
		return nil, fmt.Errorf("invalid value: %q", value)
	}
	if val < min || val > max {
		return nil, fmt.Errorf(
			"value %d out of range [%d, %d]", val, min, max)
	}
	return []int{val}, nil
}

// NextN returns the next n times after ref that match this cron expression.
// Scans minute-by-minute starting from ref+1min. Panics if n is negative
// or if ref is zero. Minute-by-minute scan is simple and correct for
// 5-field cron; worst case ~527K iterations, <10ms on modern hardware.
func (c *CronExpr) NextN(ref time.Time, n int) []time.Time {
	if ref.IsZero() {
		panic("NextN: ref must not be zero")
	}
	if n < 0 {
		panic("NextN: n must not be negative")
	}
	if c.Minutes == nil {
		panic("NextN: expression not initialized")
	}

	const maxScanMinutes = 366 * 24 * 60
	results := make([]time.Time, 0, n)
	// Start scanning from the next whole minute after ref
	t := ref.Truncate(time.Minute).Add(time.Minute)

	for i := 0; i < maxScanMinutes && len(results) < n; i++ {
		if c.Matches(t) {
			results = append(results, t)
		}
		t = t.Add(time.Minute)
	}
	return results
}

func rangeInts(min, max int) []int {
	if min > max {
		panic("rangeInts: min must not exceed max")
	}
	if max > 999 {
		panic("rangeInts: max exceeds upper bound of 999")
	}

	result := make([]int, 0, max-min+1)
	for i := min; i <= max; i++ {
		result = append(result, i)
	}
	return result
}
