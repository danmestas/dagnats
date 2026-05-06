package trigger

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
		contains(c.DaysOfMonth, t.Day()) &&
		contains(c.Months, int(t.Month())) &&
		contains(c.DaysOfWeek, int(t.Weekday()))
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
func parseField(field string, min, max int) ([]int, error) {
	if field == "" {
		panic("parseField: field must not be empty")
	}
	if min > max {
		panic("parseField: min must not exceed max")
	}

	if field == "*" {
		return rangeInts(min, max), nil
	}

	// Handle comma-separated values
	if strings.Contains(field, ",") {
		var result []int
		for _, part := range strings.Split(field, ",") {
			vals, err := parseField(part, min, max)
			if err != nil {
				return nil, err
			}
			result = append(result, vals...)
		}
		return result, nil
	}

	// Handle */N (step)
	if strings.HasPrefix(field, "*/") {
		stepStr := field[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step: %q", field)
		}
		var result []int
		for i := min; i <= max; i += step {
			result = append(result, i)
		}
		return result, nil
	}

	// Handle N-M (range)
	if strings.Contains(field, "-") {
		parts := strings.SplitN(field, "-", 2)
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %q", field)
		}
		hi, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid range end: %q", field)
		}
		if lo < min || hi > max || lo > hi {
			return nil, fmt.Errorf("range out of bounds: %q", field)
		}
		return rangeInts(lo, hi), nil
	}

	// Single value
	val, err := strconv.Atoi(field)
	if err != nil {
		return nil, fmt.Errorf("invalid value: %q", field)
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
