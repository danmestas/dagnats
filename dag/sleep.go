package dag

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/cronexpr"
)

const maxSleepDuration = 365 * 24 * time.Hour

// checkSleepBound is the single definition of the max-sleep rule, shared
// by registration-time validation and dispatch-time resolution so the
// two cannot drift.
func checkSleepBound(d time.Duration) error {
	if maxSleepDuration <= 0 {
		panic("checkSleepBound: maxSleepDuration must be positive")
	}
	if d < 0 {
		panic("checkSleepBound: duration must not be negative")
	}
	if d > maxSleepDuration {
		return fmt.Errorf(
			"sleep duration %v exceeds max %v", d, maxSleepDuration)
	}
	return nil
}

func validateSleepStep(step StepDef) error {
	if step.ID == "" {
		panic("validateSleepStep: step.ID must not be empty")
	}
	if maxSleepDuration <= 0 {
		panic("validateSleepStep: maxSleepDuration must be positive")
	}
	if step.Type != StepTypeSleep {
		return nil
	}
	// ParseSleepConfig enforces the exactly-one-form invariant, so this
	// call is also the zero-forms and multiple-forms rejection.
	cfg, err := ParseSleepConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: invalid sleep config: %w", step.ID, err)
	}
	switch {
	case cfg.Cron != "":
		// Fail malformed expressions at authoring time, not at first
		// dispatch days later.
		if _, err := cronexpr.ParseCron(cfg.Cron); err != nil {
			return fmt.Errorf(
				"step %q: invalid sleep cron: %w", step.ID, err)
		}
		return nil
	case cfg.UntilInputPath != "":
		// Non-emptiness is guaranteed by the parse-time form count; the
		// value itself is only knowable at dispatch.
		return nil
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf(
			"step %q: sleep duration must be positive, got %v",
			step.ID, cfg.Duration)
	}
	if err := checkSleepBound(cfg.Duration); err != nil {
		return fmt.Errorf("step %q: %w", step.ID, err)
	}
	return nil
}

// ResolveSleepDuration turns a sleep config into the concrete delay to
// wait, given the run input and the dispatch instant. `now` is a
// parameter so callers and tests share one clock.
//
// The returned duration is never negative. Only a past RFC3339 instant
// clamps to zero — a negative millisecond count errors instead, because
// it is far more likely a producer bug than an intentional zero sleep,
// and clamping would hide it. Every form is bound-checked
// against maxSleepDuration here, cron included — relying on the cron
// package's internal scan horizon would tie this invariant to an
// unexported constant elsewhere that could be raised without any
// compiler error or failing test here.
func ResolveSleepDuration(
	cfg SleepConfig, runInput json.RawMessage, now time.Time,
) (time.Duration, error) {
	if now.IsZero() {
		panic("ResolveSleepDuration: now must not be zero")
	}
	if maxSleepDuration <= 0 {
		panic("ResolveSleepDuration: maxSleepDuration must be positive")
	}
	// A returned error rather than a panic: this guards caller data, not
	// a programmer invariant. ResolveSleepDuration is exported, so the
	// most natural SDK mistake — passing a zero-value SleepConfig —
	// must not take down the caller's process.
	if n := cfg.formCount(); n != 1 {
		return 0, fmt.Errorf(
			"sleep config must set exactly one of duration, cron, "+
				"until_input_path; got %d", n)
	}

	var (
		resolved time.Duration
		err      error
	)
	switch {
	case cfg.Cron != "":
		resolved, err = resolveCronSleep(cfg.Cron, now)
	case cfg.UntilInputPath != "":
		resolved, err = resolveUntilSleep(cfg.UntilInputPath, runInput, now)
	default:
		// Registration rejects this, but the config is re-parsed at
		// dispatch from KV — a hand-edited def must fail the step rather
		// than trip the negative-duration contract below.
		if cfg.Duration < 0 {
			return 0, fmt.Errorf(
				"sleep duration must be positive, got %v", cfg.Duration)
		}
		resolved = cfg.Duration
	}
	if err != nil {
		return 0, err
	}
	if err := checkSleepBound(resolved); err != nil {
		return 0, err
	}
	return resolved, nil
}

// resolveCronSleep returns the delay until the next occurrence strictly
// after now.
func resolveCronSleep(expr string, now time.Time) (time.Duration, error) {
	if expr == "" {
		panic("resolveCronSleep: expr must not be empty")
	}
	if now.IsZero() {
		panic("resolveCronSleep: now must not be zero")
	}

	parsed, err := cronexpr.ParseCron(expr)
	if err != nil {
		return 0, fmt.Errorf("sleep cron %q: %w", expr, err)
	}
	next := parsed.NextN(now, 1)
	if len(next) == 0 {
		return 0, fmt.Errorf(
			"sleep cron %q: no next occurrence after %s",
			expr, now.Format(time.RFC3339))
	}
	delay := next[0].Sub(now)
	if delay <= 0 {
		panic("resolveCronSleep: next occurrence must follow now")
	}
	return delay, nil
}

// resolveUntilSleep reads a deadline from the run input. The value is
// either an RFC3339 instant or a JSON number of milliseconds — the
// latter arrives as float64 because it passes through `any`.
func resolveUntilSleep(
	path string, runInput json.RawMessage, now time.Time,
) (time.Duration, error) {
	if path == "" {
		panic("resolveUntilSleep: path must not be empty")
	}
	if now.IsZero() {
		panic("resolveUntilSleep: now must not be zero")
	}

	// ExtractDotPath panics on nil data, but a run with no input is an
	// ordinary runtime condition that must fail the step, not the engine.
	if len(runInput) == 0 {
		return 0, fmt.Errorf(
			"sleep until_input_path %q: run input is empty", path)
	}
	value, err := ExtractDotPath(path, runInput)
	if err != nil {
		return 0, fmt.Errorf("sleep until_input_path %q: %w", path, err)
	}
	return untilValueToDuration(path, value, now)
}

// untilValueToDuration narrows the extracted JSON value to a delay.
func untilValueToDuration(
	path string, value any, now time.Time,
) (time.Duration, error) {
	if path == "" {
		panic("untilValueToDuration: path must not be empty")
	}
	if now.IsZero() {
		panic("untilValueToDuration: now must not be zero")
	}

	switch v := value.(type) {
	case string:
		deadline, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return 0, fmt.Errorf(
				"sleep until_input_path %q: %q is not an RFC3339 instant",
				path, v)
		}
		if deadline.Before(now) {
			return 0, nil
		}
		return deadline.Sub(now), nil
	case float64:
		// Stated positively so NaN fails both guards rather than slipping
		// through: every comparison against NaN is false, and
		// time.Duration(NaN) is implementation-defined.
		if !(v >= 0) {
			return 0, fmt.Errorf(
				"sleep until_input_path %q: milliseconds must not be "+
					"negative, got %v", path, v)
		}
		// Reject before converting: a large JSON number would overflow
		// time.Duration's int64 nanoseconds and wrap to a negative delay.
		if !(v <= float64(maxSleepDuration/time.Millisecond)) {
			return 0, fmt.Errorf(
				"sleep until_input_path %q: %.0f ms exceeds max %v",
				path, v, maxSleepDuration)
		}
		// Truncation toward zero is deliberate: the wire unit is
		// milliseconds, so a fractional part is below the declared
		// resolution and carries no meaning worth rounding.
		return time.Duration(v) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf(
			"sleep until_input_path %q: expected RFC3339 string or "+
				"milliseconds number, got %T", path, value)
	}
}
