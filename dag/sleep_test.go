// dag/sleep_test.go

// Tests for sleep-step config validation and dispatch-time duration
// resolution across the three mutually-exclusive forms: duration, cron,
// and until_input_path.
// Methodology: pure unit tests, no NATS. `now` is injected rather than
// read from the wall clock so every expectation is exact. Each test
// checks positive space (the resolved value / the accepted config) and
// negative space (the rejection, or that the wrong form was not taken).
package dag

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// fixedNow is an arbitrary but stable reference instant. Chosen mid-year
// and off a minute boundary so cron rounding is observable.
var fixedNow = time.Date(2026, 3, 10, 14, 30, 20, 0, time.UTC)

func sleepStep(t *testing.T, cfg SleepConfig) StepDef {
	t.Helper()
	raw, err := json.Marshal(&cfg)
	if err != nil {
		t.Fatalf("marshal sleep config: %v", err)
	}
	return StepDef{ID: "nap", Type: StepTypeSleep, Config: raw}
}

// rawSleepStep builds a step from literal JSON so tests can express
// configs the SleepConfig struct would normalize away.
func rawSleepStep(body string) StepDef {
	return StepDef{
		ID:     "nap",
		Type:   StepTypeSleep,
		Config: json.RawMessage(body),
	}
}

func TestParseSleepConfigRejectsZeroForms(t *testing.T) {
	_, err := ParseSleepConfig(rawSleepStep(`{}`))
	if err == nil {
		t.Fatal("expected error for config with no form set")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("error should name the exactly-one rule, got: %v", err)
	}
}

func TestParseSleepConfigRejectsMultipleForms(t *testing.T) {
	_, err := ParseSleepConfig(rawSleepStep(
		`{"duration":1000,"cron":"* * * * *"}`))
	if err == nil {
		t.Fatal("expected error for config with two forms set")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("error should name the exactly-one rule, got: %v", err)
	}
}

func TestParseSleepConfigAcceptsSingleForm(t *testing.T) {
	cfg, err := ParseSleepConfig(rawSleepStep(`{"cron":"0 9 * * 1"}`))
	if err != nil {
		t.Fatalf("single-form config should parse, got: %v", err)
	}
	if cfg.Cron != "0 9 * * 1" {
		t.Errorf("Cron = %q, want %q", cfg.Cron, "0 9 * * 1")
	}
	if cfg.Duration != 0 || cfg.UntilInputPath != "" {
		t.Errorf("other forms should stay zero, got %+v", cfg)
	}
}

func TestValidateSleepStepAcceptsCronForm(t *testing.T) {
	err := validateSleepStep(sleepStep(t, SleepConfig{Cron: "0 9 * * 1"}))
	if err != nil {
		t.Fatalf("valid cron form should validate, got: %v", err)
	}
	if bad := validateSleepStep(
		sleepStep(t, SleepConfig{Cron: "0 9 * *"}),
	); bad == nil {
		t.Error("expected 4-field cron to be rejected")
	}
}

func TestValidateSleepStepRejectsMalformedCron(t *testing.T) {
	err := validateSleepStep(sleepStep(t, SleepConfig{Cron: "99 * * * *"}))
	if err == nil {
		t.Fatal("expected out-of-range minute field to be rejected")
	}
	if !strings.Contains(err.Error(), "nap") {
		t.Errorf("error should name the step, got: %v", err)
	}
}

func TestValidateSleepStepAcceptsUntilInputPathForm(t *testing.T) {
	err := validateSleepStep(
		sleepStep(t, SleepConfig{UntilInputPath: "deadline"}))
	if err != nil {
		t.Fatalf("until_input_path form should validate, got: %v", err)
	}
	if dur := validateSleepStep(
		sleepStep(t, SleepConfig{Duration: time.Minute}),
	); dur != nil {
		t.Errorf("duration form must still validate, got: %v", dur)
	}
}

func TestValidateSleepStepRejectsDurationOverMax(t *testing.T) {
	err := validateSleepStep(
		sleepStep(t, SleepConfig{Duration: maxSleepDuration + time.Hour}))
	if err == nil {
		t.Fatal("expected over-max duration to be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("error should name the bound, got: %v", err)
	}
}

func TestValidateSleepStepRejectsNoForm(t *testing.T) {
	err := validateSleepStep(rawSleepStep(`{}`))
	if err == nil {
		t.Fatal("expected config with no form to be rejected")
	}
	if !strings.Contains(err.Error(), "nap") {
		t.Errorf("error should name the step, got: %v", err)
	}
}

func TestResolveSleepDurationDurationForm(t *testing.T) {
	got, err := ResolveSleepDuration(
		SleepConfig{Duration: 90 * time.Second}, nil, fixedNow)
	if err != nil {
		t.Fatalf("duration form should resolve, got: %v", err)
	}
	if got != 90*time.Second {
		t.Errorf("got %v, want %v", got, 90*time.Second)
	}
}

func TestResolveSleepDurationNegativeDurationErrors(t *testing.T) {
	// Registration rejects this, but dispatch re-parses from KV, so the
	// resolver must fail the step rather than panic on a negative delay.
	_, err := ResolveSleepDuration(
		SleepConfig{Duration: -time.Minute}, nil, fixedNow)
	if err == nil {
		t.Fatal("expected negative duration to be rejected")
	}
	if !strings.Contains(err.Error(), "must be positive") {
		t.Errorf("error should say why, got: %v", err)
	}
}

func TestResolveSleepDurationCronNextOccurrence(t *testing.T) {
	// fixedNow is 14:30:20 on a Tuesday; the next 09:00 Monday is
	// 2026-03-16T09:00:00Z.
	want := time.Date(2026, 3, 16, 9, 0, 0, 0, time.UTC).Sub(fixedNow)
	got, err := ResolveSleepDuration(
		SleepConfig{Cron: "0 9 * * 1"}, nil, fixedNow)
	if err != nil {
		t.Fatalf("cron form should resolve, got: %v", err)
	}
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got <= 0 {
		t.Error("cron occurrence must be strictly after now")
	}
}

func TestResolveSleepDurationCronNoOccurrence(t *testing.T) {
	// Day-of-month 30 in February never occurs; this repo's cron ANDs
	// day-of-month with month, so the scan finds nothing.
	_, err := ResolveSleepDuration(
		SleepConfig{Cron: "0 0 30 2 *"}, nil, fixedNow)
	if err == nil {
		t.Fatal("expected error for cron with no next occurrence")
	}
	if !strings.Contains(err.Error(), "no next occurrence") {
		t.Errorf("error should say why, got: %v", err)
	}
}

func TestResolveSleepDurationCronExceedsBound(t *testing.T) {
	// From 2027-02-28 the next Feb 29 is 2028-02-29 — 366 days out,
	// past maxSleepDuration. Proves the bound is enforced in dag and
	// not merely implied by the cron scan horizon.
	from := time.Date(2027, 2, 28, 0, 0, 0, 0, time.UTC)
	_, err := ResolveSleepDuration(
		SleepConfig{Cron: "0 0 29 2 *"}, nil, from)
	if err == nil {
		t.Fatal("expected over-max cron occurrence to be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("error should name the bound, got: %v", err)
	}
}

func TestResolveSleepDurationUntilRFC3339(t *testing.T) {
	deadline := fixedNow.Add(2 * time.Hour)
	input := json.RawMessage(
		`{"deadline":"` + deadline.Format(time.RFC3339) + `"}`)
	got, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"}, input, fixedNow)
	if err != nil {
		t.Fatalf("RFC3339 deadline should resolve, got: %v", err)
	}
	// fixedNow carries no sub-second component, so the difference is exact.
	if got != 2*time.Hour {
		t.Errorf("got %v, want %v", got, 2*time.Hour)
	}
}

func TestResolveSleepDurationUntilNestedPath(t *testing.T) {
	input := json.RawMessage(`{"wait":{"ms":4500}}`)
	got, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "wait.ms"}, input, fixedNow)
	if err != nil {
		t.Fatalf("nested path should resolve, got: %v", err)
	}
	if got != 4500*time.Millisecond {
		t.Errorf("got %v, want %v", got, 4500*time.Millisecond)
	}
}

func TestResolveSleepDurationUntilMilliseconds(t *testing.T) {
	input := json.RawMessage(`{"wait_ms":3600000}`)
	got, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "wait_ms"}, input, fixedNow)
	if err != nil {
		t.Fatalf("integer milliseconds should resolve, got: %v", err)
	}
	if got != time.Hour {
		t.Errorf("got %v, want %v", got, time.Hour)
	}
}

func TestResolveSleepDurationUntilPastClampsToZero(t *testing.T) {
	past := fixedNow.Add(-72 * time.Hour).Format(time.RFC3339)
	got, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"},
		json.RawMessage(`{"deadline":"`+past+`"}`), fixedNow)
	if err != nil {
		t.Fatalf("past instant must clamp, not error, got: %v", err)
	}
	if got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestResolveSleepDurationUntilMissingPath(t *testing.T) {
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"},
		json.RawMessage(`{"other":1}`), fixedNow)
	if err == nil {
		t.Fatal("expected error for missing path")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("error should name the path, got: %v", err)
	}
}

func TestResolveSleepDurationUntilNilRunInput(t *testing.T) {
	// ExtractDotPath panics on nil data; a run with no input is an
	// ordinary runtime condition and must surface as a step error.
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"}, nil, fixedNow)
	if err == nil {
		t.Fatal("expected error for nil run input")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("error should name the path, got: %v", err)
	}
}

func TestResolveSleepDurationUntilUnparseableString(t *testing.T) {
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"},
		json.RawMessage(`{"deadline":"next tuesday"}`), fixedNow)
	if err == nil {
		t.Fatal("expected error for non-RFC3339 string")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("error should name the path, got: %v", err)
	}
}

func TestResolveSleepDurationUntilWrongType(t *testing.T) {
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"},
		json.RawMessage(`{"deadline":true}`), fixedNow)
	if err == nil {
		t.Fatal("expected error for boolean target")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("error should name the path, got: %v", err)
	}
}

func TestResolveSleepDurationUntilExceedsBound(t *testing.T) {
	beyond := fixedNow.Add(maxSleepDuration + time.Hour).
		Format(time.RFC3339)
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "deadline"},
		json.RawMessage(`{"deadline":"`+beyond+`"}`), fixedNow)
	if err == nil {
		t.Fatal("expected over-max deadline to be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("error should name the bound, got: %v", err)
	}
}

func TestResolveSleepDurationUntilMillisecondsOverflow(t *testing.T) {
	// 1e30 ms overflows int64 nanoseconds; it must be rejected before
	// the conversion wraps to a negative delay.
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "wait_ms"},
		json.RawMessage(`{"wait_ms":1e30}`), fixedNow)
	if err == nil {
		t.Fatal("expected overflowing millisecond count to be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("error should name the bound, got: %v", err)
	}
}

func TestResolveSleepDurationZeroValueConfigErrors(t *testing.T) {
	// ResolveSleepDuration is exported from the SDK; a zero-value config
	// is the most natural caller mistake and must not panic the process.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero-value config panicked: %v", r)
		}
	}()
	_, err := ResolveSleepDuration(SleepConfig{}, nil, fixedNow)
	if err == nil {
		t.Fatal("expected error for zero-value config")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("error should name the exactly-one rule, got: %v", err)
	}
}

func TestResolveSleepDurationMultiFormConfigErrors(t *testing.T) {
	_, err := ResolveSleepDuration(
		SleepConfig{Duration: time.Minute, Cron: "* * * * *"},
		nil, fixedNow)
	if err == nil {
		t.Fatal("expected error for multi-form config")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("error should name the exactly-one rule, got: %v", err)
	}
}

func TestResolveSleepDurationUntilNegativeMilliseconds(t *testing.T) {
	// A negative millisecond count is a producer bug, not an intentional
	// zero sleep — clamping would hide it. Only past RFC3339 instants
	// clamp.
	_, err := ResolveSleepDuration(
		SleepConfig{UntilInputPath: "wait_ms"},
		json.RawMessage(`{"wait_ms":-5000}`), fixedNow)
	if err == nil {
		t.Fatal("expected negative milliseconds to be rejected")
	}
	if !strings.Contains(err.Error(), "wait_ms") {
		t.Errorf("error should name the path, got: %v", err)
	}
	if !strings.Contains(err.Error(), "negative") {
		t.Errorf("error should say why, got: %v", err)
	}
}

func TestResolveSleepDurationDurationFormUnchanged(t *testing.T) {
	// Guards the no-regression criterion: a duration-only config built
	// by the builder resolves to exactly the declared duration.
	b := NewWorkflow("wf")
	b.Sleep("nap", 45*time.Second)
	def, err := b.Build()
	if err != nil {
		t.Fatalf("builder should produce a valid def, got: %v", err)
	}
	cfg, err := ParseSleepConfig(def.Steps[0])
	if err != nil {
		t.Fatalf("builder config should parse, got: %v", err)
	}
	got, err := ResolveSleepDuration(cfg, nil, fixedNow)
	if err != nil {
		t.Fatalf("builder config should resolve, got: %v", err)
	}
	if got != 45*time.Second {
		t.Errorf("got %v, want %v", got, 45*time.Second)
	}
}
