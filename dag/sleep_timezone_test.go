// dag/sleep_timezone_test.go
// Tests for the zone a cron sleep is evaluated in (#750).
// Methodology: pure unit tests with a fixed UTC `now`; each case checks the
// resolved delay against the wall-clock instant expected in the named zone.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// cronDelay resolves cfg against now and fails the test on error.
func cronDelay(
	t *testing.T, cfg SleepConfig, input string, now time.Time,
) time.Duration {
	t.Helper()
	got, err := ResolveSleepDuration(cfg, json.RawMessage(input), now)
	if err != nil {
		t.Fatalf("ResolveSleepDuration(%+v): %v", cfg, err)
	}
	return got
}

func TestCronSleep_StaticZoneShiftsTheInstant(t *testing.T) {
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC) // a Monday
	chicago := cronDelay(t, SleepConfig{
		Cron: "0 9 * * *", CronTimezone: "America/Chicago"}, "", now)
	tokyo := cronDelay(t, SleepConfig{
		Cron: "0 9 * * *", CronTimezone: "Asia/Tokyo"}, "", now)
	// Positive: 09:00 CDT is 14:00 UTC; 09:00 JST is 00:00 UTC next day.
	if chicago != 14*time.Hour {
		t.Fatalf("Chicago delay = %v, want 14h", chicago)
	}
	if tokyo != 24*time.Hour {
		t.Fatalf("Tokyo delay = %v, want 24h", tokyo)
	}
}

func TestCronSleep_DefaultZoneIsUTC(t *testing.T) {
	// A host zone must not leak into resolution: `now` carries a
	// non-UTC location and the default must still be UTC.
	chicago, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC).In(chicago)
	got := cronDelay(t, SleepConfig{Cron: "0 9 * * *"}, "", now)
	// Positive: 09:00 UTC is nine hours after 00:00 UTC.
	if got != 9*time.Hour {
		t.Fatalf("default delay = %v, want 9h", got)
	}
	// Negative: not 09:00 Chicago (14h).
	if got == 14*time.Hour {
		t.Fatalf("default zone resolved in host-local time")
	}
}

func TestCronSleep_InputPathZone(t *testing.T) {
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	cfg := SleepConfig{
		Cron: "0 9 * * *", CronTimezoneInputPath: "subscriber.timezone"}
	got := cronDelay(t, cfg,
		`{"subscriber":{"timezone":"America/Chicago"}}`, now)
	// Positive: the zone comes from the run input.
	if got != 14*time.Hour {
		t.Fatalf("input-path delay = %v, want 14h", got)
	}
	// Negative: missing, unknown and non-string zones fall back to UTC
	// and report why.
	for _, input := range []string{
		"", `{}`, `{"subscriber":{"timezone":"Mars/Olympus"}}`,
		`{"subscriber":{"timezone":42}}`,
	} {
		if d := cronDelay(t, cfg, input, now); d != 9*time.Hour {
			t.Fatalf("input %q: delay = %v, want UTC 9h", input, d)
		}
		loc, err := CronSleepLocation(cfg, json.RawMessage(input))
		if err == nil || loc != time.UTC {
			t.Fatalf("input %q: CronSleepLocation = %v, %v", input, loc, err)
		}
	}
}

func TestCronSleep_DSTBoundaries(t *testing.T) {
	// US spring-forward 2026-03-08: 02:30 does not exist in Chicago, so
	// the next 02:30 is the following day's.
	start := time.Date(2026, 3, 8, 6, 0, 0, 0, time.UTC) // 00:00 CST
	cfg := SleepConfig{Cron: "30 2 * * *", CronTimezone: "America/Chicago"}
	got := cronDelay(t, cfg, "", start)
	want := time.Date(2026, 3, 9, 7, 30, 0, 0, time.UTC) // 02:30 CDT
	if !start.Add(got).Equal(want) {
		t.Fatalf("spring-forward wake = %v, want %v", start.Add(got), want)
	}
	// Fall-back 2026-11-01: 01:30 happens twice; the first (CDT) wins.
	start = time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC) // 00:00 CDT
	cfg.Cron = "30 1 * * *"
	got = cronDelay(t, cfg, "", start)
	want = time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC) // 01:30 CDT
	if !start.Add(got).Equal(want) {
		t.Fatalf("fall-back wake = %v, want %v", start.Add(got), want)
	}
}

func TestSleepConfig_ZoneFieldRules(t *testing.T) {
	both := SleepConfig{Cron: "0 9 * * *",
		CronTimezone: "UTC", CronTimezoneInputPath: "tz"}
	orphan := SleepConfig{Duration: time.Minute, CronTimezone: "UTC"}
	for name, cfg := range map[string]SleepConfig{
		"both": both, "orphan": orphan,
	} {
		step := StepDef{ID: "s", Type: StepTypeSleep,
			Config: MarshalConfig(cfg)}
		// Negative: rejected at parse and at resolution.
		if _, err := ParseSleepConfig(step); err == nil {
			t.Fatalf("%s: ParseSleepConfig accepted %+v", name, cfg)
		}
		_, err := ResolveSleepDuration(cfg, nil, time.Now())
		if err == nil {
			t.Fatalf("%s: ResolveSleepDuration accepted %+v", name, cfg)
		}
	}
	// Registration rejects an unknown static zone and accepts a known one.
	bad := StepDef{ID: "s", Type: StepTypeSleep, Config: MarshalConfig(
		SleepConfig{Cron: "0 9 * * *", CronTimezone: "Mars/Olympus"})}
	err := validateSleepStep(bad)
	if err == nil || !strings.Contains(err.Error(), "cron_timezone") {
		t.Fatalf("validateSleepStep(unknown zone) = %v", err)
	}
	good := StepDef{ID: "s", Type: StepTypeSleep, Config: MarshalConfig(
		SleepConfig{Cron: "0 9 * * *", CronTimezone: "Europe/Berlin"})}
	if err := validateSleepStep(good); err != nil {
		t.Fatalf("validateSleepStep(known zone): %v", err)
	}
}
