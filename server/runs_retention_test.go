// Methodology: unit tests for the run-retention config knob (#453, #521).
// Verify the unset default (no age pruning; size pruning bounds the bucket),
// that 0/off/disabled also mean no age pruning, that an explicit window
// such as 168h still enables it, the
// DAGNATS_RUNS_MAX_AGE env override across Go-duration and d/w suffixes, and
// that an invalid value is a hard config-load error rather than a silent
// no-op. Positive + negative space; no NATS server needed.

package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig_RunsMaxAgeDefaultsToUnset(t *testing.T) {
	cfg := DefaultConfig()

	// Retention is the consumer's call: an unconfigured serve does no age
	// pruning. Size pruning, always on, is what bounds workflow_runs.
	if cfg.RunsMaxAge != 0 {
		t.Errorf("RunsMaxAge = %v, want 0 (unset: no age pruning)", cfg.RunsMaxAge)
	}
	if cfg.RunsMaxAge == 30*24*time.Hour {
		t.Error("RunsMaxAge still carries the old 720h default")
	}
}

func TestConfigFromEnv_RunsMaxAgeUnsetWhenSilent(t *testing.T) {
	// No file (temp CWD guarantees no dagnats.yaml) and no env override: the
	// resolved config must stay unset.
	dir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(origDir); err != nil {
			t.Fatalf("Chdir restore: %v", err)
		}
	}()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	os.Unsetenv("DAGNATS_RUNS_MAX_AGE")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 0 {
		t.Errorf("RunsMaxAge = %v, want 0 (unset when the operator is silent)",
			cfg.RunsMaxAge)
	}
}

func TestConfigFromEnv_RunsMaxAgeExplicit168h(t *testing.T) {
	// zeitforge sets 168h explicitly; it must still turn age pruning on.
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "168h")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 168*time.Hour {
		t.Errorf("RunsMaxAge = %v, want 168h", cfg.RunsMaxAge)
	}
	if cfg.RunsMaxAge == 0 {
		t.Error("an explicit window must enable age pruning")
	}
}

func TestConfigFromEnv_RunsMaxAgeOffStaysDisabled(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "off")

	cfg := ConfigFromEnv()

	// "off" means no age pruning, the same as leaving it unset.
	if cfg.RunsMaxAge != 0 {
		t.Errorf("RunsMaxAge = %v, want 0 (explicit off = disabled)",
			cfg.RunsMaxAge)
	}
}

func TestConfigFromEnv_RunsMaxAgeGoDuration(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "720h")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 720*time.Hour {
		t.Errorf("RunsMaxAge = %v, want 720h", cfg.RunsMaxAge)
	}
	if cfg.RunsMaxAge == 0 {
		t.Error("RunsMaxAge should be enabled after a valid override")
	}
}

func TestConfigFromEnv_RunsMaxAgeDaySuffix(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "30d")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 30*24*time.Hour {
		t.Errorf("RunsMaxAge = %v, want 30d (720h)", cfg.RunsMaxAge)
	}
}

func TestConfigFromEnv_RunsMaxAgeWeekSuffix(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "2w")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 14*24*time.Hour {
		t.Errorf("RunsMaxAge = %v, want 2w (336h)", cfg.RunsMaxAge)
	}
}

func TestConfigFromEnv_RunsMaxAgeZeroStaysDisabled(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "0")

	cfg := ConfigFromEnv()

	if cfg.RunsMaxAge != 0 {
		t.Errorf("RunsMaxAge = %v, want 0 (explicit 0 = disabled)",
			cfg.RunsMaxAge)
	}
}

func TestConfigWithPath_RunsMaxAgeInvalidIsError(t *testing.T) {
	t.Setenv("DAGNATS_RUNS_MAX_AGE", "not-a-duration")

	_, _, err := ConfigWithPath("")
	if err == nil {
		t.Fatal("expected error for invalid DAGNATS_RUNS_MAX_AGE, got nil")
	}
}

func TestLoadConfigFile_RunsMaxAgeFromFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	if err := os.WriteFile(
		cfgPath, []byte("runs_max_age: 30d\n"), 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgPath, &cfg); err != nil {
		t.Fatalf("loadConfigFile failed: %v", err)
	}
	if cfg.RunsMaxAge != 30*24*time.Hour {
		t.Errorf("RunsMaxAge = %v, want 30d (720h)", cfg.RunsMaxAge)
	}
}

func TestLoadConfigFile_RunsMaxAgeInvalidIsError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	if err := os.WriteFile(
		cfgPath, []byte("runs_max_age: not-a-duration\n"), 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgPath, &cfg); err == nil {
		t.Fatal("expected error for invalid runs_max_age in file, got nil")
	}
}
